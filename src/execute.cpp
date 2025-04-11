#include <iostream>
#include <hardware.h>
#include <memory>
#include <plan.h>
#include <table.h>
#include <bloom_filter.h>
#include <column_iterator.h>
#include <simple_columnar_table.h>
#include <variant>
#include <ranges>
#include <omp.h>

namespace Contest {

struct WorkContext {
    std::vector<std::shared_ptr<SimpleColumnarTable>> tables;
};

SimpleColumnarTableView execute_impl(const Plan& plan, WorkContext* context, size_t node_idx);

template<typename KeyType>
struct HashUtil {
    static size_t hash(KeyType key) {
        if constexpr (std::is_same_v<KeyType,int32_t> ||
                      std::is_same_v<KeyType,int64_t>) {
            uint64_t k = static_cast<uint64_t>(key);
            k ^= k >> 33;
            k *= 0xff51afd7ed558ccdULL;
            k ^= k >> 33;
            k *= 0xc4ceb9fe1a85ec53ULL;
            k ^= k >> 33;
            return static_cast<size_t>(k);
        } else if constexpr (std::is_same_v<KeyType,double>) {
            union { double d; uint64_t i; } conv;
            conv.d = key;
            return hash(conv.i);
        } else /* string */ {
            size_t h = 14695981039346656037ULL;
            for (char c : key) {
                h ^= static_cast<size_t>(c);
                h *= 1099511628211ULL;
            }
            return h;
        }
    }
};

template<typename KeyType>
SimpleColumnarTableView hash_join_omp(const Plan &plan,
    WorkContext* context,
    const JoinNode &join,
    const std::vector<std::tuple<size_t,DataType>> &outs) {
    // 1) materialize both sides
    auto left  = execute_impl(plan, context, join.left);
    auto right = execute_impl(plan, context, join.right);
    if (left.row_count() == 0 || right.row_count() == 0)
        return SimpleColumnarTableView(std::make_shared<SimpleColumnarTable>());

    bool build_left  = join.build_left;
    auto &build_rows = build_left ? left  : right;
    auto &probe_rows = build_left ? right : left;
    size_t build_col_idx = build_left ? join.left_attr : join.right_attr;
    size_t probe_col_idx = build_left ? join.right_attr: join.left_attr;
    size_t left_w    = left.column_count();

    size_t B = build_rows.row_count(), P = probe_rows.row_count();

    // 2) extract join keys & mark non‐null
    std::vector<KeyType> build_keys(B);
    std::vector<char>    build_valid(B, 0);
    for (size_t i = 0; i < B; ++i) {
        std::visit([&](auto const &v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr(std::is_same_v<T,KeyType>) {
                build_keys[i] = v;
                build_valid[i] = 1;
            }
        }, build_rows.get_column(build_col_idx).values[i]);
    }
    std::vector<KeyType> probe_keys(P);
    std::vector<char>    probe_valid(P, 0);
    for (size_t i = 0; i < P; ++i) {
        std::visit([&](auto const &v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr(std::is_same_v<T,KeyType>) {
                probe_keys[i] = v;
                probe_valid[i] = 1;
            }
        }, probe_rows.get_column(probe_col_idx).values[i]);
    }

    // 3) pick #buckets so each bucket ≲ L2
    constexpr size_t BYTES_PER_ENTRY = sizeof(KeyType) + sizeof(uint32_t);
    size_t approx = (B * BYTES_PER_ENTRY + SPC__LEVEL2_CACHE_SIZE - 1)
                    / SPC__LEVEL2_CACHE_SIZE;
    approx = std::clamp<size_t>(approx, 1, 128);
    size_t num_buckets = 1;
    while (num_buckets < approx) num_buckets <<= 1;
    size_t bucket_mask = num_buckets - 1;

    // 4) two-pass partitioning
    std::vector<uint32_t> build_hist(num_buckets,0), probe_hist(num_buckets,0);
    for (size_t i = 0; i < B; ++i) if (build_valid[i]) {
        auto h = HashUtil<KeyType>::hash(build_keys[i]) & bucket_mask;
        build_hist[h]++;
    }
    for (size_t i = 0; i < P; ++i) if (probe_valid[i]) {
        auto h = HashUtil<KeyType>::hash(probe_keys[i]) & bucket_mask;
        probe_hist[h]++;
    }
    std::vector<uint32_t> build_off(num_buckets+1), probe_off(num_buckets+1);
    build_off[0] = probe_off[0] = 0;
    for (size_t b = 0; b < num_buckets; ++b) {
        build_off[b+1] = build_off[b] + build_hist[b];
        probe_off[b+1] = probe_off[b] + probe_hist[b];
    }
    std::vector<uint32_t> build_buf(B), probe_buf(P);
    auto bo = build_off, po = probe_off;
    for (uint32_t i = 0; i < B; ++i) if (build_valid[i]) {
        auto h = HashUtil<KeyType>::hash(build_keys[i]) & bucket_mask;
        build_buf[bo[h]++] = i;
    }
    for (uint32_t i = 0; i < P; ++i) if (probe_valid[i]) {
        auto h = HashUtil<KeyType>::hash(probe_keys[i]) & bucket_mask;
        probe_buf[po[h]++] = i;
    }

    // 5) per-bucket join in parallel
    int nthreads = omp_get_max_threads();
    std::vector<std::vector<std::vector<Data>>> thread_out(nthreads);
    std::vector<size_t> thread_row_counts(nthreads, 0);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto &local = thread_out[tid];

        #pragma omp for schedule(dynamic,1)
        for (size_t b = 0; b < num_buckets; ++b) {
            uint32_t bs = build_off[b], be = build_off[b+1];
            uint32_t ps = probe_off[b], pe = probe_off[b+1];
            size_t cnt = be - bs;
            if (cnt == 0 || ps == pe) continue;

            // micro–hash table with per-slot vectors
            size_t cap = 1;
            while (cap < cnt*2) cap <<= 1;
            std::vector<KeyType> slot_key(cap);
            std::vector<std::vector<uint32_t>> slot_idxs(cap);
            std::vector<char> slot_used(cap,0);
            size_t mask = cap - 1;

            // build phase
            for (uint32_t idx = bs; idx < be; ++idx) {
                uint32_t row = build_buf[idx];
                auto key = build_keys[row];
                size_t h = HashUtil<KeyType>::hash(key) & mask;
                while (slot_used[h] && slot_key[h] != key) {
                    h = (h + 1) & mask;
                }
                if (!slot_used[h]) {
                    slot_used[h] = 1;
                    slot_key[h]  = key;
                }
                slot_idxs[h].push_back(row);
            }

            // probe phase
            for (uint32_t idx = ps; idx < pe; ++idx) {
                uint32_t prow = probe_buf[idx];
                auto pkey = probe_keys[prow];
                size_t h = HashUtil<KeyType>::hash(pkey) & mask;
                while (slot_used[h]) {
                    if (slot_key[h] == pkey) {
                        for (auto bi : slot_idxs[h]) {
                            size_t L = build_left ? bi : prow;
                            size_t R = build_left ? prow : bi;
                            std::vector<Data> out;
                            out.reserve(outs.size());
                            for (auto [ci,dt] : outs) {
                                if (ci < left_w) {
                                    out.push_back(left.get_column(ci).values[L]);
                                } else {
                                    out.push_back(right.get_column(ci - left_w).values[R]);
                                }
                            }
                            local.push_back(std::move(out));
                            thread_row_counts[tid]++;
                        }
                        break;
                    }
                    h = (h + 1) & mask;
                }
            }
        }
    }

    // 6) merge
    // Calculate total rows
    size_t total_rows = 0;
    for (auto count : thread_row_counts) {
        total_rows += count;
    }

    if (total_rows == 0) {
        return SimpleColumnarTableView(std::make_shared<SimpleColumnarTable>());;
    }

    // Create a new SimpleColumnarTable to hold the result
    auto result_table = std::make_shared<SimpleColumnarTable>();

    // Add columns based on output attributes
    for (auto [_, dt] : outs) {
        result_table->add_column(dt);
    }

    // Set row count and populate values
    result_table->set_row_count(total_rows);

    // Initialize column values vectors
    for (size_t col_idx = 0; col_idx < outs.size(); col_idx++) {
        result_table->get_column(col_idx).values.resize(total_rows);
    }

    // Copy data from thread_out to result_table
    size_t row_idx = 0;
    for (auto &thread_rows : thread_out) {
        for (auto &row : thread_rows) {
            for (size_t col_idx = 0; col_idx < row.size(); ++col_idx) {
                result_table->get_column(col_idx).values[row_idx] = std::move(row[col_idx]);
            }
            row_idx++;
        }
    }

    // Return a view of the entire result table
    return SimpleColumnarTableView(result_table);
}
// ---------------------------------------------------------
// The new execute_hash_join using one-pass private partition
// ---------------------------------------------------------
SimpleColumnarTableView execute_hash_join(
    const Plan& plan,
    WorkContext* context,
    const JoinNode& join,
    const std::vector<std::tuple<size_t, DataType>>& outs)
{
    DataType t = join.build_left
        ? std::get<1>(plan.nodes[join.left].output_attrs[join.left_attr])
        : std::get<1>(plan.nodes[join.right].output_attrs[join.right_attr]);

    switch (t) {
        case DataType::INT32:   return hash_join_omp<int32_t>(plan,context,join,outs);
        case DataType::INT64:   return hash_join_omp<int64_t>(plan,context,join,outs);
        case DataType::FP64:    return hash_join_omp<double>(plan,context,join,outs);
        case DataType::VARCHAR: return hash_join_omp<std::string>(plan,context,join,outs);
        default:                throw std::runtime_error("Unsupported join type");
    }
}

SimpleColumnarTableView execute_scan(const Plan& plan,
    WorkContext* context,
    const ScanNode& scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto table_id = scan.base_table_id;
    namespace views = ranges::views;
    auto view_indices = output_attrs
        | views::transform([](const auto& v) { return std::get<0>(v); })
        | ranges::to<std::vector<size_t>>();
    return SimpleColumnarTableView(context->tables[table_id], view_indices);
}

SimpleColumnarTableView execute_impl(const Plan& plan, WorkContext* context, size_t node_idx) {
    auto& node = plan.nodes[node_idx];
    return std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, JoinNode>) {
                return execute_hash_join(plan, context, value, node.output_attrs);
            } else {
                return execute_scan(plan, context, value, node.output_attrs);
            }
        },
        node.data);
}

void prepare(const Plan& plan, void* context) {
    WorkContext* work_context = static_cast<WorkContext*>(context);
    work_context->tables.clear();
    for (size_t table_id = 0; table_id < plan.inputs.size(); ++table_id) {
        const auto& table = plan.inputs[table_id];
        SimpleColumnarTable results;

        for (const auto& column : table.columns) {
            results.add_column(column.type);
        }
        results.set_row_count(table.num_rows);

        size_t i = 0;
        for (const auto& column : table.columns) {
            auto& simple_col = results.get_column(i);
            simple_col.values.reserve(table.num_rows);

            for (const auto& value : iterate(column)) {
                simple_col.values.push_back(value);
            }
            ++i;
        }
        work_context->tables.emplace_back(
            std::make_shared<SimpleColumnarTable>(std::move(results)));
    }
}

ColumnarTable execute(const Plan& plan, void* context) {
    auto ret = execute_impl(plan, static_cast<WorkContext*>(context), plan.root);
    return ret.to_columnar();
}

void* build_context() {
    return static_cast<void*>(new WorkContext());
}

void destroy_context(void* context) {
    delete static_cast<WorkContext*>(context);
}

} // namespace Contest
