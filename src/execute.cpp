#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <ranges>
#include <thread>
#include <vector>
#include <mutex>
#include <pthread.h>
#include <cmath>
#include <algorithm>
#include <variant>
#include <ranges>

namespace Contest {

using ExecuteResult = std::vector<std::vector<Data>>;

ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

// ----------------------------------------------------------------
// Constants & Helpers
// ----------------------------------------------------------------
constexpr size_t NUM_PARTITIONS = 128;
constexpr size_t NUM_CORES      = 96;
constexpr float HASH_LOAD_FACTOR = 0.5;

// Round up to next power-of-two
static inline size_t next_power_of_two(size_t x) {
    if (x <= 1) return 1;
    return 1ull << (64 - __builtin_clzll(x - 1));
}

// For thread affinity
static void pin_thread_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    
    pthread_t current_thread = pthread_self();
    pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

// ----------------------------------------------------------------
// Hash utilities for various KeyTypes
// ----------------------------------------------------------------
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
        } else if constexpr (std::is_same_v<KeyType,std::string>) {
            size_t h = 14695981039346656037ULL;
            for (char c : key) {
                h ^= static_cast<size_t>(c);
                h *= 1099511628211ULL;
            }
            return h;
        }
    }
};

// ----------------------------------------------------------------
// Simple open-address linear probe hash table
// ----------------------------------------------------------------
template<typename KeyType>
class LinearProbeHashTable {
  struct Entry {
    KeyType             key;
    std::vector<size_t> idxs;      // hold *all* matching row-indices
    bool                used = false;
  };

  std::vector<Entry> table;
  size_t             capacity_;
  size_t             mask_;        // = capacity_ - 1

public:
  LinearProbeHashTable(size_t estimated_size) {
    // reserve for load factor ~0.5
    size_t cap = static_cast<size_t>(estimated_size / HASH_LOAD_FACTOR);
    capacity_ = next_power_of_two(cap);
    mask_     = capacity_ - 1;
    table.resize(capacity_);
  }

  void insert(const KeyType &key, size_t row_idx) {
    size_t h = HashUtil<KeyType>::hash(key) & mask_;

    // find slot (either empty, or matching key)
    while (table[h].used && table[h].key != key) {
      h = (h + 1) & mask_;
    }

    if (!table[h].used) {
      // first time we see this key
      table[h].used = true;
      table[h].key  = key;
    }
    // always record this row index, even if key was seen before
    table[h].idxs.push_back(row_idx);
  }

  std::vector<size_t> find(const KeyType &key) const {
    std::vector<size_t> out;
    size_t h = HashUtil<KeyType>::hash(key) & mask_;

    // prefetch the next cache line
    __builtin_prefetch(&table[(h+1) & mask_]);

    while (table[h].used) {
      if (table[h].key == key) {
        // return all recorded indices
        out = table[h].idxs;
        break;
      }
      h = (h + 1) & mask_;
      __builtin_prefetch(&table[(h+1) & mask_]);
    }
    return out;
  }

  size_t capacity() const { return capacity_; }
};

// ----------------------------------------------------------------
// Lock-free two-phase radix partition
// ----------------------------------------------------------------
template<typename KeyT>
struct PartitionInfo {
    // bucket offsets: size = P+1
    std::vector<size_t> offsets;
    // flat arrays of all keys & row-indices
    std::vector<KeyT>   keys;
    std::vector<size_t> idxs;
    PartitionInfo(): offsets(NUM_PARTITIONS+1) {}
};

template<typename KeyT>
PartitionInfo<KeyT> parallel_radix_partition(
    const ExecuteResult &rows,
    size_t key_col)
{
    PartitionInfo<KeyT> out;
    size_t N = rows.size();
    if (N == 0) {
        // empty side → zero offsets, empty arrays
        return out;
    }
    size_t T = std::min(N, NUM_CORES);
    size_t chunk = (N + T - 1)/T;

    // 1. per-thread histograms
    std::vector<std::vector<size_t>> hist(T, std::vector<size_t>(NUM_PARTITIONS));
    std::vector<std::thread>         ths;
    for (size_t t = 0; t < T; ++t) {
        ths.emplace_back([&,t]{
            pin_thread_to_core(t);
            size_t start = t*chunk;
            size_t end   = std::min(start+chunk, N);
            auto &h = hist[t];
            for (size_t i = start; i < end; ++i) {
                std::visit([&](auto const &v){
                    using V = std::decay_t<decltype(v)>;
                    if constexpr(std::is_same_v<V,KeyT>) {
                        size_t b = HashUtil<KeyT>::hash(v) & (NUM_PARTITIONS-1);
                        h[b]++;
                    }
                }, rows[i][key_col]);
            }
        });
    }
    for (auto &th: ths) th.join();

    // 2. global prefix-sum
    auto &off = out.offsets;
    off[0]=0;
    for (size_t b=0;b<NUM_PARTITIONS;++b) {
        size_t sum=0;
        for (size_t t=0;t<T;++t) sum+=hist[t][b];
        off[b+1]=off[b]+sum;
    }
    size_t total = off[NUM_PARTITIONS];
    out.keys .resize(total);
    out.idxs.resize(total);

    // 3. compute each thread's start offset per bucket
    std::vector<std::vector<size_t>> thread_off(T, std::vector<size_t>(NUM_PARTITIONS));
    for (size_t b=0;b<NUM_PARTITIONS;++b) {
        size_t pos = off[b];
        for (size_t t=0;t<T;++t) {
            thread_off[t][b] = pos;
            pos += hist[t][b];
        }
    }

    // 4. scatter lock-free
    ths.clear();
    for (size_t t=0;t<T;++t) {
        ths.emplace_back([&,t]{
            pin_thread_to_core(t);
            size_t start = t*chunk;
            size_t end   = std::min(start+chunk, N);
            auto &toff = thread_off[t];
            for (size_t i=start;i<end;++i) {
                std::visit([&](auto const &v){
                    using V = std::decay_t<decltype(v)>;
                    if constexpr(std::is_same_v<V,KeyT>) {
                        size_t b = HashUtil<KeyT>::hash(v)&(NUM_PARTITIONS-1);
                        size_t dst = toff[b]++;
                        out.keys[dst] = v;
                        out.idxs[dst] = i;
                    }
                }, rows[i][key_col]);
            }
        });
    }
    for (auto &th: ths) th.join();

    return out;
}

// ----------------------------------------------------------------
// Per-bucket build+probe in parallel
// ----------------------------------------------------------------
template<typename KeyT>
void process_buckets(
    bool                                           build_left,
    size_t                                         left_cols,
    const PartitionInfo<KeyT>&                     build_pi,
    const PartitionInfo<KeyT>&                     probe_pi,
    const ExecuteResult&                           left_rows,
    const ExecuteResult&                           right_rows,
    const std::vector<std::tuple<size_t,DataType>>& outs,
    ExecuteResult&                                 results,
    std::mutex&                                    mtx)
{
    std::vector<std::thread> ths;
    for (size_t b = 0; b < NUM_PARTITIONS; ++b) {
        size_t b0 = build_pi.offsets[b], b1 = build_pi.offsets[b+1];
        size_t p0 = probe_pi.offsets[b], p1 = probe_pi.offsets[b+1];
        if (b0==b1 || p0==p1) continue;

        ths.emplace_back([=,&build_pi,&probe_pi,&left_rows,&right_rows,&outs,&results,&mtx](){
            // 1) build local hash table on whichever side
            LinearProbeHashTable<KeyT> ht(b1 - b0);
            for (size_t i = b0; i < b1; ++i) {
                ht.insert(build_pi.keys[i], build_pi.idxs[i]);
            }

            // 2) probe & emit matches
            ExecuteResult local;
            for (size_t i = p0; i < p1; ++i) {
                auto matches = ht.find(probe_pi.keys[i]);
                for (auto bi : matches) {
                    // compute the true left/right row indices
                    size_t left_idx, right_idx;
                    if (build_left) {
                        left_idx  = bi;
                        right_idx = probe_pi.idxs[i];
                    } else {
                        left_idx  = probe_pi.idxs[i];
                        right_idx = bi;
                    }

                    const auto &L = left_rows [ left_idx ];
                    const auto &R = right_rows[ right_idx ];

                    // build output row—*always* split on left_cols
                    std::vector<Data> row;
                    row.reserve(outs.size());
                    for (auto [ci,dt] : outs) {
                        if (ci < left_cols)        row.push_back(L[ci]);
                        else                       row.push_back(R[ci - left_cols]);
                    }
                    local.emplace_back(std::move(row));
                }
            }

            // 3) merge
            std::lock_guard lk(mtx);
            results.insert(results.end(),
                           std::make_move_iterator(local.begin()),
                           std::make_move_iterator(local.end()));
        });
    }
    for (auto &th : ths) th.join();
}


// ----------------------------------------------------------------
// Scan + Dispatch + Execution
// ----------------------------------------------------------------
ExecuteResult execute_hash_join(
    const Plan&                                    plan,
    const JoinNode&                                join,
    const std::vector<std::tuple<size_t,DataType>>& outs)
{
    // get both sides
    auto left  = execute_impl(plan, join.left);
    auto right = execute_impl(plan, join.right);

    if (left.empty() || right.empty()) return {};  // early exit

    bool  build_left = join.build_left;
    auto& build_rows = build_left ? left  : right;
    auto& probe_rows = build_left ? right : left;

    size_t build_col = build_left ? join.left_attr : join.right_attr;
    size_t probe_col = build_left ? join.right_attr : join.left_attr;

    // **always** split at the left table's width
    size_t left_cols = left.empty() ? 0 : left[0].size();

    // partition both sides
    ExecuteResult results;
    std::mutex    mtx;

    DataType t = build_left
      ? std::get<1>(plan.nodes[join.left] .output_attrs[join.left_attr])
      : std::get<1>(plan.nodes[join.right].output_attrs[join.right_attr]);

    switch (t) {
      case DataType::INT32: {
        auto bpi = parallel_radix_partition<int32_t>(build_rows, build_col);
        auto ppi = parallel_radix_partition<int32_t>(probe_rows, probe_col);
        process_buckets<int32_t>(
            build_left, left_cols,
            bpi, ppi, left, right,
            outs, results, mtx
        );
        break;
      }
      case DataType::INT64: {
        auto bpi = parallel_radix_partition<int64_t>(build_rows, build_col);
        auto ppi = parallel_radix_partition<int64_t>(probe_rows, probe_col);
        process_buckets<int64_t>(
            build_left, left_cols,
            bpi, ppi, left, right,
            outs, results, mtx
        );
        break;
      }
      case DataType::FP64: {
        auto bpi = parallel_radix_partition<double>(build_rows, build_col);
        auto ppi = parallel_radix_partition<double>(probe_rows, probe_col);
        process_buckets<double>(
            build_left, left_cols,
            bpi, ppi, left, right,
            outs, results, mtx
        );
        break;
      }
      case DataType::VARCHAR: {
        auto bpi = parallel_radix_partition<std::string>(build_rows, build_col);
        auto ppi = parallel_radix_partition<std::string>(probe_rows, probe_col);
        process_buckets<std::string>(
            build_left, left_cols,
            bpi, ppi, left, right,
            outs, results, mtx
        );
        break;
      }
    }

    return results;
}

ExecuteResult execute_scan(const Plan&               plan,
    const ScanNode&                                  scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto                           table_id = scan.base_table_id;
    auto&                          input    = plan.inputs[table_id];
    auto                           table    = Table::from_columnar(input);
    std::vector<std::vector<Data>> results;
    for (auto& record: table.table()) {
        std::vector<Data> new_record;
        new_record.reserve(output_attrs.size());
        for (auto [col_idx, _]: output_attrs) {
            new_record.emplace_back(record[col_idx]);
        }
        results.emplace_back(std::move(new_record));
    }
    return results;
}

ExecuteResult execute_impl(const Plan& plan, size_t node_idx) {
    auto& node = plan.nodes[node_idx];
    return std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, JoinNode>) {
                return execute_hash_join(plan, value, node.output_attrs);
            } else {
                return execute_scan(plan, value, node.output_attrs);
            }
        },
        node.data);
}

ColumnarTable execute(const Plan& plan, [[maybe_unused]] void* context) {
    namespace views = ranges::views;
    auto ret        = execute_impl(plan, plan.root);
    auto ret_types  = plan.nodes[plan.root].output_attrs
                   | views::transform([](const auto& v) { return std::get<1>(v); })
                   | ranges::to<std::vector<DataType>>();
    Table table{std::move(ret), std::move(ret_types)};
    return table.to_columnar();
}

void* build_context() {
    return nullptr;
}

void destroy_context([[maybe_unused]] void* context) {}

} // namespace Contest

