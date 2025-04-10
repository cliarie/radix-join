#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <execution>
#include <omp.h>
#include <memory_resource>
#include <parallel/algorithm>
#include <iostream>
#include <chrono>
#include <atomic>

namespace Contest {

// global, thread-safe accumulators (nanoseconds)
static std::atomic<long long> g_scan_time_ns{0};
static std::atomic<long long> g_sortjoin_time_ns{0};
static std::atomic<long long> g_hashjoin_time_ns{0};

using ExecuteResult = std::vector<std::vector<Data>>;

ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

struct JoinAlgorithm {
    bool                                             build_left;
    ExecuteResult&                                   left;
    ExecuteResult&                                   right;
    ExecuteResult&                                   results;
    size_t                                           left_col, right_col;
    const std::vector<std::tuple<size_t, DataType>>& output_attrs;

    template <class T>
    auto runHashJoin() {
        namespace views = ranges::views;
        std::unordered_map<T, std::vector<size_t>> hash_table;
        if (build_left) {
            for (auto&& [idx, record]: left | views::enumerate) {
                std::visit(
                    [&hash_table, idx = idx](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                                hash_table.emplace(key, std::vector<size_t>(1, idx));
                            } else {
                                itr->second.push_back(idx);
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    record[left_col]);
            }
            for (auto& right_record: right) {
                std::visit(
                    [&](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr != hash_table.end()) {
                                for (auto left_idx: itr->second) {
                                    auto&             left_record = left[left_idx];
                                    std::vector<Data> new_record;
                                    new_record.reserve(output_attrs.size());
                                    for (auto [col_idx, _]: output_attrs) {
                                        if (col_idx < left_record.size()) {
                                            new_record.emplace_back(left_record[col_idx]);
                                        } else {
                                            new_record.emplace_back(
                                                right_record[col_idx - left_record.size()]);
                                        }
                                    }
                                    results.emplace_back(std::move(new_record));
                                }
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    right_record[right_col]);
            }
        } else {
            for (auto&& [idx, record]: right | views::enumerate) {
                std::visit(
                    [&hash_table, idx = idx](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                                hash_table.emplace(key, std::vector<size_t>(1, idx));
                            } else {
                                itr->second.push_back(idx);
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    record[right_col]);
            }
            for (auto& left_record: left) {
                std::visit(
                    [&](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr != hash_table.end()) {
                                for (auto right_idx: itr->second) {
                                    auto&             right_record = right[right_idx];
                                    std::vector<Data> new_record;
                                    new_record.reserve(output_attrs.size());
                                    for (auto [col_idx, _]: output_attrs) {
                                        if (col_idx < left_record.size()) {
                                            new_record.emplace_back(left_record[col_idx]);
                                        } else {
                                            new_record.emplace_back(
                                                right_record[col_idx - left_record.size()]);
                                        }
                                    }
                                    results.emplace_back(std::move(new_record));
                                }
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    left_record[left_col]);
            }
        }
    }

    template <class T>
    void runSortMergeJoin() {
        // vector of (key, pointer to row) for both tables
        std::vector<std::pair<T, const std::vector<Data>*>> L, R;
        L.reserve(left.size());
        R.reserve(right.size());

        for (auto const& record : left) {
            if (auto key = std::get_if<T>(&record[left_col])) {
                L.emplace_back(*key, &record);
            }
        }
        for (auto const& record : right) {
            if (auto key = std::get_if<T>(&record[right_col])) {
                R.emplace_back(*key, &record);
            }
        }

        // 2) sort by key
        auto cmp = [](auto const& a, auto const& b){ return a.first < b.first; };
        std::sort(std::execution::par_unseq, L.begin(), L.end(), cmp);
        std::sort(std::execution::par_unseq, R.begin(), R.end(), cmp);

        // 3) two-pointer merge to find rows with same key
        size_t l_idx = 0;
        size_t r_idx = 0;
        while (l_idx < L.size() && r_idx < R.size()) {
            if (L[l_idx].first < R[r_idx].first) {
                ++l_idx;
            } else if (R[r_idx].first < L[l_idx].first) {
                ++r_idx;
            } else {
                // key is the same
                T key = L[l_idx].first;

                size_t l_group_end = l_idx;
                size_t r_group_end = r_idx;
                while (l_group_end < L.size() && L[l_group_end].first == key) {
                    ++l_group_end;
                }
                while (r_group_end < R.size() && R[r_group_end].first == key) {
                    ++r_group_end;
                }

                // emit Cartesian product of the group
                for (size_t a = l_idx; a < l_group_end; ++a) {
                    for (size_t b = r_idx; b < r_group_end; ++b) {
                        auto const& left_record = *L[a].second;
                        auto const& right_record = *R[b].second;
                        std::vector<Data> new_record;
                        new_record.reserve(output_attrs.size());
                        for (auto [col_idx, _]: output_attrs) {
                            if (col_idx < left_record.size()) {
                                new_record.emplace_back(left_record[col_idx]);
                            } else {
                                new_record.emplace_back(right_record[col_idx - left_record.size()]);
                            }
                        }
                        results.emplace_back(std::move(new_record));
                    }
                }

                l_idx = l_group_end;
                r_idx = r_group_end;
            }
        }
    }
};

ExecuteResult execute_hash_join(const Plan&          plan,
    const JoinNode&                                  join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto                           left_idx    = join.left;
    auto                           right_idx   = join.right;
    auto&                          left_node   = plan.nodes[left_idx];
    auto&                          right_node  = plan.nodes[right_idx];
    auto&                          left_types  = left_node.output_attrs;
    auto&                          right_types = right_node.output_attrs;
    auto left  = execute_impl(plan, left_idx);
    auto right = execute_impl(plan, right_idx);

    std::vector<std::vector<Data>> results;
    using namespace std::chrono;
    auto t0 = high_resolution_clock::now();

    JoinAlgorithm join_algorithm{.build_left = join.build_left,
        .left                                = left,
        .right                               = right,
        .results                             = results,
        .left_col                            = join.left_attr,
        .right_col                           = join.right_attr,
        .output_attrs                        = output_attrs};
    if (join.build_left) {
        switch (std::get<1>(left_types[join.left_attr])) {
        case DataType::INT32:   join_algorithm.runHashJoin<int32_t>(); break;
        case DataType::INT64:   join_algorithm.runHashJoin<int64_t>(); break;
        case DataType::FP64:    join_algorithm.runHashJoin<double>(); break;
        case DataType::VARCHAR: join_algorithm.runHashJoin<std::string>(); break;
        }
    } else {
        switch (std::get<1>(right_types[join.right_attr])) {
        case DataType::INT32:   join_algorithm.runHashJoin<int32_t>(); break;
        case DataType::INT64:   join_algorithm.runHashJoin<int64_t>(); break;
        case DataType::FP64:    join_algorithm.runHashJoin<double>(); break;
        case DataType::VARCHAR: join_algorithm.runHashJoin<std::string>(); break;
        }
    }
    auto t1 = high_resolution_clock::now();
    Contest::g_hashjoin_time_ns += duration_cast<nanoseconds>(t1 - t0).count();

    return results;
}

ExecuteResult execute_scan(const Plan&               plan,
    const ScanNode&                                  scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto                           table_id = scan.base_table_id;

    using namespace std::chrono;
    auto t0 = high_resolution_clock::now();

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


    auto t1 = high_resolution_clock::now();
    Contest::g_scan_time_ns += duration_cast<nanoseconds>(t1 - t0).count();
    return results;
}

ExecuteResult execute_sort_merge_join(const Plan&          plan,
    const JoinNode&                                        join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {

    auto                           left_idx    = join.left;
    auto                           right_idx   = join.right;
    auto&                          left_node   = plan.nodes[left_idx];
    auto&                          right_node  = plan.nodes[right_idx];
    auto&                          left_types  = left_node.output_attrs;
    auto&                          right_types = right_node.output_attrs;
    auto left  = execute_impl(plan, left_idx);
    auto right = execute_impl(plan, right_idx);

    std::vector<std::vector<Data>> results;
    using namespace std::chrono;
    auto t0 = high_resolution_clock::now();
    JoinAlgorithm join_algorithm{.build_left = join.build_left,
        .left                                = left,
        .right                               = right,
        .results                             = results,
        .left_col                            = join.left_attr,
        .right_col                           = join.right_attr,
        .output_attrs                        = output_attrs};


    if (join.build_left) {
        switch (std::get<1>(left_types[join.left_attr])) {
        case DataType::INT32:   join_algorithm.runSortMergeJoin<int32_t>(); break;
        case DataType::INT64:   join_algorithm.runSortMergeJoin<int64_t>(); break;
        case DataType::FP64:    join_algorithm.runSortMergeJoin<double>(); break;
        case DataType::VARCHAR: join_algorithm.runSortMergeJoin<std::string>(); break;
        }
    } else {
        switch (std::get<1>(right_types[join.right_attr])) {
        case DataType::INT32:   join_algorithm.runSortMergeJoin<int32_t>(); break;
        case DataType::INT64:   join_algorithm.runSortMergeJoin<int64_t>(); break;
        case DataType::FP64:    join_algorithm.runSortMergeJoin<double>(); break;
        case DataType::VARCHAR: join_algorithm.runSortMergeJoin<std::string>(); break;
        }
    }
    auto t1 = high_resolution_clock::now();
    Contest::g_sortjoin_time_ns += duration_cast<nanoseconds>(t1 - t0).count();
    return results;
}

ExecuteResult execute_impl(const Plan& plan, size_t node_idx) {
    auto& node = plan.nodes[node_idx];
    return std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, JoinNode>) {
                // return execute_hash_join(plan, value, node.output_attrs);
                return execute_sort_merge_join(plan, value, node.output_attrs);
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

void destroy_context([[maybe_unused]] void* context) {
    std::cout
    << "TOTAL scan time:     " << Contest::g_scan_time_ns.load() / 1e9 << " s\n"
    << "TOTAL sort join time: " << Contest::g_sortjoin_time_ns.load() / 1e9 << " s\n"
    << "TOTAL hash join time: " << Contest::g_hashjoin_time_ns.load() / 1e9 << " s\n";
} 

}// namespace Contest
