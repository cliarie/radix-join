#include <iostream>
#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <bloom_filter.h>
#include <column_iterator.h>
#include <simple_columnar_table.h>
#include <variant>

namespace Contest {

using ExecuteResult = std::vector<std::vector<Data>>;

SimpleColumnarTable execute_impl(const Plan& plan, size_t node_idx);

struct JoinAlgorithm {
    bool                                             build_left;
    SimpleColumnarTable&                             left;
    SimpleColumnarTable&                             right;
    SimpleColumnarTable&                             results;
    size_t                                           left_col, right_col;
    const std::vector<std::tuple<size_t, DataType>>& output_attrs;

    template <class T>
    auto run() {
        std::unordered_map<T, std::vector<size_t>> hash_table;
        BloomFilter<T> bloom_filter;

        // Initialize result columns
        for (auto [_, type] : output_attrs) {
            results.add_column(type);
        }

        size_t estimate = std::min(left.row_count(), right.row_count());
        for (size_t i = 0; i < output_attrs.size(); ++i) {
            results.get_column(i).values.reserve(estimate);
        }


        if (build_left) {
            hash_table.reserve(left.row_count());
            bloom_filter.init(left.row_count(), 0.01);
            // Build hash table from left table
            const auto& left_column = left.get_column(left_col).values;
            for (size_t idx = 0; idx < left.row_count(); idx++) {
                std::visit([&hash_table, &bloom_filter, idx](const auto& key) {
                    using Tk = std::decay_t<decltype(key)>;
                    if constexpr (std::is_same_v<Tk, T>) {
                        hash_table[key].push_back(idx);
                        bloom_filter.insert(key);
                    } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                        throw std::runtime_error("wrong type of field");
                    }
                }, left_column[idx]);
            }

            // Probe with right table
            const auto& right_column = right.get_column(right_col).values;
            size_t      result_count = 0;

            for (size_t right_idx = 0; right_idx < right.row_count(); right_idx++) {
                std::visit([&](const auto& key) {
                    using Tk = std::decay_t<decltype(key)>;
                    if constexpr (std::is_same_v<Tk, T>) {
                        if (!bloom_filter.possiblyContains(key)) {
                            return;
                        }
                        auto it = hash_table.find(key);
                        if (it != hash_table.end()) {
                            for (auto left_idx : it->second) {
                                for (size_t c = 0; c < output_attrs.size(); ++c) {
                                    auto [col_idx, _] = output_attrs[c];
                                    if (col_idx < left.column_count()) {
                                        results.get_column(c).values.push_back(left.get_column(col_idx).values[left_idx]);
                                    } else {
                                        size_t ridx = col_idx - left.column_count();
                                        results.get_column(c).values.push_back(right.get_column(ridx).values[right_idx]);
                                    }
                                }
                                ++result_count;
                            }
                        }
                    } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                        throw std::runtime_error("wrong type of field");
                    }
                }, right_column[right_idx]);
            }
            results.set_row_count(result_count);
        } else {
            hash_table.reserve(right.row_count());
            bloom_filter.init(right.row_count(), 0.01);
            // Build hash table from right table
            const auto& right_column = right.get_column(right_col).values;
            for (size_t idx = 0; idx < right.row_count(); idx++) {
                std::visit([&hash_table, &bloom_filter, idx](const auto& key) {
                    using Tk = std::decay_t<decltype(key)>;
                    if constexpr (std::is_same_v<Tk, T>) {
                        hash_table[key].push_back(idx);
                        bloom_filter.insert(key);
                    } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                        throw std::runtime_error("wrong type of field");
                    }
                }, right_column[idx]);
            }

            // Probe with left table
            const auto& left_column = left.get_column(left_col).values;
            size_t result_count = 0;
            for (size_t left_idx = 0; left_idx < left.row_count(); left_idx++) {
                std::visit([&](const auto& key) {
                    using Tk = std::decay_t<decltype(key)>;
                    if constexpr (std::is_same_v<Tk, T>) {
                        if (!bloom_filter.possiblyContains(key)) {
                            return;
                        }
                        auto it = hash_table.find(key);
                        if (it != hash_table.end()) {
                            for (auto right_idx : it->second) {
                                for (size_t c = 0; c < output_attrs.size(); ++c) {
                                    auto [col_idx, _] = output_attrs[c];
                                    if (col_idx < left.column_count()) {
                                        results.get_column(c).values.push_back(left.get_column(col_idx).values[left_idx]);
                                    } else {
                                        size_t ridx = col_idx - left.column_count();
                                        results.get_column(c).values.push_back(right.get_column(ridx).values[right_idx]);
                                    }
                                }
                                ++result_count;
                            }
                        }
                    } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                        throw std::runtime_error("wrong type of field");
                    }
                }, left_column[left_idx]);
            }
            results.set_row_count(result_count);
        }
    }
};

SimpleColumnarTable execute_hash_join(const Plan&          plan,
    const JoinNode&                                  join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto                           left_idx    = join.left;
    auto                           right_idx   = join.right;
    auto&                          left_node   = plan.nodes[left_idx];
    auto&                          right_node  = plan.nodes[right_idx];
    auto&                          left_types  = left_node.output_attrs;
    auto&                          right_types = right_node.output_attrs;
    auto                           left        = execute_impl(plan, left_idx);
    auto                           right       = execute_impl(plan, right_idx);
    SimpleColumnarTable results;

    JoinAlgorithm join_algorithm{.build_left = join.build_left,
        .left                                = left,
        .right                               = right,
        .results                             = results,
        .left_col                            = join.left_attr,
        .right_col                           = join.right_attr,
        .output_attrs                        = output_attrs};
    if (join.build_left) {
        switch (std::get<1>(left_types[join.left_attr])) {
        case DataType::INT32:   join_algorithm.run<int32_t>(); break;
        case DataType::INT64:   join_algorithm.run<int64_t>(); break;
        case DataType::FP64:    join_algorithm.run<double>(); break;
        case DataType::VARCHAR: join_algorithm.run<std::string>(); break;
        }
    } else {
        switch (std::get<1>(right_types[join.right_attr])) {
        case DataType::INT32:   join_algorithm.run<int32_t>(); break;
        case DataType::INT64:   join_algorithm.run<int64_t>(); break;
        case DataType::FP64:    join_algorithm.run<double>(); break;
        case DataType::VARCHAR: join_algorithm.run<std::string>(); break;
        }
    }

    return results;
}

SimpleColumnarTable execute_scan(const Plan& plan,
    const ScanNode& scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto table_id = scan.base_table_id;
    auto& table = plan.inputs[table_id];
    SimpleColumnarTable results;

    // Create columns with the output types
    for (auto [_, type] : output_attrs) {
        results.add_column(type);
    }
    // Set the row count
    results.set_row_count(table.num_rows);

    // Fill the columns with data using iterators
    size_t i = 0;
    for (auto [col_idx, _] : output_attrs) {
        auto& simple_col = results.get_column(i);
        simple_col.values.reserve(table.num_rows);

        for (const auto& value : iterate(table.columns[col_idx])) {
            simple_col.values.push_back(value);
        }
        ++i;
    }

    return results;
}

SimpleColumnarTable execute_impl(const Plan& plan, size_t node_idx) {
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

    return ret.to_columnar();
}

void* build_context() {
    return nullptr;
}

void destroy_context([[maybe_unused]] void* context) {}

} // namespace Contest
