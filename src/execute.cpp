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

namespace Contest {

struct WorkContext {
    std::vector<std::shared_ptr<SimpleColumnarTable>> tables;
};

SimpleColumnarTableView execute_impl(const Plan& plan, WorkContext* context, size_t node_idx);

struct JoinAlgorithm {
    bool                                             build_left;
    SimpleColumnarTableView&                         left;
    SimpleColumnarTableView&                         right;
    SimpleColumnarTable                              results;
    size_t                                           left_col, right_col;
    const std::vector<std::tuple<size_t, DataType>>& output_attrs;

    template <class T>
    auto run() {
        build_left = left.row_count() <= right.row_count();

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
                        std::cout << "left hash build k = " << key << std::endl;
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
                        std::cout << "right probe k = " << key << std::endl;
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
                        std::cout << "right hash k = " << key << std::endl;
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
                        std::cout << "left probe k = " << key << std::endl;
                        throw std::runtime_error("wrong type of field");
                    }
                }, left_column[left_idx]);
            }
            results.set_row_count(result_count);
        }
    }
};

SimpleColumnarTableView execute_hash_join(const Plan&    plan,
    WorkContext*                                     context,
    const JoinNode&                                  join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto                           left_idx    = join.left;
    auto                           right_idx   = join.right;
    auto&                          left_node   = plan.nodes[left_idx];
    auto&                          right_node  = plan.nodes[right_idx];
    auto&                          left_types  = left_node.output_attrs;
    auto&                          right_types = right_node.output_attrs;
    auto                           left        = execute_impl(plan, context, left_idx);
    auto                           right       = execute_impl(plan, context, right_idx);

    JoinAlgorithm join_algorithm{.build_left = join.build_left,
        .left                                = left,
        .right                               = right,
        .results                             = {},
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

    return std::make_shared<SimpleColumnarTable>(std::move(join_algorithm.results));
}

SimpleColumnarTableView execute_scan(const Plan& plan,
    WorkContext* context,
    const ScanNode& scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    // auto table_id = scan.base_table_id;
    // auto& table = plan.inputs[table_id];
    // SimpleColumnarTable results;

    // // Create columns with the output types
    // for (auto [_, type] : output_attrs) {
    //     results.add_column(type);
    // }
    // // Set the row count
    // results.set_row_count(table.num_rows);

    // // Fill the columns with data using iterators
    // size_t i = 0;
    // for (auto [col_idx, _] : output_attrs) {
    //     auto& simple_col = results.get_column(i);
    //     simple_col.values.reserve(table.num_rows);

    //     for (const auto& value : iterate(table.columns[col_idx])) {
    //         simple_col.values.push_back(value);
    //     }
    //     ++i;
    // }

    // return SimpleColumnarTableView(std::make_shared<SimpleColumnarTable>(std::move(results)));

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
    WorkContext* work_context = static_cast<WorkContext*>(context);
    if (work_context->tables.empty()) {
        prepare(plan, context);
    }
    auto ret = execute_impl(plan, static_cast<WorkContext*>(context), plan.root);
    work_context->tables.clear();
    return ret.to_columnar();
}

void* build_context() {
    return static_cast<void*>(new WorkContext());
}

void destroy_context(void* context) {
    delete static_cast<WorkContext*>(context);
}

} // namespace Contest

