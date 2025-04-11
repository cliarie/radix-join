#pragma once

#include <memory>
#include <vector>
#include <common.h>
#include <attribute.h>
#include <statement.h>
#include <plan.h>

inline void set_bitmap(std::vector<uint8_t>& bitmap, uint16_t idx) {
    while (bitmap.size() < idx / 8 + 1) {
        bitmap.emplace_back(0);
    }
    auto byte_idx = idx / 8;
    auto bit = idx % 8;
    bitmap[byte_idx] |= (1u << bit);
}

inline void unset_bitmap(std::vector<uint8_t>& bitmap, uint16_t idx) {
    while (bitmap.size() < idx / 8 + 1) {
        bitmap.emplace_back(0);
    }
    auto byte_idx = idx / 8;
    auto bit = idx % 8;
    bitmap[byte_idx] &= ~(1u << bit);
}

namespace Contest {

struct SimpleColumn {
    DataType type;
    std::vector<Data> values;

    SimpleColumn(DataType type) : type(type) {}
};

class SimpleColumnarTable {
private:
    size_t num_rows{0};
    std::vector<SimpleColumn> columns;

public:
    SimpleColumnarTable() = default;

    void add_column(DataType type) {
        columns.emplace_back(type);
    }

    SimpleColumn& get_column(size_t idx) {
        return columns[idx];
    }

    const SimpleColumn& get_column(size_t idx) const {
        return columns[idx];
    }

    size_t column_count() const {
        return columns.size();
    }

    size_t row_count() const {
        return num_rows;
    }

    void set_row_count(size_t count) {
        num_rows = count;
    }

    ColumnarTable to_columnar() const {
        ColumnarTable ret;
        ret.num_rows = num_rows;

        for (const auto& simple_column : columns) {
            ret.columns.emplace_back(simple_column.type);
            auto& column = ret.columns.back();

            switch (simple_column.type) {
            case DataType::INT32: {
                uint16_t num_rows = 0;
                std::vector<int32_t> data;
                std::vector<uint8_t> bitmap;
                data.reserve(2048);
                bitmap.reserve(256);

                auto save_page = [&column, &num_rows, &data, &bitmap]() {
                    auto* page = column.new_page()->data;
                    *reinterpret_cast<uint16_t*>(page) = num_rows;
                    *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(data.size());
                    memcpy(page + 4, data.data(), data.size() * 4);
                    memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                    num_rows = 0;
                    data.clear();
                    bitmap.clear();
                };

                for (const auto& value : simple_column.values) {
                    std::visit(
                        [&save_page, &column, &num_rows, &data, &bitmap](const auto& value) {
                            using T = std::decay_t<decltype(value)>;
                            if constexpr (std::is_same_v<T, int32_t>) {
                                if (4 + (data.size() + 1) * 4 + (num_rows / 8 + 1) > PAGE_SIZE) {
                                    save_page();
                                }
                                set_bitmap(bitmap, num_rows);
                                data.emplace_back(value);
                                ++num_rows;
                            } else if constexpr (std::is_same_v<T, std::monostate>) {
                                if (4 + (data.size()) * 4 + (num_rows / 8 + 1) > PAGE_SIZE) {
                                    save_page();
                                }
                                unset_bitmap(bitmap, num_rows);
                                ++num_rows;
                            }
                        },
                        value);
                }

                if (num_rows != 0) {
                    save_page();
                }
                break;
            }
            case DataType::INT64: {
                uint16_t num_rows = 0;
                std::vector<int64_t> data;
                std::vector<uint8_t> bitmap;
                data.reserve(1024);
                bitmap.reserve(128);

                auto save_page = [&column, &num_rows, &data, &bitmap]() {
                    auto* page = column.new_page()->data;
                    *reinterpret_cast<uint16_t*>(page) = num_rows;
                    *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(data.size());
                    memcpy(page + 8, data.data(), data.size() * 8);
                    memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                    num_rows = 0;
                    data.clear();
                    bitmap.clear();
                };

                for (const auto& value : simple_column.values) {
                    std::visit(
                        [&save_page, &column, &num_rows, &data, &bitmap](const auto& value) {
                            using T = std::decay_t<decltype(value)>;
                            if constexpr (std::is_same_v<T, int64_t>) {
                                if (8 + (data.size() + 1) * 8 + (num_rows / 8 + 1) > PAGE_SIZE) {
                                    save_page();
                                }
                                set_bitmap(bitmap, num_rows);
                                data.emplace_back(value);
                                ++num_rows;
                            } else if constexpr (std::is_same_v<T, std::monostate>) {
                                if (8 + (data.size()) * 8 + (num_rows / 8 + 1) > PAGE_SIZE) {
                                    save_page();
                                }
                                unset_bitmap(bitmap, num_rows);
                                ++num_rows;
                            }
                        },
                        value);
                }

                if (num_rows != 0) {
                    save_page();
                }
                break;
            }
            case DataType::FP64: {
                uint16_t num_rows = 0;
                std::vector<double> data;
                std::vector<uint8_t> bitmap;
                data.reserve(1024);
                bitmap.reserve(128);

                auto save_page = [&column, &num_rows, &data, &bitmap]() {
                    auto* page = column.new_page()->data;
                    *reinterpret_cast<uint16_t*>(page) = num_rows;
                    *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(data.size());
                    memcpy(page + 8, data.data(), data.size() * 8);
                    memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                    num_rows = 0;
                    data.clear();
                    bitmap.clear();
                };

                for (const auto& value : simple_column.values) {
                    std::visit(
                        [&save_page, &column, &num_rows, &data, &bitmap](const auto& value) {
                            using T = std::decay_t<decltype(value)>;
                            if constexpr (std::is_same_v<T, double>) {
                                if (8 + (data.size() + 1) * 8 + (num_rows / 8 + 1) > PAGE_SIZE) {
                                    save_page();
                                }
                                set_bitmap(bitmap, num_rows);
                                data.emplace_back(value);
                                ++num_rows;
                            } else if constexpr (std::is_same_v<T, std::monostate>) {
                                if (8 + (data.size()) * 8 + (num_rows / 8 + 1) > PAGE_SIZE) {
                                    save_page();
                                }
                                unset_bitmap(bitmap, num_rows);
                                ++num_rows;
                            }
                        },
                        value);
                }

                if (num_rows != 0) {
                    save_page();
                }
                break;
            }
            case DataType::VARCHAR: {
                uint16_t num_rows = 0;
                std::vector<char> data;
                std::vector<uint16_t> offsets;
                std::vector<uint8_t> bitmap;
                data.reserve(8192);
                offsets.reserve(4096);
                bitmap.reserve(512);

                auto save_long_string = [&column](std::string_view data) {
                    size_t offset = 0;
                    auto first_page = true;
                    while (offset < data.size()) {
                        auto* page = column.new_page()->data;
                        if (first_page) {
                            *reinterpret_cast<uint16_t*>(page) = 0xffff;
                            first_page = false;
                        } else {
                            *reinterpret_cast<uint16_t*>(page) = 0xfffe;
                        }
                        auto page_data_len = std::min(data.size() - offset, PAGE_SIZE - 4);
                        *reinterpret_cast<uint16_t*>(page + 2) = page_data_len;
                        memcpy(page + 4, data.data() + offset, page_data_len);
                        offset += page_data_len;
                    }
                };

                auto save_page = [&column, &num_rows, &data, &offsets, &bitmap]() {
                    auto* page = column.new_page()->data;
                    *reinterpret_cast<uint16_t*>(page) = num_rows;
                    *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(offsets.size());
                    memcpy(page + 4, offsets.data(), offsets.size() * 2);
                    memcpy(page + 4 + offsets.size() * 2, data.data(), data.size());
                    memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                    num_rows = 0;
                    data.clear();
                    offsets.clear();
                    bitmap.clear();
                };

                for (const auto& value : simple_column.values) {
                    std::visit(
                        [&save_long_string, &save_page, &column, &num_rows, &data, &offsets, &bitmap](const auto& value) {
                            using T = std::decay_t<decltype(value)>;
                            if constexpr (std::is_same_v<T, std::string>) {
                                if (value.size() > PAGE_SIZE - 7) {
                                    if (num_rows > 0) {
                                        save_page();
                                    }
                                    save_long_string(value);
                                } else {
                                    if (4 + (offsets.size() + 1) * 2 + (data.size() + value.size())
                                            + (num_rows / 8 + 1)
                                        > PAGE_SIZE) {
                                        save_page();
                                    }
                                    set_bitmap(bitmap, num_rows);
                                    data.insert(data.end(), value.begin(), value.end());
                                    offsets.emplace_back(data.size());
                                    ++num_rows;
                                }
                            } else if constexpr (std::is_same_v<T, std::monostate>) {
                                if (4 + offsets.size() * 2 + data.size() + (num_rows / 8 + 1)
                                    > PAGE_SIZE) {
                                    save_page();
                                }
                                unset_bitmap(bitmap, num_rows);
                                ++num_rows;
                            }
                        },
                        value);
                }

                if (num_rows != 0) {
                    save_page();
                }
                break;
            }
            }
        }

        return ret;
    }
};

class SimpleColumnarTableView {
private:
    std::shared_ptr<SimpleColumnarTable> base_table;
    std::vector<size_t> column_indices;

public:
    SimpleColumnarTableView(std::shared_ptr<SimpleColumnarTable> table, const std::vector<size_t>& indices)
        : base_table(table), column_indices(indices) {}

    SimpleColumnarTableView(std::shared_ptr<SimpleColumnarTable> table)
        : base_table(table) {
        for (size_t i = 0; i < table->column_count(); ++i) {
            column_indices.push_back(i);
        }
    }

    size_t column_count() const {
        return column_indices.size();
    }

    size_t row_count() const {
        return base_table->row_count();
    }

    const SimpleColumn& get_column(size_t view_idx) const {
        if (view_idx >= column_indices.size()) {
            throw std::out_of_range("Column index out of range");
        }
        return base_table->get_column(column_indices[view_idx]);
    }

    ColumnarTable to_columnar() const {
        return base_table->to_columnar();
    }
};

} // namespace Contest
