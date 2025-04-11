#pragma once

#include <cstddef>
#include <cstring>
#include <iterator>
#include <string>
#include <variant>
#include <vector>

#include <attribute.h>
#include <plan.h>
#include <statement.h>

// Helper function to check if a bit is set in a bitmap
inline bool get_bitmap(const uint8_t* bitmap, uint16_t idx) {
    auto byte_idx = idx / 8;
    auto bit_idx = idx % 8;
    return bitmap[byte_idx] & (1u << bit_idx);
}

using ColumnValue = Data;

class ColumnIterator {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = ColumnValue;
    using difference_type = std::ptrdiff_t;
    using pointer = const ColumnValue*;
    using reference = const ColumnValue&;

private:
    const Column* column;
    size_t current_page_idx = 0;
    size_t current_row_idx = 0;
    size_t total_row_idx = 0;
    bool is_end = false;

    ColumnValue current_value;

    // For string continuation tracking
    std::string current_long_string;
    bool in_long_string = false;

    // Get the data offset for fixed-length types
    size_t get_data_offset() const {
        switch (column->type) {
            case DataType::INT32: return 4;
            case DataType::INT64:
            case DataType::FP64: return 8;
            default: return 4; // VARCHAR starts at 4
        }
    }

    // Get the bitmap pointer for the current page
    const uint8_t* get_bitmap_ptr(const std::byte* page, uint16_t num_rows) const {
        return reinterpret_cast<const uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
    }

    // Process a long string page
    void process_long_string_page(const std::byte* page) {
        auto num_rows = *reinterpret_cast<const uint16_t*>(page);
        auto num_chars = *reinterpret_cast<const uint16_t*>(page + 2);
        auto* data_begin = reinterpret_cast<const char*>(page + 4);

        if (num_rows == 0xffff) {
            // First page of long string
            current_long_string = std::string(data_begin, num_chars);
            in_long_string = true;
        } else if (num_rows == 0xfffe) {
            // Continuation page of long string
            current_long_string.append(data_begin, num_chars);
        }
    }

    // Fetch the next value and store it in current_value
    void fetch_next() {
        if (is_end || !column || column->pages.empty()) {
            is_end = true;
            current_value = std::monostate{};
            return;
        }

        const std::byte* page = column->pages[current_page_idx]->data;
        uint16_t num_rows = *reinterpret_cast<const uint16_t*>(page);

        // Handle long string special case
        if (num_rows == 0xffff || num_rows == 0xfffe) {
            process_long_string_page(page);
            current_page_idx++;

            // If this is the first page of a long string, we need to return the value
            if (num_rows == 0xffff) {
                // Check if there are more pages for this string
                while (current_page_idx < column->pages.size()) {
                    const std::byte* next_page = column->pages[current_page_idx]->data;
                    uint16_t next_num_rows = *reinterpret_cast<const uint16_t*>(next_page);

                    if (next_num_rows == 0xfffe) {
                        process_long_string_page(next_page);
                        current_page_idx++;
                    } else {
                        break;
                    }
                }

                // Return the complete long string and increment total row count
                std::string result = current_long_string;
                current_long_string.clear();
                in_long_string = false;
                total_row_idx++;
                current_value = result;
                return;
            }

            // If this is a continuation page, we need to get the next page
            fetch_next();
            return;
        }

        // Regular page processing
        const uint8_t* bitmap = get_bitmap_ptr(page, num_rows);

        // Check if we've processed all rows in the current page
        if (current_row_idx >= num_rows) {
            current_page_idx++;
            current_row_idx = 0;

            // Check if we've reached the end
            if (current_page_idx >= column->pages.size()) {
                is_end = true;
                current_value = std::monostate{};
                return;
            }

            fetch_next();
            return;
        }

        // Check if the current row has a value or is NULL
        bool has_value = get_bitmap(bitmap, current_row_idx);

        if (has_value) {
            switch (column->type) {
                case DataType::INT32: {
                    auto* data_begin = reinterpret_cast<const int32_t*>(page + 4);
                    uint16_t data_idx = 0;

                    // Count non-null values before the current row
                    for (uint16_t i = 0; i < current_row_idx; i++) {
                        if (get_bitmap(bitmap, i)) {
                            data_idx++;
                        }
                    }

                    current_value = data_begin[data_idx];
                    break;
                }
                case DataType::INT64: {
                    auto* data_begin = reinterpret_cast<const int64_t*>(page + 8);
                    uint16_t data_idx = 0;

                    for (uint16_t i = 0; i < current_row_idx; i++) {
                        if (get_bitmap(bitmap, i)) {
                            data_idx++;
                        }
                    }

                    current_value = data_begin[data_idx];
                    break;
                }
                case DataType::FP64: {
                    auto* data_begin = reinterpret_cast<const double*>(page + 8);
                    uint16_t data_idx = 0;

                    for (uint16_t i = 0; i < current_row_idx; i++) {
                        if (get_bitmap(bitmap, i)) {
                            data_idx++;
                        }
                    }

                    current_value = data_begin[data_idx];
                    break;
                }
                case DataType::VARCHAR: {
                    auto num_non_null = *reinterpret_cast<const uint16_t*>(page + 2);
                    auto* offset_begin = reinterpret_cast<const uint16_t*>(page + 4);
                    auto* data_begin = reinterpret_cast<const char*>(page + 4 + num_non_null * 2);
                    auto* string_begin = data_begin;

                    uint16_t data_idx = 0;
                    for (uint16_t i = 0; i < current_row_idx; i++) {
                        if (get_bitmap(bitmap, i)) {
                            data_idx++;
                        }
                    }

                    uint16_t prev_offset = (data_idx > 0) ? offset_begin[data_idx - 1] : 0;
                    uint16_t curr_offset = offset_begin[data_idx];

                    current_value = std::string(string_begin + prev_offset, curr_offset - prev_offset);
                    break;
                }
            }
        } else {
            current_value = std::monostate{};
        }

        current_row_idx++;
        total_row_idx++;
    }

public:
    ColumnIterator() : column(nullptr), is_end(true) {}

    explicit ColumnIterator(const Column* col) : column(col), is_end(false) {
        if (column && !column->pages.empty()) {
            fetch_next();
        } else {
            is_end = true;
        }
    }

    ColumnIterator(const Column* col, bool end) : column(col), is_end(end) {
        if (!is_end && column && !column->pages.empty()) {
            fetch_next();
        }
    }

    void reset() {
        if (!column) return;

        current_page_idx = 0;
        current_row_idx = 0;
        total_row_idx = 0;
        in_long_string = false;
        current_long_string.clear();
        is_end = false;

        if (!column->pages.empty()) {
            fetch_next();
        } else {
            is_end = true;
        }
    }

    size_t position() const {
        return total_row_idx;
    }

    reference operator*() const {
        return current_value;
    }

    pointer operator->() const {
        return &current_value;
    }

    ColumnIterator& operator++() {
        fetch_next();
        return *this;
    }

    // Post-increment operator
    ColumnIterator operator++(int) {
        ColumnIterator tmp = *this;
        fetch_next();
        return tmp;
    }

    bool operator==(const ColumnIterator& other) const {
        if (is_end && other.is_end) return true;
        if (is_end || other.is_end) return false;
        return column == other.column &&
               current_page_idx == other.current_page_idx &&
               current_row_idx == other.current_row_idx;
    }

    bool operator!=(const ColumnIterator& other) const {
        return !(*this == other);
    }
};

class ColumnRange {
private:
    const Column* column;

public:
    explicit ColumnRange(const Column* col) : column(col) {}

    ColumnIterator begin() const {
        return ColumnIterator(column);
    }

    ColumnIterator end() const {
        return ColumnIterator(column, true);
    }
};

// Helper function to create a range from a column
inline ColumnRange iterate(const Column& column) {
    return ColumnRange(&column);
}