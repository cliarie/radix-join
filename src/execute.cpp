#include <hardware.h>
#include <plan.h>
#include <table.h>

namespace Contest {

using ExecuteResult = std::vector<std::vector<Data>>;

ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

constexpr size_t NUM_PARTITIONS = 128;
constexpr size_t NUM_CORES = 96;
constexpr size_t HASH_LOAD_FACTOR = 0.5;

template<typename KeyType>
class LinearProbeHashTable {
private:
    struct Entry {
        KeyType key;
        size_t value_idx;
        bool occupied;
        
        Entry() : occupied(false) {}
    };
    
    std::vector<Entry> table;
    size_t size_;
    size_t capacity_;
    
public:
    LinearProbeHashTable(size_t estimated_size) {
        // Size to maintain load factor
        capacity_ = static_cast<size_t>(estimated_size / HASH_LOAD_FACTOR);
        // Round up to next power of 2 for fast modulo with mask
        capacity_ = std::pow(2, std::ceil(std::log2(capacity_)));
        table.resize(capacity_);
        size_ = 0;
    }
    
    void insert(KeyType key, size_t value_idx) {
        size_t idx = hash_function(key) & (capacity_ - 1);
        
        while (table[idx].occupied) {
            // If key already exists, we don't insert duplicate entries for hash join
            if (table[idx].key == key) {
                return;
            }
            idx = (idx + 1) & (capacity_ - 1); // Linear probe with wrap-around
        }
        
        table[idx].key = key;
        table[idx].value_idx = value_idx;
        table[idx].occupied = true;
        size_++;
    }
    
    std::vector<size_t> find(KeyType key) {
        std::vector<size_t> matches;
        size_t idx = hash_function(key) & (capacity_ - 1);
        
        // Prefetch next potential entry
        __builtin_prefetch(&table[(idx + 1) & (capacity_ - 1)]);
        
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                matches.push_back(table[idx].value_idx);
            }
            
            idx = (idx + 1) & (capacity_ - 1);
            __builtin_prefetch(&table[(idx + 1) & (capacity_ - 1)]);
            
            // If we've wrapped all the way around, stop
            if (idx == (hash_function(key) & (capacity_ - 1))) {
                break;
            }
        }
        
        return matches;
    }
    
    size_t hash_function(KeyType key) {
        if constexpr (std::is_same_v<KeyType, int32_t> || 
                     std::is_same_v<KeyType, int64_t>) {
            // MurmurHash2 integer finalizer
            uint64_t k = static_cast<uint64_t>(key);
            k ^= k >> 33;
            k *= 0xff51afd7ed558ccdULL;
            k ^= k >> 33;
            k *= 0xc4ceb9fe1a85ec53ULL;
            k ^= k >> 33;
            return static_cast<size_t>(k);
        } else if constexpr (std::is_same_v<KeyType, double>) {
            // Convert double to bits and hash as integer
            union { double d; uint64_t i; } converter;
            converter.d = key;
            return hash_function(converter.i);
        } else if constexpr (std::is_same_v<KeyType, std::string>) {
            // FNV-1a hash for strings
            size_t hash = 14695981039346656037ULL; // FNV offset basis
            for (char c : key) {
                hash ^= static_cast<size_t>(c);
                hash *= 1099511628211ULL; // FNV prime
            }
            return hash;
        }
    }
    
    size_t size() const { return size_; }
    size_t capacity() const { return capacity_; }
};

template<typename KeyType>
struct PartitionInfo {
    std::vector<std::vector<std::pair<KeyType, size_t>>> partitions;
    std::vector<size_t> histogram;
    PartitionInfo(size_t num_partitions) : partitions(num_partitions), histogram(num_partitions, 0) {}
};

// For thread affinity
void pin_thread_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    
    pthread_t current_thread = pthread_self();
    pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

// Partition Phase: two-pass radix partitioning -> first pass to get histogram, second pass to scatter into partitions
template<typename KeyType>
PartitionInfo<KeyType> radix_partition(size_t start, size_t end, const std::vector<std::vector<Data>>& input, size_t key_col){
    PartitionInfo<KeyType> result(NUM_PARTITIONS);
    // First pass: compute histogram
    for (size_t i = start; i < end; ++i) {
        auto& record = input[i];
        std::visit([&](const auto& key) {
            using Tk = std::decay_t<decltype(key)>;
            if constexpr (std::is_same_v<Tk, KeyType>) {
                size_t partition_idx = LinearProbeHashTable<KeyType>().hash_function(key) % NUM_PARTITIONS;// some hash function to get partition index
                result.histogram[partition_idx]++;
            }
        }, record[key_col]);
    }
    // Resize partitions based on histogram
    for (size_t i = 0; i < NUM_PARTITIONS; ++i) {}
        result.partitions[i].resize(result.histogram[i]);
    }
    // Second pass: scatter into partitions
    for (size_t i = start; i < end; ++i) {}
        auto& record = input[i];
        std::visit([&](const auto& key) {
            using Tk = std::decay_t<decltype(key)>;
            if constexpr (std::is_same_v<Tk, KeyType>) {
                size_t partition_idx = LinearProbeHashTable<KeyType>().hash_function(key) % NUM_PARTITIONS;// some hash function to get partition index
                result.partitions[partition_idx].emplace_back(key, i);
            }
        }, record[key_col]);
    }
    return result;
}

// Process each partition in parallel 
template <typename KeyType>
std::vector<PartitionInfo<KeyType>> parallel_partition(
    const std::vector<std::vector<Data>>& input_relation,
    size_t key_col) {
    
    size_t num_threads = std::min(NUM_CORES, input_relation.size());
    if (num_threads == 0) return {};
    
    std::vector<std::thread> threads;
    std::vector<PartitionInfo<KeyType>> thread_partitions(num_threads, PartitionInfo<KeyType>(NUM_PARTITIONS));
    
    size_t chunk_size = (input_relation.size() + num_threads - 1) / num_threads;
    
    for (size_t i = 0; i < num_threads; i++) {
        size_t start_idx = i * chunk_size;
        size_t end_idx = std::min(start_idx + chunk_size, input_relation.size());
        
        threads.emplace_back([&, i, start_idx, end_idx]() {
            pin_thread_to_core(i);
            thread_partitions[i] = radix_partition<KeyType>(input_relation, key_col, start_idx, end_idx);
        });
    }
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    return thread_partitions;
}

struct JoinAlgorithm {
    bool                                             build_left;
    ExecuteResult&                                   left;
    ExecuteResult&                                   right;
    ExecuteResult&                                   results;
    size_t                                           left_col, right_col;
    const std::vector<std::tuple<size_t, DataType>>& output_attrs;

    template <class T>
    auto run() {
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
    auto                           left        = execute_impl(plan, left_idx);
    auto                           right       = execute_impl(plan, right_idx);
    std::vector<std::vector<Data>> results;

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
