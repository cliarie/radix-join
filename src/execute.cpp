#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <thread>
#include <pthread.h>
#include <cmath>
#include <atomic>
#include <memory>
#include <algorithm>

namespace Contest {

using ExecuteResult = std::vector<std::vector<Data>>;

ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

constexpr size_t NUM_PARTITIONS = 128;
constexpr size_t NUM_CORES = 96;
constexpr float HASH_LOAD_FACTOR = 0.5;

static inline size_t next_power_of_two(size_t x) {
    if (x <= 1) return 1;
    return 1ull << (64 - __builtin_clzll(x - 1));
}

template<typename KeyType>
struct HashUtil {
    static size_t hash(KeyType key) {
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
            return hash(converter.i);
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
};

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
        capacity_ = next_power_of_two(capcacity_);
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
        return HashUtil<KeyType>::hash(key);
    }
    
    size_t size() const { return size_; }
    size_t capacity() const { return capacity_; }
};

template<typename KeyType>
struct PartitionInfo {
    std::vector<size_t> histogram;
    std::vector<size_t> offsets;
    std::vector<KeyType> keys;
    std::vector<size_t> idxs;
    PartitionInfo(size_t num_partitions) : histogram(num_partitions), offsets(num_partitions + 1) {}
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
// Single threaded radix partition over start to end
template<typename KeyType>
PartitionInfo<KeyType> radix_partition(const std::vector<std::vector<Data>>& input, size_t key_col, size_t start, size_t end){
    PartitionInfo<KeyType> result(NUM_PARTITIONS);
    // First pass: compute histogram
    for (size_t i = start; i < end; ++i) {
        auto& record = input[i];
        std::visit([&](const auto& key) {
            using Tk = std::decay_t<decltype(key)>;
            if constexpr (std::is_same_v<Tk, KeyType>) {
                size_t partition_idx = HashUtil<KeyType>::hash(key) % NUM_PARTITIONS;// some hash function to get partition index
                result.histogram[partition_idx]++;
            }
        }, record[key_col]);
    }
    // Resize partitions based on histogram
    results.offsets[0] = 0;
    for (size_t i = 0; i < NUM_PARTITIONS; ++i) {
      result.offsets[i + 1] = result.offsets[i] + result.histogram[i];
    }
    size_t total = results.offsets[NUM_PARTITIONS];
    result.keys.resize(total);
    result.idxs.resize(total);
    
    std::vector<size_t> cursor(result.offsets.begin(), result.offsets.end());
    
    // Second pass: scatter into partitions
    for (size_t i = start; i < end; ++i) {
        auto& record = input[i];
        std::visit([&](const auto& key) {
            using Tk = std::decay_t<decltype(key)>;
            if constexpr (std::is_same_v<Tk, KeyType>) {
                size_t partition_idx = HashUtil<KeyType>::hash(key) % NUM_PARTITIONS;// some hash function to get partition index
                size_t dst = cursor[partition_idx]++;
                result.keys[dst] = key;
                result.idxs[dst] = i;
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

// Merge partitions from all threads
template <typename KeyType>
std::vector<std::vector<std::pair<KeyType, size_t>>> merge_partitions(
    const std::vector<PartitionInfo<KeyType>>& thread_partitions) {
    
    std::vector<std::vector<std::pair<KeyType, size_t>>> merged(NUM_PARTITIONS);
    
    // Calculate total size for each merged partition
    for (size_t p = 0; p < NUM_PARTITIONS; p++) {
        size_t total_size = 0;
        for (const auto& tp : thread_partitions) {
            total_size += tp.partitions[p].size();
        }
        merged[p].reserve(total_size);
    }
    
    // Merge partitions
    for (size_t p = 0; p < NUM_PARTITIONS; p++) {
        for (const auto& tp : thread_partitions) {
            merged[p].insert(merged[p].end(), tp.partitions[p].begin(), tp.partitions[p].end());
        }
    }
    
    return merged;
}

// Build and probe phase, executed in parallel per partition
template <typename KeyType>
void process_partition_join(
    size_t partition_idx,
    const std::vector<std::pair<KeyType, size_t>>& build_partition,
    const std::vector<std::pair<KeyType, size_t>>& probe_partition,
    const std::vector<std::vector<Data>>& build_relation,
    const std::vector<std::vector<Data>>& probe_relation,
    size_t build_offset,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs,
    std::vector<std::vector<Data>>& results,
    std::mutex& results_mutex) {
    
    // Pin thread to core
    pin_thread_to_core(partition_idx % NUM_CORES);
    
    // Skip empty partitions
    if (build_partition.empty() || probe_partition.empty()) {
        return;
    }
    
    // Build phase: create hash table
    LinearProbeHashTable<KeyType> hash_table(build_partition.size());
    for (const auto& [key, row_idx] : build_partition) {
        hash_table.insert(key, row_idx);
    }
    
    // Probe phase: look for matches
    std::vector<std::vector<Data>> partition_results;
    partition_results.reserve(probe_partition.size()); // Estimate
    
    // Process probe keys in batches of 8 for potential SIMD optimization
    constexpr size_t BATCH_SIZE = 8;
    size_t i = 0;
    
    // Process full batches
    for (; i + BATCH_SIZE <= probe_partition.size(); i += BATCH_SIZE) {
        // Prefetch hash table entries for all keys in batch
        for (size_t j = 0; j < BATCH_SIZE; j++) {
            const auto& [key, _] = probe_partition[i + j];
            size_t idx = hash_table.hash_function(key) & (hash_table.capacity() - 1);
            __builtin_prefetch(&hash_table, 0, 0);
        }
        
        // Process each key in batch
        for (size_t j = 0; j < BATCH_SIZE; j++) {
            const auto& [key, probe_idx] = probe_partition[i + j];
            auto matches = hash_table.find(key);
            
            for (size_t build_idx : matches) {
                const auto& build_record = build_relation[build_idx];
                const auto& probe_record = probe_relation[probe_idx];
                
                // Construct output record
                std::vector<Data> new_record;
                new_record.reserve(output_attrs.size());
                
                for (auto [col_idx, _] : output_attrs) {
                    if (col_idx < build_offset) {
                        new_record.emplace_back(build_record[col_idx]);
                    } else {
                        new_record.emplace_back(probe_record[col_idx - build_offset]);
                    }
                }
                
                partition_results.emplace_back(std::move(new_record));
            }
        }
    }
    
    // Process remaining keys
    for (; i < probe_partition.size(); i++) {
        const auto& [key, probe_idx] = probe_partition[i];
        auto matches = hash_table.find(key);
        
        for (size_t build_idx : matches) {
            const auto& build_record = build_relation[build_idx];
            const auto& probe_record = probe_relation[probe_idx];
            
            std::vector<Data> new_record;
            new_record.reserve(output_attrs.size());
            
            for (auto [col_idx, _] : output_attrs) {
                if (col_idx < build_offset) {
                    new_record.emplace_back(build_record[col_idx]);
                } else {
                    new_record.emplace_back(probe_record[col_idx - build_offset]);
                }
            }
            
            partition_results.emplace_back(std::move(new_record));
        }
    }
    
    // Merge results
    {
        std::lock_guard<std::mutex> lock(results_mutex);
        results.insert(results.end(), 
                      std::make_move_iterator(partition_results.begin()),
                      std::make_move_iterator(partition_results.end()));
    }
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

ExecuteResult execute_hash_join(
    const Plan& plan,
    const JoinNode& join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    
    auto left_idx = join.left;
    auto right_idx = join.right;
    auto& left_node = plan.nodes[left_idx];
    auto& right_node = plan.nodes[right_idx];
    auto& left_types = left_node.output_attrs;
    auto& right_types = right_node.output_attrs;
    
    auto left = execute_impl(plan, left_idx);
    auto right = execute_impl(plan, right_idx);
    
    // Determine build and probe relations
    const bool build_left = join.build_left;
    auto& build_relation = build_left ? left : right;
    auto& probe_relation = build_left ? right : left;
    const size_t build_col = build_left ? join.left_attr : join.right_attr;
    const size_t probe_col = build_left ? join.right_attr : join.left_attr;
    const size_t build_relation_size = build_left ? left[0].size() : right[0].size();
    
    // Result container
    std::vector<std::vector<Data>> results;
    std::mutex results_mutex;
    
    // Execute type-specific join
    DataType join_type = build_left ? 
        std::get<1>(left_types[join.left_attr]) : 
        std::get<1>(right_types[join.right_attr]);
    
    switch (join_type) {
        case DataType::INT32: {
            // 1. Partition phase
            auto build_partitions_info = parallel_partition<int32_t>(build_relation, build_col);
            auto probe_partitions_info = parallel_partition<int32_t>(probe_relation, probe_col);
            
            auto build_partitions = merge_partitions(build_partitions_info);
            auto probe_partitions = merge_partitions(probe_partitions_info);
            
            // 2+3. Build and probe phases (per partition)
            std::vector<std::thread> threads;
            for (size_t p = 0; p < NUM_PARTITIONS; p++) {
                threads.emplace_back([&, p]() {
                    process_partition_join<int32_t>(
                        p, build_partitions[p], probe_partitions[p],
                        build_relation, probe_relation, build_relation_size,
                        output_attrs, results, results_mutex);
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
            break;
        }
        case DataType::INT64: {
            // Same pattern as INT32
            auto build_partitions_info = parallel_partition<int64_t>(build_relation, build_col);
            auto probe_partitions_info = parallel_partition<int64_t>(probe_relation, probe_col);
            
            auto build_partitions = merge_partitions(build_partitions_info);
            auto probe_partitions = merge_partitions(probe_partitions_info);
            
            std::vector<std::thread> threads;
            for (size_t p = 0; p < NUM_PARTITIONS; p++) {
                threads.emplace_back([&, p]() {
                    process_partition_join<int64_t>(
                        p, build_partitions[p], probe_partitions[p],
                        build_relation, probe_relation, build_relation_size,
                        output_attrs, results, results_mutex);
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
            break;
        }
        case DataType::FP64: {
            // Same pattern as INT32
            auto build_partitions_info = parallel_partition<double>(build_relation, build_col);
            auto probe_partitions_info = parallel_partition<double>(probe_relation, probe_col);
            
            auto build_partitions = merge_partitions(build_partitions_info);
            auto probe_partitions = merge_partitions(probe_partitions_info);
            
            std::vector<std::thread> threads;
            for (size_t p = 0; p < NUM_PARTITIONS; p++) {
                threads.emplace_back([&, p]() {
                    process_partition_join<double>(
                        p, build_partitions[p], probe_partitions[p],
                        build_relation, probe_relation, build_relation_size,
                        output_attrs, results, results_mutex);
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
            break;
        }
        case DataType::VARCHAR: {
            // Same pattern as INT32
            auto build_partitions_info = parallel_partition<std::string>(build_relation, build_col);
            auto probe_partitions_info = parallel_partition<std::string>(probe_relation, probe_col);
            
            auto build_partitions = merge_partitions(build_partitions_info);
            auto probe_partitions = merge_partitions(probe_partitions_info);
            
            std::vector<std::thread> threads;
            for (size_t p = 0; p < NUM_PARTITIONS; p++) {
                threads.emplace_back([&, p]() {
                    process_partition_join<std::string>(
                        p, build_partitions[p], probe_partitions[p],
                        build_relation, probe_relation, build_relation_size,
                        output_attrs, results, results_mutex);
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
            break;
        }
    }
    
    return results;
}

// ExecuteResult execute_hash_join(const Plan&          plan,
//     const JoinNode&                                  join,
//     const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
//     auto                           left_idx    = join.left;
//     auto                           right_idx   = join.right;
//     auto&                          left_node   = plan.nodes[left_idx];
//     auto&                          right_node  = plan.nodes[right_idx];
//     auto&                          left_types  = left_node.output_attrs;
//     auto&                          right_types = right_node.output_attrs;
//     auto                           left        = execute_impl(plan, left_idx);
//     auto                           right       = execute_impl(plan, right_idx);
//     std::vector<std::vector<Data>> results;
//
//     JoinAlgorithm join_algorithm{.build_left = join.build_left,
//         .left                                = left,
//         .right                               = right,
//         .results                             = results,
//         .left_col                            = join.left_attr,
//         .right_col                           = join.right_attr,
//         .output_attrs                        = output_attrs};
//     if (join.build_left) {
//         switch (std::get<1>(left_types[join.left_attr])) {
//         case DataType::INT32:   join_algorithm.run<int32_t>(); break;
//         case DataType::INT64:   join_algorithm.run<int64_t>(); break;
//         case DataType::FP64:    join_algorithm.run<double>(); break;
//         case DataType::VARCHAR: join_algorithm.run<std::string>(); break;
//         }
//     } else {
//         switch (std::get<1>(right_types[join.right_attr])) {
//         case DataType::INT32:   join_algorithm.run<int32_t>(); break;
//         case DataType::INT64:   join_algorithm.run<int64_t>(); break;
//         case DataType::FP64:    join_algorithm.run<double>(); break;
//         case DataType::VARCHAR: join_algorithm.run<std::string>(); break;
//         }
//     }
//
//     return results;
// }
//
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
