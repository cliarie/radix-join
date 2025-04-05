#pragma once

// ==============================================
// Hardware topology for AMD Ryzen Threadripper PRO 7995WX (96-Core / 192-Thread)
// ==============================================

// Architecture from `uname -srm`.
#define SPC__X86_64

// CPU identification (from /proc/cpuinfo).
#define SPC__CPU_NAME "AMD Ryzen Threadripper PRO 7995WX"

// Physical/logical processors
#define SPC__CORE_COUNT                         96
#define SPC__THREAD_COUNT                       192

// NUMA configuration
#define SPC__NUMA_NODE_COUNT                    1
#define SPC__NUMA_NODES_ACTIVE_IN_BENCHMARK     1

// Main memory per NUMA node (MB).
// from `free` showing total Mem: 515214 MB
#define SPC__NUMA_NODE_DRAM_MB                  515214

// OS and kernel
#define SPC__OS                                 "Ubuntu 24.04.1 LTS"
#define SPC__KERNEL                             "Linux 6.8.0-45-generic x86_64"

// ==============================================
// Cache hierarchy (per-instance sizes)
// ==============================================

// L1 Instruction Cache: 32 KiB, 8-way, 64 B lines
#define SPC__LEVEL1_ICACHE_SIZE                 32768
#define SPC__LEVEL1_ICACHE_ASSOC                8
#define SPC__LEVEL1_ICACHE_LINESIZE             64

// L1 Data Cache: 32 KiB, 8-way, 64 B lines
#define SPC__LEVEL1_DCACHE_SIZE                 32768
#define SPC__LEVEL1_DCACHE_ASSOC                8
#define SPC__LEVEL1_DCACHE_LINESIZE             64

// L2 Cache: 1 MiB, 8-way, 64 B lines (per core)
#define SPC__LEVEL2_CACHE_SIZE                  1048576
#define SPC__LEVEL2_CACHE_ASSOC                 8
#define SPC__LEVEL2_CACHE_LINESIZE              64

// L3 Cache: 32 MiB, 16-way, 64 B lines (per CCD slice)
#define SPC__LEVEL3_CACHE_SIZE                  33554432
#define SPC__LEVEL3_CACHE_ASSOC                 16
#define SPC__LEVEL3_CACHE_LINESIZE              64

// No L4 cache
#define SPC__LEVEL4_CACHE_SIZE                  0
#define SPC__LEVEL4_CACHE_ASSOC                 0
#define SPC__LEVEL4_CACHE_LINESIZE              0

