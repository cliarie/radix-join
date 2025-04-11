#pragma once

#include <array>
#include <cstdint>
#include <cstddef>
#include <functional>

template<class Key, std::size_t NumBits = 1 << 22, std::size_t NumHashes = 3>
class BloomFilter {
    static_assert((NumBits & (NumBits - 1)) == 0, "NumBits must be power-of-two");
    std::array<std::uint64_t, NumBits / 64> bits_{};

    static constexpr std::size_t mask_ = NumBits - 1;

    // 128-bit mix so we can split into k 64-bit hashes cheaply
    static std::uint64_t mix(std::uint64_t x) {
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        return x;
    }
    public:
    void insert(const Key& key) noexcept {
    std::uint64_t h = mix(std::hash<Key>{}(key));
    #pragma unroll
    for (std::size_t i = 0; i < NumHashes; ++i) {
        std::size_t bit = (h >> (i * 16)) & mask_;
        bits_[bit >> 6] |= 1ULL << (bit & 63);
    }
    }
    [[nodiscard]] bool possiblyContains(const Key& key) const noexcept {
    std::uint64_t h = mix(std::hash<Key>{}(key));
    #pragma unroll
    for (std::size_t i = 0; i < NumHashes; ++i) {
        std::size_t bit = (h >> (i * 16)) & mask_;
        if ((bits_[bit >> 6] & (1ULL << (bit & 63))) == 0) return false;
    }
    return true;
    }
};
