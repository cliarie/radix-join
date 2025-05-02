#pragma once
#include <vector>
#include <cstdint>
#include <cmath>

template <class Key>
class BloomFilter {
    std::vector<std::uint64_t> bits_;
    std::size_t                num_bits_   = 0;
    std::size_t                num_hashes_ = 0;
    std::size_t                mask_       = 0;

    // 128-bit mix
    static std::uint64_t mix(std::uint64_t x) {
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        return x;
     }

  public:
    /// initialize for up to `n` inserts and target false-positive rate `p`
    void init(std::size_t n, double p) {
        // optimal #bits m = –(n ln p) / (ln2)^2
        double m_d = -double(n) * std::log(p) / (std::log(2)*std::log(2));
        // round up to next power of two for fast mod:
        std::size_t m = std::size_t(std::ceil(m_d));
        std::size_t pow2 = 1u << (32 - __builtin_clz(m-1));  // next power-of-two
        num_bits_   = pow2;
        mask_       = num_bits_ - 1;

        // optimal #hashes k = (m/n) ln2
        num_hashes_ = std::max<std::size_t>(1,
            std::size_t(std::round((double(num_bits_)/n) * std::log(2)))
        );

        // allocate storage
        bits_.assign((num_bits_ + 63)/64, 0);
    }

    void insert(const Key& key) noexcept {
        auto h = mix(std::hash<Key>{}(key));
        for (std::size_t i = 0; i < num_hashes_; ++i) {
            auto bit = (h >> (i * 16)) & mask_;
            bits_[bit >> 6] |= 1ULL << (bit & 63);
        }
    }

    [[nodiscard]] bool possiblyContains(const Key& key) const noexcept {
        auto h = mix(std::hash<Key>{}(key));
        for (std::size_t i = 0; i < num_hashes_; ++i) {
            auto bit = (h >> (i * 16)) & mask_;
            if ((bits_[bit >> 6] & (1ULL << (bit & 63))) == 0) return false;
        }
        return true;
    }
};