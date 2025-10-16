#ifndef BENCHPRESS_CONVERT_H
#define BENCHPRESS_CONVERT_H


#include <cstdint>
#include <type_traits>

template <typename T>
constexpr T byteswap(T value) noexcept {
    static_assert(std::is_integral_v<T>, "Integral type required");
    if constexpr (sizeof(T) == 2) {
        return static_cast<T>(
                ((value & 0x00FFu) << 8) |
                ((value & 0xFF00u) >> 8));
    } else if constexpr (sizeof(T) == 4) {
        return static_cast<T>(
                ((value & 0x000000FFu) << 24) |
                ((value & 0x0000FF00u) << 8)  |
                ((value & 0x00FF0000u) >> 8)  |
                ((value & 0xFF000000u) >> 24));
    } else if constexpr (sizeof(T) == 8) {
        return static_cast<T>(
                ((value & 0x00000000000000FFull) << 56) |
                ((value & 0x000000000000FF00ull) << 40) |
                ((value & 0x0000000000FF0000ull) << 24) |
                ((value & 0x00000000FF000000ull) << 8 ) |
                ((value & 0x000000FF00000000ull) >> 8 ) |
                ((value & 0x0000FF0000000000ull) >> 24) |
                ((value & 0x00FF000000000000ull) >> 40) |
                ((value & 0xFF00000000000000ull) >> 56));
    } else {
        return value; // 1-byte values unchanged
    }
}

template <typename T>
constexpr T host_to_network(T value) noexcept {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return byteswap(value);
#else
    return value;
#endif
}

template <typename T>
constexpr T network_to_host(T value) noexcept {
    return host_to_network(value); // same operation
}


inline const int& atomic_increased_value(std::atomic_int& value, int& result){
    result = ++value;
    return result;
}

#endif //BENCHPRESS_CONVERT_H
