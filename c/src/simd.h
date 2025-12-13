
#ifndef FLINTDB_SIMD_H
#define FLINTDB_SIMD_H

// SIMD headers for optimizations
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    #include <arm_neon.h>
    #define SIMD_HAS_NEON 1
#elif defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    #include <emmintrin.h>  // SSE2
    #define SIMD_HAS_SSE2 1
    #if defined(__AVX2__)
        #include <immintrin.h>
        #define SIMD_HAS_AVX2 1
    #endif
#endif

// ============================================================================
// OPTIMIZED MEMORY COPY FUNCTIONS
// ============================================================================

// Optimized memcpy for buffer operations with SIMD
static inline void simd_memcpy(void* restrict dst, const void* restrict src, size_t n) {
    unsigned char* d = (unsigned char*)dst;  // char 대신 unsigned char
    const unsigned char* s = (const unsigned char*)src;  // 🔥 src로 수정!
    
    if (n == 0) return;
    
    // 작은 크기: overlapping 기법
    if (n <= 16) {
        if (n >= 8) {
            uint64_t first, last;
            memcpy(&first, s, 8);
            memcpy(&last, s + n - 8, 8);
            memcpy(d, &first, 8);
            memcpy(d + n - 8, &last, 8);
        } else if (n >= 4) {
            uint32_t first, last;
            memcpy(&first, s, 4);
            memcpy(&last, s + n - 4, 4);
            memcpy(d, &first, 4);
            memcpy(d + n - 4, &last, 4);
        } else {
            // 1-3 바이트
            if (n >= 1) d[0] = s[0];
            if (n >= 2) d[1] = s[1];
            if (n >= 3) d[2] = s[2];
        }
        return;
    }
    
#if defined(SIMD_HAS_AVX2)
    while (n >= 32) {
        _mm256_storeu_si256((__m256i*)d, _mm256_loadu_si256((const __m256i*)s));
        d += 32;
        s += 32;
        n -= 32;
    }
#endif

#if defined(SIMD_HAS_AVX2) || defined(SIMD_HAS_SSE2)
    while (n >= 16) {
        _mm_storeu_si128((__m128i*)d, _mm_loadu_si128((const __m128i*)s));
        d += 16;
        s += 16;
        n -= 16;
    }
#elif defined(SIMD_HAS_NEON)
    while (n >= 16) {
        vst1q_u8((uint8_t*)d, vld1q_u8((const uint8_t*)s));
        d += 16;
        s += 16;
        n -= 16;
    }
#else
    while (n >= 8) {
        uint64_t tmp;
        memcpy(&tmp, s, 8);
        memcpy(d, &tmp, 8);
        d += 8;
        s += 8;
        n -= 8;
    }
#endif
    
    // 남은 0-15 바이트: overlapping
    if (n > 0) {
        if (n >= 8) {
            uint64_t first, last;
            memcpy(&first, s, 8);
            memcpy(&last, s + n - 8, 8);
            memcpy(d, &first, 8);
            memcpy(d + n - 8, &last, 8);
        } else if (n >= 4) {
            uint32_t first, last;
            memcpy(&first, s, 4);
            memcpy(&last, s + n - 4, 4);
            memcpy(d, &first, 4);
            memcpy(d + n - 4, &last, 4);
        } else {
            if (n >= 1) d[0] = s[0];
            if (n >= 2) d[1] = s[1];
            if (n >= 3) d[2] = s[2];
        }
    }
}

// Optimized memory comparison for row/variant data
static inline int simd_memcmp(const void *s1, const void *s2, size_t n) {
    if (n == 0) return 0;
    
#if defined(SIMD_HAS_AVX2)
    // AVX2: 32-byte chunks
    if (n >= 32) {
        const unsigned char *a = (const unsigned char *)s1;
        const unsigned char *b = (const unsigned char *)s2;
        size_t chunks = n / 32;
        
        for (size_t i = 0; i < chunks; i++) {
            __m256i va = _mm256_loadu_si256((const __m256i *)a);
            __m256i vb = _mm256_loadu_si256((const __m256i *)b);
            __m256i vcmp = _mm256_cmpeq_epi8(va, vb);
            unsigned int mask = (unsigned int)_mm256_movemask_epi8(vcmp);
            
            if (mask != 0xFFFFFFFFU) {
                return memcmp(a, b, 32);
            }
            a += 32;
            b += 32;
        }
        
        size_t remaining = n % 32;
        if (remaining > 0) {
            return memcmp(a, b, remaining);
        }
        return 0;
    }
    
#elif defined(SIMD_HAS_SSE2)
    // SSE2: 16-byte chunks
    if (n >= 16) {
        const unsigned char *a = (const unsigned char *)s1;
        const unsigned char *b = (const unsigned char *)s2;
        size_t chunks = n / 16;
        
        for (size_t i = 0; i < chunks; i++) {
            __m128i va = _mm_loadu_si128((const __m128i *)a);
            __m128i vb = _mm_loadu_si128((const __m128i *)b);
            __m128i vcmp = _mm_cmpeq_epi8(va, vb);
            unsigned int mask = (unsigned int)_mm_movemask_epi8(vcmp);
            
            if (mask != 0xFFFFU) {
                return memcmp(a, b, 16);
            }
            a += 16;
            b += 16;
        }
        
        size_t remaining = n % 16;
        if (remaining > 0) {
            return memcmp(a, b, remaining);
        }
        return 0;
    }
    
#elif defined(SIMD_HAS_NEON)
    // NEON: 16-byte chunks
    if (n >= 16) {
        const uint8_t *a = (const uint8_t *)s1;
        const uint8_t *b = (const uint8_t *)s2;
        size_t chunks = n / 16;
        
        for (size_t i = 0; i < chunks; i++) {
            uint8x16_t va = vld1q_u8(a);
            uint8x16_t vb = vld1q_u8(b);
            uint8x16_t vcmp = vceqq_u8(va, vb);
            uint64x2_t vcmp64 = vreinterpretq_u64_u8(vcmp);
            uint64_t low = vgetq_lane_u64(vcmp64, 0);
            uint64_t high = vgetq_lane_u64(vcmp64, 1);
            
            if (low != 0xFFFFFFFFFFFFFFFFULL || high != 0xFFFFFFFFFFFFFFFFULL) {
                return memcmp(a, b, 16);
            }
            a += 16;
            b += 16;
        }
        
        size_t remaining = n % 16;
        if (remaining > 0) {
            return memcmp(a, b, remaining);
        }
        return 0;
    }
#endif
    
    // Fallback
    return memcmp(s1, s2, n);
}


#ifndef htonll

#ifdef linux
// https://man7.org/linux/man-pages/man3/endian.3.html
#include <endian.h>
#endif

#if __BIG_ENDIAN__ && REG_DWORD == REG_DWORD_BIG_ENDIAN
# define htonll(x) (x)
# define ntohll(x) (x)
#else
# define htonll(x) (((uint64_t)htonl((x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))
# define ntohll(x) (((uint64_t)ntohl((x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))
#endif

#endif // htonll


// ============================================================================
// OPTIMIZED BYTE ORDER CONVERSION
// ============================================================================

// Fast byte swap for 16-bit values
static inline uint16_t bswap16_opt(uint16_t x) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap16(x);
#elif defined(_MSC_VER)
    return _byteswap_ushort(x);
#else
    return (x >> 8) | (x << 8);
#endif
}

// Fast byte swap for 32-bit values
static inline uint32_t bswap32_opt(uint32_t x) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap32(x);
#elif defined(_MSC_VER)
    return _byteswap_ulong(x);
#else
    return ((x >> 24) & 0x000000FF) |
           ((x >> 8)  & 0x0000FF00) |
           ((x << 8)  & 0x00FF0000) |
           ((x << 24) & 0xFF000000);
#endif
}

// Fast byte swap for 64-bit values
static inline uint64_t bswap64_opt(uint64_t x) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap64(x);
#elif defined(_MSC_VER)
    return _byteswap_uint64(x);
#else
    return ((x >> 56) & 0x00000000000000FFULL) |
           ((x >> 40) & 0x000000000000FF00ULL) |
           ((x >> 24) & 0x0000000000FF0000ULL) |
           ((x >> 8)  & 0x00000000FF000000ULL) |
           ((x << 8)  & 0x000000FF00000000ULL) |
           ((x << 24) & 0x0000FF0000000000ULL) |
           ((x << 40) & 0x00FF000000000000ULL) |
           ((x << 56) & 0xFF00000000000000ULL);
#endif
}

#endif // FLINTDB_SIMD_H