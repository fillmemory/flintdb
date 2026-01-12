//
//
//
#ifndef FLINTDB_TYPE_H
#define FLINTDB_TYPE_H

#include <stdint.h>
#ifndef _WIN32
typedef int8_t i8;
typedef uint8_t u8;
typedef int16_t i16;
typedef uint16_t u16;
typedef int32_t i32;
typedef uint32_t u32;
typedef int64_t i64;
typedef uint64_t u64;
typedef double f64;
typedef uintptr_t var;
#else
typedef int8_t i8;
typedef uint8_t u8;
typedef int16_t i16;
typedef uint16_t u16;
typedef int32_t i32;
typedef uint32_t u32;
typedef int64_t i64;
typedef uint64_t u64;
typedef double f64;
typedef uintptr_t var;
#endif

typedef u64 keytype;
typedef u64 valtype;
#define VALUETYPE_NULL ((valtype) - 1LL)
#define NOT_FOUND -1L
#endif // FLINTDB_TYPE_H
