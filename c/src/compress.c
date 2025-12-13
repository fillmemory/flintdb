#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <zlib.h>
// #include <lz4.h>
// #include <zstd.h>
// #include <snappy-c.h>
#include "flintdb.h"

#define Z_DEF_MEM_LEVEL 8
#define FORMAT_Z       1
// #define FORMAT_LZ4     2
// #define FORMAT_ZSTD    3
// #define FORMAT_SNAPPY  4


i32 compress_z(const char *in, const i32 len, char *out, i32 out_len, char **e) {
    z_stream deflator;
    deflator.zalloc = Z_NULL;
    deflator.zfree = Z_NULL;
    deflator.opaque = Z_NULL;

    deflator.avail_in = (uInt)len;
    deflator.next_in = (Bytef *)in;
    deflator.avail_out = (uInt)out_len;
    deflator.next_out = (Bytef *)out;

    int nowrap = 1;
    int windowBits = nowrap ? -MAX_WBITS : MAX_WBITS;
    int ok = deflateInit2(&deflator, Z_DEFAULT_COMPRESSION, Z_DEFLATED, windowBits, Z_DEF_MEM_LEVEL, Z_DEFAULT_STRATEGY);
    // int ok = deflateInit(&deflator, Z_DEFAULT_COMPRESSION);
    assert(ok == Z_OK);
    if (Z_OK == ok) {
        deflate(&deflator, Z_FINISH);
        deflateEnd(&deflator);
    }
    return deflator.total_out;
}

i32 decompress_z(const char *in, const i32 len, char *out, i32 out_len, char **e) {
    z_stream inflator;
    inflator.zalloc = Z_NULL;
    inflator.zfree = Z_NULL;
    inflator.opaque = Z_NULL;

    inflator.avail_in = (uInt)len;
    inflator.next_in = (Bytef *)in;
    inflator.avail_out = (uInt)out_len;
    inflator.next_out = (Bytef *)out;

    int nowrap = 1;
    int windowBits = nowrap ? -MAX_WBITS : MAX_WBITS;
    int ok = inflateInit2(&inflator, windowBits);
    assert(ok == Z_OK);
    if (Z_OK == ok) {
        inflate(&inflator, Z_FINISH);
        inflateEnd(&inflator);
    }
    return inflator.total_out;
}

// i32 compress_lz4(const char *in, const i32 len, char *out, i32 out_len, char **e) {
//     return LZ4_compress_default(in, out, len, out_len);
// }

// i32 decompress_lz4(const char *in, const i32 len, char *out, i32 out_len, char **e) {
//     return LZ4_decompress_safe(in, out, len, out_len);
// }

// i32 compress_zstd(const char *in, const i32 len, char *out, i32 out_len, char **e) {
//     // compression level 1~22, default : 3
//     return ZSTD_compress(out, out_len, in, len, 3);
// }

// i32 decompress_zstd(const char *in, const i32 len, char *out, i32 out_len, char **e) {
//     return ZSTD_decompress(out, out_len, in, len);
// }

// // brew install snappy
// i32 compress_snappy(const char *in, const i32 len, char *out, i32 out_len, char **e) {
//     size_t l = out_len;
//     snappy_status ok = snappy_compress(in, len, out, &l);
//     if (ok == SNAPPY_OK) {
//         return l;
//     }
//     return 0;
// }

// i32 decompress_snappy(const char *in, const i32 len, char *out, i32 out_len, char **e) {
//     size_t l = out_len;
//     snappy_status ok = snappy_uncompress(in, len, out, &l);
//     if (ok == SNAPPY_OK) {
//         return l;
//     }
//     return 0;
// }

// Encode message to compress internally
i32 stream_compress(u8 format, const char *in, const i32 len, char *out, i32 out_len, char **e) {
	switch(format & 0x7F) {
	// case FORMAT_SNAPPY:
	// 	return compress_snappy(in, len, out, out_len, e);
	// case FORMAT_LZ4:
	// 	return compress_lz4(in, len, out, out_len, e);
	// case FORMAT_ZSTD:
	// 	return compress_zstd(in, len, out, out_len, e);
	case FORMAT_Z:
		return compress_z(in, len, out, out_len, e);
	}
	memcpy(out, in, len);
    return len;
}

// Decode compressed message  
i32 stream_decompress(u8 format, const char *in, const i32 len, char *out, i32 out_len, char **e) {
	switch(format & 0x7F) {
	// case FORMAT_SNAPPY:
	// 	return decompress_snappy(in, len, out, out_len, e);
	// case FORMAT_LZ4:
	// 	return decompress_lz4(in, len, out, out_len, e);
	// case FORMAT_ZSTD:
	// 	return decompress_zstd(in, len, out, out_len, e);
	case FORMAT_Z:
		return decompress_z(in, len, out, out_len, e);
	}
	memcpy(out, in, len);
    return len;
}
