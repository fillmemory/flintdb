#ifndef FLINTDB_STORAGE_H
#define FLINTDB_STORAGE_H

#include <limits.h>
#include "flintdb.h"
#include "types.h"
#include "buffer.h"
#include "hashmap.h"
#include "runtime.h"


struct storage_opts {
    char file[PATH_MAX];
    enum flintdb_open_mode mode;
    int block_bytes;
    int extra_header_bytes;
    int compact;
    int increment;
    char type[16];
    char compress[16];
};

struct storage {
    struct storage_opts opts;
    int fd;
    i64 count;
    struct hashmap *cache;  // buffer cache for mmap/memory storage

    // Ownership/lifetime
    // 0: caller owns storage and must close/free it
    // 1: WAL owns this wrapper; it will be closed/freed by wal_close()
    u8 managed_by_wal;

    struct buffer *h; // mmaped header
    int block_bytes; // including header
    int increment; // file size increment step
    int mmap_bytes; // mmap size
    int mem_bytes;  // memory buffer size (for memory storage)
    char *clean; // zeroed block buffer

    // Direct I/O scratch buffers (allocated only for TYPE_DIO)
    void *dio_scratch; // aligned, size=block_bytes
    void *dio_chunk;   // aligned, size=mmap_bytes (chunk init)
    u32 dio_chunk_bytes;

    // Cache the most recently inflated chunk index to avoid repeated probe syscalls.
    i64 dio_last_inflated_chunk;

    i64 free; // The front of deleted blocks (allocatable)
    int dirty; // dirty write counter

	void (*close)(struct storage *me);

	i64 (*count_get)(struct storage *me);
	i64 (*bytes_get)(struct storage *me);

	struct buffer * (*read)(struct storage *me, i64 offset, char **e);
	i64 (*write)(struct storage *me, struct buffer *in, char **e);
	i64 (*write_at)(struct storage *me, i64 offset, struct buffer *in, char **e);
    i32 (*delete)(struct storage *me, i64 offset, char **e);

    void (*transaction)(struct storage *me, i64 id, char **e); // wal transaction support

    struct buffer * (*mmap)(struct storage *me, i64 offset, i32 length, char **e);
    struct buffer * (*head)(struct storage *me, i64 offset, i32 length, char **e);
};


int storage_open(struct storage * s, struct storage_opts opts, char **e);

#define OS_PAGE_SIZE 16384 // 16KB
#define HEADER_BYTES OS_PAGE_SIZE 
#define BLOCK_HEADER_BYTES (1 + 1 + 2 + 4 + 8) // 16 

#define TYPE_MMAP "MMAP"
#define TYPE_DIO "DIO" // O_DIRECT or F_NOCACHE

#define TYPE_Z "Z"
#define TYPE_LZ4 "LZ4"
#define TYPE_ZSTD "ZSTD"
#define TYPE_SNAPPY "SNAPPY"

#define TYPE_MEMORY "MEMORY"
#define TYPE_DEFAULT TYPE_MMAP

#endif // FLINTDB_STORAGE_H