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

#ifdef STORAGE_DIO_USE_BUFFER_POOL
    struct buffer_pool_safe *pool; // Direct I/O buffer pool
#endif
    void *priv; // storage-specific private data

    // Ownership/lifetime
    // 0: caller owns storage and must close/free it
    // 1: WAL owns this wrapper; it will be closed/freed by wal_close()
    u8 managed_by_wal;

    struct buffer *h; // mmaped header
    int block_bytes; // including header
    int increment; // file size increment step
    int mmap_bytes; // mmap size (for mmap, memory, dio)
    char *clean; // zeroed block buffer

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

// FlintDB on-disk file header size. Keep this stable for compatibility.
// NOTE: This is NOT necessarily the OS VM page size (which can be 4KB or 16KB depending on platform).
#define FLINTDB_FILE_HEADER_BYTES 16384
// Legacy name kept for compatibility within the codebase; represents file header granularity.
#define OS_PAGE_SIZE FLINTDB_FILE_HEADER_BYTES
#define HEADER_BYTES FLINTDB_FILE_HEADER_BYTES
#define BLOCK_HEADER_BYTES (1 + 1 + 2 + 4 + 8) // 16 

#define TYPE_MMAP "MMAP"
#define TYPE_DIO "DIO" // O_DIRECT or F_NOCACHE (Experimental Feature)

#define TYPE_Z "Z"
#define TYPE_LZ4 "LZ4"
#define TYPE_ZSTD "ZSTD"
#define TYPE_SNAPPY "SNAPPY"

#define TYPE_MEMORY "MEMORY"
#define TYPE_DEFAULT TYPE_MMAP

#endif // FLINTDB_STORAGE_H