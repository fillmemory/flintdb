#include "wal.h"
#include "allocator.h"
#include "runtime.h"
#include "buffer.h"
#include "hashmap.h"
#include "list.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <assert.h>
#include <zlib.h>
#ifndef _WIN32
#include <sys/mman.h>
#endif

#include <limits.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#define HEADER_SIZE 4096  // Match filesystem block size for atomic writes
#define DEFAULT_BATCH_SIZE 10000
#define BATCH_BUFFER_SIZE (4 * 1024 * 1024) // 4MB batch buffer
#define DEFAULT_COMPRESSION_THRESHOLD 8192 // Compress if data > 8KB bytes
#define FLAG_COMPRESSED 0x01
#define FLAG_METADATA_ONLY 0x02

// Get environment variable as integer with default value
static int get_env_int(const char *name, int default_value) {
    const char *val = getenv(name);
    if (val) {
        int result = atoi(val);
        return result > 0 ? result : default_value;
    }
    return default_value;
}

// WAL implementation structure
struct wal_impl {
    int fd;                     // WAL file descriptor
    char path[PATH_MAX];        // WAL file path
    i64 max_size;              // Maximum WAL file size
    i64 transaction_id;        // Current transaction ID
    i64 transaction_count;     // Total transaction count
    i64 committed_offset;      // Committed offset
    i64 checkpoint_offset;     // Checkpoint offset
    i64 current_position;      // Current write position
    i32 total_count;           // Total transaction count
    i32 processed_count;       // Processed transaction count
    int auto_truncate;         // Auto truncate mode (1 for TRUNCATE, 0 for LOG)
    i64 checkpoint_interval;   // Checkpoint every N transactions
    
    // Batch logging
    char *batch_buffer;        // Batch buffer
    i32 batch_size;            // Current batch size
    i32 batch_count;           // Batch record count
    i32 batch_size_limit;      // Batch size limit (configurable)
    i32 compression_threshold; // Compression threshold (configurable)
    
    // Multiple storages management
    struct hashmap *storages;  // Map of file -> wal_storage
};

// Storage wrapper structure
struct wal_storage {
    struct storage base;        // Base storage structure (must be first!)
    struct storage *origin;     // Original storage
    struct wal_impl *logger;    // WAL logger
    int identifier;             // File identifier
    i64 transaction;            // Current transaction ID
    int (*callback)(const void *obj, i64 offset); // Cache synchronization callback
    const void *callback_obj;   // Callback object context
};

// A WAL implementation that does nothing

static i64 wal_none_op(struct wal *me, char **e) {
    return 1; // Always return 1 for no-transaction WAL
}

static i64 wal_none_id(struct wal *me, i64 id, char **e) {
    return 1;
}   

static void wal_none_close(struct wal *me) {
    TRACE("Closing WAL_NONE");
}

struct wal WAL_NONE = {
    .close = wal_none_close,
    .begin = wal_none_op,
    .commit = wal_none_id,
    .recover = wal_none_op,
    .rollback = wal_none_id,
    .checkpoint = wal_none_op,
};

//
// WAL implementation
//

// Forward declarations
static i64 wal_checkpoint(struct wal *me, char **e);
static void wal_log(struct wal_impl *impl, u8 operation, i64 transaction_id, i32 file_id, i64 page_offset, const char *page_data, i32 data_size, int metadata_only);
static void wal_flush_batch(struct wal_impl *impl);
static void wal_flush_header(struct wal_impl *impl);
static char* wal_compress_data(const char *data, i32 size, i32 *compressed_size);

static i64 wal_begin(struct wal *me, char **e) {
    struct wal_impl *impl = me->impl;
    return ++impl->transaction_id;
}

static i64 wal_commit(struct wal *me, i64 id, char **e) {
    struct wal_impl *impl = me->impl;
    
    // Log commit record
    wal_log(impl, OP_COMMIT, id, 0, 0, NULL, 0, 0);
    impl->committed_offset = impl->current_position;
    impl->total_count++;
    impl->transaction_count++;
    
    // // Debug: Print WAL status every 10000 transactions
    // if (impl->transaction_count % 10000 == 0) {
    //     DEBUG("WAL: %lld transactions, position: %lld bytes, compression: %s", 
    //          impl->transaction_count, impl->current_position, 
    //          impl->enable_compression ? "ON" : "OFF");
    // }
    
    // Auto checkpoint every N transactions
    if (impl->auto_truncate && impl->transaction_count >= impl->checkpoint_interval) {
        wal_checkpoint(me, e);
        impl->transaction_count = 0;
    }
    return 0;
}

static i64 wal_rollback(struct wal *me, i64 id, char **e) {
    struct wal_impl *impl = me->impl;
    
    // Log rollback record
    wal_log(impl, OP_ROLLBACK, id, 0, 0, NULL, 0, 0);
    impl->total_count++;
    
    return 0;
}

static i64 wal_recover(struct wal *me, char **e) {
    // TODO: Implement WAL recovery logic
    return 0;
}

static void wal_flush_header(struct wal_impl *impl) {
    if (impl->fd <= 0) return;
    
    char header[HEADER_SIZE];
    memset(header, 0, HEADER_SIZE);
    
    // Write header fields
    *(i32*)(header + 0) = 0x57414C21;  // Magic 'WAL!'
    *(i16*)(header + 4) = 1;           // Version
    *(i16*)(header + 6) = HEADER_SIZE; // Header size
    *(i64*)(header + 8) = 0;           // Timestamp (placeholder)
    *(i64*)(header + 16) = impl->transaction_id;
    *(i64*)(header + 24) = impl->committed_offset;
    *(i64*)(header + 32) = impl->checkpoint_offset;
    *(i32*)(header + 40) = impl->total_count;
    *(i32*)(header + 44) = impl->processed_count;
    
    lseek(impl->fd, 0, SEEK_SET);
    write(impl->fd, header, HEADER_SIZE);
}

static void wal_flush_batch(struct wal_impl *impl) {
    if (impl->batch_count == 0 || impl->fd <= 0) return;
    
    lseek(impl->fd, impl->current_position, SEEK_SET);
    ssize_t written = write(impl->fd, impl->batch_buffer, impl->batch_size);
    if (written > 0) {
        impl->current_position += written;
    }
    
    // Sync after batch flush for durability
    // Platform-specific sync: macOS needs F_FULLFSYNC, Linux/MinGW use fdatasync
    #ifdef __APPLE__
        fcntl(impl->fd, F_FULLFSYNC);
    #else
        // Linux/Unix/MinGW: fdatasync is faster than fsync (doesn't update metadata)
        fdatasync(impl->fd);
    #endif
    
    impl->batch_size = 0;
    impl->batch_count = 0;
}

static void wal_log(struct wal_impl *impl, u8 operation, i64 transaction_id, i32 file_id, i64 page_offset, const char *page_data, i32 data_size, int metadata_only) {
    if (impl->fd <= 0) return;
    
    u8 flags = 0;
    char *compressed_data = NULL;
    i32 compressed_size = 0;
    i32 original_size = data_size;
    i32 final_size = 0;
    
    if (metadata_only) {
        flags |= FLAG_METADATA_ONLY;
    } else if (page_data && data_size > 0) {
        // Always try compression for data larger than threshold
        if (data_size > impl->compression_threshold) {
            compressed_data = wal_compress_data(page_data, data_size, &compressed_size);
            if (compressed_data && compressed_size < data_size * 0.9) { // Only use if compression saves >10%
                flags |= FLAG_COMPRESSED;
                final_size = compressed_size;
            } else {
                // Compression didn't help, use original
                if (compressed_data) FREE(compressed_data);
                compressed_data = NULL;
                final_size = data_size;
            }
        } else {
            final_size = data_size;
        }
    }
    
    // Calculate record size: operation(1) + txid(8) + checksum(2) + fileid(4) + offset(8) + flags(1) + size(4)
    i32 record_size = 1 + 8 + 2 + 4 + 8 + 1 + 4;
    if ((flags & FLAG_COMPRESSED) != 0) {
        record_size += 4 + final_size; // compressed_size(4) + compressed_data
    } else if ((flags & FLAG_METADATA_ONLY) == 0 && final_size > 0) {
        record_size += final_size; // original data
    }
    
    // Flush batch if not enough space
    if (impl->batch_size + record_size > BATCH_BUFFER_SIZE) {
        wal_flush_batch(impl);
    }
    
    char *buf = impl->batch_buffer + impl->batch_size;
    i16 checksum = 0; // Placeholder
    
    // Write record to batch buffer
    *(u8*)(buf + 0) = operation;
    *(i64*)(buf + 1) = transaction_id;
    *(i16*)(buf + 9) = checksum;
    *(i32*)(buf + 11) = file_id;
    *(i64*)(buf + 15) = page_offset;
    *(u8*)(buf + 23) = flags;
    *(i32*)(buf + 24) = original_size;
    
    int pos = 28;
    if ((flags & FLAG_COMPRESSED) != 0) {
        // Write compressed data
        *(i32*)(buf + pos) = compressed_size;
        pos += 4;
        memcpy(buf + pos, compressed_data, compressed_size);
        FREE(compressed_data);
    } else if ((flags & FLAG_METADATA_ONLY) == 0 && page_data && final_size > 0) {
        // Write original data
        memcpy(buf + pos, page_data, final_size);
    }
    
    impl->batch_size += record_size;
    impl->batch_count++;
    
    // Flush batch if reached batch count limit
    if (impl->batch_count >= impl->batch_size_limit) {
        wal_flush_batch(impl);
    }
}

static char* wal_compress_data(const char *data, i32 size, i32 *compressed_size) {
    // Allocate worst-case size for compression
    uLong max_compressed = compressBound(size);
    char *compressed = CALLOC(1, max_compressed);
    if (!compressed) return NULL;
    
    z_stream stream = {0};
    stream.next_in = (Bytef*)data;
    stream.avail_in = size;
    stream.next_out = (Bytef*)compressed;
    stream.avail_out = max_compressed;
    
    // Use deflateInit2 with negative windowBits for raw deflate (no zlib header)
    if (deflateInit2(&stream, Z_BEST_COMPRESSION, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
        FREE(compressed);
        return NULL;
    }
    
    if (deflate(&stream, Z_FINISH) != Z_STREAM_END) {
        deflateEnd(&stream);
        FREE(compressed);
        return NULL;
    }
    
    *compressed_size = stream.total_out;
    deflateEnd(&stream);
    
    return compressed;
}

static i64 wal_checkpoint(struct wal *me, char **e) {
    struct wal_impl *impl = me->impl;
    
    // Flush any pending batch
    wal_flush_batch(impl);
    wal_log(impl, OP_CHECKPOINT, impl->transaction_id, 0, 0, NULL, 0, 0);
    wal_flush_batch(impl);
    
    impl->checkpoint_offset = impl->current_position;
    impl->total_count++;
    wal_flush_header(impl);
    
    // Only truncate if checkpoint is at the end (within 64 bytes tolerance)
    // This matches Java version behavior - prevents truncating mid-transaction batch
    if (impl->auto_truncate && impl->fd > 0 && 
        impl->checkpoint_offset >= impl->current_position - 64) {
        // Truncate WAL file to header only
        if (ftruncate(impl->fd, HEADER_SIZE) != 0) {
            THROW(e, "Failed to truncate WAL file: %s", strerror(errno));
        }
        impl->current_position = HEADER_SIZE;
        impl->batch_size = 0;
        impl->batch_count = 0;
    }
    return 0;
    
EXCEPTION:
    return -1;
}

//
// Storage wrapper implementation
//

static void wal_storage_close(struct storage *me) {
    struct wal_storage *ws = (struct wal_storage*)me;
    if (ws->origin) {
        ws->origin->close(ws->origin);
        FREE(ws->origin);
    }
    // FREE(ws); // Do not free here, will be freed by caller
}

static i64 wal_storage_count_get(struct storage *me) {
    struct wal_storage *ws = (struct wal_storage*)me;
    return ws->origin->count_get(ws->origin);
}

static i64 wal_storage_bytes_get(struct storage *me) {
    struct wal_storage *ws = (struct wal_storage*)me;
    return ws->origin->bytes_get(ws->origin);
}

static struct buffer* wal_storage_read(struct storage *me, i64 offset, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    return ws->origin->read(ws->origin, offset, e);
}

static i64 wal_storage_write(struct storage *me, struct buffer *in, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    // Write-through: write to origin immediately
    i64 index = ws->origin->write(ws->origin, in, e);
    
    // Log metadata only to WAL if transaction is active (data already in origin storage)
    if (ws->transaction > 0 && ws->logger && index >= 0) {
        wal_log(ws->logger, OP_WRITE, ws->transaction, ws->identifier, index, NULL, 0, 1);
    }
    
    return index;
}

static i64 wal_storage_write_at(struct storage *me, i64 offset, struct buffer *in, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    
    // Log actual data to WAL if transaction is active (for crash recovery)
    if (ws->transaction > 0 && ws->logger && in && in->array) {
        i32 data_size = in->limit - in->position;
        wal_log(ws->logger, OP_UPDATE, ws->transaction, ws->identifier, offset, 
                in->array + in->position, data_size, 0); // Log full data, not metadata-only
    }
    
    // Write-through: update origin immediately
    i64 result = ws->origin->write_at(ws->origin, offset, in, e);
    return result;
}

static i32 wal_storage_delete(struct storage *me, i64 offset, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    
    // Log to WAL if transaction is active
    if (ws->transaction > 0 && ws->logger) {
        wal_log(ws->logger, OP_DELETE, ws->transaction, ws->identifier, offset, NULL, 0, 0);
    }
    
    // Delete from origin storage
    i32 result = ws->origin->delete(ws->origin, offset, e);
    
    // Invoke callback to synchronize cache
    if (result && ws->callback != NULL) {
        ws->callback(ws->callback_obj, offset);
    }
    
    return result;
}

static void wal_storage_transaction(struct storage *me, i64 id, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    ws->transaction = id;
}

static struct buffer* wal_storage_mmap(struct storage *me, i64 offset, i32 length, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    return ws->origin->mmap(ws->origin, offset, length, e);
}

static struct buffer* wal_storage_head(struct storage *me, i64 offset, i32 length, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    return ws->origin->head(ws->origin, offset, length, e);
}

static struct storage* wal_wrap_storage(struct storage* origin, struct wal* wal, int (*callback)(const void *obj, i64 offset), const void *callback_obj, char** e) {
    struct wal_storage* ws = CALLOC(1, sizeof(struct wal_storage));
    if (!ws) THROW(e, "Out of memory");

    ws->origin = origin;
    ws->logger = wal->impl;
    ws->identifier = 0; // Will be set by caller
    ws->transaction = -1;
    ws->callback = callback;
    ws->callback_obj = callback_obj;

    // Setup storage function pointers in base
    ws->base.close = wal_storage_close;
    ws->base.count_get = wal_storage_count_get;
    ws->base.bytes_get = wal_storage_bytes_get;
    ws->base.read = wal_storage_read;
    ws->base.write = wal_storage_write;
    ws->base.write_at = wal_storage_write_at;
    ws->base.delete = wal_storage_delete;
    ws->base.transaction = wal_storage_transaction;
    ws->base.mmap = wal_storage_mmap;
    ws->base.head = wal_storage_head;

    return (struct storage*)ws;

EXCEPTION:
    if (ws) FREE(ws);
    return NULL;
}

struct storage* wal_wrap(struct wal* wal, struct storage_opts* opts, int (*refresh)(const void *obj, i64 offset), const void *callback_obj, char** e) {
    assert(wal != NULL);
    struct storage* origin = NULL;
    struct storage* wrapped = NULL;

    origin = CALLOC(1, sizeof(struct storage));
    // TRACE("wal_wrap: opening origin storage");
    if (!origin) THROW(e, "Out of memory");

    if (storage_open(origin, *opts, e) != 0) THROW_S(e);
    if (wal == &WAL_NONE) {
        // TRACE("wal_wrap: WAL_NONE, returning origin storage directly"); 
        return origin;
    }
    
    wrapped = wal_wrap_storage(origin, wal, refresh, callback_obj, e);
    if (e && *e) THROW_S(e);

    return wrapped;

EXCEPTION:    
    WARN("wal_wrap: exception occurred, cleaning up : %s", e && *e ? *e : "unknown");
    if (origin)  origin->close(origin);
    if (wrapped)  wrapped->close(wrapped);
    return NULL;
}

static void wal_close(struct wal *me) {
    assert(me != NULL);

    if (me->impl) {
        struct wal_impl *impl = me->impl;
        // Flush any pending batch before closing
        wal_flush_batch(impl);
        wal_flush_header(impl);
        
        if (impl->fd > 0) {
            close(impl->fd);
        }
        if (impl->batch_buffer) {
            FREE(impl->batch_buffer);
        }
        FREE(me->impl);
    }
    FREE(me);
}

struct wal* wal_open(const char *path, const struct flintdb_meta *meta, char** e) {
    struct wal* w = NULL;
    struct wal_impl* impl = NULL;
    
    if (!path || !meta) THROW(e, "Invalid parameters to wal_open");

    w = CALLOC(1, sizeof(struct wal));
    if (!w) THROW(e, "Out of memory");

    impl = CALLOC(1, sizeof(struct wal_impl));
    if (!impl) THROW(e, "Out of memory");

    // Set mode: TRUNCATE (default) or LOG
    // Compression is always enabled for better I/O performance
    impl->auto_truncate = (strcmp(meta->wal, WAL_OPT_TRUNCATE) == 0) ? 1 : 0;
    impl->transaction_id = 0;
    impl->transaction_count = 0;
    impl->committed_offset = 0;
    impl->checkpoint_offset = 0;
    impl->total_count = 0;
    impl->processed_count = 0;
    impl->checkpoint_interval = meta->wal_checkpoint_interval > 0 ? meta->wal_checkpoint_interval : get_env_int("FLINTDB_WAL_CHECKPOINT_INTERVAL", 10000);
    impl->batch_size_limit = meta->wal_batch_size > 0 ? meta->wal_batch_size : get_env_int("FLINTDB_WAL_BATCH_SIZE", DEFAULT_BATCH_SIZE);
    impl->compression_threshold = meta->wal_compression_threshold > 0 ? meta->wal_compression_threshold : get_env_int("FLINTDB_WAL_COMPRESSION_THRESHOLD", DEFAULT_COMPRESSION_THRESHOLD);
    strncpy(impl->path, path, PATH_MAX - 1);

    // Allocate batch buffer
    impl->batch_buffer = CALLOC(1, BATCH_BUFFER_SIZE);
    if (!impl->batch_buffer) THROW(e, "Failed to allocate batch buffer");
    impl->batch_size = 0;
    impl->batch_count = 0;

    // Open WAL file
    impl->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (impl->fd < 0) {
        FREE(impl->batch_buffer);
        THROW(e, "Failed to open WAL file: %s", strerror(errno));
    }
    
    // Check if new file or existing
    struct stat st;
    if (fstat(impl->fd, &st) == 0 && st.st_size == 0) {
        // New file, write header
        impl->current_position = HEADER_SIZE;
        wal_flush_header(impl);
    } else if (st.st_size >= HEADER_SIZE) {
        // Existing file, read header
        char header[HEADER_SIZE];
        lseek(impl->fd, 0, SEEK_SET);
        if (read(impl->fd, header, HEADER_SIZE) == HEADER_SIZE) {
            impl->transaction_id = *(i64*)(header + 16);
            impl->committed_offset = *(i64*)(header + 24);
            impl->checkpoint_offset = *(i64*)(header + 32);
            impl->total_count = *(i32*)(header + 40);
            impl->processed_count = *(i32*)(header + 44);
            impl->current_position = st.st_size;
        } else {
            impl->current_position = HEADER_SIZE;
        }
    } else {
        impl->current_position = HEADER_SIZE;
    }

    w->impl = impl;
    w->begin = wal_begin;
    w->commit = wal_commit;
    w->rollback = wal_rollback;
    w->recover = wal_recover;
    w->checkpoint = wal_checkpoint;
    w->close = wal_close;
    
    return w;

EXCEPTION:
    if (impl) {
        if (impl->fd > 0) close(impl->fd);
        FREE(impl);
    }
    if (w) FREE(w);
    return NULL;
}
