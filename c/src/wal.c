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
#ifndef _WIN32
#include <sys/uio.h>
#endif

// Platform-specific I/O headers
#if defined(__linux__) && defined(HAVE_LIBURING)
#include <liburing.h>
#endif

#define HEADER_SIZE 4096  // Match filesystem block size for atomic writes
#define DEFAULT_BATCH_SIZE 10000
#define DEFAULT_BATCH_BUFFER_SIZE (4 * 1024 * 1024) // 4MB batch buffer
#define DEFAULT_COMPRESSION_THRESHOLD 8192 // Compress if data > 8KB bytes
#define DEFAULT_DIRECT_WRITE_THRESHOLD (64 * 1024) // Direct-write large records to avoid memcpy into batch buffer
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
    i32 batch_capacity;         // Batch buffer capacity (bytes)
    i32 batch_size_limit;      // Batch size limit (configurable)
    i32 compression_threshold; // Compression threshold (configurable)

    // Sync behavior
    i32 sync_mode;             // WAL_SYNC_DEFAULT|WAL_SYNC_OFF|WAL_SYNC_NORMAL|WAL_SYNC_FULL

    // WAL payload policy
    i32 log_page_data;         // 1=log page images for UPDATE/DELETE, 0=metadata only

    // Large record policy
    i32 direct_write_threshold; // If record size exceeds this, write directly (writev) instead of copying into batch buffer
    
    // Multiple storages management
    struct hashmap *storages;  // Map of file -> wal_storage
    
    // Platform-specific I/O context
#if defined(__linux__) && defined(HAVE_LIBURING)
    struct io_uring ring;      // io_uring instance for Linux
    int io_uring_enabled;      // Flag to indicate if io_uring is successfully initialized
#endif
};

// Platform-specific I/O initialization and cleanup
#if defined(__linux__) && defined(HAVE_LIBURING)
static int wal_io_init_linux(struct wal_impl *impl) {
    // Initialize io_uring with queue depth of 256
    int ret = io_uring_queue_init(256, &impl->ring, 0);
    if (ret < 0) {
        WARN("Failed to initialize io_uring: %s, falling back to standard I/O", strerror(-ret));
        impl->io_uring_enabled = 0;
        return -1;
    }
    impl->io_uring_enabled = 1;
    return 0;
}

static void wal_io_cleanup_linux(struct wal_impl *impl) {
    if (impl->io_uring_enabled) {
        io_uring_queue_exit(&impl->ring);
        impl->io_uring_enabled = 0;
    }
}
#elif defined(__linux__)
static int wal_io_init_linux(struct wal_impl *impl) {
    // liburing not available, no initialization needed
    (void)impl;
    return 0;
}

static void wal_io_cleanup_linux(struct wal_impl *impl) {
    // No cleanup needed without liburing
    (void)impl;
}
#endif

#ifdef __APPLE__
static int wal_io_init_macos(struct wal_impl *impl) {
    // F_NOCACHE is already set on the file descriptor, no additional setup needed
    // We don't use dispatch_queue as it requires Objective-C blocks
    (void)impl;
    return 0;
}

static void wal_io_cleanup_macos(struct wal_impl *impl) {
    // No cleanup needed for F_NOCACHE approach
    (void)impl;
}
#endif

#ifdef _WIN32
static ssize_t wal_write_all(int fd, const void *buf, size_t len) {
    const char *p = (const char*)buf;
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t n = write(fd, p, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return (ssize_t)(len - remaining);
        p += n;
        remaining -= (size_t)n;
    }
    return (ssize_t)len;
}
#endif

// Forward declarations
static ssize_t wal_pwrite_all(int fd, const void *buf, size_t len, off_t offset);
#ifndef _WIN32
static ssize_t wal_pwritev_all(int fd, const struct iovec *iov, int iovcnt, off_t offset);
#endif

// Platform-optimized write functions
#if defined(__linux__) && defined(HAVE_LIBURING)
static ssize_t wal_pwrite_linux_io_uring(struct wal_impl *impl, const void *buf, size_t len, off_t offset) {
    if (!impl->io_uring_enabled) {
        // Fallback to standard pwrite
        return pwrite(impl->fd, buf, len, offset);
    }
    
    struct io_uring_sqe *sqe = io_uring_get_sqe(&impl->ring);
    if (!sqe) {
        // Queue is full, submit and retry
        io_uring_submit(&impl->ring);
        sqe = io_uring_get_sqe(&impl->ring);
        if (!sqe) return -1;
    }
    
    io_uring_prep_write(sqe, impl->fd, buf, len, offset);
    sqe->user_data = (uintptr_t)buf;
    
    int ret = io_uring_submit(&impl->ring);
    if (ret < 0) return ret;
    
    struct io_uring_cqe *cqe;
    ret = io_uring_wait_cqe(&impl->ring, &cqe);
    if (ret < 0) return ret;
    
    ssize_t result = cqe->res;
    io_uring_cqe_seen(&impl->ring, cqe);
    
    return result;
}

static ssize_t wal_pwritev_linux_io_uring(struct wal_impl *impl, const struct iovec *iov, int iovcnt, off_t offset) {
    if (!impl->io_uring_enabled) {
        // Fallback to standard pwritev
        return wal_pwritev_all(impl->fd, iov, iovcnt, offset);
    }
    
    struct io_uring_sqe *sqe = io_uring_get_sqe(&impl->ring);
    if (!sqe) {
        io_uring_submit(&impl->ring);
        sqe = io_uring_get_sqe(&impl->ring);
        if (!sqe) return -1;
    }
    
    io_uring_prep_writev(sqe, impl->fd, iov, iovcnt, offset);
    sqe->user_data = (uintptr_t)iov;
    
    int ret = io_uring_submit(&impl->ring);
    if (ret < 0) return ret;
    
    struct io_uring_cqe *cqe;
    ret = io_uring_wait_cqe(&impl->ring, &cqe);
    if (ret < 0) return ret;
    
    ssize_t result = cqe->res;
    io_uring_cqe_seen(&impl->ring, cqe);
    
    return result;
}
#elif defined(__linux__)
// Fallback for Linux without liburing
static ssize_t wal_pwrite_linux_io_uring(struct wal_impl *impl, const void *buf, size_t len, off_t offset) {
    return pwrite(impl->fd, buf, len, offset);
}

static ssize_t wal_pwritev_linux_io_uring(struct wal_impl *impl, const struct iovec *iov, int iovcnt, off_t offset) {
    return wal_pwritev_all(impl->fd, iov, iovcnt, offset);
}
#endif

#ifdef __APPLE__
// macOS uses F_NOCACHE to bypass page cache, but for simplicity we'll use standard pwrite
// with F_NOCACHE set on the fd. dispatch_io requires Objective-C blocks which are complex in pure C.
// For optimal performance with pure C, we rely on F_NOCACHE + standard pwrite.
static ssize_t wal_pwrite_macos_dispatch(struct wal_impl *impl, const void *buf, size_t len, off_t offset) {
    // With F_NOCACHE already set on fd, standard pwrite will bypass page cache
    return pwrite(impl->fd, buf, len, offset);
}

static ssize_t wal_pwritev_macos_dispatch(struct wal_impl *impl, const struct iovec *iov, int iovcnt, off_t offset) {
    // For vectored writes on macOS with F_NOCACHE, use standard pwritev helper
    return wal_pwritev_all(impl->fd, iov, iovcnt, offset);
}
#endif

static ssize_t wal_pwrite_all(int fd, const void *buf, size_t len, off_t offset) {
    const char *p = (const char*)buf;
    size_t remaining = len;
    off_t off = offset;
    while (remaining > 0) {
        ssize_t n;
#if defined(_WIN32)
    // Windows: prefer pwrite compatibility layer (OVERLAPPED-based) to avoid changing file offset.
    // If EMULATE_PREAD_PWRITE_WIN32 is enabled, pwrite() is available.
    // Otherwise fall back to pwrite_win32() using the underlying HANDLE.
    #ifdef EMULATE_PREAD_PWRITE_WIN32
        n = (ssize_t)pwrite(fd, p, (u64)remaining, (u64)off);
    #else
        HANDLE fh = (HANDLE)(_get_osfhandle(fd));
        if (fh == INVALID_HANDLE_VALUE) return -1;
        n = (ssize_t)pwrite_win32(fh, p, (u64)remaining, (u64)off);
    #endif
#else
        n = pwrite(fd, p, remaining, off);
#endif
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return (ssize_t)(len - remaining);
        p += n;
        off += n;
        remaining -= (size_t)n;
    }
    return (ssize_t)len;
}

#ifndef _WIN32
static ssize_t wal_pwritev_all(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    // Portable-ish pwritev: write each segment with pwrite and advance offset.
    // Avoids changing the file descriptor's seek position.
    off_t off = offset;
    for (int i = 0; i < iovcnt; i++) {
        if (iov[i].iov_len == 0) continue;
        if (wal_pwrite_all(fd, iov[i].iov_base, iov[i].iov_len, off) < 0) return -1;
        off += (off_t)iov[i].iov_len;
    }
    return 1;
}
#endif

static inline void wal_encode_record_header(
    char *buf,
    u8 operation,
    i64 transaction_id,
    i16 checksum,
    i32 file_id,
    i64 page_offset,
    u8 flags,
    i32 original_size
) {
    // Avoid UB from misaligned typed stores by using memcpy into the byte buffer.
    // NOTE: This preserves the existing on-disk byte order (host endianness).
    // The WAL format in this project assumes little-endian hosts.
    buf[0] = (char)operation;
    memcpy(buf + 1, &transaction_id, sizeof(transaction_id));
    memcpy(buf + 9, &checksum, sizeof(checksum));
    memcpy(buf + 11, &file_id, sizeof(file_id));
    memcpy(buf + 15, &page_offset, sizeof(page_offset));
    buf[23] = (char)flags;
    memcpy(buf + 24, &original_size, sizeof(original_size));
}

// Dirty page entry for uncommitted UPDATE/DELETE operations.
// For UPDATE: store page bytes inline and expose a buffer view (free is no-op for callers).
// For DELETE: data_size=0 and is_delete=1.
struct dirty_page {
    i64 offset;                 // Page offset
    int is_delete;              // 1 if this is a DELETE operation
    i32 data_size;              // Page data size (0 for DELETE)
    struct buffer buf;          // Buffer view of data[] (free is no-op)
    char data[];                // Flexible array for page bytes
};

static void dirty_page_buffer_free(struct buffer *me) {
    (void)me;
    // No-op: dirty pages are owned/freed by WAL storage.
}

static void dirty_page_free(struct dirty_page *page) {
    if (!page) return;
    FREE(page);
}

// Storage wrapper structure
struct wal_storage {
    struct storage base;        // Base storage structure (must be first!)
    struct storage *origin;     // Original storage
    struct wal_impl *logger;    // WAL logger
    int identifier;             // File identifier
    i64 transaction;            // Current transaction ID
    int (*callback)(const void *obj, i64 offset); // Cache synchronization callback
    const void *callback_obj;   // Callback object context
    struct hashmap *dirty_pages; // Hashmap of offset -> dirty_page (O(1) lookup)
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
static void wal_flush_batch(struct wal_impl *impl, int do_sync);
static void wal_flush_header(struct wal_impl *impl);
static char* wal_compress_data(const char *data, i32 size, i32 *compressed_size);
static void wal_storage_commit(struct wal_storage *ws, i64 id, char **e);
static void wal_storage_rollback(struct wal_storage *ws, i64 id);

static void wal_do_sync(struct wal_impl *impl) {
    if (!impl || impl->fd <= 0) return;
    if (impl->sync_mode == WAL_SYNC_OFF) return;

    // Platform default behavior (backward compatible)
    i32 mode = impl->sync_mode;
    if (mode == WAL_SYNC_DEFAULT) {
        #ifdef __APPLE__
            mode = WAL_SYNC_FULL;
        #else
            mode = WAL_SYNC_NORMAL;
        #endif
    }

    #ifdef __APPLE__
        if (mode == WAL_SYNC_FULL) {
            fcntl(impl->fd, F_FULLFSYNC);
        } else {
            fsync(impl->fd);
        }
    #else
        if (mode == WAL_SYNC_FULL) {
            fsync(impl->fd);
        } else {
            // Linux/Unix/MinGW: fdatasync is faster than fsync (doesn't update metadata)
            fdatasync(impl->fd);
        }
    #endif
}

static i64 wal_begin(struct wal *me, char **e) {
    struct wal_impl *impl = me->impl;
    i64 id = ++impl->transaction_id;
    
    // Set transaction on all storages (match Java behavior)
    if (impl->storages) {
        struct map_iterator it = {0};
        while (impl->storages->iterate(impl->storages, &it)) {
            struct wal_storage *ws = (struct wal_storage*)(uintptr_t)it.val;
            if (ws) {
                ws->transaction = id;
            }
        }
    }
    
    return id;
}

static i64 wal_commit(struct wal *me, i64 id, char **e) {
    struct wal_impl *impl = me->impl;
    
    // Flush dirty pages to origin storage first
    if (impl->storages) {
        struct map_iterator it = {0};
        while (impl->storages->iterate(impl->storages, &it)) {
            struct wal_storage *ws = (struct wal_storage*)(uintptr_t)it.val;
            if (ws) {
                wal_storage_commit(ws, id, e);
                if (e && *e) return -1;
            }
        }
    }
    
    // Then log commit record to WAL
    wal_log(impl, OP_COMMIT, id, 0, 0, NULL, 0, 0);
    impl->committed_offset = impl->current_position;
    impl->total_count++;
    impl->transaction_count++;
    
    // Auto checkpoint every N transactions
    if (impl->auto_truncate && impl->transaction_count >= impl->checkpoint_interval) {
        wal_checkpoint(me, e);
        impl->transaction_count = 0;
    }
    return 0;
}

static i64 wal_rollback(struct wal *me, i64 id, char **e) {
    struct wal_impl *impl = me->impl;
    
    // Rollback all storages (discard dirty pages)
    if (impl->storages) {
        struct map_iterator it = {0};
        while (impl->storages->iterate(impl->storages, &it)) {
            struct wal_storage *ws = (struct wal_storage*)(uintptr_t)it.val;
            if (ws) {
                wal_storage_rollback(ws, id);
            }
        }
    }
    
    // Then log rollback record
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
    
    // Best-effort; header flush happens on checkpoint/close.
    wal_pwrite_all(impl->fd, header, HEADER_SIZE, 0);
}

static void wal_flush_batch(struct wal_impl *impl, int do_sync) {
    if (impl->batch_count == 0 || impl->fd <= 0) return;

    // Platform-optimized batch write
    ssize_t written;
#ifdef __linux__
    written = wal_pwrite_linux_io_uring(impl, impl->batch_buffer, (size_t)impl->batch_size, (off_t)impl->current_position);
#elif defined(__APPLE__)
    written = wal_pwrite_macos_dispatch(impl, impl->batch_buffer, (size_t)impl->batch_size, (off_t)impl->current_position);
#else
    written = wal_pwrite_all(impl->fd, impl->batch_buffer, (size_t)impl->batch_size, (off_t)impl->current_position);
#endif
    if (written > 0) impl->current_position += written;

    // Only sync when explicitly requested (commit/checkpoint/close)
    if (do_sync) {
        wal_do_sync(impl);
    }
    
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
    
    // If record is large, bypass batch buffer to avoid memcpy of payload.
    // This is especially impactful when logging full page images.
    if (impl->direct_write_threshold > 0 && record_size >= impl->direct_write_threshold) {
        // Flush pending batch first (no sync).
        wal_flush_batch(impl, 0);

        char header[28];
        i16 checksum = 0; // Placeholder
        wal_encode_record_header(header, operation, transaction_id, checksum, file_id, page_offset, flags, original_size);

#ifndef _WIN32
        struct iovec iov[3];
        int iovcnt = 0;
        iov[iovcnt++] = (struct iovec){ .iov_base = header, .iov_len = sizeof(header) };
        i32 cs = compressed_size;
        if ((flags & FLAG_COMPRESSED) != 0) {
            iov[iovcnt++] = (struct iovec){ .iov_base = &cs, .iov_len = sizeof(i32) };
            iov[iovcnt++] = (struct iovec){ .iov_base = compressed_data, .iov_len = (size_t)compressed_size };
        } else if ((flags & FLAG_METADATA_ONLY) == 0 && page_data && final_size > 0) {
            iov[iovcnt++] = (struct iovec){ .iov_base = (void*)page_data, .iov_len = (size_t)final_size };
        }

        // Platform-optimized direct write
#ifdef __linux__
        wal_pwritev_linux_io_uring(impl, iov, iovcnt, (off_t)impl->current_position);
#elif defined(__APPLE__)
        wal_pwritev_macos_dispatch(impl, iov, iovcnt, (off_t)impl->current_position);
#else
        wal_pwritev_all(impl->fd, iov, iovcnt, (off_t)impl->current_position);
#endif
#else
        // Windows: offset-based writes (wal_pwrite_all may internally lseek).
        off_t off = (off_t)impl->current_position;
        wal_pwrite_all(impl->fd, header, sizeof(header), off);
        off += (off_t)sizeof(header);
        if ((flags & FLAG_COMPRESSED) != 0) {
            wal_pwrite_all(impl->fd, &compressed_size, sizeof(i32), off);
            off += (off_t)sizeof(i32);
            wal_pwrite_all(impl->fd, compressed_data, (size_t)compressed_size, off);
        } else if ((flags & FLAG_METADATA_ONLY) == 0 && page_data && final_size > 0) {
            wal_pwrite_all(impl->fd, page_data, (size_t)final_size, off);
        }
#endif

        impl->current_position += record_size;
        if (compressed_data) FREE(compressed_data);
        return;
    }

    // Flush batch if not enough space
    if (impl->batch_size + record_size > impl->batch_capacity) {
        // For throughput, do not sync on intermediate flushes.
        wal_flush_batch(impl, 0);
    }

    char *buf = impl->batch_buffer + impl->batch_size;
    i16 checksum = 0; // Placeholder

    // Write record to batch buffer
    wal_encode_record_header(buf, operation, transaction_id, checksum, file_id, page_offset, flags, original_size);

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
        // For throughput, do not sync on intermediate flushes.
        wal_flush_batch(impl, 0);
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
    wal_flush_batch(impl, 0);
    wal_log(impl, OP_CHECKPOINT, impl->transaction_id, 0, 0, NULL, 0, 0);
    wal_flush_batch(impl, 1);
    
    impl->checkpoint_offset = impl->current_position;
    impl->total_count++;
    wal_flush_header(impl);
    wal_do_sync(impl);
    
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
    (void)ws;
    // IMPORTANT:
    // wal_wrap() caches a single wal_storage instance per file in wal_impl->storages
    // and may return the same pointer across multiple bplustree/table opens.
    // Therefore, a "client close" must NOT close/free the shared origin storage,
    // otherwise subsequent wal_wrap() callers will receive a stale wal_storage
    // with a freed origin (use-after-free -> intermittent SIGSEGV).
    //
    // The WAL owner (wal_close) is responsible for final cleanup.
}

static void wal_storage_close_internal(struct wal_storage *ws) {
    if (!ws) return;

    // Free dirty pages hashmap
    if (ws->dirty_pages) {
        struct map_iterator it = {0};
        while (ws->dirty_pages->iterate(ws->dirty_pages, &it)) {
            struct dirty_page *page = (struct dirty_page*)(uintptr_t)it.val;
            dirty_page_free(page);
        }
        ws->dirty_pages->free(ws->dirty_pages);
        ws->dirty_pages = NULL;
    }

    if (ws->origin) {
        ws->origin->close(ws->origin);
        FREE(ws->origin);
        ws->origin = NULL;
    }
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
    
    // Check dirty page cache first (read-your-own-writes) - O(1) lookup
    if (ws->dirty_pages) {
        valtype val = (valtype)ws->dirty_pages->get(ws->dirty_pages, (keytype)offset);
        if (val != HASHMAP_INVALID_VAL) {
            struct dirty_page *page = (struct dirty_page*)(uintptr_t)val;
            if (page->is_delete) {
                THROW(e, "Page %lld has been deleted in current transaction", offset);
            }
            // Return dirty page buffer view (caller may free; it's a no-op)
            return &page->buf;
        }
    }
    
    // Not in dirty cache, read from origin
    return ws->origin->read(ws->origin, offset, e);
    
EXCEPTION:
    return NULL;
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
    
    if (ws->transaction > 0) {
        // UPDATE within transaction: don't write to origin immediately
        // Store in dirty page cache and log to WAL
        i32 data_size = (i32)(in->limit - in->position);

        // If this offset already has a dirty entry in this transaction, free it (avoid leaks).
        if (ws->dirty_pages) {
            valtype existing_val = (valtype)ws->dirty_pages->get(ws->dirty_pages, (keytype)offset);
            if (existing_val != HASHMAP_INVALID_VAL) {
                struct dirty_page *existing = (struct dirty_page*)(uintptr_t)existing_val;
                dirty_page_free(existing);
            }
        }

        // Allocate dirty page as a single block: struct + inline data.
        struct dirty_page *page = CALLOC(1, (size_t)sizeof(struct dirty_page) + (size_t)data_size);
        if (!page) THROW(e, "Out of memory");

        page->offset = offset;
        page->is_delete = 0;
        page->data_size = data_size;

        // Copy updated bytes once.
        memcpy(page->data, in->array + in->position, (size_t)data_size);

        // Initialize buffer view backed by page->data.
        buffer_wrap(page->data, (u32)data_size, &page->buf);
        page->buf.limit = (u32)data_size;
        page->buf.position = 0;
        page->buf.free = &dirty_page_buffer_free;

        // Log to WAL after we have a stable copy (enables direct-write path w/o extra memcpy).
        if (ws->logger && ws->logger->log_page_data) {
            wal_log(ws->logger, OP_UPDATE, ws->transaction, ws->identifier, offset,
                    page->data, data_size, 0);
        } else {
            // Fast mode: record metadata only (reduces WAL I/O significantly)
            wal_log(ws->logger, OP_UPDATE, ws->transaction, ws->identifier, offset, NULL, 0, 1);
        }

        // Add to hashmap (O(1) insert)
        ws->dirty_pages->put(ws->dirty_pages, (keytype)offset, (valtype)(uintptr_t)page, NULL);
        
        return 0;
    } else {
        // No transaction: direct write to origin
        i64 result = ws->origin->write_at(ws->origin, offset, in, e);
        return result;
    }
    
EXCEPTION:
    return -1;
}

static i32 wal_storage_delete(struct storage *me, i64 offset, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    
    if (ws->transaction > 0) {
        // DELETE within transaction: don't delete from origin immediately
        // Mark as deleted in cache and log to WAL
        if (ws->logger && ws->logger->log_page_data) {
            wal_log(ws->logger, OP_DELETE, ws->transaction, ws->identifier, offset, NULL, 0, 0);
        } else {
            wal_log(ws->logger, OP_DELETE, ws->transaction, ws->identifier, offset, NULL, 0, 1);
        }
        
        // If this offset already has a dirty entry, replace it.
        if (ws->dirty_pages) {
            valtype existing_val = (valtype)ws->dirty_pages->get(ws->dirty_pages, (keytype)offset);
            if (existing_val != HASHMAP_INVALID_VAL) {
                struct dirty_page *existing = (struct dirty_page*)(uintptr_t)existing_val;
                dirty_page_free(existing);
            }
        }

        struct dirty_page *page = CALLOC(1, sizeof(struct dirty_page));
        if (!page) THROW(e, "Out of memory");

        page->offset = offset;
        page->is_delete = 1;
        page->data_size = 0;
        
        // Add to hashmap (O(1) insert)
        ws->dirty_pages->put(ws->dirty_pages, (keytype)offset, (valtype)(uintptr_t)page, NULL);
        
        return 1;
    } else {
        // No transaction: direct delete from origin
        i32 result = ws->origin->delete(ws->origin, offset, e);
        if (result && ws->callback != NULL) {
            ws->callback(ws->callback_obj, offset);
        }
        return result;
    }
    
EXCEPTION:
    return 0;
}

static void wal_storage_transaction(struct storage *me, i64 id, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    ws->transaction = id;
}

static void wal_storage_commit(struct wal_storage *ws, i64 id, char **e) {
    if (ws->transaction != id) {
        return; // Not our transaction
    }
    
    // Flush dirty pages to origin storage
    if (ws->dirty_pages) {
        // Commit ordering matters for crash-safety without recovery:
        // apply all non-root offsets first, then apply root (offset 0) last.
        // This ensures a crash mid-commit yields only orphan pages, not a broken tree.
        struct dirty_page *root_page = NULL;
        struct map_iterator it = {0};
        while (ws->dirty_pages->iterate(ws->dirty_pages, &it)) {
            struct dirty_page *page = (struct dirty_page*)(uintptr_t)it.val;

            if (page->offset == 0) {
                root_page = page;
                continue;
            }

            if (page->is_delete) {
                // Apply delete
                ws->origin->delete(ws->origin, page->offset, e);
                if (e && *e) return;
                if (ws->callback) {
                    ws->callback(ws->callback_obj, page->offset);
                }
            } else if (page->data_size > 0) {
                // Apply update
                page->buf.position = 0;
                ws->origin->write_at(ws->origin, page->offset, &page->buf, e);
                if (e && *e) return;
            }
        }

        // Apply root pointer record last (if present)
        if (root_page) {
            if (root_page->is_delete) {
                ws->origin->delete(ws->origin, root_page->offset, e);
                if (e && *e) return;
                if (ws->callback) {
                    ws->callback(ws->callback_obj, root_page->offset);
                }
            } else if (root_page->data_size > 0) {
                root_page->buf.position = 0;
                ws->origin->write_at(ws->origin, root_page->offset, &root_page->buf, e);
                if (e && *e) return;
            }
        }
        
        // Clear dirty pages
        it = (struct map_iterator){0};
        while (ws->dirty_pages->iterate(ws->dirty_pages, &it)) {
            struct dirty_page *page = (struct dirty_page*)(uintptr_t)it.val;
            dirty_page_free(page);
        }
        ws->dirty_pages->clear(ws->dirty_pages);
    }
    
    ws->transaction = -1;
}

static void wal_storage_rollback(struct wal_storage *ws, i64 id) {
    if (ws->transaction != id) {
        return; // Not our transaction
    }
    
    // Discard dirty pages (don't apply to origin)
    if (ws->dirty_pages) {
        struct map_iterator it = {0};
        while (ws->dirty_pages->iterate(ws->dirty_pages, &it)) {
            struct dirty_page *page = (struct dirty_page*)(uintptr_t)it.val;
            dirty_page_free(page);
        }
        ws->dirty_pages->clear(ws->dirty_pages);
    }
    
    ws->transaction = -1;
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
    ws->base.managed_by_wal = 1;
    
    // Initialize dirty pages hashmap (offset -> dirty_page)
    ws->dirty_pages = hashmap_new(256, hashmap_int_hash, hashmap_int_cmpr);
    if (!ws->dirty_pages) THROW(e, "Failed to create dirty pages map");

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
    if (wal == NULL) {
        wal = &WAL_NONE;
    }
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
    
    // Check if storage already exists for this file (match Java behavior)
    struct wal_impl *impl = wal->impl;
    if (impl->storages) {
        valtype existing_val = (valtype)impl->storages->get(impl->storages, (keytype)opts->file);
        if (existing_val != HASHMAP_INVALID_VAL) {
            origin->close(origin);
            FREE(origin);
            return (struct storage*)(uintptr_t)existing_val;
        }
    }
    
    wrapped = wal_wrap_storage(origin, wal, refresh, callback_obj, e);
    if (e && *e) THROW_S(e);
    
    // Register storage in map and set identifier (match Java: storages.put(options.file, storage))
    if (impl->storages) {
        struct wal_storage *ws = (struct wal_storage*)wrapped;
        ws->identifier = impl->storages->count_get(impl->storages);
        impl->storages->put(impl->storages, (keytype)opts->file, (valtype)(uintptr_t)wrapped, NULL);
    }

    return wrapped;

EXCEPTION:    
    WARN("wal_wrap: exception occurred, cleaning up : %s", e && *e ? *e : "unknown");
    if (wrapped) {
        if (wal != &WAL_NONE) {
            wal_storage_close_internal((struct wal_storage*)wrapped);
            FREE(wrapped);
        } else {
            wrapped->close(wrapped);
            FREE(wrapped);
        }
        return NULL;
    }
    if (origin) {
        origin->close(origin);
        FREE(origin);
    }
    return NULL;
}

static void wal_close(struct wal *me) {
    assert(me != NULL);

    if (me->impl) {
        struct wal_impl *impl = me->impl;
        // Flush any pending batch before closing
        wal_flush_batch(impl, 1);
        wal_flush_header(impl);
        wal_do_sync(impl);
        
        // Close all registered storages (match Java: storages.forEach(entry -> IO.close(storage)))
        if (impl->storages) {
            struct map_iterator it = {0};
            while (impl->storages->iterate(impl->storages, &it)) {
                struct wal_storage *ws = (struct wal_storage*)(uintptr_t)it.val;
                if (ws) {
                    wal_storage_close_internal(ws);
                    FREE(ws);
                }
            }
            impl->storages->free(impl->storages);
        }
        
        // Platform-specific I/O cleanup
#ifdef __linux__
        wal_io_cleanup_linux(impl);
#endif
#ifdef __APPLE__
        wal_io_cleanup_macos(impl);
#endif
        
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
    impl->sync_mode = meta->wal_sync != 0 ? meta->wal_sync : get_env_int("FLINTDB_WAL_SYNC", WAL_SYNC_DEFAULT);
    impl->batch_capacity = meta->wal_buffer_size > 0 ? meta->wal_buffer_size : get_env_int("FLINTDB_WAL_BUFFER_SIZE", DEFAULT_BATCH_BUFFER_SIZE);
    if (impl->batch_capacity < (256 * 1024)) impl->batch_capacity = (256 * 1024);
    impl->log_page_data = meta->wal_page_data ? 1 : 0;
    // Direct-write threshold: allow override via env, else default.
    // If batch buffer is very small, keep threshold <= batch_capacity to avoid unnecessary flushes.
    impl->direct_write_threshold = get_env_int("FLINTDB_WAL_DIRECT_WRITE_THRESHOLD", DEFAULT_DIRECT_WRITE_THRESHOLD);
    if (impl->direct_write_threshold > impl->batch_capacity) impl->direct_write_threshold = impl->batch_capacity;
    strncpy(impl->path, path, PATH_MAX - 1);

    // Initialize storages map (match Java: Map<File, WALStorage> storages = new HashMap<>())
    impl->storages = hashmap_new(16, hashmap_string_hash, hashmap_string_cmpr);
    if (!impl->storages) THROW(e, "Failed to create storages map");

    // Allocate batch buffer
    impl->batch_buffer = CALLOC(1, impl->batch_capacity);
    if (!impl->batch_buffer) THROW(e, "Failed to allocate batch buffer");
    impl->batch_size = 0;
    impl->batch_count = 0;

    // Open WAL file with platform-specific optimizations
    int open_flags = O_RDWR | O_CREAT;
    
    impl->fd = open(path, open_flags, 0644);
    if (impl->fd < 0) {
        FREE(impl->batch_buffer);
        THROW(e, "Failed to open WAL file: %s", strerror(errno));
    }
    
    // Platform-specific I/O initialization and optimization hints
#ifdef __linux__
    // Linux: Use io_uring for async I/O performance
    // Note: O_DIRECT is not used because it requires strict alignment that
    // is difficult to maintain with variable-sized WAL records
    wal_io_init_linux(impl);
    // Advise kernel about sequential write pattern
    posix_fadvise(impl->fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
#ifdef __APPLE__
    // macOS: Use F_NOCACHE to avoid caching WAL writes
    fcntl(impl->fd, F_NOCACHE, 1);
    wal_io_init_macos(impl);
#endif
    
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
#ifdef __linux__
        wal_io_cleanup_linux(impl);
#endif
#ifdef __APPLE__
        wal_io_cleanup_macos(impl);
#endif
        if (impl->fd > 0) close(impl->fd);
        FREE(impl);
    }
    if (w) FREE(w);
    return NULL;
}
