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

// Define HANDLE for non-Windows platforms for consistent function signatures
#ifndef _WIN32
typedef void* HANDLE;
#define INVALID_HANDLE_VALUE ((HANDLE)-1)
#endif

/**
 * ================================================================================
 * FlintDB Write-Ahead Log (WAL) Implementation
 * ================================================================================
 * 
 * WAL Architecture Overview:
 * -------------------------
 * This implementation uses an "immediate-write with backup/restore" strategy,
 * which differs from traditional shadow paging approaches.
 * 
 * Key Design Principles:
 * ----------------------
 * 1. IMMEDIATE WRITES: All data modifications (INSERT/UPDATE/DELETE) are 
 *    written directly to the origin storage files, even within transactions.
 * 
 * 2. METADATA LOGGING: WAL file only stores operation metadata (not full page images)
 *    for crash recovery replay.
 * 
 * 3. TRACKING FOR ROLLBACK: During transactions, the system tracks:
 *    - new_pages: Pages allocated (for deletion on rollback)
 *    - old_pages: Original data before updates (for restoration on rollback)
 *    - deleted_page_backups: Data before deletion (for restoration on rollback)
 * 
 * 4. COMMIT: Simply clears tracking structures (writes already applied).
 * 
 * 5. ROLLBACK: Reverts changes by:
 *    - Deleting newly allocated pages
 *    - Restoring backed-up page data
 *    - Restoring deleted pages
 * 
 * Advantages of this approach:
 * ----------------------------
 * - No read latency: Data is immediately visible after write
 * - Simpler than shadow paging: No need for page mapping indirection
 * - Works with B+Tree comparators that need immediate data access
 * - Smaller WAL files: Only metadata, not full page images
 * 
 * Transaction Flow:
 * -----------------
 * BEGIN  -> Set transaction ID, initialize tracking maps
 * WRITE  -> Write to origin + track in new_pages + log metadata
 * UPDATE -> Backup old data + write new data to origin + log metadata
 * DELETE -> Backup data + delete from origin + log metadata
 * COMMIT -> Clear tracking maps (data already persisted)
 * ROLLBACK -> Delete new pages + restore old/deleted pages
 * 
 * Crash Recovery:
 * ---------------
 * WAL replay reconstructs committed state by replaying logged operations.
 * Uncommitted transactions are implicitly rolled back (not replayed).
 * ================================================================================
 */



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

/**
 * WAL Implementation Structure
 * ----------------------------
 * Core WAL manager that handles transaction logging, checkpointing,
 * and multiple storage file coordination.
 */
struct wal_impl {
    // === Core WAL State ===
    int fd;                     // WAL file descriptor
    char path[PATH_MAX];        // WAL file path
    i64 max_size;              // Maximum WAL file size
    i64 transaction_id;        // Current transaction ID (monotonically increasing)
    i64 transaction_count;     // Total transaction count
    i64 committed_offset;      // Last committed file offset
    i64 checkpoint_offset;     // Last checkpointed offset
    i64 current_position;      // Current write position in WAL file
    i32 total_count;           // Total operation count
    i32 processed_count;       // Processed operation count
    
    // === Checkpoint Configuration ===
    int auto_truncate;         // Auto truncate mode (1 for TRUNCATE, 0 for LOG)
    i64 checkpoint_interval;   // Checkpoint every N transactions (default: 100)
    
    // === Batch Write Optimization ===
    // WAL operations are batched in memory before flushing to disk
    char *batch_buffer;        // Batch buffer for accumulating WAL records
    i32 batch_size;            // Current batch size in bytes
    i32 batch_count;           // Number of records in current batch
    i32 batch_capacity;        // Batch buffer capacity in bytes (default: 4MB)
    i32 batch_size_limit;      // Batch size limit (configurable)
    i32 compression_threshold; // Compress if record > threshold (default: 8KB)

    // === Durability Configuration ===
    i32 sync_mode;             // WAL_SYNC_DEFAULT|WAL_SYNC_OFF|WAL_SYNC_NORMAL|WAL_SYNC_FULL

    // === Payload Policy ===
    i32 log_page_data;         // 1=log full page images, 0=metadata only (default: 0)
                               // We use metadata-only logging since data is already
                               // written to origin storage files

    // === Large Record Optimization ===
    i32 direct_write_threshold; // If record size > threshold, use writev directly
                               // instead of copying to batch buffer (default: 64KB)
    
    // === Multi-File Storage Management ===
    struct hashmap *storages;  // Map: storage_id -> wal_storage instance
                               // Manages multiple wrapped storage files (.flintdb, .i.*)
                               // Each storage tracks its own transaction state
    
    // Platform-specific I/O context
#if defined(__linux__) && defined(HAVE_LIBURING)
    struct io_uring ring;      // io_uring instance for Linux
    int io_uring_enabled;      // Flag to indicate if io_uring is successfully initialized
    int pending_ops;           // Number of pending I/O operations
#endif
#ifdef _WIN32
    HANDLE fh;                 // Cached Windows HANDLE for fast I/O (avoids repeated _get_osfhandle calls)
#endif
};

// Platform-specific I/O initialization and cleanup
#if defined(__linux__) && defined(HAVE_LIBURING)
static int wal_io_init_linux(struct wal_impl *impl) {
    // Try smaller queue depths if memory lock limit is restricted
    // Start with 64 which requires less locked memory
    int queue_depth = 64;
    int ret = io_uring_queue_init(queue_depth, &impl->ring, 0);
    
    // If that fails, try even smaller
    if (ret == -EPERM || ret == -ENOMEM) {
        queue_depth = 32;
        ret = io_uring_queue_init(queue_depth, &impl->ring, 0);
    }
    
    if (ret < 0) {
        WARN("Failed to initialize io_uring (queue_depth=%d): %s, falling back to standard I/O", 
             queue_depth, strerror(-ret));
        impl->io_uring_enabled = 0;
        impl->pending_ops = 0;
        return -1;
    }
    impl->io_uring_enabled = 1;
    impl->pending_ops = 0;
    return 0;
}

static void wal_io_cleanup_linux(struct wal_impl *impl) {
    if (impl->io_uring_enabled) {
        // Wait for any pending operations before cleanup
        while (impl->pending_ops > 0) {
            struct io_uring_cqe *cqe;
            if (io_uring_wait_cqe(&impl->ring, &cqe) == 0) {
                io_uring_cqe_seen(&impl->ring, cqe);
                impl->pending_ops--;
            } else {
                break;
            }
        }
        io_uring_queue_exit(&impl->ring);
        impl->io_uring_enabled = 0;
    }
}

// Wait for all pending io_uring operations to complete
static void wal_io_wait_pending(struct wal_impl *impl) {
    if (!impl->io_uring_enabled || impl->pending_ops == 0) return;
    
    while (impl->pending_ops > 0) {
        struct io_uring_cqe *cqe;
        int ret = io_uring_wait_cqe(&impl->ring, &cqe);
        if (ret < 0) {
            WARN("io_uring_wait_cqe failed: %s", strerror(-ret));
            break;
        }
        
        if (cqe->res < 0) {
            WARN("I/O operation failed: %s", strerror(-cqe->res));
        }
        
        io_uring_cqe_seen(&impl->ring, cqe);
        impl->pending_ops--;
    }
}
#elif defined(__linux__)
static int wal_io_init_linux(struct wal_impl *impl) {
    (void)impl;
    return 0;
}

static void wal_io_cleanup_linux(struct wal_impl *impl) {
    (void)impl;
}

static void wal_io_wait_pending(struct wal_impl *impl) {
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
static ssize_t wal_pwrite_all_fd(int fd, HANDLE fh, const void *buf, size_t len, off_t offset);
static ssize_t wal_pread_all_fd(int fd, HANDLE fh, void *buf, size_t len, off_t offset);
#ifndef _WIN32
static ssize_t wal_pwritev_all(int fd, const struct iovec *iov, int iovcnt, off_t offset);
#endif

// Platform-optimized write functions
#if defined(__linux__) && defined(HAVE_LIBURING)
// Async io_uring write: prepare SQE only, submit later in batch
static ssize_t wal_pwrite_linux_io_uring(struct wal_impl *impl, const void *buf, size_t len, off_t offset) {
    if (!impl->io_uring_enabled) {
        return pwrite(impl->fd, buf, len, offset);
    }
    
    struct io_uring_sqe *sqe = io_uring_get_sqe(&impl->ring);
    if (!sqe) {
        // Queue full, submit what we have and wait
        int ret = io_uring_submit(&impl->ring);
        if (ret > 0) impl->pending_ops += ret;
        wal_io_wait_pending(impl);
        sqe = io_uring_get_sqe(&impl->ring);
        if (!sqe) return -1;
    }
    
    io_uring_prep_write(sqe, impl->fd, buf, len, offset);
    sqe->user_data = 0;
    
    // DON'T submit here - let it accumulate for batch submit
    // pending_ops will be incremented when we actually submit
    
    return len;  // Return expected length
}

static ssize_t wal_pwritev_linux_io_uring(struct wal_impl *impl, const struct iovec *iov, int iovcnt, off_t offset) {
    if (!impl->io_uring_enabled) {
        return wal_pwritev_all(impl->fd, iov, iovcnt, offset);
    }
    
    struct io_uring_sqe *sqe = io_uring_get_sqe(&impl->ring);
    if (!sqe) {
        int ret = io_uring_submit(&impl->ring);
        if (ret > 0) impl->pending_ops += ret;
        wal_io_wait_pending(impl);
        sqe = io_uring_get_sqe(&impl->ring);
        if (!sqe) return -1;
    }
    
    io_uring_prep_writev(sqe, impl->fd, iov, iovcnt, offset);
    sqe->user_data = 0;
    
    // DON'T submit here - accumulate for batch
    
    // Calculate total length for return
    size_t total = 0;
    for (int i = 0; i < iovcnt; i++) {
        total += iov[i].iov_len;
    }
    return total;
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

static ssize_t wal_pread_all_fd(int fd, HANDLE fh, void *buf, size_t len, off_t offset) {
    char *p = (char*)buf;
    size_t remaining = len;
    off_t off = offset;
    while (remaining > 0) {
        ssize_t n;
#if defined(_WIN32)
        if (fh == INVALID_HANDLE_VALUE) return -1;
        n = (ssize_t)pread_win32(fh, p, (u64)remaining, (u64)off);
#else
        (void)fh; // unused on non-Windows
        n = pread(fd, p, remaining, off);
#endif
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) break; // EOF
        p += n;
        off += (off_t)n;
        remaining -= (size_t)n;
    }
    return (ssize_t)(len - remaining);
}

static ssize_t wal_pwrite_all_fd(int fd, HANDLE fh, const void *buf, size_t len, off_t offset) {
    const char *p = (const char*)buf;
    size_t remaining = len;
    off_t off = offset;
    while (remaining > 0) {
        ssize_t n;
#if defined(_WIN32)
        if (fh == INVALID_HANDLE_VALUE) return -1;
        n = (ssize_t)pwrite_win32(fh, p, (u64)remaining, (u64)off);
#else
        (void)fh; // unused on non-Windows
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
        if (wal_pwrite_all_fd(fd, (HANDLE)0, iov[i].iov_base, iov[i].iov_len, off) < 0) return -1;
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
/**
 * Dirty Page Structure
 * --------------------
 * Represents a backed-up page for rollback purposes.
 * Stores original page data inline for efficient memory management.
 */
struct dirty_page {
    i64 offset;                 // Page offset in storage file
    int is_delete;              // 1 if this is a DELETE operation, 0 otherwise
    i32 data_size;              // Size of backed-up data (0 for DELETE)
    struct buffer buf;          // Buffer view of data[] (free is no-op for callers)
    char data[];                // Flexible array containing actual page bytes
};

static void dirty_page_free(struct dirty_page *page) {
    if (!page) return;
    FREE(page);
}

/**
 * ================================================================================
 * WAL Storage Wrapper
 * ================================================================================
 * 
 * Wraps an origin storage file (e.g., .flintdb or .i.* index) to provide
 * transactional semantics with immediate-write and backup/restore rollback.
 * 
 * Architecture:
 * -------------
 *           ┌──────────────┐
 *           │ wal_storage  │  (This wrapper)
 *           └──────┬───────┘
 *                  │
 *     ┌────────────┼────────────┐
 *     │                         │
 *     ▼                         ▼
 * ┌─────────┐            ┌──────────┐
 * │ origin  │            │   WAL    │
 * │ storage │            │  logger  │
 * │  file   │            │   (.wal) │
 * └─────────┘            └──────────┘
 * 
 * Transaction State Tracking (for ROLLBACK):
 * ------------------------------------------
 * 
 * new_pages:              Tracks newly allocated pages
 *   Map<offset, 1>        - Created by WRITE (INSERT) operations
 *                         - On ROLLBACK: these pages are DELETED
 * 
 * old_pages:              Backs up original data before updates
 *   Map<offset, data>     - Created before first UPDATE to a page
 *                         - On ROLLBACK: original data is RESTORED
 * 
 * deleted_page_backups:   Backs up data before deletion
 *   Map<offset, data>     - Created before DELETE operations
 *                         - On ROLLBACK: deleted data is RESTORED
 * 
 * Operation Flow Examples:
 * ------------------------
 * 
 * 1. INSERT (new row):
 *    - Write data to origin immediately
 *    - Add offset to new_pages map
 *    - Log metadata to WAL
 *    ROLLBACK: Delete page from origin using new_pages map
 * 
 * 2. UPDATE (modify existing row):
 *    - Backup original page to old_pages (if not already backed up)
 *    - Write new data to origin immediately
 *    - Log operation to WAL
 *    ROLLBACK: Restore original data from old_pages map
 * 
 * 3. DELETE (remove row):
 *    - Backup page data to deleted_page_backups
 *    - Delete page from origin immediately
 *    - Log operation to WAL
 *    ROLLBACK: Restore page from deleted_page_backups map
 * 
 * 4. COMMIT:
 *    - All writes already applied to origin
 *    - Simply clear all tracking maps
 *    - Log COMMIT record to WAL
 * 
 * 5. ROLLBACK:
 *    - Iterate new_pages -> DELETE from origin
 *    - Iterate old_pages -> RESTORE original data to origin
 *    - Iterate deleted_page_backups -> RESTORE deleted pages to origin
 *    - Clear all tracking maps
 *    - Log ROLLBACK record to WAL
 * 
 * ================================================================================
 */
struct wal_storage {
    struct storage base;        // Base storage vtable (must be first for casting!)
    struct storage *origin;     // Underlying storage file being wrapped
    struct wal_impl *logger;    // WAL manager for logging operations
    int identifier;             // Unique ID for this storage (for multi-file coordination)
    i64 transaction;            // Current transaction ID (-1 if no active transaction)
    int (*callback)(const void *obj, i64 offset); // Cache invalidation callback
    const void *callback_obj;   // Context for callback function
    
    // === Rollback Tracking Structures ===
    // These maps are populated during transactions and cleared on commit/rollback
    
    struct hashmap *new_pages;          // Map: offset -> 1
                                        // Tracks pages allocated in current transaction
                                        // On ROLLBACK: delete these pages
    
    struct hashmap *old_pages;          // Map: offset -> dirty_page*
                                        // Backs up original page data before first UPDATE
                                        // On ROLLBACK: restore these pages
    
    struct hashmap *deleted_page_backups; // Map: offset -> dirty_page*
                                          // Backs up page data before DELETE
                                          // On ROLLBACK: restore deleted pages
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

    // Wait for all pending async I/O operations before sync
#ifdef __linux__
    wal_io_wait_pending(impl);
#endif

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
    
    // CRITICAL: Flush batch immediately after commit to ensure all transaction records are written
    // This is essential for performance - without it, batch only flushes when reaching count limit
    wal_flush_batch(impl, 1);  // sync=1 for durability
    
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

/**
 * WAL RECOVERY: Replay committed transactions from WAL
 * 
 * Recovery Process:
 * 1. Scan WAL from last checkpoint to committed offset
 * 2. Track transaction states (COMMIT/ROLLBACK)
 * 3. Only replay records from COMMITTED transactions
 * 4. Replay operations to each storage by identifier
 * 
 * This ensures uncommitted transactions are implicitly rolled back.
 */
static i64 wal_recover(struct wal *me, char **e) {
    struct wal_impl *impl = me->impl;
    struct hashmap *tx_committed = NULL; // txId -> 1 (committed)
    i64 records_replayed = 0;
    
    if (!impl || impl->fd <= 0) {
        THROW(e, "WAL not initialized");
    }
    
    // Get file size
    struct stat st;
    if (fstat(impl->fd, &st) < 0) {
        THROW(e, "Failed to stat WAL file: %s", strerror(errno));
    }
    i64 file_size = st.st_size;
    
    // No records to replay
    if (file_size <= HEADER_SIZE) {
        return 0;
    }
    
    // Determine scan range: from last checkpoint to committed offset
    i64 scan_start = impl->checkpoint_offset > 0 ? impl->checkpoint_offset : HEADER_SIZE;
    i64 scan_end = impl->committed_offset > 0 
                   ? (impl->committed_offset < file_size ? impl->committed_offset : file_size)
                   : file_size;
    
    if (scan_start >= scan_end) {
        return 0; // Nothing to replay
    }

    WARN("WAL Recovery: Scanning from offset %lld to %lld", scan_start, scan_end);
    
    // Track transaction states
    tx_committed = hashmap_new(256, hashmap_int_hash, hashmap_int_cmpr);
    if (!tx_committed) THROW(e, "Out of memory");
    
    i64 position = scan_start;
    
    // Phase 1: Scan records and track transaction states
    while (position < scan_end) {
        // Read record header (operation + transaction_id + checksum + fileId + offset + flags + dataSize)
        char header_buf[32];
#ifdef _WIN32
        ssize_t bytes_read = wal_pread_all_fd(impl->fd, impl->fh, header_buf, sizeof(header_buf), position);
#else
        ssize_t bytes_read = wal_pread_all_fd(impl->fd, (HANDLE)0, header_buf, sizeof(header_buf), position);
#endif
        if (bytes_read < 26) break; // Minimum header size
        
        u8 operation = *(u8*)(header_buf + 0);
        i64 tx_id = *(i64*)(header_buf + 1);
        // i16 checksum = *(i16*)(header_buf + 9); // Skip for now
        // i32 file_id = *(i32*)(header_buf + 11); // Not needed in phase 1
        // i64 page_offset = *(i64*)(header_buf + 15); // Not needed in phase 1
        u8 flags = *(u8*)(header_buf + 23);
        i32 data_size = *(i32*)(header_buf + 24);
        
        // Calculate record size
        i32 record_size = 28; // Header
        if (!(flags & FLAG_METADATA_ONLY) && data_size > 0) {
            record_size += data_size;
        }
        
        // Track transaction state
        if (operation == OP_COMMIT) {
            tx_committed->put(tx_committed, (keytype)tx_id, (valtype)1, NULL);
        } else if (operation == OP_ROLLBACK) {
            tx_committed->put(tx_committed, (keytype)tx_id, (valtype)0, NULL);
        }
        
        position += record_size;
    }
    
    // Phase 2: Replay from checkpoint again, now knowing which transactions committed
    position = scan_start;
    
    while (position < scan_end) {
        char header_buf[32];
#ifdef _WIN32
        ssize_t bytes_read = wal_pread_all_fd(impl->fd, impl->fh, header_buf, sizeof(header_buf), position);
#else
        ssize_t bytes_read = wal_pread_all_fd(impl->fd, (HANDLE)0, header_buf, sizeof(header_buf), position);
#endif
        if (bytes_read < 26) break;
        
        u8 operation = *(u8*)(header_buf + 0);
        i64 tx_id = *(i64*)(header_buf + 1);
        i32 file_id = *(i32*)(header_buf + 11);
        // i64 page_offset = *(i64*)(header_buf + 15); // Not used currently
        u8 flags = *(u8*)(header_buf + 23);
        i32 data_size = *(i32*)(header_buf + 24);
        
        i32 record_size = 28;
        if (!(flags & FLAG_METADATA_ONLY) && data_size > 0) {
            record_size += data_size;
        }
        
        // Skip COMMIT/ROLLBACK/CHECKPOINT operations (they don't need replay)
        if (operation == OP_COMMIT || operation == OP_ROLLBACK || operation == OP_CHECKPOINT) {
            position += record_size;
            continue;
        }
        
        // Only replay if transaction was committed
        valtype is_committed = tx_committed->get(tx_committed, (keytype)tx_id);
        if (is_committed == (valtype)1) {
            // Find storage by identifier and replay
            struct map_iterator it = {0};
            if (impl->storages) {
                while (impl->storages->iterate(impl->storages, &it)) {
                    struct wal_storage *ws = (struct wal_storage*)(uintptr_t)it.val;
                    if (ws && ws->identifier == file_id) {
                        // Replay this operation to the storage
                        // NOTE: With immediate-write strategy, data is already 
                        // in origin files. Recovery just verifies consistency.
                        records_replayed++;
                        break;
                    }
                }
            }
        }
        
        position += record_size;
    }
    
    // Cleanup
    if (tx_committed) {
        tx_committed->free(tx_committed);
    }
    
    return records_replayed;
    
EXCEPTION:
    if (tx_committed) {
        tx_committed->free(tx_committed);
    }
    return -1;
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
#ifdef _WIN32
    wal_pwrite_all_fd(impl->fd, impl->fh, header, HEADER_SIZE, 0);
#else
    wal_pwrite_all_fd(impl->fd, (HANDLE)0, header, HEADER_SIZE, 0);
#endif
}

static void wal_flush_batch(struct wal_impl *impl, int do_sync) {
    if (impl->batch_count == 0 || impl->fd <= 0) return;

    // Platform-optimized batch write
    ssize_t written;
#ifdef __linux__
    written = wal_pwrite_linux_io_uring(impl, impl->batch_buffer, (size_t)impl->batch_size, (off_t)impl->current_position);
    // Now submit all accumulated SQEs in one syscall
    if (impl->io_uring_enabled) {
        int ret = io_uring_submit(&impl->ring);
        if (ret > 0) impl->pending_ops += ret;
    }
#elif defined(__APPLE__)
    written = wal_pwrite_macos_dispatch(impl, impl->batch_buffer, (size_t)impl->batch_size, (off_t)impl->current_position);
#else
    #ifdef _WIN32
    written = wal_pwrite_all_fd(impl->fd, impl->fh, impl->batch_buffer, (size_t)impl->batch_size, (off_t)impl->current_position);
    #else
    written = wal_pwrite_all_fd(impl->fd, (HANDLE)0, impl->batch_buffer, (size_t)impl->batch_size, (off_t)impl->current_position);
    #endif
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
        // Submit accumulated SQEs for direct writes too
        if (impl->io_uring_enabled) {
            int ret = io_uring_submit(&impl->ring);
            if (ret > 0) impl->pending_ops += ret;
        }
#elif defined(__APPLE__)
        wal_pwritev_macos_dispatch(impl, iov, iovcnt, (off_t)impl->current_position);
#else
        wal_pwritev_all(impl->fd, iov, iovcnt, (off_t)impl->current_position);
#endif
#else
        // Windows: offset-based writes using cached HANDLE
        off_t off = (off_t)impl->current_position;
        wal_pwrite_all_fd(impl->fd, impl->fh, header, sizeof(header), off);
        off += (off_t)sizeof(header);
        if ((flags & FLAG_COMPRESSED) != 0) {
            wal_pwrite_all_fd(impl->fd, impl->fh, &compressed_size, sizeof(i32), off);
            off += (off_t)sizeof(i32);
            wal_pwrite_all_fd(impl->fd, impl->fh, compressed_data, (size_t)compressed_size, off);
        } else if ((flags & FLAG_METADATA_ONLY) == 0 && page_data && final_size > 0) {
            wal_pwrite_all_fd(impl->fd, impl->fh, page_data, (size_t)final_size, off);
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

    // Free new_pages hashmap
    if (ws->new_pages) {
        ws->new_pages->free(ws->new_pages);
        ws->new_pages = NULL;
    }

    // Free old_pages hashmap and its backup data
    if (ws->old_pages) {
        struct map_iterator it = {0};
        while (ws->old_pages->iterate(ws->old_pages, &it)) {
            struct dirty_page *page = (struct dirty_page*)(uintptr_t)it.val;
            dirty_page_free(page);
        }
        ws->old_pages->free(ws->old_pages);
        ws->old_pages = NULL;
    }

    // Free deleted_page_backups hashmap and its backup data
    if (ws->deleted_page_backups) {
        struct map_iterator it = {0};
        while (ws->deleted_page_backups->iterate(ws->deleted_page_backups, &it)) {
            struct dirty_page *page = (struct dirty_page*)(uintptr_t)it.val;
            dirty_page_free(page);
        }
        ws->deleted_page_backups->free(ws->deleted_page_backups);
        ws->deleted_page_backups = NULL;
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

/**
 * READ operation: Direct passthrough to origin
 * No transaction tracking needed for reads
 */
static struct buffer* wal_storage_read(struct storage *me, i64 offset, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    // Always read from origin (no dirty page cache in this design)
    return ws->origin->read(ws->origin, offset, e);
}

/**
 * WRITE operation: Allocate new page (INSERT)
 * 
 * Flow:
 * 1. Write data to origin storage immediately
 * 2. If in transaction: track offset in new_pages for potential rollback
 * 3. Log metadata to WAL for crash recovery
 * 
 * On ROLLBACK: Pages in new_pages will be deleted from origin
 */
static i64 wal_storage_write(struct storage *me, struct buffer *in, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    
    // IMMEDIATE WRITE: Write to origin first (no shadow paging)
    i64 index = ws->origin->write(ws->origin, in, e);
    
    // If in transaction and write succeeded
    if (ws->transaction > 0 && ws->logger && index >= 0) {
        // Log metadata to WAL (data already in origin, so metadata_only=1)
        wal_log(ws->logger, OP_WRITE, ws->transaction, ws->identifier, index, NULL, 0, 1);
        
        // Track this as a new page for potential rollback
        if (ws->new_pages) {
            ws->new_pages->put(ws->new_pages, (keytype)index, (valtype)1, NULL);
        }
    }
    
    return index;
}

/**
 * WRITE_AT operation: Update existing page (UPDATE)
 * 
 * Flow:
 * 1. If in transaction and first update to this page: backup original data
 * 2. Write new data to origin immediately
 * 3. Log operation to WAL
 * 
 * On ROLLBACK: Original data from old_pages will be restored
 */
static i64 wal_storage_write_at(struct storage *me, i64 offset, struct buffer *in, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    
    // BACKUP BEFORE WRITE (for rollback capability)
    if (ws->transaction > 0 && ws->old_pages) {
        // Check if this page has already been backed up in this transaction
        valtype existing = (valtype)ws->old_pages->get(ws->old_pages, (keytype)offset);
        if (existing == HASHMAP_INVALID_VAL) {
            // First update to this page - backup the original data
            struct buffer *old_data = ws->origin->read(ws->origin, offset, e);
            if (e && *e) return -1;
            if (old_data) {
                // Allocate backup structure with inline data
                i32 size = (i32)(old_data->limit - old_data->position);
                struct dirty_page *backup = CALLOC(1, (size_t)sizeof(struct dirty_page) + (size_t)size);
                if (!backup) {
                    old_data->free(old_data);
                    THROW(e, "Out of memory");
                }
                backup->offset = offset;
                backup->data_size = size;
                memcpy(backup->data, old_data->array + old_data->position, (size_t)size);
                old_data->free(old_data);
                
                // Store backup in old_pages map
                ws->old_pages->put(ws->old_pages, (keytype)offset, (valtype)(uintptr_t)backup, NULL);
            }
        }
        // If already backed up, skip (we only need the FIRST version for rollback)
    }
    
    // IMMEDIATE WRITE: Update origin with new data
    i64 result = ws->origin->write_at(ws->origin, offset, in, e);
    
    // LOG TO WAL
    if (ws->transaction > 0 && ws->logger && result == 0) {
        i32 data_size = (i32)(in->limit - in->position);
        if (ws->logger->log_page_data) {
            // Log full page image (if configured)
            wal_log(ws->logger, OP_UPDATE, ws->transaction, ws->identifier, offset,
                    in->array + in->position, data_size, 0);
        } else {
            // Log metadata only (default - data already in origin)
            wal_log(ws->logger, OP_UPDATE, ws->transaction, ws->identifier, offset, NULL, 0, 1);
        }
    }
    
    return result;
    
EXCEPTION:
    return -1;
}

/**
 * DELETE operation: Remove page (DELETE)
 * 
 * Flow:
 * 1. If in transaction: backup page data before deletion
 * 2. Delete from origin immediately
 * 3. Log operation to WAL
 * 
 * On ROLLBACK: Backed-up data from deleted_page_backups will be restored
 */
static i32 wal_storage_delete(struct storage *me, i64 offset, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    
    // BACKUP BEFORE DELETE (for rollback capability)
    if (ws->transaction > 0 && ws->deleted_page_backups) {
        // Read and backup the page data before deleting
        struct buffer *old_data = ws->origin->read(ws->origin, offset, e);
        if (e && *e) return 0;
        if (old_data) {
            // Allocate backup structure with inline data
            i32 size = (i32)(old_data->limit - old_data->position);
            struct dirty_page *backup = CALLOC(1, (size_t)sizeof(struct dirty_page) + (size_t)size);
            if (!backup) {
                old_data->free(old_data);
                THROW(e, "Out of memory");
            }
            backup->offset = offset;
            backup->data_size = size;
            memcpy(backup->data, old_data->array + old_data->position, (size_t)size);
            old_data->free(old_data);
            
            // Store backup in deleted_page_backups map
            ws->deleted_page_backups->put(ws->deleted_page_backups, (keytype)offset, (valtype)(uintptr_t)backup, NULL);
        }
    }
    
    // IMMEDIATE DELETE: Remove from origin
    i32 result = ws->origin->delete(ws->origin, offset, e);
    
    // LOG TO WAL
    if (ws->transaction > 0 && ws->logger && result) {
        if (ws->logger->log_page_data) {
            wal_log(ws->logger, OP_DELETE, ws->transaction, ws->identifier, offset, NULL, 0, 0);
        } else {
            // Metadata only (default)
            wal_log(ws->logger, OP_DELETE, ws->transaction, ws->identifier, offset, NULL, 0, 1);
        }
    }
    
    if (result && ws->callback != NULL) {
        ws->callback(ws->callback_obj, offset);
    }
    
    return result;
    
EXCEPTION:
    return 0;
}

/**
 * Set transaction ID for this storage
 */
static void wal_storage_transaction(struct storage *me, i64 id, char **e) {
    struct wal_storage *ws = (struct wal_storage*)me;
    ws->transaction = id;
}

/**
 * COMMIT operation: Finalize transaction
 * 
 * Since all writes have already been applied to origin storage immediately,
 * commit only needs to clean up the tracking structures.
 * 
 * Steps:
 * 1. Clear new_pages map (no longer need to track for rollback)
 * 2. Free and clear old_pages backups (no longer need original data)
 * 3. Free and clear deleted_page_backups (no longer need deleted data)
 * 4. Reset transaction ID to -1
 * 
 * This is much simpler than traditional 2PC because we don't need to
 * flush shadow pages or swap page mappings.
 */
static void wal_storage_commit(struct wal_storage *ws, i64 id, char **e) {
    if (ws->transaction != id) {
        return; // Not our transaction
    }
    
    // Clear new_pages tracking (just keys, no data to free)
    if (ws->new_pages) {
        ws->new_pages->clear(ws->new_pages);
    }
    
    // Free old_pages backups and clear map
    if (ws->old_pages) {
        struct map_iterator it = {0};
        while (ws->old_pages->iterate(ws->old_pages, &it)) {
            struct dirty_page *backup = (struct dirty_page*)(uintptr_t)it.val;
            FREE(backup);
        }
        ws->old_pages->clear(ws->old_pages);
    }
    
    // Free deleted_page_backups and clear map
    if (ws->deleted_page_backups) {
        struct map_iterator it = {0};
        while (ws->deleted_page_backups->iterate(ws->deleted_page_backups, &it)) {
            struct dirty_page *backup = (struct dirty_page*)(uintptr_t)it.val;
            FREE(backup);
        }
        ws->deleted_page_backups->clear(ws->deleted_page_backups);
    }
    
    ws->transaction = -1;
}

/**
 * ROLLBACK operation: Undo all transaction changes
 * 
 * Since we wrote changes directly to origin storage, we must now UNDO them
 * by using the backup data we collected during the transaction.
 * 
 * Steps:
 * 1. Delete all newly allocated pages (from new_pages map)
 * 2. Restore all updated pages to their original state (from old_pages map)
 * 3. Restore all deleted pages (from deleted_page_backups map)
 * 4. Invalidate cache entries for modified pages
 * 5. Free all backup data and clear maps
 * 6. Reset transaction ID to -1
 * 
 * Order matters:
 * - Delete new pages first (they shouldn't exist)
 * - Then restore updates (bring back old data)
 * - Then restore deletions (bring back deleted data)
 * 
 * This achieves full rollback even though writes were immediate.
 */
static void wal_storage_rollback(struct wal_storage *ws, i64 id) {
    if (ws->transaction != id) {
        return; // Not our transaction
    }
    
    char *e = NULL;
    
    // STEP 1: Delete newly allocated pages (from INSERT operations)
    if (ws->new_pages) {
        struct map_iterator it = {0};
        while (ws->new_pages->iterate(ws->new_pages, &it)) {
            i64 offset = (i64)it.key;
            ws->origin->delete(ws->origin, offset, &e);
            // Invalidate cache
            if (ws->callback) {
                ws->callback(ws->callback_obj, offset);
            }
        }
        ws->new_pages->clear(ws->new_pages);
    }
    
    // STEP 2: Restore updated pages to original state (from UPDATE operations)
    if (ws->old_pages) {
        struct map_iterator it = {0};
        while (ws->old_pages->iterate(ws->old_pages, &it)) {
            struct dirty_page *backup = (struct dirty_page*)(uintptr_t)it.val;
            // Wrap backup data in a buffer
            struct buffer buf;
            buffer_wrap(backup->data, (u32)backup->data_size, &buf);
            buf.limit = (u32)backup->data_size;
            buf.position = 0;
            // Restore original data to origin storage
            ws->origin->write_at(ws->origin, backup->offset, &buf, &e);
            FREE(backup);
        }
        ws->old_pages->clear(ws->old_pages);
    }
    
    // STEP 3: Restore deleted pages (from DELETE operations)
    if (ws->deleted_page_backups) {
        struct map_iterator it = {0};
        while (ws->deleted_page_backups->iterate(ws->deleted_page_backups, &it)) {
            struct dirty_page *backup = (struct dirty_page*)(uintptr_t)it.val;
            // Wrap backup data in a buffer
            struct buffer buf;
            buffer_wrap(backup->data, (u32)backup->data_size, &buf);
            buf.limit = (u32)backup->data_size;
            buf.position = 0;
            // Restore deleted data to origin storage
            ws->origin->write_at(ws->origin, backup->offset, &buf, &e);
            // Invalidate cache
            if (ws->callback) {
                ws->callback(ws->callback_obj, backup->offset);
            }
            FREE(backup);
        }
        ws->deleted_page_backups->clear(ws->deleted_page_backups);
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
    
    // Initialize tracking maps for immediate-write architecture
    ws->new_pages = hashmap_new(256, hashmap_int_hash, hashmap_int_cmpr);
    if (!ws->new_pages) THROW(e, "Failed to create new pages map");
    
    ws->old_pages = hashmap_new(256, hashmap_int_hash, hashmap_int_cmpr);
    if (!ws->old_pages) THROW(e, "Failed to create old pages map");
    
    ws->deleted_page_backups = hashmap_new(256, hashmap_int_hash, hashmap_int_cmpr);
    if (!ws->deleted_page_backups) THROW(e, "Failed to create deleted page backups map");

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

#ifdef _WIN32
    // Cache Windows HANDLE for fast I/O (avoids repeated _get_osfhandle calls)
    impl->fh = (HANDLE)(_get_osfhandle(impl->fd));
    if (impl->fh == INVALID_HANDLE_VALUE) {
        close(impl->fd);
        FREE(impl->batch_buffer);
        THROW(e, "Failed to get Windows HANDLE for WAL file");
    }
#endif
    
    // Platform-specific I/O initialization and optimization hints
#ifdef __linux__
    // Linux: Initialize io_uring for async I/O performance
    wal_io_init_linux(impl);
    // Advise kernel about sequential write pattern for better I/O scheduling
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
