# WAL Platform-Specific I/O Optimization

## Overview

The WAL (Write-Ahead Logging) implementation in `wal.c` has been optimized with platform-specific I/O strategies to maximize performance while maintaining cross-platform compatibility.

## Platform Strategies

### Linux
- **io_uring**: Modern asynchronous I/O interface for high-performance operations
- **Sequential I/O hints**: `POSIX_FADV_SEQUENTIAL` to optimize kernel caching behavior
- **Note**: O_DIRECT is not used due to strict alignment requirements that conflict with variable-sized WAL records
  - Queue depth: 256 operations
  - Supports both single writes and vectored writes (writev)
  - Graceful fallback to standard pwrite if io_uring initialization fails

### macOS
- **F_NOCACHE**: Bypass unified buffer cache for WAL writes
- Uses standard `pwrite()` with F_NOCACHE flag
- Simple and reliable approach without requiring Objective-C blocks or dispatch framework

### Windows (MinGW)
- **No changes**: Maintains existing implementation
- Uses standard file I/O with pwrite compatibility layer
- Proven stable and reliable on Windows platform

## Implementation Details

### Key Functions

#### Platform-Specific Write Functions

**Linux (io_uring)**
```c
static ssize_t wal_pwrite_linux_io_uring(struct wal_impl *impl, const void *buf, size_t len, off_t offset)
static ssize_t wal_pwritev_linux_io_uring(struct wal_impl *impl, const struct iovec *iov, int iovcnt, off_t offset)
```
- Submits I/O operations to io_uring
- Waits for completion synchronously (suitable for WAL's durability requirements)
- Falls back to standard pwrite if io_uring not available

**macOS (F_NOCACHE)**
```c
static ssize_t wal_pwrite_macos_dispatch(struct wal_impl *impl, const void *buf, size_t len, off_t offset)
static ssize_t wal_pwritev_macos_dispatch(struct wal_impl *impl, const struct iovec *iov, int iovcnt, off_t offset)
```
- Simple wrapper around standard pwrite() with F_NOCACHE already set on file descriptor
- No additional complexity - just standard POSIX calls

#### Initialization and Cleanup

**Linux**
```c
static int wal_io_init_linux(struct wal_impl *impl)
static void wal_io_cleanup_linux(struct wal_impl *impl)
```
- Initializes io_uring queue on WAL open
- Cleans up io_uring resources on WAL close

**macOS**
```c
static int wal_io_init_macos(struct wal_impl *impl)
static void wal_io_cleanup_macos(struct wal_impl *impl)
```
- No-op functions since F_NOCACHE is set at file open time
- Maintains symmetry with Linux initialization pattern

### File Opening Strategy

```c
// Standard file opening (no O_DIRECT on Linux due to alignment constraints)
int open_flags = O_RDWR | O_CREAT;
impl->fd = open(path, open_flags, 0644);

// Linux: Provide sequential I/O hint to kernel
#ifdef __linux__
    posix_fadvise(impl->fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif

// macOS: F_NOCACHE after opening
#ifdef __APPLE__
    fcntl(impl->fd, F_NOCACHE, 1);
#endif
```

## Integration Points

### wal_log()
Direct write path for large records now uses platform-optimized functions:
```c
#ifdef __linux__
    wal_pwritev_linux_io_uring(impl, iov, iovcnt, offset);
#elif defined(__APPLE__)
    wal_pwritev_macos_dispatch(impl, iov, iovcnt, offset);
#else
    wal_pwritev_all(impl->fd, iov, iovcnt, offset);
#endif
```

### wal_flush_batch()
Batch writes use platform-optimized single-buffer writes:
```c
#ifdef __linux__
    written = wal_pwrite_linux_io_uring(impl, buffer, size, offset);
#elif defined(__APPLE__)
    written = wal_pwrite_macos_dispatch(impl, buffer, size, offset);
#else
    written = wal_pwrite_all(impl->fd, buffer, size, offset);
#endif
```

## Performance Benefits

### Linux (io_uring + Sequential I/O hints)
- **Reduced CPU overhead**: io_uring minimizes system call overhead with batched operations
- **Lower latency**: Asynchronous I/O reduces blocking on writes
- **Optimized caching**: POSIX_FADV_SEQUENTIAL helps kernel optimize read-ahead and cache management
- **Compatibility**: Standard file I/O works with all buffer alignments and sizes

### macOS (F_NOCACHE)
- **Cache bypass**: Prevents WAL data from polluting page cache
- **Predictable performance**: No cache interference with other I/O
- **Simplicity**: Standard POSIX pwrite() with F_NOCACHE flag - no special frameworks needed
- **Reliability**: Well-tested approach on macOS/Darwin systems

### Windows
- **Stability**: Maintains proven implementation
- **Compatibility**: Works across all Windows versions
- **Simplicity**: No additional dependencies required

## Build Requirements

### Linux
- **liburing**: Development headers and library
  ```bash
  # Ubuntu/Debian
  sudo apt-get install liburing-dev
  
  # Fedora/RHEL
  sudo dnf install liburing-devel
  ```
- Link with `-luring` flag

### macOS
- No additional dependencies
- Uses standard POSIX and Darwin APIs

### Windows
- No additional dependencies
- Uses existing runtime compatibility layer

## Fallback Behavior

All platforms gracefully fall back to standard I/O if optimizations fail:
- **Linux**: Falls back to pwrite() if io_uring initialization fails
- **macOS**: Falls back to cached pwrite() if F_NOCACHE fails (rare)
- **Windows**: Uses existing stable implementation

## Testing Recommendations

1. **Linux**: Verify io_uring is available and functional
   ```bash
   # Check kernel version (io_uring requires 5.1+)
   uname -r
   # Run with FLINTDB_WAL_SYNC=2 for full fsync testing
   ```

2. **macOS**: Verify F_NOCACHE behavior
   ```bash
   # Monitor I/O with fs_usage
   sudo fs_usage -w -f filesystem | grep flintdb
   ```

3. **Windows**: Verify existing behavior unchanged
   ```cmd
   # Run existing test suite
   testcase.bat
   ```

## Future Enhancements

Potential improvements for consideration:
- **Linux**: Batch multiple operations in io_uring for higher throughput
- **macOS**: Current approach with F_NOCACHE is optimal for standard C without Objective-C dependencies
- **Windows**: Investigate FILE_FLAG_NO_BUFFERING + OVERLAPPED I/O
- **All platforms**: Adaptive tuning based on device characteristics

## References

- [Linux io_uring Documentation](https://kernel.dk/io_uring.pdf)
- [macOS F_NOCACHE Documentation](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html)
- [O_DIRECT Performance Analysis](https://www.kernel.org/doc/Documentation/filesystems/dax.txt)
