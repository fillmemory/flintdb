#ifndef FLINTDB_OS_RUNTIME_FUNCTIONS_H
#define FLINTDB_OS_RUNTIME_FUNCTIONS_H

#ifndef _WIN32
#include <time.h>
#include <pthread.h>
# include <sys/types.h>
# include <arpa/inet.h>
//# include "runtime_linux.h"
#else
# include "runtime_win32.h"
#endif
#include <ctype.h>
#include <unistd.h>

// File permission/mode macros (S_IRUSR, S_IRWXU, ...)
// Needed by open()/mkdir() call sites across the codebase.
#include <sys/stat.h>

// Windows-specific fallbacks for S_IRUSR/S_IRWXU/etc. live in runtime_win32.h


#ifndef PATH_CHAR
#define PATH_CHAR '/'
#endif

#include "types.h"
#include "allocator.h"


#ifndef NDEBUG
  #define TRACE(format, ...) ({ char __NOW[32] = {0}; fprintf(stdout, "%s %s %s:%04d %s " format "\n", l_now(__NOW, sizeof(__NOW)),   "TRACE", __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); fflush(stdout); })
  #define DEBUG(format, ...) ({ char __NOW[32] = {0}; fprintf(stdout, "%s %s %s:%04d %s " format "\n", l_now(__NOW, sizeof(__NOW)),   "DEBUG", __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); fflush(stdout); })
  #define   LOG(format, ...)   ({ char __NOW[32] = {0}; fprintf(stdout, "%s %s %s:%04d %s " format "\n", l_now(__NOW, sizeof(__NOW)), "  LOG", __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); fflush(stdout); })
  #define  WARN(format, ...)   ({ char __NOW[32] = {0}; fprintf(stdout, "%s %s %s:%04d %s " format "\n", l_now(__NOW, sizeof(__NOW)), " WARN", __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); fflush(stdout); })
#else
  #define TRACE(...)
  #define DEBUG(...)
  #define   LOG(format, ...)   ({ char __NOW[32] = {0}; fprintf(stdout, "%s %s %s:%04d %s " format "\n", l_now(__NOW, sizeof(__NOW)),  "  LOG", __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); fflush(stdout); })
  #define  WARN(format, ...)   ({ char __NOW[32] = {0}; fprintf(stdout, "%s %s %s:%04d %s " format "\n", l_now(__NOW, sizeof(__NOW)),  " WARN", __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); fflush(stdout); })
#endif
  #define PANIC(format, ...)   ({ char __NOW[32] = {0}; fprintf(stdout, "%s %s %s:%04d %s " format "\n", l_now(__NOW, sizeof(__NOW)), " PANIC", __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); fflush(stdout); abort(); })


#define ERROR_BUFSZ 2048
extern __thread char TL_ERROR[ERROR_BUFSZ]; // thread-local error buffer
// Usage: char *e = NULL; ... THROW(&e, "Error"); ... EXCEPTION: fprintf(stderr, "Error: %s\n", e);
#define THROW(e, format, ...) { snprintf(TL_ERROR, ERROR_BUFSZ-1, "%s:%d %s " format, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); if (e) *e = TL_ERROR; else fprintf(stdout, "UNCAUGHTED: %s\n", TL_ERROR); goto EXCEPTION; }
#define THROW_S(s) { goto EXCEPTION; } // use existing string s


#ifdef __GNUC__
    #define HOT_PATH    __attribute__((hot))
    #define COLD_PATH   __attribute__((cold))
    #define LIKELY(x)   __builtin_expect(!!(x), 1)
    #define UNLIKELY(x) __builtin_expect(!!(x), 0)
    #define INLINE      __attribute__((always_inline)) inline
    #define NO_INLINE   __attribute__((noinline))
#else
    #define HOT_PATH
    #define COLD_PATH
    #define LIKELY(x)   (x)
    #define UNLIKELY(x) (x)
    #define INLINE      inline
    #define NO_INLINE
#endif

// Mark intentionally-unused helper functions without compiler warnings
#ifndef UNUSED_FN
#if defined(__GNUC__)
#define UNUSED_FN __attribute__((unused))
#else
#define UNUSED_FN
#endif
#endif



#define STRING_INET_ADDRSTRLEN 100

// Cross-platform clock helper:
// - Windows: uses timespec_get (provided in runtime_win32.* when needed)
// - Apple/Linux: prefers clock_gettime when available
static inline void flintdb_timespec_utc(struct timespec *ts) {
#if defined(_WIN32)
  timespec_get(ts, TIME_UTC);
#elif defined(CLOCK_REALTIME)
  clock_gettime(CLOCK_REALTIME, ts);
#else
  timespec_get(ts, TIME_UTC);
#endif
}

// Thread-safe localtime wrapper
// - POSIX: localtime_r
// - Windows: localtime_s
static inline void flintdb_localtime_r(const time_t *t, struct tm *out) {
#if defined(_WIN32)
  localtime_s(out, t);
#else
  localtime_r(t, out);
#endif
}

// Cross-platform file flush helpers
static inline int flintdb_fsync(int fd) {
#if defined(_WIN32)
  HANDLE h = (HANDLE)_get_osfhandle(fd);
  if (h == INVALID_HANDLE_VALUE) {
    return -1;
  }
  return FlushFileBuffers(h) ? 0 : -1;
#else
  return fsync(fd);
#endif
}

static inline int flintdb_fdatasync(int fd) {
#if defined(_WIN32)
  // No fdatasync on Windows CRT; best-effort full flush.
  return flintdb_fsync(fd);
#elif defined(__APPLE__)
  // macOS doesn't guarantee fdatasync; use fsync.
  return fsync(fd);
#else
  return fdatasync(fd);
#endif
}

#define STOPWATCH_START(watch) struct timespec watch; flintdb_timespec_utc(&watch);
// #define STOPWATCH_ELAPSED(watch) time_elapsed(&watch);


char* l_now(char* buff, size_t bsz); // logging
u64 time_elapsed(struct timespec *watch);
f64 time_ops(i64 rows, struct timespec *watch);
char * time_dur(u64 ms, char *buf, i32 len);

// Returns the OS virtual memory page size in bytes (e.g., 4096 on many Intel macOS/Linux,
// 16384 on many Apple Silicon macOS). Cached after first call.
int flintdb_os_page_size(void);

static inline int strempty(const char *s) { return !s || !*s; }

static inline int suffix(const char *str, const char *suffix) {
  int str_len = strlen(str);
  int suffix_len = strlen(suffix);
  return (str_len >= suffix_len) && (0 == strcmp(str + (str_len-suffix_len), suffix));
}

i8  dir_exists(const char *path);
i8  file_exists(const char *file);
i64 file_length(const char *file);
i64 file_modified(const char *file);
void mkdirs(const char *path, mode_t mode);
void rmdirs(const char *path);
const char* getdir(const char *file, char *out);
const char* getname(const char *file, char *out);


#endif // FLINTDB_OS_RUNTIME_FUNCTIONS_H

