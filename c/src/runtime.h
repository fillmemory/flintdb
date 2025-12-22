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

// File permission/mode macros (S_IRUSR, S_IRWXU, ...)
// Needed by open()/mkdir() call sites across the codebase.
#include <sys/stat.h>



// MinGW/MSYS2 headers don't always expose POSIX S_I* names.
// Provide fallbacks on Windows so code can still compile.
#if defined(_WIN32)
#ifndef S_IRUSR
#  ifdef _S_IREAD
#    define S_IRUSR _S_IREAD
#  else
#    define S_IRUSR 0400
#  endif
#endif
#ifndef S_IWUSR
#  ifdef _S_IWRITE
#    define S_IWUSR _S_IWRITE
#  else
#    define S_IWUSR 0200
#  endif
#endif
#ifndef S_IXUSR
#  ifdef _S_IEXEC
#    define S_IXUSR _S_IEXEC
#  else
#    define S_IXUSR 0100
#  endif
#endif
#ifndef S_IRGRP
#  define S_IRGRP 0
#endif
#ifndef S_IWGRP
#  define S_IWGRP 0
#endif
#ifndef S_IXGRP
#  define S_IXGRP 0
#endif
#ifndef S_IROTH
#  define S_IROTH 0
#endif
#ifndef S_IWOTH
#  define S_IWOTH 0
#endif
#ifndef S_IXOTH
#  define S_IXOTH 0
#endif
#ifndef S_IRWXU
#  define S_IRWXU (S_IRUSR | S_IWUSR | S_IXUSR)
#endif
#endif // defined(_WIN32)


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

