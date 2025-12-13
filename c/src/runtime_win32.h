#ifdef _WIN32
#ifndef FLINTDB_OS_RUNTIME_WIN32_FUNCTIONS_H
#define FLINTDB_OS_RUNTIME_WIN32_FUNCTIONS_H

// #include <winsock2.h>
// #include <ws2tcpip.h>
#include <windows.h>
#include <io.h> // get_osfhandle
#include <time.h>
#include <pthread.h>

#include "types.h"


#define PATH_CHAR '\\'

#ifndef _DIRENT_H_
#define _DIRENT_H_
#define _DIRENT_H_SELF_

#ifndef DT_DIR
#  define DT_UNKNOWN       0
#  define DT_FIFO          1
#  define DT_CHR           2
#  define DT_DIR           4
#  define DT_BLK           6
#  define DT_REG           8
#  define DT_LNK          10
#  define DT_SOCK         12
#  define DT_WHT          14
#endif

#define IS_DIR(e) (DT_DIR == e->d_type)

struct dirent {
    char d_name[MAX_PATH];
    unsigned char d_type;
};

typedef struct _DIR {
    HANDLE hFind;
    WIN32_FIND_DATAA FindData;
    struct dirent entry;
    int eof;
} DIR;

DIR* opendir(const char* dirname);
struct dirent* readdir(DIR* dir);
int closedir(DIR* dir);

#endif

// 
#ifndef WIN32_TIMESPECT_GET
#define TIME_UTC 1
int timespec_get(struct timespec *ts, int base);
#endif


#define PROT_READ 1
#define PROT_WRITE 2
#define MAP_SHARED 1
#define MAP_FAILED      ((void *)-1)    /* [MF|SHM] mmap failed */

void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset);
int munmap(void *addr, size_t length);
int msync(void *addr, size_t length, int flags);

// 
#ifdef EMULATE_PREAD_PWRITE_WIN32
i64 pread (int fd, void* buf      , u64 size, u64 offset);
i64 pwrite(int fd, const void* buf, u64 size, u64 offset);
#endif

i64 pread_win32 (HANDLE fh, void* buf, u64 size, u64 offset);
i64 pwrite_win32(HANDLE fh, const void* buf, u64 size, u64 offset);

// Declare commonly used POSIX functions for MSYS2/Cygwin environments
// These may not be visible due to Windows header pollution
#if defined(_POSIX_) || defined(__MSYS__) || defined(__CYGWIN__)
#include <stdio.h>
#include <string.h>
#include <unistd.h>
// Ensure these POSIX functions are declared
#ifndef _POSIX_C_SOURCE
extern int fileno(FILE *stream);
extern char *strdup(const char *s);
extern size_t strnlen(const char *s, size_t maxlen);
extern int strcasecmp(const char *s1, const char *s2);
extern int strncasecmp(const char *s1, const char *s2, size_t n);
extern char *strcasestr(const char *haystack, const char *needle);
extern char *strtok_r(char *str, const char *delim, char **saveptr);
extern int ftruncate(int fd, off_t length);
extern int getpagesize(void);
#endif
#endif

#endif // FLINTDB_OS_RUNTIME_WIN32_FUNCTIONS_H

#endif // _WIN32