#ifdef _WIN32
#ifndef FLINTDB_OS_RUNTIME_WIN32_FUNCTIONS_H
#define FLINTDB_OS_RUNTIME_WIN32_FUNCTIONS_H

// #include <winsock2.h>
// #include <ws2tcpip.h>
#include <windows.h>
#include <io.h> // get_osfhandle
#include <time.h>
#include <pthread.h>
#include <sys/stat.h>

#include "types.h"


#define PATH_CHAR '\\'

// FlintDB-specific page protection aliases used by runtime_win32.c
#ifndef PAGE_FLINTDB_RDWR
#define PAGE_FLINTDB_RDWR PAGE_READWRITE
#endif
#ifndef PAGE_FLINTDB_RDONLY
#define PAGE_FLINTDB_RDONLY PAGE_READONLY
#endif

// MinGW/MSYS2 headers don't always expose POSIX S_I* names.
// Provide fallbacks on Windows so code can compile consistently.
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

// Group/other bits don't have meaningful equivalents for Windows CRT open(); keep them defined.
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
extern char *strtok_r(char *str, const char *delim, char **saveptr);
extern int ftruncate(int fd, off_t length);
extern int getpagesize(void);
#endif
#endif

#endif // FLINTDB_OS_RUNTIME_WIN32_FUNCTIONS_H

#endif // _WIN32
