#ifdef _WIN32 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <windows.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "runtime.h"
#include "runtime_win32.h"

#ifndef WIN32_TIMESPECT_GET

int timespec_get(struct timespec *ts, int base) {
    if (base != TIME_UTC) {
        return 0;
    }

    FILETIME ft;
    ULARGE_INTEGER uli;
    GetSystemTimeAsFileTime(&ft);

    uli.LowPart = ft.dwLowDateTime;
    uli.HighPart = ft.dwHighDateTime;

    // Convert to Unix epoch time
    uli.QuadPart -= 116444736000000000ULL; // Number of 100-nanosecond intervals between 1601-01-01 and 1970-01-01
    uli.QuadPart /= 10; // Convert from 100-nanosecond intervals to microseconds

    ts->tv_sec = (time_t)(uli.QuadPart / 1000000ULL);
    ts->tv_nsec = (long)((uli.QuadPart % 1000000ULL) * 1000);

    return base;
}
#endif

//mkdirs(LOGS_PATH, S_IRWXU);
void mkdirs(const char *path, mode_t mode) {
    char tmp[PATH_MAX];
    char *p = NULL;
    size_t len;

    (void)mode; // MinGW mkdir() takes only one argument; mode is not used on Windows.

    snprintf(tmp, sizeof(tmp),"%s",path);
    len = strlen(tmp);
    if (tmp[len - 1] == '\\')
        tmp[len - 1] = 0;
    for (p = tmp + 1; *p; p++)
        if (*p == '\\') {
            *p = 0;
            mkdir(tmp);
            *p = '\\';
        }
    mkdir(tmp);
}

void rmdirs(const char *path) {
    WIN32_FIND_DATA e;
    HANDLE h;
    
    char f[MAX_PATH];
    snprintf(f, sizeof(f), "%s\\*", path);
    
    h = FindFirstFile(f, &e);
    if (h == INVALID_HANDLE_VALUE) 
        return;
    
    do {
        if (strcmp(e.cFileName, ".") == 0 || 
            strcmp(e.cFileName, "..") == 0) {
            continue;
        }
        
        snprintf(f, sizeof(f), "%s\\%s", path, e.cFileName);
        if (e.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
            rmdirs(f);
        } else {
            DeleteFile(f);
        }
    } while (FindNextFile(h, &e));
    
    FindClose(h);
    rmdir(path);
}


// getdir("/home/my/a.exe", out)
const char* getdir(const char *file, char *out) {
	char *pos = strrchr(file, '\\');
	if (pos) {
		int sz = pos - file;
		strncpy(out, file, sz);
		out[sz] = 0;
	} else {
		strcpy(out, "");
	}
	return out;
}

const char* getname(const char *file, char *out) {
    const char *pos = strrchr(file, '\\');
    const char *basename = (pos != NULL) ? pos + 1 : file;
    
    strcpy(out, basename);
    char* dot = strrchr(out, '.');
    if (dot != NULL) {
        *dot = '\0';
    }
    return out;
}

#ifdef _DIRENT_H_SELF_

DIR* opendir(const char* dirname) {
    DIR* dir;
    char search_path[MAX_PATH];
    
    if (dirname == NULL) {
        return NULL;
    }
    
    dir = (DIR*)MALLOC(sizeof(DIR));
    if (dir == NULL) {
        return NULL;
    }
    
    snprintf(search_path, MAX_PATH, "%s\\*", dirname);
    
    dir->hFind = FindFirstFileA(search_path, &dir->FindData);
    if (dir->hFind == INVALID_HANDLE_VALUE) {
        FREE(dir);
        return NULL;
    }
    
    dir->eof = 0;
    return dir;
}

struct dirent* readdir(DIR* dir) {
    if (dir == NULL || dir->eof) {
        return NULL;
    }
    
    if (dir->entry.d_name[0] != '\0') {
        if (!FindNextFileA(dir->hFind, &dir->FindData)) {
            dir->eof = 1;
            return NULL;
        }
    }
    
    strncpy(dir->entry.d_name, dir->FindData.cFileName, MAX_PATH);

    if (dir->FindData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
        dir->entry.d_type = DT_DIR;
    } else {
        dir->entry.d_type = DT_REG;
    }
    
    return &dir->entry;
}

int closedir(DIR* dir) {
    if (dir == NULL) {
        return -1;
    }
    
    if (dir->hFind != INVALID_HANDLE_VALUE) {
        FindClose(dir->hFind);
    }
    
    FREE(dir);
    return 0;
}

#endif

//

void* mmap(void* addr, size_t length, int prot, int flags, int fd, off_t offset) {
    HANDLE hMap;
    void* map = NULL;
    DWORD flProtect = 0;
    DWORD dwDesiredAccess = 0;

    if (prot & PROT_READ) {
        if (prot & PROT_WRITE) {
            flProtect = PAGE_FLINTDB_RDWR;
            dwDesiredAccess = FILE_MAP_WRITE;
        } else {
            flProtect = PAGE_FLINTDB_RDONLY;
            dwDesiredAccess = FILE_MAP_READ;
        }
    } else if (prot & PROT_WRITE) {
        flProtect = PAGE_WRITECOPY;
        dwDesiredAccess = FILE_MAP_COPY;
    }

    hMap = CreateFileMapping((HANDLE)_get_osfhandle(fd), NULL, flProtect, 0, length, NULL);
    if (hMap == NULL) {
        return MAP_FAILED;
    }

    map = MapViewOfFile(hMap, dwDesiredAccess, 0, offset, length);
    CloseHandle(hMap);

    if (map == NULL) {
        return MAP_FAILED;
    }

    return map;
}

int munmap(void* addr, size_t length) {
    if (UnmapViewOfFile(addr) == 0) {
        return -1;
    }
    return 0;
}

int msync(void *addr, size_t length, int flags) {
    if (FlushViewOfFile(addr, length) == 0) {
        return -1;
    }
    return 0;
}

#ifdef EMULATE_PREAD_PWRITE_WIN32
// 
i64 pread(int fd, void* buf, u64 size, u64 offset) {
    // do not use by performance reason
    HANDLE file = (HANDLE)(_get_osfhandle(fd));
    if (file == INVALID_HANDLE_VALUE) 
        return -1;

    OVERLAPPED overlapped = {0};
    overlapped.Offset = offset & 0xFFFFFFFF;
    overlapped.OffsetHigh = (offset >> 32) & 0xFFFFFFFF;

    DWORD bytes;
    if (!ReadFile(file, buf, size, &bytes, &overlapped))
        return -1;
    return bytes;
}

i64 pwrite(int fd, const void* buf, u64 size, u64 offset) {
    // do not use by performance reason
    HANDLE file = (HANDLE)(_get_osfhandle(fd));
    if (file == INVALID_HANDLE_VALUE) 
        return -1;

    OVERLAPPED overlapped = {0};
    overlapped.Offset = offset & 0xFFFFFFFF;
    overlapped.OffsetHigh = (offset >> 32) & 0xFFFFFFFF;

    DWORD bytes;
    if (!WriteFile(file, buf, size, &bytes, &overlapped)) 
        return -1;
    return bytes;
}
#endif

i64 pread_win32(HANDLE fh, void* buf, u64 size, u64 offset) {
    OVERLAPPED overlapped = {0};
    overlapped.Offset = offset & 0xFFFFFFFF;
    overlapped.OffsetHigh = (offset >> 32) & 0xFFFFFFFF;

    DWORD bytes;
    if (!ReadFile(fh, buf, size, &bytes, &overlapped)) 
        return -1;
    return bytes;
}

i64 pwrite_win32(HANDLE fh, const void* buf, u64 size, u64 offset) {
    OVERLAPPED overlapped = {0};
    overlapped.Offset = offset & 0xFFFFFFFF;
    overlapped.OffsetHigh = (offset >> 32) & 0xFFFFFFFF;

    DWORD bytes;
    if (!WriteFile(fh, buf, size, &bytes, &overlapped)) 
        return -1;
    return bytes;
}

#endif // _WIN32