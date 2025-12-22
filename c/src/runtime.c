#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "runtime.h"

#ifdef _WIN32
#include "runtime_win32.h"
#endif

#include <fcntl.h>
#include <ctype.h>
#include <dirent.h>
#include <unistd.h>
#include <limits.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/file.h>

int flintdb_os_page_size(void) {
#ifndef _WIN32
	static int cached = 0;
	if (cached > 0) return cached;

	long sz = -1;
#ifdef _SC_PAGESIZE
	sz = sysconf(_SC_PAGESIZE);
#elif defined(_SC_PAGE_SIZE)
	sz = sysconf(_SC_PAGE_SIZE);
#endif
	if (sz <= 0) {
		sz = (long)getpagesize();
	}
	if (sz <= 0) {
		sz = 4096;
	}
	cached = (int)sz;
	return cached;
#else
	// runtime_win32.h provides getpagesize().
	int sz = getpagesize();
	return (sz > 0) ? sz : 4096;
#endif
}

__thread char TL_ERROR[ERROR_BUFSZ] = {0}; // thread-local error buffer

char* l_now(char* buff, size_t bsz) {
	struct timespec ts;
	flintdb_timespec_utc(&ts);
	struct tm *st = localtime(&ts.tv_sec);	
	strftime(buff, bsz, "%m-%d %H:%M:%S", st);
	snprintf(buff+14, 5, ".%03d", (int)(ts.tv_nsec / 1000 / 1000));
	return buff;
}

// return elapsed time in milliseconds
u64 time_elapsed(struct timespec *watch) {
	struct timespec now;
	flintdb_timespec_utc(&now);
	u64 ms = (now.tv_sec - watch->tv_sec) * 1000 + (now.tv_nsec - watch->tv_nsec) / 1000 / 1000;
	return ms;
}

f64 time_ops(i64 rows, struct timespec *watch) {
	u64 ms = time_elapsed(watch);
	if (ms == 0) return (f64)rows;
	return ((f64)rows * 1000.0) / (f64)ms;
}

char * time_dur(u64 ms, char *buf, i32 len) {
	u64 s = (ms / 1000);

	if (s > (365 * 24 * 3600)) {
		i32 days = (i32) (s / (24 * 3600));
		i32 years = (days / 365);
		i32 dd = (days % 365);
		if (dd > 0) 
			snprintf(buf, len, "%dY%dD", years, dd);
		else
			snprintf(buf, len, "%dY", years);
		return buf;
	} else if (s > (24 * 3600)) {
		i32 days = (i32) (s / (24 * 3600));
		s = s % (24 * 3600);
		i32 hours = (i32) (s / 3600);
		if (hours > 0) {
			snprintf(buf, len, "%dD%dh", days, hours);
			return buf;
		}
		snprintf(buf, len-1, "%dD", days);
		return buf;
	} else if (s > 3600) {
		i32 hours = (i32) (s / 3600);
		i32 remainder = (i32) s - hours * 3600;
		i32 mins = remainder / 60;
		if (mins > 0) {
			snprintf(buf, len, "%dh%dm", hours, mins);
			return buf;
		}
		snprintf(buf, len, "%dh", hours);
		return buf;
	} else if (s > 60) {
		int mins = (int) (s / 60);
		int remainder = (int) (s - mins * 60);
		int secs = remainder;
		if (mins > 0) {
			snprintf(buf, len, "%dm%ds", mins, secs);
			return buf;
		}
		snprintf(buf, len, "%dm", mins);
		return buf;
	} else {
		// For durations under an hour: show seconds if >= 1s, otherwise show milliseconds
		if (s >= 1) {
			snprintf(buf, len, "%llus", s);
			return buf;
		}
	}
	// s == 0 here, so render the actual millisecond value
	snprintf(buf, len, "%llums", ms);
	return buf;
}

// int suffix(const char *str, const char *suffix) {
// 	int str_len = strlen(str);
//  	int suffix_len = strlen(suffix);
// 	return (str_len >= suffix_len) && (0 == strcmp(str + (str_len-suffix_len), suffix));
// }

i64 file_length(const char *file) {
	if (access(file, F_OK) == 0) {
		struct stat st;
		int fd = open(file, O_RDONLY, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
		if (fd < 0) {
			// open failed - return -1
			return -1;
		}
		fstat(fd, &st);
		i64 l = st.st_size;
		close(fd);
		return l;
	}
	return -1;
}

i64 file_modified(const char *file) {
	struct stat attr;
    stat(file, &attr);
#ifdef __APPLE__
	return (attr.st_mtimespec.tv_sec * 1000) + (attr.st_mtimespec.tv_nsec / 1000 / 1000);
#elif _WIN32
	return (attr.st_mtime * 1000); // + (attr.st_mtime_nsec / 1000 / 1000);
#else
	return (attr.st_mtim.tv_sec * 1000) + (attr.st_mtim.tv_nsec / 1000 / 1000);
#endif
}

i8 file_exists(const char *file) {
	return (access(file, F_OK) == 0) ? 1 : 0;
}

i8 dir_exists(const char *path) {
	struct stat st;
	return stat(path, &st) == 0 && S_ISDIR(st.st_mode) ? 1 : 0;
}


#ifndef _WIN32 

//mkdirs(LOGS_PATH, S_IRWXU);
void mkdirs(const char *path, mode_t mode) {
    char tmp[PATH_MAX];
    char *p = NULL;
    size_t len;

    snprintf(tmp, sizeof(tmp),"%s",path);
    len = strlen(tmp);
    if (tmp[len - 1] == '/')
        tmp[len - 1] = 0;
    for (p = tmp + 1; *p; p++)
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, mode);
            *p = '/';
        }
    mkdir(tmp, mode);
}

void rmdirs(const char *path) {
	if (!dir_exists(path)) return;

	DIR *d;
	struct dirent *e;
	d = opendir(path);
	if (NULL != d) {
		while ((e = readdir(d))) {
			char f[PATH_MAX] = {0, };
			snprintf(f, PATH_MAX, "%s%c%s", path, PATH_CHAR, e->d_name);
			// printf("rm : %s\n", f);
			if (e->d_type != DT_DIR) {
				unlink(f);
			} else if (e->d_name[0] != '.') {
				rmdirs(f);
			}
		}
		closedir(d);
		rmdir(path);
	}
}

// getdir("/home/my/a.exe", out)
const char* getdir(const char *file, char *out) {
	char *pos = strrchr(file, '/');
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
	char *pos = strrchr(file, '/');
	if (pos) {
		pos++;
		int sz = strlen(file) - ((pos) - file);
		strncpy(out, pos, sz);
		out[sz] = 0;
	} else {
		strcpy(out, file);
	}
	return out;
}

#endif
