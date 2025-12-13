//
//
//
#ifndef FLINTDB_ALLOCATOR_H
#define FLINTDB_ALLOCATOR_H

#ifdef MTRACE

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#define MALLOC(size) d_malloc(size, __FILE__, __LINE__, __FUNCTION__)
#define CALLOC(num, size) d_calloc(num, size, __FILE__, __LINE__, __FUNCTION__)
#define REALLOC(p, size) d_realloc(p, size, __FILE__, __LINE__, __FUNCTION__)
#define STRDUP(string) d_strdup(string, __FILE__, __LINE__, __FUNCTION__)
#define FREE(p) d_free(p, __FILE__, __LINE__, __FUNCTION__)

void * d_malloc(size_t size, const char *f, int l, const char *fn);
void * d_calloc(size_t num, size_t size, const char *f, int l, const char *fn);
void * d_realloc(void *p, size_t size, const char *f, int l, const char *fn);
char * d_strdup(const char *s, const char *f, int l, const char *fn);
void d_free(void *p, const char *f, int l, const char *fn);

#else

#define MALLOC(l) malloc(l)
#define CALLOC(n, l) calloc(n, l)
#define REALLOC(p, l) realloc(p, l)
#define STRDUP(string) strdup(string)
#define FREE(x) free(x)

#endif

#endif // FLINTDB_ALLOCATOR_H