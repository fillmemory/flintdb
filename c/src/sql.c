// Port of java/src/main/java/flint/db/SQL.java (best effort)

#include "sql.h"
#include "allocator.h"
#include "buffer.h"
#include "internal.h" // unified internal string helpers
#include "runtime.h"

#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SQL_TERM "<END>"

// Thread-local pool for sql_context allocation
#ifndef sql_parser_POOL_CAPACITY
#define sql_parser_POOL_CAPACITY 32
#endif

static pthread_key_t sql_parser_POOL_KEY;
static pthread_once_t sql_parser_POOL_KEY_ONCE = PTHREAD_ONCE_INIT;

struct flintdb_sql_pool {
    int capacity;
    int top;
    struct flintdb_sql **items;

    struct flintdb_sql *(*borrow)(struct flintdb_sql_pool *pool);
    void (*return_context)(struct flintdb_sql_pool *pool, struct flintdb_sql *q);
    void (*free)(struct flintdb_sql_pool *pool);
};

static struct flintdb_sql *sql_pool_borrow(struct flintdb_sql_pool *pool) {
    if (!pool)
        return NULL;
    if (pool->top > 0) {
        return pool->items[--pool->top];
    }
    // Pool empty, allocate new
    struct flintdb_sql *q = (struct flintdb_sql *)CALLOC(1, sizeof(struct flintdb_sql));
    if (q) {
        // Initialize pointer fields to NULL
        q->object = NULL;
        q->index = NULL;
        q->ignore = NULL;
        q->limit = NULL;
        q->orderby = NULL;
        q->groupby = NULL;
        q->having = NULL;
        q->from = NULL;
        q->into = NULL;
        q->where = NULL;
        q->connect = NULL;
        q->dictionary = NULL;
        q->directory = NULL;
        q->compressor = NULL;
        q->compact = NULL;
        q->cache = NULL;
        q->date = NULL;
        q->storage = NULL;
        q->header = NULL;
        q->delimiter = NULL;
        q->quote = NULL;
        q->nullString = NULL;
        q->format = NULL;
        q->wal = NULL;
        q->option = NULL;
    }
    return q;
}

static void sql_pool_return(struct flintdb_sql_pool *pool, struct flintdb_sql *q) {
    if (!pool || !q)
        return;
    
    // Free dynamically allocated fields (only if non-NULL to avoid mtrace warnings)
    if (q->object) FREE(q->object);
    if (q->index) FREE(q->index);
    if (q->ignore) FREE(q->ignore);
    if (q->limit) FREE(q->limit);
    if (q->orderby) FREE(q->orderby);
    if (q->groupby) FREE(q->groupby);
    if (q->having) FREE(q->having);
    if (q->from) FREE(q->from);
    if (q->into) FREE(q->into);
    if (q->where) FREE(q->where);
    if (q->connect) FREE(q->connect);
    if (q->dictionary) FREE(q->dictionary);
    if (q->directory) FREE(q->directory);
    if (q->compressor) FREE(q->compressor);
    if (q->compact) FREE(q->compact);
    if (q->cache) FREE(q->cache);
    if (q->date) FREE(q->date);
    if (q->storage) FREE(q->storage);
    if (q->header) FREE(q->header);
    if (q->delimiter) FREE(q->delimiter);
    if (q->quote) FREE(q->quote);
    if (q->nullString) FREE(q->nullString);
    if (q->format) FREE(q->format);
    if (q->wal) FREE(q->wal);
    if (q->option) FREE(q->option);
    
    if (pool->top < pool->capacity) {
        // Clear the context before returning to pool
        memset(q, 0, sizeof(struct flintdb_sql));
        pool->items[pool->top++] = q;
    } else {
        // Pool full, free the context
        FREE(q);
    }
}

static void sql_pool_free(struct flintdb_sql_pool *pool) {
    if (!pool)
        return;
    if (pool->items) {
        for (int i = 0; i < pool->top; i++) {
            FREE(pool->items[i]);
        }
        FREE(pool->items);
    }
    FREE(pool);
}

static struct flintdb_sql_pool *sql_pool_create(int capacity) {
    struct flintdb_sql_pool *pool = (struct flintdb_sql_pool *)CALLOC(1, sizeof(struct flintdb_sql_pool));
    if (!pool)
        return NULL;

    pool->items = (struct flintdb_sql **)CALLOC(capacity, sizeof(struct flintdb_sql *));
    if (!pool->items) {
        FREE(pool);
        return NULL;
    }

    pool->capacity = capacity;
    pool->top = 0;
    pool->borrow = sql_pool_borrow;
    pool->return_context = sql_pool_return;
    pool->free = sql_pool_free;

    return pool;
}

static void sql_pool_destroy(void *p) {
    if (p)
        ((struct flintdb_sql_pool *)p)->free((struct flintdb_sql_pool *)p);
    DEBUG("SQL context pool destroyed");
}

static void sql_pool_make_key(void) {
    (void)pthread_key_create(&sql_parser_POOL_KEY, sql_pool_destroy);
    DEBUG("SQL context pool created");
}

static inline struct flintdb_sql_pool *sql_pool_get(void) {
    (void)pthread_once(&sql_parser_POOL_KEY_ONCE, sql_pool_make_key);
    struct flintdb_sql_pool *pool = (struct flintdb_sql_pool *)pthread_getspecific(sql_parser_POOL_KEY);
    if (!pool) {
        pool = sql_pool_create(sql_parser_POOL_CAPACITY);
        (void)pthread_setspecific(sql_parser_POOL_KEY, pool);
    }
    return pool;
}

// --- small string helpers
static inline int starts_with(const char *s, char ch) { return s && s[0] == ch; }
static inline int ends_with(const char *s, char ch) { return s && s[0] && s[strlen(s) - 1] == ch; }
static inline int equals_ic(const char *a, const char *b) {
    if (!a || !b)
        return 0;
    for (; *a && *b; a++, b++) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b))
            return 0;
    }
    return *a == '\0' && *b == '\0';
}

static i32 parse_wal_sync_mode(const char *v) {
    if (!v || !*v) return 0;
    if (equals_ic(v, "DEFAULT")) return WAL_SYNC_DEFAULT;
    if (equals_ic(v, "OFF") || equals_ic(v, "0")) return WAL_SYNC_OFF;
    if (equals_ic(v, "NORMAL") || equals_ic(v, "FSYNC") || equals_ic(v, "1")) return WAL_SYNC_NORMAL;
    if (equals_ic(v, "FULL") || equals_ic(v, "FULLFSYNC") || equals_ic(v, "2")) return WAL_SYNC_FULL;
    // Unknown: leave unset so platform default behavior applies
    return WAL_SYNC_DEFAULT;
}

static i32 parse_on_off_default(const char *v, i32 default_value) {
    if (!v || !*v) return default_value;
    if (equals_ic(v, "DEFAULT")) return default_value;
    if (equals_ic(v, "ON") || equals_ic(v, "TRUE") || equals_ic(v, "YES") || equals_ic(v, "1")) return 1;
    if (equals_ic(v, "OFF") || equals_ic(v, "FALSE") || equals_ic(v, "NO") || equals_ic(v, "0")) return 0;
    return default_value;
}

// s_copy, s_cat, trim now provided inline in internal.h

// Helper function to set dynamically allocated string fields in struct flintdb_sql
static inline void sql_set_string(char **dest, const char *src) {
    if (!dest)
        return;
    
    // Free existing allocation
    if (*dest) {
        FREE(*dest);
        *dest = NULL;
    }
    
    // Allocate and copy if src is not empty
    if (src && src[0] != '\0') {
        size_t len = strlen(src);
        *dest = (char *)MALLOC(len + 1);
        if (*dest) {
            memcpy(*dest, src, len + 1);
        }
    }
}

// Remove comments from SQL string
// Handles single-line comments (--) and multi-line comments (/* */)
// Replaces comments with a single space to maintain token boundaries
static void remove_comments(const char *s, char *out, size_t cap) {
    if (!s || !out || cap == 0)
        return;

    const char *p = s;
    size_t n = 0;
    int quote = 0; // inside quote (quote type: 0=none, '\''=single, '`'=backtick)
    char prev = '\0';

    // comment_end: 0=not in comment, '\n'=single-line, '*'=multi-line
    char comment_end = '\0';

    while (*p && n + 1 < cap) {
        const char ch = *p;

        // Skip comment detection when inside quotes
        if (quote == 0 && comment_end == '\0') {
            // Check for single-line comment start: --
            if (ch == '-' && *(p + 1) == '-') {
                comment_end = '\n'; // ends at newline
                p += 2;             // skip both '-' chars
                continue;
            }
            // Check for multi-line comment start: /*
            else if (ch == '/' && *(p + 1) == '*') {
                comment_end = '*'; // ends at */
                p += 2;            // skip both chars
                continue;
            }
        }

        // Inside comment - look for end marker
        if (comment_end != '\0') {
            if (comment_end == '\n') {
                // Single-line comment ends at newline
                if (ch == '\n') {
                    comment_end = '\0';
                    if (n + 1 < cap)
                        out[n++] = ' '; // replace comment with space
                }
            } else if (comment_end == '*') {
                // Multi-line comment ends at */
                if (ch == '*' && *(p + 1) == '/') {
                    comment_end = '\0';
                    p++; // skip the '/' char
                    if (n + 1 < cap)
                        out[n++] = ' '; // replace comment with space
                }
            }
            p++;
            continue; // skip all chars in comment mode
        }

        // Track quotes (not in comment)
        if (quote != 0 && prev != '\\' && (ch == '\'' || ch == '`')) {
            // End of quoted string
            if (ch == quote)
                quote = 0;
            if (n + 1 < cap)
                out[n++] = ch;
        } else if (quote != 0) {
            // Inside quoted string
            if (n + 1 < cap)
                out[n++] = ch;
        } else if (ch == '\'' || ch == '`') {
            // Start of quoted string
            quote = ch;
            if (n + 1 < cap)
                out[n++] = ch;
        } else {
            // Normal character (not in quote, not in comment)
            if (n + 1 < cap)
                out[n++] = ch;
        }

        prev = ch;
        p++;
    }

    if (n >= cap)
        n = cap - 1;
    out[n] = '\0';
}

// collapse multiple whitespaces to single space (outside quotes/parentheses) similar to Java trim_mws
static void trim_mws(const char *in, char *out, size_t cap) {
    if (!in || !out || cap == 0)
        return;

    char temp[SQL_STRING_LIMIT];
    remove_comments(in, temp, SQL_STRING_LIMIT);

    const char *p = temp;
    size_t n = 0;
    int q = 0;   // inside quote
    int par = 0; // parentheses depth
    char prev = '\0';
    for (; *p; p++) {
        char ch = *p;
        char emit = 0;
        char cur = ch;
        if (ch == '\n' || ch == '\r' || ch == '\t')
            ch = ' ';

        if (par > 0 && ch == ')') {
            par--;
            emit = 1;
            cur = ch;
        } else if (ch == '(') {
            par++;
            emit = 1;
            cur = ch;
        } else if (par > 0) {
            emit = 1;
            cur = ch;
        } else if (q && prev != '\\' && (ch == '\'' || ch == '`')) {
            q = 0;
            emit = 1;
            cur = ch;
        } else if (q) {
            emit = 1;
            cur = ch;
        } else if (ch == '\'' || ch == '`') {
            q = 1;
            emit = 1;
            cur = ch;
        } else if (ch == ' ' && prev == ' ') {
            emit = 0;
        } else if (ch == ',' && prev == ' ') {
            // replace prev space with comma
            if (n > 0)
                out[n - 1] = ',';
            emit = 0;
        } else if (ch == ' ' && prev == '(') {
            emit = 1;
            cur = ch; // keep
        } else if (ch == ')' && prev == ' ') {
            if (n > 0)
                out[n - 1] = ')';
            emit = 0;
        } else if (ch == '(' && prev != ' ') {
            if (n + 2 < cap) {
                out[n++] = ' ';
                out[n++] = '(';
            }
            emit = 0;
        } else {
            emit = 1;
            cur = ch;
        }

        if (emit && n + 1 < cap)
            out[n++] = cur;
        prev = ch;
    }
    if (n >= cap)
        n = cap - 1;
    out[n] = '\0';
    trim(out);
}

/**
 * @brief Unwrap surrounding quotes('|"|`) from a string if present
 *
 * @param s Input string to unwrap
 * @return char* Unwrapped string (same pointer as input)
 */
static inline char *sql_unwrap(char *s) {
    // Remove surrounding quotes if present
    size_t len = strlen(s);
    if (len >= 2) {
        if ((s[0] == '\'' && s[len - 1] == '\'') ||
            (s[0] == '\"' && s[len - 1] == '\"') ||
            (s[0] == '`' && s[len - 1] == '`')) {
            // Shift left and null-terminate
            memmove(s, s + 1, len - 2);
            s[len - 2] = '\0';
        }
    }
    return s;
}

// tokenize: split by spaces except inside quotes and parentheses; keep (...) as one token; append <END>
struct tokens {
    char **a;
    int n;
};

static void tokens_free(struct tokens *t) {
    if (!t || !t->a)
        return;
    for (int i = 0; i < t->n; i++)
        FREE(t->a[i]);
    FREE(t->a);
    t->a = NULL;
    t->n = 0;
}

static int tokens_push(struct tokens *t, const char *s, size_t len, char **e) {
    if (len == 0)
        return 0;
    char *dup = (char *)MALLOC(len + 1);
    if (!dup)
        THROW(e, "failed to allocate memory for token (size: %zu)", len + 1);
    memcpy(dup, s, len);
    dup[len] = '\0';
    char **na = (char **)REALLOC(t->a, sizeof(char *) * (t->n + 1));
    if (!na) {
        FREE(dup);
        THROW(e, "failed to reallocate tokens array (count: %d)", t->n + 1);
    }
    t->a = na;
    t->a[t->n++] = dup;
    return 0;

EXCEPTION:
    return -1;
}

static int tokenize(const char *input, struct tokens *out, char **e) {
    memset(out, 0, sizeof(*out));
    if (!input)
        return 0;
    size_t L = strlen(input);
    // allow one extra space sentinel
    char *buf = (char *)MALLOC(L + 2);
    if (!buf)
        THROW(e, "failed to allocate tokenize buffer (size: %zu)", L + 2);
    memcpy(buf, input, L);
    buf[L] = ' ';
    buf[L + 1] = '\0';
    int q = 0, par = 0;
    char prev = '\0';
    size_t start = 0;
    size_t i;
    for (i = 0; i < L + 1; i++) {
        char ch = buf[i];
        if (par > 0 && ch == ')') {
            par--; // include
        } else if (ch == '(') {
            if (par == 0 && start < i) {
                // flush previous token
                if (tokens_push(out, buf + start, i - start, e) < 0) {
                    FREE(buf);
                    return -1;
                }
                // start new token at '('
                start = i;
            }
            par++;
        } else if (par > 0) {
            // continue
        } else if (q && prev != '\\' && (ch == '\'' || ch == '`')) {
            q = 0;
        } else if (q) {
            // continue
        } else if (ch == '\'' || ch == '`') {
            q = 1;
        } else if (isspace((unsigned char)ch)) {
            if (i > start) {
                if (tokens_push(out, buf + start, i - start, e) < 0) {
                    FREE(buf);
                    return -1;
                }
            }
            start = i + 1;
        }
        prev = ch;
    }
    // append terminator
    tokens_push(out, SQL_TERM, strlen(SQL_TERM), e);
    FREE(buf);
    return 0;

EXCEPTION:
    FREE(buf);
    return -1;
}

// seek: join tokens with spaces until control keywords
static char *seek_tokens(char **a, int n, int offset) {
    size_t cap = SQL_STRING_LIMIT;
    char *res = (char *)CALLOC(1, cap);
    if (!res)
        return NULL;
    for (int i = offset; i < n; i++) {
        const char *s = a[i];
        if (equals_ic(s, SQL_TERM))
            break;
        if (equals_ic(s, "LIMIT") || equals_ic(s, "INTO") || equals_ic(s, "CONNECT") ||
            equals_ic(s, "USE") || equals_ic(s, "OPTION") || equals_ic(s, "HAVING"))
            break;
        if (equals_ic(s, "ORDER") && i + 1 < n && equals_ic(a[i + 1], "BY"))
            break;
        if (equals_ic(s, "GROUP") && i + 1 < n && equals_ic(a[i + 1], "BY"))
            break;
        if (res[0] != '\0')
            s_cat(res, cap, " ");
        s_cat(res, cap, s);
    }
    return res;
}

// split string by top-level delimiter (ignore parentheses and quotes)
struct strlist {
    char **a;
    int n;
};

static void strlist_free(struct strlist *l) {
    if (!l || !l->a)
        return;
    for (int i = 0; i < l->n; i++)
        FREE(l->a[i]);
    FREE(l->a);
    l->a = NULL;
    l->n = 0;
}

static int strlist_push(struct strlist *l, const char *s, size_t len, char **e) {
    char *dup = (char *)MALLOC(len + 1);
    if (!dup)
        THROW(e, "failed to allocate memory for string (size: %zu)", len + 1);
    memcpy(dup, s, len);
    dup[len] = '\0';
    char **na = (char **)REALLOC(l->a, sizeof(char *) * (l->n + 1));
    if (!na) {
        FREE(dup);
        THROW(e, "failed to reallocate string list array (count: %d)", l->n + 1);
    }
    l->a = na;
    l->a[l->n++] = trim(dup);
    return 0;

EXCEPTION:
    return -1;
}

static int split_top(const char *s, char delim, struct strlist *out, char **e) {
    memset(out, 0, sizeof(*out));
    if (!s)
        return 0;
    size_t L = strlen(s);
    char *buf = (char *)MALLOC(L + 2);
    if (!buf)
        THROW(e, "failed to allocate split buffer (size: %zu)", L + 2);
    memcpy(buf, s, L);
    buf[L] = delim;
    buf[L + 1] = '\0';
    int q = 0, par = 0;
    char prev = '\0';
    size_t start = 0;
    for (size_t i = 0; i < L + 1; i++) {
        char ch = buf[i];
        if (par > 0 && ch == ')') {
            par--;
        } else if (ch == '(') {
            par++;
        } else if (par > 0) {
            // continue
        } else if (q && prev != '\\' && (ch == '\'' || ch == '`')) {
            q = 0;
        } else if (q) {
        } else if (ch == '\'' || ch == '`') {
            q = 1;
        } else if (ch == delim) {
            if (i >= start) {
                if (strlist_push(out, buf + start, i - start, e) < 0) {
                    FREE(buf);
                    return -1;
                }
            }
            start = i + 1;
        }
        prev = ch;
    }
    FREE(buf);
    return 0;

EXCEPTION:
    FREE(buf);
    return -1;
}

// parse comma-separated VALUES string into out->values
static void parse_values_into(const char *s, struct flintdb_sql *q, char **e) {
    q->values.length = 0;
    if (strempty(s))
        return;
    struct strlist list;
    memset(&list, 0, sizeof(list));
    if (split_top(s, ',', &list, e) != 0)
        return;
    for (int i = 0; i < list.n && q->values.length < SQL_COLUMNS_LIMIT; i++) {
        char *v = list.a[i];
        trim(v);
        if (equals_ic(v, "NULL")) {
            q->values.value[q->values.length][0] = '\0';
        } else if (starts_with(v, '\'') && ends_with(v, '\'')) {
            // unescape simple sequences
            v[strlen(v) - 1] = '\0';
            v++;
            // replace escaped sequences in place
            char *src = v, *dst = v;
            while (*src) {
                if (src[0] == '\\' && src[1] != '\0') {
                    if (src[1] == 'n') {
                        *dst++ = '\n';
                        src += 2;
                        continue;
                    }
                    if (src[1] == 'r') {
                        *dst++ = '\r';
                        src += 2;
                        continue;
                    }
                    if (src[1] == 't') {
                        *dst++ = '\t';
                        src += 2;
                        continue;
                    }
                    *dst++ = src[1];
                    src += 2;
                    continue;
                }
                *dst++ = *src++;
            }
            *dst = '\0';
            s_copy(q->values.value[q->values.length], SQL_OBJECT_STRING_LIMIT, v);
            v -= 0; // no-op, v modified; list holds allocated block
        } else {
            s_copy(q->values.value[q->values.length], SQL_OBJECT_STRING_LIMIT, v);
        }
        q->values.length++;
    }
    strlist_free(&list);
}

// parse bytes string like 1K/1M/1G
static int parse_bytes(const char *s) {
    if (strempty(s))
        return 0;
    char buf[64];
    s_copy(buf, sizeof(buf), s);
    size_t n = strlen(buf);
    int m = 1;
    if (n > 0) {
        char c = (char)toupper((unsigned char)buf[n - 1]);
        if (c == 'K' || c == 'M' || c == 'G') {
            if (c == 'K')
                m = 1024;
            else if (c == 'M')
                m = 1024 * 1024;
            else if (c == 'G')
                m = 1024 * 1024 * 1024;
            buf[n - 1] = '\0';
        }
    }
    return atoi(buf) * m;
}

static long parse_long(const char *s) {
    if (strempty(s))
        return 0;
    char buf[64];
    s_copy(buf, sizeof(buf), s);
    size_t n = strlen(buf);
    int m = 1;
    if (n > 0) {
        char c = (char)toupper((unsigned char)buf[n - 1]);
        if (c == 'K' || c == 'M' || c == 'G') {
            if (c == 'K')
                m = 1000;
            else if (c == 'M')
                m = 1000 * 1000;
            else if (c == 'G')
                m = 1000 * 1000 * 1000;
            buf[n - 1] = '\0';
        }
    }
    return atoi(buf) * m;
}

// Column type mapping (case-insensitive; accepts optional TYPE_ prefix)
static enum flintdb_variant_type  parse_column_type(const char *typeName) {
    if (strempty(typeName))
        return VARIANT_NULL;
    char up[64];
    s_copy(up, sizeof(up), typeName);
    for (char *p = up; *p; ++p)
        *p = (char)toupper((unsigned char)*p);
    if (strncmp(up, "TYPE_", 5) != 0) {
        // shift right to add TYPE_
        char tmp[64];
        s_copy(tmp, sizeof(tmp), up);
        snprintf(up, sizeof(up), "TYPE_%s", tmp);
    }
    if (equals_ic(up, "TYPE_INT"))
        return VARIANT_INT32;
    if (equals_ic(up, "TYPE_UINT"))
        return VARIANT_UINT32;
    if (equals_ic(up, "TYPE_INT8"))
        return VARIANT_INT8;
    if (equals_ic(up, "TYPE_UINT8"))
        return VARIANT_UINT8;
    if (equals_ic(up, "TYPE_INT16"))
        return VARIANT_INT16;
    if (equals_ic(up, "TYPE_UINT16"))
        return VARIANT_UINT16;
    if (equals_ic(up, "TYPE_INT64"))
        return VARIANT_INT64;
    if (equals_ic(up, "TYPE_DOUBLE"))
        return VARIANT_DOUBLE;
    if (equals_ic(up, "TYPE_FLOAT"))
        return VARIANT_FLOAT;
    if (equals_ic(up, "TYPE_DATE"))
        return VARIANT_DATE;
    if (equals_ic(up, "TYPE_TIME"))
        return VARIANT_TIME;
    if (equals_ic(up, "TYPE_UUID"))
        return VARIANT_UUID;
    if (equals_ic(up, "TYPE_IPV6"))
        return VARIANT_IPV6;
    if (equals_ic(up, "TYPE_STRING"))
        return VARIANT_STRING;
    if (equals_ic(up, "TYPE_DECIMAL"))
        return VARIANT_DECIMAL;
    if (equals_ic(up, "TYPE_BYTES"))
        return VARIANT_BYTES;
    if (equals_ic(up, "TYPE_BLOB"))
        return VARIANT_BLOB;
    if (equals_ic(up, "TYPE_OBJECT"))
        return VARIANT_OBJECT;
    return VARIANT_NULL;
}

static inline int is_var_type(enum flintdb_variant_type  t) {
    return (t == VARIANT_STRING || t == VARIANT_DECIMAL || t == VARIANT_BYTES || t == VARIANT_BLOB || t == VARIANT_OBJECT);
}

static int column_bytes(enum flintdb_variant_type  t, int bytes, int precision) {
    switch (t) {
    case VARIANT_STRING:
        if (bytes <= 0)
            return -1; // caller should enforce > 0
        return bytes;
    case VARIANT_DATE:
        return 3;
    case VARIANT_TIME:
        return 8;
    case VARIANT_INT32:
    case VARIANT_UINT32:
        return 4;
    case VARIANT_INT8:
    case VARIANT_UINT8:
        return 1;
    case VARIANT_INT16:
    case VARIANT_UINT16:
        return 2;
    case VARIANT_INT64:
        return 8;
    case VARIANT_DOUBLE:
        return 8;
    case VARIANT_FLOAT:
        return 4;
    case VARIANT_UUID:
    case VARIANT_IPV6:
        return 16;
    case VARIANT_DECIMAL:
        if (bytes <= 0 && precision > 0) {
            double bitsf = precision * 3.3219280948873626 + 0.999999;
            int bits = (int)bitsf;
            int req = (bits + 7) / 8 + 1;
            if (req <= 0)
                req = 9;
            if (req > 32767)
                req = 32767; // clamp
            return req;
        }
        if (bytes <= 0)
            return 9;
        return bytes;
    case VARIANT_BYTES:
        return bytes;
    case VARIANT_BLOB:
    case VARIANT_OBJECT:
    default:
        return -1;
    }
}

/**
 * Checks if a token is a valid SQL keyword that can appear after FROM clause
 */
static int is_valid_sql_keyword_after_from(const char *token) {
    return equals_ic(token, "WHERE") || 
           equals_ic(token, "LIMIT") || 
           equals_ic(token, "ORDER") || 
           equals_ic(token, "GROUP") || 
           equals_ic(token, "HAVING") || 
           equals_ic(token, "INTO") ||
           equals_ic(token, "USE") ||
           equals_ic(token, "INDEX") ||
           equals_ic(token, "CONNECT") ||
           equals_ic(token, SQL_TERM);
}

/**
 * Checks if a token looks like it could be part of a WHERE condition
 */
static int looks_like_condition(const char *token, char **a, int i, int n) {
    if (!token)
        return 0;
    // Check if token contains comparison operators
    if (strstr(token, "=") || strstr(token, ">") || strstr(token, "<"))
        return 1;
    // Check if next token is a comparison operator
    if (i + 1 < n && (strcmp(a[i + 1], "=") == 0 || strcmp(a[i + 1], ">") == 0 || strcmp(a[i + 1], "<") == 0))
        return 1;
    return 0;
}

static void parse_statements(struct tokens *toks, struct flintdb_sql *q, char **e) {
    if (!toks || toks->n == 0)
        return;
    char **a = toks->a;
    int n = toks->n;
    s_copy(q->statement, sizeof(q->statement), a[0]);

    if (equals_ic(a[0], "SELECT")) {
        char part[16] = {0};
        char cols[SQL_STRING_LIMIT] = {0};
        int distinct = 0;
        for (int i = 1; i < n; i++) {
            char *s = a[i];
            if (equals_ic(s, SQL_TERM))
                break;
            if (equals_ic(s, "DISTINCT")) {
                distinct = 1;
                continue;
            }
            if (equals_ic(s, "FROM")) {
                s_copy(part, sizeof(part), "FROM");
                if (i + 1 < n) {
                    if (strlen(a[i + 1]) >= PATH_MAX) {
                        if (e) {
                            snprintf(TL_ERROR, ERROR_BUFSZ - 1, "Table path too long (%zu bytes, max: %d)", strlen(a[i + 1]), PATH_MAX - 1);
                            *e = TL_ERROR;
                        }
                        return;
                    }
                    s_copy(q->table, sizeof(q->table), a[i + 1]);
                    
                    // Check for missing WHERE clause after FROM
                    if (i + 2 < n) {
                        char *next_token = a[i + 2];
                        if (!is_valid_sql_keyword_after_from(next_token) && looks_like_condition(next_token, a, i + 2, n)) {
                            if (e) {
                                snprintf(TL_ERROR, ERROR_BUFSZ - 1, "SQL syntax error: Missing WHERE keyword before condition. Did you mean: SELECT ... FROM %s WHERE %s ... ?", q->table, next_token);
                                *e = TL_ERROR;
                            }
                            return;
                        }
                    }
                }
                s_copy(q->columns.name[0], sizeof(q->columns.name[0]), cols);
            } else if (equals_ic(s, "USE")) {
                s_copy(part, sizeof(part), "USE");
            } else if (equals_ic(s, "INDEX")) {
                if (i + 1 < n) {
                    char *v = a[i + 1];
                    size_t L = strlen(v);
                    if (L >= 2) {
                        char tmp[SQL_OBJECT_STRING_LIMIT];
                        memcpy(tmp, v + 1, L - 2);
                        tmp[L - 2] = '\0';
                        sql_set_string(&q->index, tmp);
                        
                        // Check for missing WHERE clause after USE INDEX
                        if (i + 2 < n) {
                            char *next_token = a[i + 2];
                            if (!is_valid_sql_keyword_after_from(next_token) && looks_like_condition(next_token, a, i + 2, n)) {
                                if (e) {
                                    snprintf(TL_ERROR, ERROR_BUFSZ - 1, "SQL syntax error: Missing WHERE keyword before condition. Did you mean: ... USE INDEX(%s) WHERE %s ... ?", tmp, next_token);
                                    *e = TL_ERROR;
                                }
                                return;
                            }
                        }
                    }
                }
            } else if (equals_ic(s, "CONNECT")) {
                s_copy(part, sizeof(part), "CONNECT");
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    // strip (..)
                    if (starts_with(v, '(') && ends_with(v, ')')) {
                        v[strlen(v) - 1] = '\0';
                        v++;
                    }
                    sql_set_string(&q->connect, v);
                    FREE(v);
                }
            } else if (equals_ic(s, "WHERE")) {
                s_copy(part, sizeof(part), "WHERE");
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    if (strlen(v) >= SQL_STRING_LIMIT) {
                        if (e) {
                            snprintf(TL_ERROR, ERROR_BUFSZ - 1, "WHERE clause too long (%zu bytes, max: %d)", strlen(v), SQL_STRING_LIMIT - 1);
                            *e = TL_ERROR;
                        }
                        FREE(v);
                        return;
                    }
                    sql_set_string(&q->where, v);
                    FREE(v);
                }
            } else if (equals_ic(s, "LIMIT")) {
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    sql_set_string(&q->limit, v);
                    FREE(v);
                }
            } else if (equals_ic(s, "ORDER")) {
                s_copy(part, sizeof(part), "ORDER");
            } else if (equals_ic(s, "GROUP")) {
                s_copy(part, sizeof(part), "GROUP");
            } else if (equals_ic(s, "BY")) {
                if (equals_ic(part, "ORDER")) {
                    char *v = seek_tokens(a, n, i + 1);
                    if (v) {
                        if (strlen(v) >= SQL_STRING_LIMIT) {
                            if (e) {
                                snprintf(TL_ERROR, ERROR_BUFSZ - 1, "ORDER BY clause too long (%zu bytes, max: %d)", strlen(v), SQL_STRING_LIMIT - 1);
                                *e = TL_ERROR;
                            }
                            FREE(v);
                            return;
                        }
                        sql_set_string(&q->orderby, v);
                        FREE(v);
                    }
                } else if (equals_ic(part, "GROUP")) {
                    char *v = seek_tokens(a, n, i + 1);
                    if (v) {
                        if (strlen(v) >= SQL_STRING_LIMIT) {
                            if (e) {
                                snprintf(TL_ERROR, ERROR_BUFSZ - 1, "GROUP BY clause too long (%zu bytes, max: %d)", strlen(v), SQL_STRING_LIMIT - 1);
                                *e = TL_ERROR;
                            }
                            FREE(v);
                            return;
                        }
                        sql_set_string(&q->groupby, v);
                        FREE(v);
                    }
                }
            } else if (equals_ic(s, "HAVING")) {
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    sql_set_string(&q->having, v);
                    FREE(v);
                }
            } else if (equals_ic(s, "INTO")) {
                s_copy(part, sizeof(part), "INTO");
                if (i + 1 < n)
                    sql_set_string(&q->into, a[i + 1]);
            }
            if (part[0] == '\0') {
                if (cols[0] != '\0')
                    strncat(cols, " ", sizeof(cols) - strlen(cols) - 1);
                strncat(cols, s, sizeof(cols) - strlen(cols) - 1);
            }
        }
        q->distinct = (i8)distinct;
        // store columns as a single tokenized string list as best-effort; also split into array
        if (cols[0]) {
            struct strlist list;
            memset(&list, 0, sizeof(list));
            split_top(cols, ',', &list, e);
            q->columns.length = 0;
            for (int i = 0; i < list.n && q->columns.length < SQL_COLUMNS_LIMIT; i++) {
                s_copy(q->columns.name[q->columns.length++], SQL_OBJECT_STRING_LIMIT, list.a[i]);
            }
            strlist_free(&list);
        }
        return;
    }

    if (equals_ic(a[0], "DELETE")) {
        for (int i = 1; i < n; i++) {
            char *s = a[i];
            if (equals_ic(s, SQL_TERM))
                break;
            if (equals_ic(s, "FROM") && i + 1 < n) {
                s_copy(q->table, sizeof(q->table), a[i + 1]);
            } else if (equals_ic(s, "INDEX") && i + 1 < n) {
                char *v = a[i + 1];
                size_t L = strlen(v);
                if (L >= 2) {
                    char tmp[SQL_OBJECT_STRING_LIMIT];
                    memcpy(tmp, v + 1, L - 2);
                    tmp[L - 2] = '\0';
                    sql_set_string(&q->index, tmp);
                }
            } else if (equals_ic(s, "WHERE")) {
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    sql_set_string(&q->where, v);
                    FREE(v);
                }
            } else if (equals_ic(s, "LIMIT")) {
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    sql_set_string(&q->limit, v);
                    FREE(v);
                }
            }
        }
        return;
    }

    if (equals_ic(a[0], "UPDATE")) {
        if (n >= 2)
            s_copy(q->table, sizeof(q->table), a[1]);
        char part[8] = {0};
        struct strlist c;
        memset(&c, 0, sizeof(c));
        for (int i = 2; i < n; i++) {
            char *s = a[i];
            if (equals_ic(s, SQL_TERM))
                break;
            if (equals_ic(s, "SET")) {
                s_copy(part, sizeof(part), "SET");
                continue;
            } else if (equals_ic(s, "USE")) {
                s_copy(part, sizeof(part), "USE");
            } else if (equals_ic(s, "INDEX") && i + 1 < n) {
                char *v = a[i + 1];
                size_t L = strlen(v);
                if (L >= 2) {
                    char tmp[SQL_OBJECT_STRING_LIMIT];
                    memcpy(tmp, v + 1, L - 2);
                    tmp[L - 2] = '\0';
                    sql_set_string(&q->index, tmp);
                }
            } else if (equals_ic(s, "WHERE")) {
                s_copy(part, sizeof(part), "WHERE");
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    sql_set_string(&q->where, v);
                    FREE(v);
                }
            } else if (equals_ic(s, "LIMIT")) {
                s_copy(part, sizeof(part), "LIMIT");
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    sql_set_string(&q->limit, v);
                    FREE(v);
                }
            }
            if (equals_ic(part, "SET")) {
                strlist_push(&c, s, strlen(s), e);
            }
        }
        // Convert c into columns/values pairs: support both "col = val" and "col=val"
        q->columns.length = 0;
        q->values.length = 0;
        for (int i = 0; i < c.n;) {
            const char *tok = c.a[i];
            const char *eq = strchr(tok, '=');
            if (eq) {
                // split inline form
                char left[SQL_OBJECT_STRING_LIMIT];
                char right[SQL_OBJECT_STRING_LIMIT];
                size_t ln = (size_t)(eq - tok);
                if (ln >= sizeof(left))
                    ln = sizeof(left) - 1;
                memcpy(left, tok, ln);
                left[ln] = '\0';
                s_copy(right, sizeof(right), eq + 1);
                if (q->columns.length < SQL_COLUMNS_LIMIT)
                    s_copy(q->columns.name[q->columns.length++], SQL_OBJECT_STRING_LIMIT, trim(left));
                if (q->values.length < SQL_COLUMNS_LIMIT)
                    s_copy(q->values.value[q->values.length++], SQL_OBJECT_STRING_LIMIT, trim(right));
                i++;
                continue;
            }
            // handle spaced triplet: col = val
            if ((i + 2) < c.n && strcmp(c.a[i + 1], "=") == 0) {
                const char *col = c.a[i];
                const char *val = c.a[i + 2];
                if (q->columns.length < SQL_COLUMNS_LIMIT)
                    s_copy(q->columns.name[q->columns.length++], SQL_OBJECT_STRING_LIMIT, col);
                if (q->values.length < SQL_COLUMNS_LIMIT)
                    s_copy(q->values.value[q->values.length++], SQL_OBJECT_STRING_LIMIT, val);
                i += 3;
                // skip trailing comma token if present
                continue;
            }
            // unknown token form; skip
            i++;
        }
        strlist_free(&c);
        return;
    }

    if (equals_ic(a[0], "INSERT") || equals_ic(a[0], "REPLACE")) {
        char part[8] = {0};
        for (int i = 1; i < n; i++) {
            char *s = a[i];
            if (equals_ic(s, SQL_TERM))
                break;
            if (equals_ic(s, "IGNORE")) {
                sql_set_string(&q->ignore, "IGNORE");
            } else if (equals_ic(s, "INTO")) {
                s_copy(part, sizeof(part), "INTO");
                if (i + 1 < n)
                    s_copy(q->table, sizeof(q->table), a[i + 1]);
            } else if (equals_ic(s, "VALUES")) {
                s_copy(part, sizeof(part), "VALUES");
            } else if (equals_ic(s, "FROM")) {
                s_copy(part, sizeof(part), "FROM");
                if (i + 1 < n)
                    sql_set_string(&q->from, a[i + 1]);
            } else if (equals_ic(part, "INTO") && starts_with(s, '(') && ends_with(s, ')')) {
                char tmp[SQL_STRING_LIMIT];
                s_copy(tmp, sizeof(tmp), s);
                tmp[strlen(tmp) - 1] = '\0';
                s_copy(tmp, sizeof(tmp), tmp + 1);
                // split columns by comma top-level
                struct strlist list;
                memset(&list, 0, sizeof(list));
                split_top(tmp, ',', &list, e);
                q->columns.length = 0;
                for (int k = 0; k < list.n && q->columns.length < SQL_COLUMNS_LIMIT; k++)
                    s_copy(q->columns.name[q->columns.length++], SQL_OBJECT_STRING_LIMIT, list.a[k]);
                strlist_free(&list);
            } else if (equals_ic(part, "VALUES") && starts_with(s, '(') && ends_with(s, ')')) {
                char tmp[SQL_STRING_LIMIT];
                s_copy(tmp, sizeof(tmp), s);
                tmp[strlen(tmp) - 1] = '\0';
                parse_values_into(tmp + 1, q, e);
            } else if (equals_ic(s, "LIMIT")) {
                s_copy(part, sizeof(part), "LIMIT");
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    sql_set_string(&q->limit, v);
                    FREE(v);
                }
            } else if (equals_ic(s, "WHERE")) {
                s_copy(part, sizeof(part), "WHERE");
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    sql_set_string(&q->where, v);
                    FREE(v);
                }
            }
        }
        return;
    }

    if (equals_ic(a[0], "BEGIN")) { // BEGIN TRANSACTION <table> => table must be specified
        if (n > 2 && equals_ic(a[1], "TRANSACTION"))
            s_copy(q->table, sizeof(q->table), a[2]);
        return;
    }

    if (equals_ic(a[0], "COMMIT")) {
        return;
    }

    if (equals_ic(a[0], "ROLLBACK")) {
        return;
    }

    if (equals_ic(a[0], "DESC") || equals_ic(a[0], "META")) {
        if (n > 1)
            s_copy(q->table, sizeof(q->table), a[1]);
        for (int i = 2; i < n; i++) {
            char *s = a[i];
            if (equals_ic(s, SQL_TERM))
                break;
            if (equals_ic(s, "CONNECT")) {
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    if (starts_with(v, '(') && ends_with(v, ')')) {
                        v[strlen(v) - 1] = '\0';
                        v++;
                    }
                    sql_set_string(&q->connect, v);
                    FREE(v);
                }
            } else if (equals_ic(s, "INTO") && i + 1 < n) {
                sql_set_string(&q->into, a[i + 1]);
            }
        }
        return;
    }

    if (equals_ic(a[0], "SHOW")) {
        if (n > 1)
            sql_set_string(&q->object, a[1]);
        for (int i = 2; i < n; i++) {
            char *s = a[i];
            if (equals_ic(s, SQL_TERM))
                break;
            if (equals_ic(s, "WHERE")) {
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    sql_set_string(&q->where, v);
                    FREE(v);
                }
            } else if (equals_ic(s, "OPTION")) {
                char *v = seek_tokens(a, n, i + 1);
                if (v) {
                    sql_set_string(&q->option, v);
                    FREE(v);
                }
            }
        }
        return;
    }

    if (equals_ic(a[0], "DROP") && n >= 3 && equals_ic(a[1], "TABLE")) {
        s_copy(q->table, sizeof(q->table), a[2]);
        return;
    }

    if (equals_ic(a[0], "CREATE") && n >= 3) {
        int i = 2;
        if (equals_ic(a[1], "TEMPORARY") && n >= 3 && equals_ic(a[2], "TABLE"))
            i = 3;
        else if (equals_ic(a[1], "TABLE"))
            i = 2;
        if (i >= n)
            return;
        s_copy(q->table, sizeof(q->table), a[i++]);
        if (i >= n)
            return;
        char def[SQL_STRING_LIMIT] = {0};
        s_copy(def, sizeof(def), a[i++]);
        // parse options key=value ... until <END>
        for (; i < n; i++) {
            char *s = a[i];
            if (equals_ic(s, SQL_TERM))
                break;
            size_t L = strlen(s);
            char *eq = strchr(s, '=');
            // Trim trailing comma only if it's not the entire value (e.g., preserve DELIMITER=,)
            if (L > 0 && s[L - 1] == ',') {
                if (!(eq && eq[1] != '\0' && eq[2] == '\0')) {
                    // cases like KEY=VAL, (where value length >= 1 not exactly 1-char)
                    s[L - 1] = '\0';
                    L--;
                }
            }
            const char *k = s;
            const char *v = "";
            if (eq) {
                *eq = '\0';
                v = eq + 1;
            }
            if (equals_ic(k, "DIRECTORY"))
                sql_set_string(&q->directory, v);
            else if (equals_ic(k, "STORAGE"))
                sql_set_string(&q->storage, v);
            else if (equals_ic(k, "WAL"))
                sql_set_string(&q->wal, v);
            else if (equals_ic(k, "WAL_BATCH_SIZE"))
                q->wal_batch_size = parse_long(v);
            else if (equals_ic(k, "WAL_CHECKPOINT_INTERVAL"))
                q->wal_checkpoint_interval = parse_long(v);
                        else if (equals_ic(k, "WAL_SYNC"))
                            q->wal_sync = parse_wal_sync_mode(v);
                        else if (equals_ic(k, "WAL_BUFFER_SIZE"))
                            q->wal_buffer_size = parse_bytes(v);
                        else if (equals_ic(k, "WAL_PAGE_DATA"))
                            q->wal_page_data = parse_on_off_default(v, 1);
            else if (equals_ic(k, "WAL_COMPRESSION_THRESHOLD"))
                q->wal_compression_threshold = parse_bytes(v);
            else if (equals_ic(k, "DICTIONARY"))
                sql_set_string(&q->dictionary, v);
            else if (equals_ic(k, "COMPRESSOR")) {
                char tmp[SQL_OBJECT_STRING_LIMIT];
                s_copy(tmp, sizeof(tmp), v);
                for (char *p = tmp; *p; ++p)
                    *p = (char)tolower((unsigned char)*p);
                sql_set_string(&q->compressor, tmp);
            } else if (equals_ic(k, "COMPACT")) {
                char tmp[SQL_OBJECT_STRING_LIMIT];
                s_copy(tmp, sizeof(tmp), v);
                for (char *p = tmp; *p; ++p)
                    *p = (char)toupper((unsigned char)*p);
                sql_set_string(&q->compact, tmp);
            }
            // else if (equals_ic(k, "INCREMENT")) { char tmp[SQL_OBJECT_STRING_LIMIT]; s_copy(tmp, sizeof(tmp), v); for (char *p = tmp; *p; ++p) *p = (char)toupper((unsigned char)*p); s_copy(q->increment, sizeof(q->increment), tmp); }
            else if (equals_ic(k, "CACHE")) {
                char tmp[SQL_OBJECT_STRING_LIMIT];
                s_copy(tmp, sizeof(tmp), v);
                for (char *p = tmp; *p; ++p)
                    *p = (char)toupper((unsigned char)*p);
                sql_set_string(&q->cache, tmp);
            } else if (equals_ic(k, "DATE")) {
                char tmp[SQL_OBJECT_STRING_LIMIT];
                s_copy(tmp, sizeof(tmp), v);
                for (char *p = tmp; *p; ++p)
                    *p = (char)toupper((unsigned char)*p);
                sql_set_string(&q->date, tmp);
            } else if (equals_ic(k, "HEADER"))
                sql_set_string(&q->header, v);
            else if (equals_ic(k, "DELIMITER"))
                sql_set_string(&q->delimiter, v);
            else if (equals_ic(k, "QUOTE"))
                sql_set_string(&q->quote, v);
            else if (equals_ic(k, "NULL"))
                sql_set_string(&q->nullString, v);
            else if (equals_ic(k, "FORMAT"))
                sql_set_string(&q->format, v);
            else if (equals_ic(k, "MAX")) { /* ignore */
            }
        }
        // store definition parts
        if (starts_with(def, '(') && ends_with(def, ')')) {
            def[strlen(def) - 1] = '\0';
        }
        char *def_body = def + (starts_with(def, '(') ? 1 : 0);
        struct strlist list;
        memset(&list, 0, sizeof(list));
        split_top(def_body, ',', &list, e);
        q->definition.length = 0;
        for (int k = 0; k < list.n && q->definition.length < SQL_COLUMNS_LIMIT; k++) {
            if (strlen(list.a[k]) >= SQL_OBJECT_STRING_LIMIT) {
                strlist_free(&list);
                if (e) {
                    snprintf(TL_ERROR, ERROR_BUFSZ - 1, "Column definition too long (%zu bytes, max: %d)", strlen(list.a[k]), SQL_OBJECT_STRING_LIMIT - 1);
                    *e = TL_ERROR;
                }
                return;
            }
            s_copy(q->definition.object[q->definition.length++], SQL_OBJECT_STRING_LIMIT, list.a[k]);
        }
        strlist_free(&list);
        return;
    }
}

void flintdb_sql_free(struct flintdb_sql *q) {
    if (!q)
        return;
    // Return to thread-local pool instead of freeing
    struct flintdb_sql_pool *pool = sql_pool_get();
    if (pool) {
        pool->return_context(pool, q);
    } else {
        // No pool available, free directly
        if (q->object) FREE(q->object);
        if (q->index) FREE(q->index);
        if (q->ignore) FREE(q->ignore);
        if (q->limit) FREE(q->limit);
        if (q->orderby) FREE(q->orderby);
        if (q->groupby) FREE(q->groupby);
        if (q->having) FREE(q->having);
        if (q->from) FREE(q->from);
        if (q->into) FREE(q->into);
        if (q->where) FREE(q->where);
        if (q->connect) FREE(q->connect);
        if (q->dictionary) FREE(q->dictionary);
        if (q->directory) FREE(q->directory);
        if (q->compressor) FREE(q->compressor);
        if (q->compact) FREE(q->compact);
        if (q->cache) FREE(q->cache);
        if (q->date) FREE(q->date);
        if (q->storage) FREE(q->storage);
        if (q->header) FREE(q->header);
        if (q->delimiter) FREE(q->delimiter);
        if (q->quote) FREE(q->quote);
        if (q->nullString) FREE(q->nullString);
        if (q->format) FREE(q->format);
        if (q->wal) FREE(q->wal);
        if (q->option) FREE(q->option);
        FREE(q);
    }
}

struct flintdb_sql *flintdb_sql_parse(const char *sql, char **e) {
    struct flintdb_sql *out = NULL;
    struct tokens toks = {0};

    if (!sql)
        THROW(e, "sql is NULL");

    // Check SQL length before processing
    size_t sql_len = strlen(sql);
    if (sql_len >= SQL_STRING_LIMIT)
        THROW(e, "SQL statement too long (%zu bytes, max: %d)", sql_len, SQL_STRING_LIMIT - 1);

    // Use thread-local pool allocator for sql_context
    struct flintdb_sql_pool *pool = sql_pool_get();
    if (pool) {
        out = pool->borrow(pool);
    } else {
        out = (struct flintdb_sql *)CALLOC(1, sizeof(struct flintdb_sql));
    }
    if (!out)
        THROW(e, "failed to allocate memory for sql_context");

#ifndef NDEBUG
    s_copy(out->origin, sizeof(out->origin), sql);
#endif
    char norm[SQL_STRING_LIMIT];
    memset(norm, 0, sizeof(norm));
    trim_mws(sql, norm, sizeof(norm));
    if (tokenize(norm, &toks, e) != 0)
        THROW(e, "failed to tokenize SQL statement");
    parse_statements(&toks, out, e);
    if (e && *e)
        THROW_S(e); // Check if parse_statements set an error
    tokens_free(&toks);

    sql_unwrap(out->table);
    for (int i = 0; i < out->columns.length; i++) {
        sql_unwrap(out->columns.name[i]);
    }

    return out;

EXCEPTION:
    tokens_free(&toks);
    if (out)
        flintdb_sql_free(out);
    return NULL;
}

struct flintdb_sql *flintdb_sql_from_file(const char *file, char **e) {
    struct flintdb_sql *out = NULL;
    FILE *fp = NULL;

    if (!file)
        THROW(e, "file is NULL");

    fp = fopen(file, "rb");
    if (!fp)
        THROW(e, "fopen failed: %s", file);

    char *buf = (char *)CALLOC(1, SQL_STRING_LIMIT);
    if (!buf)
        THROW(e, "malloc failed for file buffer (size: %d)", SQL_STRING_LIMIT);

    size_t n = fread(buf, 1, SQL_STRING_LIMIT - 1, fp);
    (void)n;
    fclose(fp);
    // DEBUG("sql: %s", buf);

    out = flintdb_sql_parse(buf, e);
    FREE(buf);
    return out;

EXCEPTION:
    if (out)
        flintdb_sql_free(out);
    if (fp)
        fclose(fp);
    return NULL;
}

int flintdb_sql_to_string(struct flintdb_sql *in, char *s, int len, char **e) {
    if (!in || !s || len <= 0)
        return -1;
    s[0] = '\0';
    char tmp[SQL_STRING_LIMIT];
    tmp[0] = '\0';
    s_copy(tmp, sizeof(tmp), in->statement);
    if (!strempty(in->ignore)) {
        s_cat(tmp, sizeof(tmp), " ");
        s_cat(tmp, sizeof(tmp), in->ignore);
    }
    if (in->table[0]) {
        s_cat(tmp, sizeof(tmp), ", TABLE : ");
        s_cat(tmp, sizeof(tmp), in->table);
    }
    if (!strempty(in->connect)) {
        s_cat(tmp, sizeof(tmp), ", CONNECT : ");
        s_cat(tmp, sizeof(tmp), in->connect);
    }
    if (!strempty(in->object)) {
        s_cat(tmp, sizeof(tmp), ", OBJECT : ");
        s_cat(tmp, sizeof(tmp), in->object);
    }
    if (!strempty(in->index)) {
        s_cat(tmp, sizeof(tmp), ", INDEX : ");
        s_cat(tmp, sizeof(tmp), in->index);
    }
    if (!strempty(in->where)) {
        s_cat(tmp, sizeof(tmp), ", WHERE : ");
        s_cat(tmp, sizeof(tmp), in->where);
    }
    if (!strempty(in->groupby)) {
        s_cat(tmp, sizeof(tmp), ", GROUP BY : ");
        s_cat(tmp, sizeof(tmp), in->groupby);
    }
    if (!strempty(in->having)) {
        s_cat(tmp, sizeof(tmp), ", HAVING : ");
        s_cat(tmp, sizeof(tmp), in->having);
    }
    if (!strempty(in->orderby)) {
        s_cat(tmp, sizeof(tmp), ", ORDER BY : ");
        s_cat(tmp, sizeof(tmp), in->orderby);
    }
    if (!strempty(in->limit)) {
        s_cat(tmp, sizeof(tmp), ", LIMIT : ");
        s_cat(tmp, sizeof(tmp), in->limit);
    }
    if (in->columns.length > 0) {
        s_cat(tmp, sizeof(tmp), ", COLUMNS : [");
        for (int i = 0; i < in->columns.length; i++) {
            if (i > 0)
                s_cat(tmp, sizeof(tmp), ", ");
            s_cat(tmp, sizeof(tmp), in->columns.name[i]);
        }
        s_cat(tmp, sizeof(tmp), "]");
    }
    if (in->values.length > 0) {
        s_cat(tmp, sizeof(tmp), ", VALUES : [");
        for (int i = 0; i < in->values.length; i++) {
            if (i > 0)
                s_cat(tmp, sizeof(tmp), ", ");
            s_cat(tmp, sizeof(tmp), in->values.value[i][0] ? in->values.value[i] : "NULL");
        }
        s_cat(tmp, sizeof(tmp), "]");
    }
    if (!strempty(in->from)) {
        s_cat(tmp, sizeof(tmp), ", FROM : ");
        s_cat(tmp, sizeof(tmp), in->from);
    }
    if (!strempty(in->into)) {
        s_cat(tmp, sizeof(tmp), ", INTO : ");
        s_cat(tmp, sizeof(tmp), in->into);
    }
#ifndef NDEBUG
    if (in->origin[0]) {
        s_cat(tmp, sizeof(tmp), ", SQL : ");
        s_cat(tmp, sizeof(tmp), in->origin);
    }
#endif
    s_copy(s, len, tmp);
    return (int)strlen(s);
}

int flintdb_sql_to_meta(struct flintdb_sql *in, struct flintdb_meta *out, char **e) {
    // TODO: check overflow for columns/indexes

    if (!in)
        THROW(e, "input context is NULL");
    if (!out)
        THROW(e, "output meta is NULL");
    if (!equals_ic(in->statement, "CREATE"))
        THROW(e, "not a CREATE statement (found: %s)", in->statement);
    if (in->definition.length <= 0)
        THROW(e, "no column/index definition found in CREATE statement");

    const char *tablename = (strempty(in->table)) ? "*" : in->table;
    *out = flintdb_meta_new(tablename, e);

    // parse column/index definitions
    char part_key[8] = {0};
    for (int i = 0; i < in->definition.length; i++) {
        char def[SQL_STRING_LIMIT];
        s_copy(def, sizeof(def), in->definition.object[i]);
        // tokenize this def line
        struct tokens toks = {0};
        if (tokenize(def, &toks, e) != 0) {
            tokens_free(&toks);
            THROW(e, "failed to tokenize column definition: %s", def);
        }
        if (toks.n == 0) {
            tokens_free(&toks);
            continue;
        }

        if (equals_ic(toks.a[0], "PRIMARY") && toks.n >= 2 && equals_ic(toks.a[1], "KEY")) {
            // find (...) token
            const char *grp = NULL;
            for (int j = 2; j < toks.n; j++) {
                if (starts_with(toks.a[j], '(') && ends_with(toks.a[j], ')')) {
                    grp = toks.a[j];
                    break;
                }
            }
            if (grp) {
                char tmp[SQL_STRING_LIMIT];
                s_copy(tmp, sizeof(tmp), grp);
                tmp[strlen(tmp) - 1] = '\0';
                char *body = tmp + 1;
                struct strlist keys;
                memset(&keys, 0, sizeof(keys));
                if (split_top(body, ',', &keys, e) != 0) {
                    tokens_free(&toks);
                    THROW(e, "failed to parse primary key columns: %s", body);
                }
                char keys_arr[MAX_INDEX_KEYS_LIMIT][MAX_COLUMN_NAME_LIMIT];
                int kcnt = 0;
                for (int k = 0; k < keys.n && kcnt < MAX_INDEX_KEYS_LIMIT; k++) {
                    // remove all spaces inside key token
                    char *v = keys.a[k];
                    char cleaned[MAX_COLUMN_NAME_LIMIT] = {0};
                    for (char *p = v; *p; ++p)
                        if (*p != ' ')
                            strncat(cleaned, p, 1);
                    s_copy(keys_arr[kcnt++], sizeof(keys_arr[0]), cleaned);
                }
                char *idx_err = NULL;
                flintdb_meta_indexes_add(out, PRIMARY_NAME, NULL, (const char (*)[MAX_COLUMN_NAME_LIMIT])keys_arr, kcnt, &idx_err);
                if (idx_err) {
                    strlist_free(&keys);
                    tokens_free(&toks);
                    THROW(e, "failed to add primary key index: %s", idx_err);
                }
                strlist_free(&keys);
                s_copy(part_key, sizeof(part_key), "KEY");
            }
            tokens_free(&toks);
            continue;
        }

        if (equals_ic(toks.a[0], "KEY") && equals_ic(part_key, "KEY")) {
            if (toks.n >= 3) {
                const char *name = toks.a[1];
                char grp[SQL_STRING_LIMIT];
                s_copy(grp, sizeof(grp), toks.a[2]);
                grp[strlen(grp) - 1] = '\0';
                char *body = grp + 1;
                struct strlist keys;
                memset(&keys, 0, sizeof(keys));
                if (split_top(body, ',', &keys, e) != 0) {
                    tokens_free(&toks);
                    THROW(e, "failed to parse index key columns: %s", body);
                }
                char keys_arr[MAX_INDEX_KEYS_LIMIT][MAX_COLUMN_NAME_LIMIT];
                int kcnt = 0;
                for (int k = 0; k < keys.n && kcnt < MAX_INDEX_KEYS_LIMIT; k++) {
                    char *v = keys.a[k];
                    char cleaned[MAX_COLUMN_NAME_LIMIT] = {0};
                    for (char *p = v; *p != '\0'; ++p)
                        if (*p != ' ')
                            strncat(cleaned, p, 1);
                    s_copy(keys_arr[kcnt++], sizeof(keys_arr[0]), cleaned);
                }
                char *idx_err = NULL;
                flintdb_meta_indexes_add(out, name, NULL, (const char (*)[MAX_COLUMN_NAME_LIMIT])keys_arr, kcnt, &idx_err);
                if (idx_err) {
                    strlist_free(&keys);
                    tokens_free(&toks);
                    THROW(e, "failed to add index '%s': %s", name, idx_err);
                }
                strlist_free(&keys);
            }
            tokens_free(&toks);
            continue;
        }

        // Column definition
        if (toks.n >= 2) {
            char name[MAX_COLUMN_NAME_LIMIT];
            s_copy(name, sizeof(name), toks.a[0]);
            char tname[SQL_OBJECT_STRING_LIMIT];
            s_copy(tname, sizeof(tname), toks.a[1]);
            // Support TYPE(bytes) attached
            char preBytes[SQL_OBJECT_STRING_LIMIT] = {0};
            char *par = strchr(tname, '(');
            if (par && ends_with(tname, ')')) {
                s_copy(preBytes, sizeof(preBytes), par);
                *par = '\0';
            }
            enum flintdb_variant_type  ctype = parse_column_type(tname);
            int bytes = -1, precision = -1;
            int i = 2;
            if (preBytes[0]) {
                char tmp[SQL_OBJECT_STRING_LIMIT];
                s_copy(tmp, sizeof(tmp), preBytes);
                tmp[strlen(tmp) - 1] = '\0';
                char *body = tmp + 1;
                struct strlist parts;
                memset(&parts, 0, sizeof(parts));
                if (split_top(body, ',', &parts, e) != 0) {
                    tokens_free(&toks);
                    THROW(e, "failed to parse column type parameters (prefix): %s", body);
                }
                if (parts.n >= 1 && !strempty(parts.a[0]))
                    bytes = atoi(parts.a[0]);
                if (parts.n >= 2 && !strempty(parts.a[1]))
                    precision = atoi(parts.a[1]);
                strlist_free(&parts);
            } else if (i < toks.n && starts_with(toks.a[i], '(') && ends_with(toks.a[i], ')')) {
                char tmp[SQL_OBJECT_STRING_LIMIT];
                s_copy(tmp, sizeof(tmp), toks.a[i]);
                tmp[strlen(tmp) - 1] = '\0';
                char *body = tmp + 1;
                i++;
                struct strlist parts;
                memset(&parts, 0, sizeof(parts));
                if (split_top(body, ',', &parts, e) != 0) {
                    tokens_free(&toks);
                    THROW(e, "failed to parse column type parameters: %s", body);
                }
                if (parts.n >= 1 && !strempty(parts.a[0]))
                    bytes = atoi(parts.a[0]);
                if (parts.n >= 2 && !strempty(parts.a[1]))
                    precision = atoi(parts.a[1]);
                strlist_free(&parts);
            }
            char defv[SQL_OBJECT_STRING_LIMIT] = {0};
            char comment[SQL_OBJECT_STRING_LIMIT] = {0};
            enum flintdb_null_spec nullspec = SPEC_NULLABLE;
            for (; i < toks.n; i++) {
                char *x = toks.a[i];
                if (equals_ic(x, "NOT") && (i + 1) < toks.n && equals_ic(toks.a[i + 1], "NULL")) {
                    i++;
                    nullspec = SPEC_NOT_NULL;
                    continue;
                } else if (equals_ic(x, "NULL")) { // no-op
                    continue;
                } else if (equals_ic(x, "DEFAULT") && (i + 1) < toks.n) {
                    i++;
                    char *v = toks.a[i];
                    if (starts_with(v, '\'') && ends_with(v, '\'')) {
                        v[strlen(v) - 1] = '\0';
                        v++;
                    }
                    s_copy(defv, sizeof(defv), v);
                } else if (equals_ic(x, "COMMENT") && (i + 1) < toks.n) {
                    i++;
                    char *v = toks.a[i];
                    if (starts_with(v, '\'') && ends_with(v, '\'')) {
                        v[strlen(v) - 1] = '\0';
                        v++;
                    }
                    s_copy(comment, sizeof(comment), v);
                }
            }
            if (bytes < 0 && !is_var_type(ctype)) {
                int cb = column_bytes(ctype, 0, precision);
                if (cb >= 0)
                    bytes = cb;
            }
            char *col_err = NULL;
            flintdb_meta_columns_add(out, name, ctype, bytes, precision, nullspec, strempty(defv) ? NULL : defv, strempty(comment) ? NULL : comment, &col_err);
            if (col_err) {
                tokens_free(&toks);
                THROW(e, "failed to add column '%s': %s", name, col_err);
            }
        }
        tokens_free(&toks);
    }

    // extras -> meta fields
    if (in->storage && !strempty(in->storage))
        s_copy(out->storage, sizeof(out->storage), in->storage);
    if (in->directory && !strempty(in->directory)) { /* meta has no explicit directory; keep for future or ignore */
    }
    if (in->compressor && !strempty(in->compressor))
        s_copy(out->compressor, sizeof(out->compressor), in->compressor);
    if (in->compact && !strempty(in->compact))
        out->compact = (i16)parse_bytes(in->compact);
    // if (!strempty(in->increment)) out->increment = parse_bytes(in->increment);
    if (in->cache && !strempty(in->cache))
        out->cache = parse_bytes(in->cache);
    if (in->date && !strempty(in->date))
        s_copy(out->date, sizeof(out->date), in->date);
    // Text file options
    if (in->header && !strempty(in->header))
        out->absent_header = (i8)(equals_ic(in->header, "ABSENT") || equals_ic(in->header, "SKIP") ? 1 : 0);
    if (in->delimiter && !strempty(in->delimiter) && strlen(in->delimiter) == 1)
        out->delimiter = *in->delimiter;
    if (in->quote && !strempty(in->quote) && strlen(in->quote) == 1)
        out->quote = *in->quote;
    if (in->nullString && !strempty(in->nullString))
        s_copy(out->nil_str, sizeof(out->nil_str), in->nullString);
    if (in->format && !strempty(in->format))
        s_copy(out->format, sizeof(out->format), in->format);
    if (in->wal && !strempty(in->wal))
        s_copy(out->wal, sizeof(out->wal), in->wal);
    if (in->wal_batch_size > 0)
        out->wal_batch_size = in->wal_batch_size;
    if (in->wal_checkpoint_interval > 0)
        out->wal_checkpoint_interval = in->wal_checkpoint_interval;
    if (in->wal_compression_threshold > 0)
        out->wal_compression_threshold = in->wal_compression_threshold;
    if (in->wal_sync != 0)
        out->wal_sync = in->wal_sync;
    if (in->wal_buffer_size > 0)
        out->wal_buffer_size = in->wal_buffer_size;
    if (in->wal_page_data == 0)
        out->wal_page_data = 0;
    return 0;

EXCEPTION:
    flintdb_meta_close(out);
    return -1;
}

// --- stringify helpers for meta -> SQL
const char * flintdb_variant_type_name(enum flintdb_variant_type  t) {
    switch (t) {
    case VARIANT_INT32:
        return "INT";
    case VARIANT_UINT32:
        return "UINT";
    case VARIANT_INT8:
        return "INT8";
    case VARIANT_UINT8:
        return "UINT8";
    case VARIANT_INT16:
        return "INT16";
    case VARIANT_UINT16:
        return "UINT16";
    case VARIANT_INT64:
        return "INT64";
    case VARIANT_DOUBLE:
        return "DOUBLE";
    case VARIANT_FLOAT:
        return "FLOAT";
    case VARIANT_STRING:
        return "STRING";
    case VARIANT_DECIMAL:
        return "DECIMAL";
    case VARIANT_BYTES:
        return "BYTES";
    case VARIANT_DATE:
        return "DATE";
    case VARIANT_TIME:
        return "TIME";
    case VARIANT_UUID:
        return "UUID";
    case VARIANT_IPV6:
        return "IPV6";
    case VARIANT_BLOB:
        return "BLOB";
    case VARIANT_OBJECT:
        return "OBJECT";
    case VARIANT_NULL:
    default:
        return "NIL";
    }
}

static void append_quoted_single(char *dst, size_t cap, const char *val) {
    if (!val)
        return;
    s_cat(dst, cap, " '");
    for (const char *p = val; *p; ++p) {
        if (*p == '\'' || *p == '\\') {
            char esc[3] = {'\\', *p, 0};
            s_cat(dst, cap, esc);
        } else {
            char ch[2] = {*p, 0};
            s_cat(dst, cap, ch);
        }
    }
    s_cat(dst, cap, "'");
}

static void append_bytes_unit(char *dst, size_t cap, long long v) {
    char buf[64];
    if (v <= 0) {
        snprintf(buf, sizeof(buf), "%lld", v);
        s_cat(dst, cap, buf);
        return;
    }
    if (v % (1024LL * 1024LL * 1024LL) == 0) {
        snprintf(buf, sizeof(buf), "%lldG", v / (1024LL * 1024LL * 1024LL));
    } else if (v % (1024LL * 1024LL) == 0) {
        snprintf(buf, sizeof(buf), "%lldM", v / (1024LL * 1024LL));
    } else if (v % 1024LL == 0) {
        snprintf(buf, sizeof(buf), "%lldK", v / 1024LL);
    } else {
        snprintf(buf, sizeof(buf), "%lld", v);
    }
    s_cat(dst, cap, buf);
}

int flintdb_meta_to_sql_string(const struct flintdb_meta *m, char *s, i32 len, char **e) {
    if (!m)
        THROW(e, "meta is NULL");
    if (!s)
        THROW(e, "output buffer is NULL");
    if (len <= 0)
        THROW(e, "output buffer length is invalid: %d", len);
    // CREATE TABLE ...
    char tmp[SQL_STRING_LIMIT];
    tmp[0] = '\0';
    s_cat(tmp, sizeof(tmp), "CREATE TABLE ");
    s_cat(tmp, sizeof(tmp), m->name);
    s_cat(tmp, sizeof(tmp), " (\n");

    for (int i = 0; i < m->columns.length; i++) {
        const struct flintdb_column *c = &m->columns.a[i];
        if (i > 0) {
            s_cat(tmp, sizeof(tmp), ", \n");
        }

        // column definition
        s_cat(tmp, sizeof(tmp), "  ");

        s_cat(tmp, sizeof(tmp), c->name);
        s_cat(tmp, sizeof(tmp), " ");
        s_cat(tmp, sizeof(tmp), flintdb_variant_type_name(c->type));
        if (c->bytes > 0 || c->precision > 0) {
            s_cat(tmp, sizeof(tmp), "(");
            char nb[32];
            if (c->bytes > 0) {
                snprintf(nb, sizeof(nb), "%d", c->bytes);
                s_cat(tmp, sizeof(tmp), nb);
            }
            if (c->precision > 0) {
                if (c->bytes > 0)
                    s_cat(tmp, sizeof(tmp), ",");
                snprintf(nb, sizeof(nb), "%d", c->precision);
                s_cat(tmp, sizeof(tmp), nb);
            }
            s_cat(tmp, sizeof(tmp), ")");
        }

        if (c->nullspec == SPEC_NOT_NULL) 
            s_cat(tmp, sizeof(tmp), " NOT NULL");

        if (c->value[0]) {
            s_cat(tmp, sizeof(tmp), " DEFAULT");
            append_quoted_single(tmp, sizeof(tmp), c->value);
        }
        if (c->comment[0]) {
            s_cat(tmp, sizeof(tmp), " COMMENT");
            append_quoted_single(tmp, sizeof(tmp), c->comment);
        }
    }

    // indexes
    for (int i = 0; i < m->indexes.length; i++) {
        s_cat(tmp, sizeof(tmp), ", \n  ");
        const struct flintdb_index *idx = &m->indexes.a[i];
        if (equals_ic(idx->name, PRIMARY_NAME)) {
            s_cat(tmp, sizeof(tmp), "PRIMARY KEY ");
        } else {
            s_cat(tmp, sizeof(tmp), "KEY ");
            s_cat(tmp, sizeof(tmp), idx->name);
            s_cat(tmp, sizeof(tmp), " ");
        }
        s_cat(tmp, sizeof(tmp), "(");
        for (int k = 0; k < idx->keys.length; k++) {
            if (k > 0)
                s_cat(tmp, sizeof(tmp), ", ");
            s_cat(tmp, sizeof(tmp), idx->keys.a[k]);
        }
        s_cat(tmp, sizeof(tmp), ")");
    }
    s_cat(tmp, sizeof(tmp), "\n)");

    // options - add comma between options like Java implementation
    int extras = 0;
    if (m->storage[0]) {
        s_cat(tmp, sizeof(tmp), extras > 0 ? ", " : " ");
        s_cat(tmp, sizeof(tmp), "STORAGE=");
        s_cat(tmp, sizeof(tmp), m->storage);
        extras++;
    }
    if (m->compressor[0]) {
        s_cat(tmp, sizeof(tmp), extras > 0 ? ", " : " ");
        s_cat(tmp, sizeof(tmp), "COMPRESSOR=");
        s_cat(tmp, sizeof(tmp), m->compressor);
        extras++;
    }
    if (m->compact >= 0) {
        s_cat(tmp, sizeof(tmp), extras > 0 ? ", " : " ");
        s_cat(tmp, sizeof(tmp), "COMPACT=");
        append_bytes_unit(tmp, sizeof(tmp), m->compact);
        extras++;
    }
    // if (m->increment > 0) { s_cat(tmp, sizeof(tmp), extras > 0 ? ", " : " "); s_cat(tmp, sizeof(tmp), "INCREMENT="); append_bytes_unit(tmp, sizeof(tmp), m->increment); extras++; }
    if (m->cache > 0) {
        s_cat(tmp, sizeof(tmp), extras > 0 ? ", " : " ");
        s_cat(tmp, sizeof(tmp), "CACHE=");
        append_bytes_unit(tmp, sizeof(tmp), m->cache);
        extras++;
    }
    if (m->date[0]) {
        s_cat(tmp, sizeof(tmp), extras > 0 ? ", " : " ");
        s_cat(tmp, sizeof(tmp), "DATE=");
        s_cat(tmp, sizeof(tmp), m->date);
        extras++;
    }
    if (m->absent_header) {
        s_cat(tmp, sizeof(tmp), extras > 0 ? ", " : " ");
        s_cat(tmp, sizeof(tmp), "HEADER=ABSENT");
        extras++;
    }
    if (m->delimiter && m->delimiter != '\t') {
        s_cat(tmp, sizeof(tmp), extras > 0 ? ", " : " ");
        s_cat(tmp, sizeof(tmp), "DELIMITER=");
        char d[2] = {(char)m->delimiter, 0};
        s_cat(tmp, sizeof(tmp), d);
        extras++;
    }
    if (m->quote && m->quote != '"') {
        s_cat(tmp, sizeof(tmp), extras > 0 ? ", " : " ");
        s_cat(tmp, sizeof(tmp), "QUOTE=");
        char q[2] = {(char)m->quote, 0};
        s_cat(tmp, sizeof(tmp), q);
        extras++;
    }
    if (m->nil_str[0]) {
        s_cat(tmp, sizeof(tmp), extras > 0 ? ", " : " ");
        s_cat(tmp, sizeof(tmp), "NULL=");
        s_cat(tmp, sizeof(tmp), m->nil_str);
        extras++;
    }
    if (m->format[0]) {
        s_cat(tmp, sizeof(tmp), extras > 0 ? ", " : " ");
        s_cat(tmp, sizeof(tmp), "FORMAT=");
        s_cat(tmp, sizeof(tmp), m->format);
        extras++;
    }
    if (m->wal[0]) {
        s_cat(tmp, sizeof(tmp), extras > 0 ? ", " : " ");
        s_cat(tmp, sizeof(tmp), "WAL=");
        s_cat(tmp, sizeof(tmp), m->wal);
        extras++;

        if (m->wal_batch_size > 0) {
            s_cat(tmp, sizeof(tmp), ", WAL_BATCH_SIZE=");
            char bs[32];
            snprintf(bs, sizeof(bs), "%d", m->wal_batch_size);
            s_cat(tmp, sizeof(tmp), bs);
        }

        if (m->wal_checkpoint_interval > 0) {
            s_cat(tmp, sizeof(tmp), ", WAL_CHECKPOINT_INTERVAL=");
            char cp[32];
            snprintf(cp, sizeof(cp), "%d", m->wal_checkpoint_interval);
            s_cat(tmp, sizeof(tmp), cp);
        }

        if (m->wal_compression_threshold > 0) {
            s_cat(tmp, sizeof(tmp), ", WAL_COMPRESSION_THRESHOLD=");
            char ct[32];
            snprintf(ct, sizeof(ct), "%d", m->wal_compression_threshold);
            s_cat(tmp, sizeof(tmp), ct);
        }
        if (m->wal_sync != 0) {
            s_cat(tmp, sizeof(tmp), ", WAL_SYNC=");
            if (m->wal_sync == WAL_SYNC_OFF) s_cat(tmp, sizeof(tmp), "OFF");
            else if (m->wal_sync == WAL_SYNC_NORMAL) s_cat(tmp, sizeof(tmp), "NORMAL");
            else if (m->wal_sync == WAL_SYNC_FULL) s_cat(tmp, sizeof(tmp), "FULL");
            else s_cat(tmp, sizeof(tmp), "DEFAULT");
        }

        if (m->wal_buffer_size > 0) {
            s_cat(tmp, sizeof(tmp), ", WAL_BUFFER_SIZE=");
            append_bytes_unit(tmp, sizeof(tmp), m->wal_buffer_size);
        }

        if (m->wal_page_data == 0) {
            s_cat(tmp, sizeof(tmp), ", WAL_PAGE_DATA=OFF");
        }
    }

    s_cat(tmp, sizeof(tmp), "\n");
    s_copy(s, len, tmp);
    return 0;

EXCEPTION:
    return -1;
}

// ============================================================================
// Parsing utility functions
// ============================================================================

/**
 * Extract alias from SQL expression (e.g., "COUNT(*) AS total" -> "total")
 * Returns 1 if alias found, 0 otherwise
 */
int sql_extract_alias(const char *expr, char *alias_out, size_t alias_cap) {
    if (!expr || !alias_out || alias_cap == 0)
        return 0;
    // Create uppercase copy for searching
    char upper[1024];
    size_t len = strlen(expr);
    if (len >= sizeof(upper))
        len = sizeof(upper) - 1;
    for (size_t i = 0; i < len; i++) {
        upper[i] = (char)toupper((unsigned char)expr[i]);
    }
    upper[len] = '\0';
    // Search for " AS " pattern (ensure surrounded by whitespace or start/end)
    const char *pos = strstr(upper, " AS ");
    if (!pos) {
        // Also handle patterns like " AS\t" or multiple spaces: find " AS" then skip spaces
        pos = strstr(upper, " AS");
        if (pos) {
            const char *after = pos + 3; // after ' AS'
            if (*after != '\0' && (*after == ' ' || *after == '\t')) {
                // treat as found
            } else {
                pos = NULL; // not a valid delimiter
            }
        }
    }
    if (!pos) {
        // Support trailing alias without AS, e.g., "COUNT(*) v"
        // Only apply on expressions that likely contain a function call
        // Find last non-space token
        const char *endp = expr + strlen(expr);
        while (endp > expr && (endp[-1] == ' ' || endp[-1] == '\t' || endp[-1] == '\n' || endp[-1] == '\r'))
            endp--;
        const char *p = endp;
        // scan backwards to start of identifier [A-Za-z_][A-Za-z0-9_]*
        while (p > expr) {
            char c = p[-1];
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
                p--;
                continue;
            }
            break;
        }
        // Require at least one whitespace before alias token
        if (p > expr && (p[-1] == ' ' || p[-1] == '\t')) {
            // Also ensure alias appears after any closing parenthesis (for func calls)
            const char *rparen = strrchr(expr, ')');
            if (!rparen || p > rparen) {
                size_t a_len = (size_t)(endp - p);
                if (a_len > 0) {
                    if (a_len >= alias_cap)
                        a_len = alias_cap - 1;
                    memcpy(alias_out, p, a_len);
                    alias_out[a_len] = '\0';
                    return 1;
                }
            }
        }
        return 0;
    }
    // Map back to original string index
    size_t offset = (size_t)(pos - upper);
    const char *alias_start = expr + offset + 3; // skip AS
    while (*alias_start == ' ' || *alias_start == '\t')
        alias_start++;
    // Copy alias
    size_t a_len = strlen(alias_start);
    // trim trailing spaces
    while (a_len > 0 && (alias_start[a_len - 1] == ' ' || alias_start[a_len - 1] == '\t' || alias_start[a_len - 1] == '\n' || alias_start[a_len - 1] == '\r'))
        a_len--;
    if (a_len >= alias_cap)
        a_len = alias_cap - 1;
    memcpy(alias_out, alias_start, a_len);
    alias_out[a_len] = '\0';
    return 1;
}

/**
 * Parse GROUP BY column names from comma-separated string
 * Returns the number of columns parsed
 */
int sql_parse_groupby_columns(const char *groupby, char cols[][MAX_COLUMN_NAME_LIMIT]) {
    if (!groupby || !*groupby)
        return 0;
    int count = 0;
    const char *p = groupby;
    while (*p && count < MAX_COLUMNS_LIMIT) {
        while (*p == ' ' || *p == '\t' || *p == ',')
            p++; // skip delimiters
        if (!*p)
            break;
        const char *start = p;
        while (*p && *p != ',')
            p++;
        int len = (int)(p - start);
        while (len > 0 && (start[len - 1] == ' ' || start[len - 1] == '\t'))
            len--; // trim trailing spaces
        if (len <= 0)
            continue; // empty token (consecutive commas)
        if (len >= MAX_COLUMN_NAME_LIMIT)
            len = MAX_COLUMN_NAME_LIMIT - 1;
        strncpy(cols[count], start, len);
        cols[count][len] = '\0';
        count++;
    }
    return count;
}

/**
 * Parse ORDER BY clause with ASC/DESC support
 * Returns the number of columns parsed
 */
int sql_parse_orderby_clause(const char *orderby, char cols[][MAX_COLUMN_NAME_LIMIT], i8 desc_flags[], int *ecount) {
    if (!orderby || !cols || !desc_flags || !ecount)
        return 0;
    int count = 0;
    const char *p = orderby;
    while (*p) {
        while (*p == ' ' || *p == '\t' || *p == ',')
            p++; // skip delimiters
        if (*p == '\0')
            break;
        const char *start = p;
        while (*p && *p != ',' && *p != ' ')
            p++;
        int len = (int)(p - start);
        if (len > 0) {
            if (len >= MAX_COLUMN_NAME_LIMIT)
                len = MAX_COLUMN_NAME_LIMIT - 1;
            strncpy(cols[count], start, len);
            cols[count][len] = '\0';
            // Skip spaces to check ASC/DESC
            while (*p == ' ' || *p == '\t')
                p++;
            i8 descending = 0;
            if (strncasecmp(p, "DESC", 4) == 0) {
                descending = 1;
                p += 4;
            } else if (strncasecmp(p, "ASC", 3) == 0) {
                descending = 0;
                p += 3;
            }
            desc_flags[count] = descending;
            count++;
            if (count >= MAX_COLUMNS_LIMIT)
                break;
            // advance to next comma
            while (*p && *p != ',')
                p++;
        }
        if (*p == ',')
            p++; // consume comma
    }
    *ecount = count;
    return count;
}