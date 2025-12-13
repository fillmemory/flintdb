#include "../../src/flintdb.h"
#include "../../src/runtime.h"
#include "../../src/internal.h"
#include "../../src/iostream.h"
#include "../../src/filter.h"
#include "../../src/sql.h"
#include <cjson/cJSON.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

#define LINE_BUFSZ (1 << 18) // 256KB max line
#define IO_BUFSZ (1 << 20)   // 1MB buffered IO

struct jsonlfile_priv {
    char file[PATH_MAX];
    enum flintdb_open_mode mode;
    struct flintdb_meta meta;
    i64 rows_count;
};

// Parse cJSON and populate row
static int jsonl_parse_row(cJSON *json, struct flintdb_row *r, const struct flintdb_meta *m, char **e) {
    if (!json || !r || !m)
        return -1;
    
    for (i32 i = 0; i < m->columns.length; i++) {
        const char *col_name = m->columns.a[i].name;
        cJSON *item = cJSON_GetObjectItemCaseSensitive(json, col_name);
        
        if (!item || cJSON_IsNull(item)) {
            struct flintdb_variant *v = r->get(r, i, e);
            if (v) flintdb_variant_null_set(v);
            continue;
        }
        
        if (cJSON_IsBool(item)) {
            if (r->i8_set)
                r->i8_set(r, i, cJSON_IsTrue(item) ? 1 : 0, e);
        } else if (cJSON_IsNumber(item)) {
            double val = cJSON_GetNumberValue(item);
            if (val == (i64)val && r->i64_set) {
                r->i64_set(r, i, (i64)val, e);
            } else if (r->f64_set) {
                r->f64_set(r, i, val, e);
            }
        } else if (cJSON_IsString(item)) {
            if (r->string_set) {
                const char *str = cJSON_GetStringValue(item);
                r->string_set(r, i, str, e);
            }
        } else if (cJSON_IsArray(item) || cJSON_IsObject(item)) {
            if (r->string_set) {
                char *json_str = cJSON_PrintUnformatted(item);
                if (json_str) {
                    r->string_set(r, i, json_str, e);
                    cJSON_free(json_str);
                }
            }
        }
    }
    
    return 0;
}

// Infer schema from first JSON line using bufio
static struct flintdb_meta jsonl_infer_schema(const char *file, char **e) {
    struct flintdb_meta m = {0};
    cJSON *json = NULL;
    
    // Use file_bufio_open - automatically handles .gz files
    struct bufio *bio = file_bufio_open(file, FLINTDB_RDONLY, IO_BUFSZ, e);
    if (e && *e)
        THROW_S(e);
    
    char line[LINE_BUFSZ];
    
    // Read first non-empty line
    while (1) {
        ssize_t n = bio->readline(bio, line, sizeof(line), e);
        if (n <= 0)
            break;
        
        // Remove trailing newline
        if (n > 0 && line[n-1] == '\n') {
            line[n-1] = '\0';
            n--;
        }
        
        if (n == 0 || line[0] == '\0')
            continue;
            
        json = cJSON_Parse(line);
        if (!json)
            continue;
            
        // Infer schema from first valid JSON object
        if (cJSON_IsObject(json)) {
            cJSON *item = NULL;
            cJSON_ArrayForEach(item, json) {
                if (m.columns.length >= MAX_COLUMNS_LIMIT)
                    break;
                
                struct flintdb_column *col = &m.columns.a[m.columns.length];
                strncpy(col->name, item->string, sizeof(col->name) - 1);
                col->name[sizeof(col->name) - 1] = '\0';
                
                if (cJSON_IsBool(item)) {
                    col->type = VARIANT_INT8;
                    col->bytes = 1;
                } else if (cJSON_IsNumber(item)) {
                    double val = cJSON_GetNumberValue(item);
                    if (val == (i64)val) {
                        col->type = VARIANT_INT64;
                        col->bytes = 8;
                    } else {
                        col->type = VARIANT_DOUBLE;
                        col->bytes = 8;
                    }
                } else {
                    col->type = VARIANT_STRING;
                    col->bytes = 0;
                }
                m.columns.length++;
            }
        }
        break;
    }
    
    cJSON_Delete(json);
    bio->close(bio);
    return m;
    
EXCEPTION:
    if (json) cJSON_Delete(json);
    if (bio) bio->close(bio);
    return m;
}

// Cursor using bufio
struct jsonl_cursor_priv {
    struct bufio *bio;
    const struct flintdb_meta *meta;
    struct filter *filter;      // compiled filter (WHERE clause)
    struct limit limit;         // LIMIT/OFFSET tracking
    i64 rowidx;                 // current physical line number
    i8 initialized;             // future use (placeholder)
    char line[LINE_BUFSZ];      // current raw line buffer
};

static struct flintdb_row *jsonl_cursor_next(struct flintdb_cursor_row *c, char **e) {
    if (!c || !c->p)
        return NULL;
    struct jsonl_cursor_priv *cp = (struct jsonl_cursor_priv*)c->p;
    struct flintdb_row *r = NULL;

    while (1) {
        ssize_t n = cp->bio->readline(cp->bio, cp->line, sizeof(cp->line), e);
        if (n <= 0)
            return NULL; // EOF

        // Trim trailing newline
        if (n > 0 && cp->line[n-1] == '\n') {
            cp->line[n-1] = '\0';
            n--;
        }

        // Skip empty lines early
        if (n == 0 || cp->line[0] == '\0') {
            cp->rowidx++;
            continue;
        }

        // Apply offset skipping before decoding to minimize work
        if (cp->limit.skip && cp->limit.skip(&cp->limit)) {
            cp->rowidx++;
            continue;
        }

        cJSON *json = cJSON_Parse(cp->line);
        if (!json) { // malformed line - skip
            cp->rowidx++;
            continue;
        }

        r = flintdb_row_new((struct flintdb_meta*)cp->meta, e);
        if (e && *e) {
            cJSON_Delete(json);
            THROW_S(e);
        }
        if (!r) {
            cJSON_Delete(json);
            return NULL;
        }

        jsonl_parse_row(json, r, cp->meta, e);
        cJSON_Delete(json);
        if (e && *e) {
            r->free(r);
            r = NULL;
            THROW_S(e);
        }

        // Filter evaluation
        int matched = 1;
        if (cp->filter) {
            int cmp = filter_compare(cp->filter, r, e);
            if (e && *e) {
                r->free(r);
                r = NULL;
                THROW_S(e);
            }
            matched = (cmp == 0);
        }

        if (matched) {
            if (cp->limit.remains && cp->limit.remains(&cp->limit) <= 0) {
                r->free(r);
                r = NULL;
                return NULL; // limit exhausted
            }
            cp->rowidx++;
            return r;
        }

        // Not matched, free and continue
        cp->rowidx++;
        r->free(r);
        r = NULL;
    }

EXCEPTION:
    if (r)
        r->free(r);
    return NULL;
}

static void jsonl_cursor_close(struct flintdb_cursor_row *c) {
    if (!c || !c->p)
        return;
    struct jsonl_cursor_priv *cp = (struct jsonl_cursor_priv*)c->p;
    if (cp->bio) {
        cp->bio->close(cp->bio);
        cp->bio = NULL;
    }
    if (cp->filter) {
        filter_free(cp->filter);
        cp->filter = NULL;
    }
    FREE(cp);
    FREE(c);
}

static struct flintdb_cursor_row *jsonlfile_find(const struct flintdb_genericfile *gf, struct limit limit, struct filter *filter, char **e) {
    struct jsonlfile_priv *priv = NULL;
    struct bufio *bio = NULL;
    struct flintdb_cursor_row *cursor = NULL;

    if (!gf || !gf->priv)
        THROW(e, "invalid jsonlfile");
    priv = (struct jsonlfile_priv*)gf->priv;

    bio = file_bufio_open(priv->file, FLINTDB_RDONLY, IO_BUFSZ, e);
    if (e && *e)
        THROW_S(e);

    cursor = CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!cursor)
        THROW(e, "Failed to allocate memory for cursor");
    cursor->p = CALLOC(1, sizeof(struct jsonl_cursor_priv));
    if (!cursor->p)
        THROW(e, "Failed to allocate memory for cursor private data");

    struct jsonl_cursor_priv *cp = (struct jsonl_cursor_priv*)cursor->p;
    cp->bio = bio;
    cp->meta = &priv->meta;
    cp->filter = (struct filter*)filter;
    cp->limit = limit;
    cp->limit.priv.n = (cp->limit.priv.limit < 0) ? INT_MAX : cp->limit.priv.limit; // initialize counters
    cp->limit.priv.o = cp->limit.priv.offset;
    cp->rowidx = 0;
    cp->initialized = 0;

    cursor->next = jsonl_cursor_next;
    cursor->close = jsonl_cursor_close;
    return cursor;

EXCEPTION:
    if (cursor) {
        if (cursor->p)
            FREE(cursor->p);
        FREE(cursor);
    }
    if (bio)
        bio->close(bio);
    if (filter)
        filter_free(filter);
    return NULL;
}

static struct flintdb_cursor_row *jsonlfile_find_where(const struct flintdb_genericfile *gf, const char *where, char **e) {
    if (!gf || !gf->priv)
        return NULL;

    struct jsonlfile_priv *priv = (struct jsonlfile_priv*)gf->priv;
    struct filter *filter = NULL;
    struct flintdb_sql *q = NULL;

    char sql[SQL_STRING_LIMIT] = {0};
    if (where && where[0]) {
        snprintf(sql, sizeof(sql), "SELECT * FROM %s %s", priv->file, where);
    } else {
        snprintf(sql, sizeof(sql), "SELECT * FROM %s", priv->file);
    }

    q = flintdb_sql_parse(sql, e);
    if (e && *e)
        THROW_S(e);

    filter = filter_compile(q->where, &priv->meta, e);
    if (e && *e)
        THROW_S(e);

    struct limit lim = !strempty(q->limit) ? limit_parse(q->limit) : NOLIMIT;
    flintdb_sql_free(q);

    return jsonlfile_find(gf, lim, filter, e);

EXCEPTION:
    if (filter)
        filter_free(filter);
    if (q)
        flintdb_sql_free(q);
    return NULL;
}

static i64 jsonlfile_rows(const struct flintdb_genericfile *gf, char **e) {
    if (!gf || !gf->priv)
        return -1;
    
    struct jsonlfile_priv *p = (struct jsonlfile_priv*)gf->priv;
    if (p->rows_count >= 0)
        return p->rows_count;
    
    // Use file_bufio_open - automatically handles .gz files
    struct bufio *bio = file_bufio_open(p->file, FLINTDB_RDONLY, IO_BUFSZ, e);
    if (e && *e)
        return -1;
    
    i64 count = 0;
    char line[LINE_BUFSZ];
    
    while (1) {
        ssize_t n = bio->readline(bio, line, sizeof(line), e);
        if (n <= 0)
            break;
        if (line[0] != '\0' && line[0] != '\n')
            count++;
    }
    
    bio->close(bio);
    p->rows_count = count;
    return count;
}

static i64 jsonlfile_bytes(const struct flintdb_genericfile *gf, char **e) {
    if (!gf || !gf->priv)
        return -1;
    
    struct jsonlfile_priv *p = (struct jsonlfile_priv*)gf->priv;
    struct stat st;
    if (stat(p->file, &st) != 0)
        return -1;
    return st.st_size;
}

static const struct flintdb_meta *jsonlfile_meta(const struct flintdb_genericfile *gf, char **e) {
    if (!gf || !gf->priv)
        return NULL;
    return &((struct jsonlfile_priv*)gf->priv)->meta;
}

static i64 jsonlfile_write(struct flintdb_genericfile *gf, struct flintdb_row *r, char **e) {
    THROW(e, "JSONL write not yet implemented");
EXCEPTION:
    return -1;
}

static void jsonlfile_close(struct flintdb_genericfile *gf) {
    if (!gf)
        return;
    if (gf->priv) {
        FREE(gf->priv);
        gf->priv = NULL;
    }
    FREE(gf);
}

struct flintdb_genericfile *jsonlfile_open(const char *file, enum flintdb_open_mode mode, const struct flintdb_meta *meta, char **e) {
    struct flintdb_genericfile *f = NULL;
    struct jsonlfile_priv *priv = NULL;
    if (!file || !*file)
        THROW(e, "file path is empty");
    
    f = CALLOC(1, sizeof(struct flintdb_genericfile));
    if (!f)
        THROW(e, "Out of memory");
    
    priv = CALLOC(1, sizeof(struct jsonlfile_priv));
    if (!priv)
        THROW(e, "Out of memory");
    
    priv->mode = mode;
    strncpy(priv->file, file, PATH_MAX - 1);
    priv->file[PATH_MAX - 1] = '\0';
    priv->rows_count = -1;
    
    // Infer schema
    priv->meta = jsonl_infer_schema(file, e);
    if (e && *e)
        THROW_S(e);
    
    f->priv = priv;
    f->find = jsonlfile_find_where;
    f->rows = jsonlfile_rows;
    f->bytes = jsonlfile_bytes;
    f->meta = jsonlfile_meta;
    f->write = jsonlfile_write;
    f->close = jsonlfile_close;
    
    return f;
    
EXCEPTION:
    if (f)
        FREE(f);
    if (priv)
        FREE(priv);
    return NULL;
}
