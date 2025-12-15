#include "flintdb.h"
#include "runtime.h"
#include "buffer.h"
#include "filter.h"
#include "internal.h"
#include "iostream.h"
#include "sql.h"
#include "plugin.h"

#include <assert.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <unistd.h>

// TODO: O_APPEND flag support for genericfile_open

// Tuned buffer sizes for faster sequential text IO
// - IO_BUFSZ: size of the underlying buffered IO for file reads/writes
// - LINE_BUFSZ: maximum size of an accumulated logical record (handles multi-line CSV)
// - HEADER_BUFSZ: header read buffer (small, one line)
#define IO_BUFSZ (1 << 20)   // 1MB buffered IO (default, can be overridden by env IO_BUFSZ)
#define LINE_BUFSZ (1 << 18) // 256KB max logical line
#define HEADER_BUFSZ 8192

// Check if accumulated CSV/TSV record is complete respecting quote rules
static int record_completed_helper(const struct flintdb_meta *m, const char *s, size_t len) {
    if (!m || !s)
        return 1;
    if (m->quote == 0)
        return 1; // TSV-like: each physical line is a record
    int qoute = 0;
    char Q = m->quote;
    for (size_t i = 0; i < len; ++i) {
        char ch = s[i];
        char next = (i + 1 < len) ? s[i + 1] : '\0';
        if (qoute > 0 && Q == ch && Q == next) {
            i++;
            continue;
        } else if (qoute > 0 && Q == ch) {
            qoute = 0;
        } else if (Q == ch) {
            qoute = 1;
        }
    }
    return qoute == 0;
}

enum file_data_header {
    HEADER_ABSENT = 0,
    HEADER_PRESENT = 1,
};

struct flintdb_genericfile_priv {
    // private data for file implementation
    char file[PATH_MAX];
    enum flintdb_open_mode mode;
    struct flintdb_meta meta;

    i64 rows; // cached row count for parquet (CSV, TSV, JSONL, ... => always -1)

    struct formatter formatter; // formatter for encoding/decoding rows
    // lazy writer for FLINTDB_RDWR mode
    struct bufio *wbio;
    i8 header_written;
    enum file_data_header file_data_header;
};

// Parse an environment variable representing bytes. Supports optional K/M/G suffixes.
static size_t parse_env_bytes(const char *name, size_t defval) {
    const char *s = getenv(name);
    if (!s || !*s)
        return defval;
    char *end = NULL;
    long long v = strtoll(s, &end, 10);
    if (v <= 0)
        return defval;
    while (end && *end && isspace((unsigned char)*end))
        end++;
    if (end && *end) {
        if (*end == 'K' || *end == 'k')
            v *= 1024LL;
        else if (*end == 'M' || *end == 'm')
            v *= (1024LL * 1024LL);
        else if (*end == 'G' || *end == 'g')
            v *= (1024LL * 1024LL * 1024LL);
    }
    if (v <= 0)
        return defval;
    return (size_t)v;
}

static inline size_t io_buf_size_default() {
    return parse_env_bytes("IO_BUFSZ", (size_t)IO_BUFSZ);
}

struct flintdb_genericfile_cursor_priv {
    struct formatter *formatter;
    struct bufio *bio;
    char line[LINE_BUFSZ];
    struct filter *filter;
    struct limit limit;
    i64 rowidx;     // current data row index (after header)
    i8 initialized; // init guard for header handling
    enum file_data_header file_data_header;

    // Cursor-owned last returned row (BORROWED by caller). Freed on next() or close().
    struct flintdb_row *last_row;
};

void flintdb_genericfile_drop(const char *file, char **e) {
    if (!file)
        return;
    char dir[PATH_MAX] = {0};
    getdir(file, dir);
    if (!dir_exists(dir))
        return;

    DIR *d = opendir(dir);
    if (!d)
        THROW(e, "Failed to open directory: %s", dir);

    char base[PATH_MAX] = {0};
    getname(file, base);
    size_t base_len = strlen(base);
    struct dirent *de = NULL;
    while ((de = readdir(d)) != NULL) {
        if (0 == strncmp(de->d_name, base, base_len)) {
            char fpath[PATH_MAX] = {0};
            snprintf(fpath, sizeof(fpath), "%s%c%s", dir, PATH_CHAR, de->d_name);
            unlink(fpath);
        }
    }
    closedir(d);
    return;

EXCEPTION:
    if (d)
        closedir(d);
    return;
}

static i64 genericfile_rows(const struct flintdb_genericfile *me, char **e) {
    if (!me || !me->priv)
        return -1;
    struct flintdb_genericfile_priv *priv = (struct flintdb_genericfile_priv *)me->priv;
    return priv->rows;
}

static i64 genericfile_bytes(const struct flintdb_genericfile *me, char **e) {
    return file_length(((struct flintdb_genericfile_priv *)me->priv)->file);
}

static const struct flintdb_meta *genericfile_meta(const struct flintdb_genericfile *me, char **e) {
    return &((struct flintdb_genericfile_priv *)me->priv)->meta;
}

static i64 genericfile_write(struct flintdb_genericfile *me, struct flintdb_row *r, char **e) {
    if (!me || !me->priv || !r)
        return -1;
    struct flintdb_genericfile_priv *priv = (struct flintdb_genericfile_priv *)me->priv;
    if (priv->mode != FLINTDB_RDWR) {
        THROW(e, "file not opened for write: %s", priv->file);
    }

    // Initialize writer lazily (first write truncates/creates the file)
    if (!priv->wbio) {
        // ensure parent directory exists (genericfile_open did this, but be robust)
        char dir[PATH_MAX] = {0};
        getdir(priv->file, dir);
        if (dir[0])
            mkdirs(dir, S_IRWXU);
        DEBUG("genericfile_write: open writer for %s", priv->file);
        size_t iobsz = io_buf_size_default();
        priv->wbio = file_bufio_open(priv->file, FLINTDB_RDWR, iobsz, e);
        if (e && *e)
            THROW_S(e);
        priv->header_written = 0;
        // reset rows counter on first open for write
        if (priv->rows < 0)
            priv->rows = 0;
    }

    // Emit header once for text formats when meta.absent_header is set
    if (!priv->header_written && priv->formatter.meta && priv->formatter.meta->absent_header) {
        const struct flintdb_meta *m = priv->formatter.meta;
        // Build header line: column names separated by delimiter
        char delim = m->delimiter ? m->delimiter : '\t';
        // Conservative fixed buffer; column names are MAX_COLUMN_NAME_LIMIT each
        // If overflow risk, write incrementally via wbio
        char line[4096];
        size_t ln = 0;
        for (int i = 0; i < m->columns.length; i++) {
            const char *name = m->columns.a[i].name;
            if (i > 0) {
                if (ln + 1 >= sizeof(line))
                    break; // fallback to partial; extremely unlikely
                line[ln++] = delim;
            }
            size_t nl = strlen(name);
            if (ln + nl >= sizeof(line))
                nl = sizeof(line) - ln - 1;
            memcpy(line + ln, name, nl);
            ln += nl;
        }
        // Write header with newline
        DEBUG("genericfile_write: write header (%d cols)", m->columns.length);
        ssize_t wn = priv->wbio->writeline(priv->wbio, line, ln, e);
        if (wn < 0)
            THROW_S(e);
        priv->header_written = 1;
    }

    // Encode row using formatter (CSV/TSV encoders append newline)
    struct buffer *bout = buffer_alloc(1024);
    if (!bout)
        THROW(e, "Out of memory");
    int enc = priv->formatter.encode(&priv->formatter, r, bout, e);
    if (enc != 0) {
        if (bout)
            bout->free(bout);
        THROW_S(e);
    }
    // Write encoded bytes
    u32 nbytes = bout->limit; // after flip(), position=0, limit=length
    const char *data = bout->array;
    DEBUG("genericfile_write: write data %u bytes", nbytes);
    ssize_t wn = priv->wbio->write(priv->wbio, data, nbytes, e);
    // Free temporary buffer regardless of write result
    if (bout)
        bout->free(bout);
    if (wn < 0)
        THROW_S(e);

    // Update rows counter if tracked
    if (priv->rows >= 0)
        priv->rows++;
    return 0;

EXCEPTION:
    return -1;
}

static void genericfile_cursor_close(struct flintdb_cursor_row *cursor) {
    if (!cursor)
        return;
    if (cursor->p) {
        struct flintdb_genericfile_cursor_priv *cp = (struct flintdb_genericfile_cursor_priv *)cursor->p;
        if (cp) {
            if (cp->last_row) {
                cp->last_row->free(cp->last_row);
                cp->last_row = NULL;
            }
            if (cp->bio) {
                cp->bio->close(cp->bio);
                cp->bio = NULL;
            }

            if (cp->filter) {
                filter_free(cp->filter);
                cp->filter = NULL;
            }
        }
        FREE(cursor->p);
        cursor->p = NULL;
    }
    FREE(cursor);
}

static struct flintdb_row *genericfile_cursor_next(struct flintdb_cursor_row *cursor, char **e) {
    if (!cursor || !cursor->p)
        return NULL;
    struct flintdb_genericfile_cursor_priv *cp = (struct flintdb_genericfile_cursor_priv *)cursor->p;
    struct filter *filter = cp->filter;
    struct limit *limit = &cp->limit;
    struct bufio *bio = cp->bio;
    struct formatter *f = cp->formatter;
    struct flintdb_row *r = NULL;

    // Release cursor's reference to the previously returned row.
    // If caller retained it, it will remain alive; otherwise this frees it.
    if (cp->last_row) {
        cp->last_row->free(cp->last_row);
        cp->last_row = NULL;
    }

    // one-time initialization (header skip)
    if (!cp->initialized) {
        cp->initialized = 1;
        cp->rowidx = 0;
        // If meta says to skip header, consume one physical line
        if (cp->file_data_header == HEADER_PRESENT) {
            ssize_t hn = bio->readline(bio, cp->line, sizeof(cp->line), e);
            if (hn < 0) {
                if (e && *e)
                    THROW_S(e);
                return NULL; // empty file
            }
        }
    }

    for (;;) {
        // Read one physical line
        ssize_t n = bio->readline(bio, cp->line, sizeof(cp->line), e);
        if (n < 0) {
            if (e && *e)
                THROW_S(e);
            return NULL; // EOF
        }

        // Accumulate additional lines if CSV record continues (when quoted multi-line)
        while (!record_completed_helper(f ? f->meta : NULL, cp->line, (size_t)n)) {
            if ((size_t)n + 1 >= sizeof(cp->line))
                break; // avoid overflow
            cp->line[n++] = '\n';
            ssize_t n2 = bio->readline(bio, cp->line + n, sizeof(cp->line) - (size_t)n, e);
            if (n2 < 0)
                break; // EOF mid-record; best-effort
            n += n2;
        }

        // Apply offset skipping without decoding to avoid unnecessary work
        if (limit->skip(limit)) {
            cp->rowidx++;
            continue;
        }

        // Decode line into row - use formatter's meta (which points to genericfile_priv->meta)
        r = flintdb_row_new((struct flintdb_meta *)f->meta, e);
        if (e && *e)
            THROW_S(e);

        struct buffer in = {
            0,
        };
        buffer_wrap(cp->line, (u32)n, &in);
        if (f->decode(f, &in, r, e) != 0) {
            r->free(r);
            r = NULL;
            THROW_S(e);
        }

        // Apply filters (both indexable and non-indexable parts)
        int matched = 1;
        if (filter != NULL) {
            int cmp = filter_compare(filter, r, e);
            if (e && *e)
                THROW_S(e);
            matched = (cmp == 0); // 0 means match
        }

        if (matched) {
            // Handle offset (skip) and limit (remains)
            if (limit->skip(limit)) {
                // skipped due to offset
                r->free(r);
                r = NULL;
            } else {
                if (limit->remains(limit) <= 0) {
                    r->free(r);
                    return NULL;
                }
                // Set rowid to current row index (before increment), then increment
                // r->rowid = cp->rowidx;
                cp->rowidx++;
                cp->last_row = r;
                return r;
            }
        }

        // Not matched or skipped; advance row index and continue
        cp->rowidx++;
        if (r) {
            r->free(r);
            r = NULL;
        }
    }

EXCEPTION:
    if (r)
        r->free(r);
    return NULL;
}

static struct flintdb_cursor_row *genericfile_find(const struct flintdb_genericfile *me, struct limit limit, struct filter *filter, char **e) {
    struct flintdb_genericfile_priv *priv = NULL;
    struct bufio *bio = NULL;
    struct flintdb_cursor_row *cursor = NULL;

    if (!me || !me->priv)
        THROW(e, "invalid genericfile");
    priv = (struct flintdb_genericfile_priv *)me->priv;

    size_t iobsz = io_buf_size_default();
    bio = file_bufio_open(priv->file, FLINTDB_RDONLY, iobsz, e);
    if (e && *e)
        THROW_S(e);

    cursor = CALLOC(1, sizeof(struct flintdb_cursor_row));
    if (!cursor)
        THROW(e, "Failed to allocate memory for cursor");
    cursor->p = CALLOC(1, sizeof(struct flintdb_genericfile_cursor_priv));
    if (!cursor->p)
        THROW(e, "Failed to allocate memory for cursor private data");

    struct flintdb_genericfile_cursor_priv *cp = (struct flintdb_genericfile_cursor_priv *)cursor->p;
    cp->bio = bio;
    cp->formatter = &priv->formatter;
    cp->filter = (struct filter *)filter;
    cp->limit = limit;
    // initialize limit counters like table_find()
    cp->limit.priv.n = (cp->limit.priv.limit < 0) ? INT_MAX : cp->limit.priv.limit;
    cp->limit.priv.o = cp->limit.priv.offset;
    cp->rowidx = 0;
    cp->initialized = 0;
    cp->file_data_header = priv->file_data_header;

    cursor->next = genericfile_cursor_next;
    cursor->close = genericfile_cursor_close;

    // header handling is performed lazily on first cursor->next() call

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

static struct flintdb_cursor_row *genericfile_find_where(const struct flintdb_genericfile *me, const char *where, char **e) {
    if (!me || !me->priv)
        return NULL;

    struct flintdb_genericfile_priv *priv = (struct flintdb_genericfile_priv *)me->priv;
    struct filter *filter = NULL;
    struct flintdb_sql *q = NULL;

    // Build SQL: SELECT * FROM <file> WHERE <where>
    char sql[SQL_STRING_LIMIT] = {0};
    if (where && where[0]) {
        snprintf(sql, sizeof(sql), "SELECT * FROM %s %s", priv->file, where); // snprintf(sql, sizeof(sql), "SELECT * FROM %s WHERE %s", priv->file, where);
    } else {
        snprintf(sql, sizeof(sql), "SELECT * FROM %s", priv->file);
    }

    // Parse SQL
    q = flintdb_sql_parse(sql, e);
    if (e && *e)
        THROW_S(e);

    // Compile WHERE without index (text files have no index)
    filter = filter_compile(q->where, &priv->meta, e);
    if (e && *e)
        THROW_S(e);

    // Parse LIMIT
    struct limit lim = !strempty(q->limit) ? limit_parse(q->limit) : NOLIMIT;
    flintdb_sql_free(q);

    return genericfile_find(me, lim, filter, e);

EXCEPTION:
    if (filter)
        filter_free(filter);
    if (q)
        flintdb_sql_free(q);
    return NULL;
}

static struct flintdb_meta genericfile_meta_from_header(const char *file, char **e) {
    // Initialize meta with sensible defaults (version/date/delims)
    char base[PATH_MAX] = {0};
    getname(file, base);
    struct flintdb_meta m = flintdb_meta_new(base, e);
    if (e && *e) { /* keep zero-initialized fallback if needed */
        m = (struct flintdb_meta){0,};
    }

    struct bufio *bio = NULL;
    char line[HEADER_BUFSZ];
    ssize_t n = 0;

    size_t iobsz = io_buf_size_default();
    bio = file_bufio_open(file, FLINTDB_RDONLY, iobsz, e);
    if (e && *e)
        THROW_S(e);

    // Read first line as header
    n = bio->readline(bio, line, sizeof(line), e);
    if (n <= 0) {
        THROW(e, "Failed to read header line from file: %s", file);
    }

    // Determine delimiter/quote from file format or probe header
    enum fileformat ff = detect_file_format(file);
    if (ff == FORMAT_CSV) {
        m.delimiter = ',';
        m.quote = '"';
    } else if (ff == FORMAT_TSV) {
        m.delimiter = '\t';
        m.quote = '\0'; // TSV typically has no quoting semantics
    } else {
        // Heuristic: choose the more frequent of comma or tab in the header
        int ct_tab = 0, ct_comma = 0;
        for (ssize_t i = 0; i < n; ++i) {
            ct_tab += (line[i] == '\t');
            ct_comma += (line[i] == ',');
        }
        if (ct_comma > ct_tab) {
            m.delimiter = ',';
            m.quote = '"';
        } else {
            m.delimiter = '\t';
            m.quote = '\0';
        }
    }
    m.escape = '\\';
    m.absent_header = 0; // absent_header indicates whether 1st line is data

    // Trim trailing CR/LF from the header slice for robust parsing
    char *ptr = line;
    char *end = line + n;
    while (end > ptr && (end[-1] == '\n' || end[-1] == '\r'))
        end--;

    // Parse header line into column names using detected delimiter
    int colidx = 0;
    while (ptr < end && colidx < MAX_COLUMNS_LIMIT) {
        char *delim = ptr;
        while (delim < end && *delim != m.delimiter)
            delim++;

        size_t len = (size_t)(delim - ptr);
        if (len > 0) {
            struct flintdb_column *col = &m.columns.a[colidx++];
            memset(col, 0, sizeof(struct flintdb_column));
            size_t copylen = (len < (size_t)MAX_COLUMN_NAME_LIMIT - 1) ? len : ((size_t)MAX_COLUMN_NAME_LIMIT - 1);
            memcpy(col->name, ptr, copylen);
            col->name[copylen] = '\0';
            col->type = VARIANT_STRING; // default type when inferring from header
            col->bytes = 65535; // large placeholder width for strings
        }
        if (delim >= end)
            break;
        ptr = delim + 1; // skip delimiter
    }
    m.columns.length = colidx;

    bio->close(bio);
    return m;

EXCEPTION:
    if (bio)
        bio->close(bio);
    return m;
}

static void genericfile_close(struct flintdb_genericfile *me) {
    if (!me)
        return;

    if (me->priv) {
        DEBUG("close file: %s", ((struct flintdb_genericfile_priv *)me->priv)->file);
        struct flintdb_genericfile_priv *priv = (struct flintdb_genericfile_priv *)me->priv;
        if (priv) {
            if (priv->wbio) {
                // best-effort flush/close
                priv->wbio->close(priv->wbio);
                priv->wbio = NULL;
            }

            if (priv->formatter.close) {
                priv->formatter.close(&priv->formatter);
            }
            flintdb_meta_close(&priv->meta);
        }
        FREE(priv);
        me->priv = NULL;
    }
    FREE(me);
    DEBUG("closed");
}

static struct flintdb_genericfile *textfile_open(const char *file, enum flintdb_open_mode mode, const struct flintdb_meta *meta, char **e) {
    // Fast-fail on obvious issues before allocating resources
    if (!file || !*file) {
        if (e)
            *e = "file path is empty";
        return NULL;
    }
    struct flintdb_genericfile *f = CALLOC(1, sizeof(struct flintdb_genericfile));
    struct flintdb_genericfile_priv *priv = NULL;
    if (!f)
        THROW(e, "Failed to allocate memory for file");

    enum fileformat fmt = detect_file_format(file);
    if (fmt == FORMAT_UNKNOWN)
        THROW(e, "Unknown file format for file: %s", file);
    if (fmt == FORMAT_BIN)
        THROW(e, "Binary format not supported for file: %s", file);

    // If opening for FLINTDB_RDONLY, ensure the data file actually exists to avoid
    // later failures that can lead to unsafe cleanup paths in callers.
    if (mode == FLINTDB_RDONLY) {
        if (access(file, F_OK) != 0) {
            THROW(e, "data file does not exist: %s", file);
        }
    }

    f->close = genericfile_close;
    f->rows = genericfile_rows;
    f->bytes = genericfile_bytes;
    f->meta = genericfile_meta;
    f->write = genericfile_write;
    f->find = genericfile_find_where;

    priv = f->priv = CALLOC(1, sizeof(struct flintdb_genericfile_priv));
    if (!priv)
        THROW(e, "Failed to allocate memory for file private data");
    strncpy(priv->file, file, PATH_MAX - 1);
    priv->mode = mode;
    priv->rows = -1; // cached row count for optimization
    // meta handling similar to table_open
    if (NULL == meta) { // no meta provided: read from .desc or infer from header
        char desc[PATH_MAX] = {0};
        snprintf(desc, sizeof(desc), "%s%s", file, META_NAME_SUFFIX);
        if (access(desc, F_OK) == 0) {
            priv->meta = flintdb_meta_open(desc, e);
            if (priv->meta.columns.length <= 0)
                THROW(e, "meta has no columns");
        } else {
            // THROW(e, "meta file does not exist: %s", desc);
            priv->meta = genericfile_meta_from_header(file, e);
            if (e && *e)
                THROW_S(e);
            if (priv->meta.columns.length <= 0)
                THROW(e, "meta has no columns");
        }
    } else if (mode == FLINTDB_RDWR) {
        char dir[PATH_MAX] = {0};
        getdir(file, dir);
        mkdirs(dir, S_IRWXU);
        char desc[PATH_MAX] = {0};
        snprintf(desc, sizeof(desc), "%s%s", file, META_NAME_SUFFIX);
        if (access(desc, F_OK) != 0) {
            if (meta->columns.length <= 0)
                THROW(e, "meta has no columns");
            if (flintdb_meta_write(meta, desc, e) != 0)
                THROW_S(e);
            priv->meta = *meta;
            // Do not take ownership of caller's meta internals
            priv->meta.priv = NULL;
        } else {
            struct flintdb_meta existing = flintdb_meta_open(desc, e);
            if (existing.columns.length <= 0)
                THROW(e, "existing meta has no columns");
            if (flintdb_meta_compare(&existing, meta) != 0)
                THROW(e, "meta does not match existing: %s", desc);
            flintdb_meta_close(&existing);
            priv->meta = *meta;
            // Do not take ownership of caller's meta internals
            priv->meta.priv = NULL;
        }
    } else {
        // FLINTDB_RDONLY with provided meta: borrow schema but do not own internals
        priv->meta = *meta;
        priv->meta.priv = NULL;
    }

    formatter_init(fmt, &priv->meta, &priv->formatter, e);
    if (e && *e)
        THROW_S(e);

    priv->file_data_header = priv->meta.absent_header ? HEADER_ABSENT : HEADER_PRESENT;

    return f;

EXCEPTION:
    genericfile_close(f);
    return NULL;
}

// --

// Helper: extract file extension from path
static const char *get_file_extension(const char *file) {
    if (!file)
        return NULL;
    const char *dot = strrchr(file, '.');
    return dot ? dot : "";
}

struct flintdb_genericfile *flintdb_genericfile_open(const char *file, enum flintdb_open_mode mode, const struct flintdb_meta *meta, char **e) {
    if (!file || !*file)
        THROW(e, "file path is empty");

#define FLINTDB_USE_PLUGINS 1
#ifdef FLINTDB_USE_PLUGINS
    // Initialize plugin manager if not already initialized
    static int plugin_init_done = 0;
    if (!plugin_init_done) {
        plugin_manager_init(e);
        if (e && *e)
            THROW_S(e);
        plugin_init_done = 1;
    }
    
    // Try to find a plugin by full filename suffix first (supports .json.gz, .jsonl.gz, etc.)
    struct plugin_interface *plugin = plugin_find_by_suffix(file, e);
    
    // If not found, try by last extension only
    if (!plugin) {
        const char *ext = get_file_extension(file);
        plugin = plugin_find_by_extension(ext, e);
    }
    
    if (plugin && plugin->open) {
        DEBUG("genericfile_open: using plugin '%s' for file '%s'", plugin->name, file);
        return plugin->open(file, mode, meta, e);
    }
#endif
    
    enum fileformat fmt = detect_file_format(file);
    switch(fmt) {
        case FORMAT_TSV:
        case FORMAT_CSV:
            // Default: text-based formats (CSV, TSV, etc.)
            return textfile_open(file, mode, meta, e);
        case FORMAT_PARQUET:
            THROW(e, "Parquet format requires plugin. Install libflintdb_parquet plugin to lib/ directory: %s", file);
            break;
        case FORMAT_JSONL:
            THROW(e, "JSONL format requires plugin. Install libflintdb_jsonl plugin to lib/ directory: %s", file);
            break;
        default:
            THROW(e, "Unsupported file format for file: %s", file);
    }
    
EXCEPTION:
    return NULL;
}