#include "iostream.h"
#include "flintdb.h"
#include "runtime.h"

#include <assert.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h> // .tsv.gz / .csv.gz support

struct stream_priv {
    int fd;
    enum flintdb_open_mode mode;
};

struct gzstream_priv {
    gzFile gz;
    enum flintdb_open_mode mode;
};

struct bufio_priv {
    struct stream *underlying;
    char *buffer;
    size_t buffer_size;
    size_t position;
    size_t limit;
    enum flintdb_open_mode mode;
};

static ssize_t stream_read(struct stream *s, char *data, size_t size, char **e) {
    if (!s || !s->priv || !data || size == 0)
        return 0;
    struct stream_priv *p = (struct stream_priv *)s->priv;
    ssize_t n = read(p->fd, data, size);
    if (n < 0) {
        if (e) {
            THROW(e, "read failed: %s", strerror(errno));
        }
        return -1;
    }
    return n;

EXCEPTION:
    return -1;
}

static ssize_t stream_write(struct stream *s, const char *data, size_t size, char **e) {
    if (!s || !s->priv || !data || size == 0)
        return 0;
    struct stream_priv *p = (struct stream_priv *)s->priv;
    size_t written = 0;
    while (written < size) {
        ssize_t n = write(p->fd, data + written, size - written);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (e) {
                THROW(e, "write failed: %s", strerror(errno));
            }
            return -1;
        }
        if (n == 0)
            break;
        written += (size_t)n;
    }
    return (ssize_t)written;

EXCEPTION:
    return -1;
}

static void stream_close(struct stream *s) {
    if (!s)
        return;
    if (s->priv) {
        struct stream_priv *p = (struct stream_priv *)s->priv;
        if (p->fd >= 0)
            close(p->fd);
        FREE(p);
        s->priv = NULL;
    }
    FREE(s);
}

static ssize_t stream_gzip_read(struct stream *s, char *data, size_t size, char **e) {
    if (!s || !s->priv || !data || size == 0)
        return 0;
    struct gzstream_priv *p = (struct gzstream_priv *)s->priv;
    int n = gzread(p->gz, data, (unsigned int)size);
    if (n < 0) {
        int errnum = 0;
        const char *msg = gzerror(p->gz, &errnum);
        if (e) THROW(e, "gzread failed: %s", msg ? msg : "unknown"); 
        return -1;
    }
    return (ssize_t)n;

EXCEPTION:
    return -1;
}

static ssize_t stream_gzip_write(struct stream *s, const char *data, size_t size, char **e) {
    if (!s || !s->priv || !data || size == 0)
        return 0;
    struct gzstream_priv *p = (struct gzstream_priv *)s->priv;
    size_t written = 0;
    while (written < size) {
        int n = gzwrite(p->gz, data + written, (unsigned int)(size - written));
        if (n == 0) {
            int errnum = 0;
            const char *msg = gzerror(p->gz, &errnum);
            if (e) {
                THROW(e, "gzwrite failed: %s", msg ? msg : "unknown");
            }
            return -1;
        }
        written += (size_t)n;
    }
    return (ssize_t)written;

EXCEPTION:
    return -1;
}

static void stream_gzip_close(struct stream *s) {
    if (!s)
        return;
    if (s->priv) {
        struct gzstream_priv *p = (struct gzstream_priv *)s->priv;
        if (p->gz)
            gzclose(p->gz);
        FREE(p);
        s->priv = NULL;
    }
    FREE(s);
}

static ssize_t bufio_read(struct bufio *b, char *data, size_t size, char **e) {
    if (!b || !b->priv || !data || size == 0)
        return 0;
    struct bufio_priv *p = (struct bufio_priv *)b->priv;
    size_t copied = 0;
    while (copied < size) {
        if (p->position >= p->limit) {
            // refill
            p->position = 0;
            p->limit = 0;
            ssize_t n = p->underlying->read(p->underlying, p->buffer, p->buffer_size, e);
            if (n < 0)
                return -1;
            if (n == 0)
                break; // EOF
            p->limit = (size_t)n;
        }
        size_t avail = p->limit - p->position;
        size_t need = size - copied;
        size_t take = avail < need ? avail : need;
        memcpy(data + copied, p->buffer + p->position, take);
        p->position += take;
        copied += take;
    }
    return (ssize_t)copied;
}

static int bufio_flush(struct bufio *b, char **e) {
    struct bufio_priv *p = (struct bufio_priv *)b->priv;
    if (!p)
        return 0;
    if (p->position == 0)
        return 0;
    size_t off = 0;
    while (off < p->position) {
        ssize_t n = p->underlying->write(p->underlying, p->buffer + off, p->position - off, e);
        if (n < 0)
            return -1;
        off += (size_t)n;
    }
    p->position = 0;
    p->limit = 0;
    return 0;
}

static ssize_t bufio_readline(struct bufio *b, char *data, size_t size, char **e) {
    if (!b || !b->priv || !data || size == 0)
        return 0;
    struct bufio_priv *p = (struct bufio_priv *)b->priv;
    size_t copied = 0;

    while (copied < size) {
        if (p->position >= p->limit) {
            // refill
            p->position = 0;
            p->limit = 0;
            ssize_t n = p->underlying->read(p->underlying, p->buffer, p->buffer_size, e);
            if (n < 0)
                return -1;
            if (n == 0)
                break; // EOF
            p->limit = (size_t)n;
        }

        size_t avail = p->limit - p->position;
        char *start = p->buffer + p->position;
        char *nl = (char *)memchr(start, '\n', avail);
        size_t chunk = nl ? (size_t)((nl - start) + 1) : avail; // include '\n' if found

        size_t remain = size - copied;
        size_t take = chunk <= remain ? chunk : remain;
        memcpy(data + copied, start, take);
        p->position += take;
        copied += take;

        // If we found a newline and copied it fully, stop at line boundary
        if (nl && take == chunk) {
            break;
        }
        // Otherwise continue (either buffer exhausted without newline, or caller buffer filled)
        if (copied >= size)
            break;
    }
    return (ssize_t)copied;
}

static ssize_t bufio_writeline(struct bufio *b, const char *data, size_t size, char **e) {
    if (!b || !b->priv)
        return 0;
    ssize_t wn = 0;
    if (data && size > 0) {
        ssize_t n = b->write(b, data, size, e);
        if (n < 0)
            return -1;
        wn += n;
    }
    // Append a single newline if not already present as the last byte
    if (!(size > 0 && data && data[size - 1] == '\n')) {
        char nl = '\n';
        ssize_t n2 = b->write(b, &nl, 1, e);
        if (n2 < 0)
            return -1;
        wn += n2;
    }
    return wn;
}

static ssize_t bufio_write(struct bufio *b, const char *data, size_t size, char **e) {
    if (!b || !b->priv || !data || size == 0)
        return 0;
    struct bufio_priv *p = (struct bufio_priv *)b->priv;
    size_t written = 0;
    while (written < size) {
        size_t avail = p->buffer_size - p->position;
        if (avail == 0) {
            if (bufio_flush(b, e) != 0)
                return -1;
            avail = p->buffer_size;
        }
        size_t remain = size - written;
        size_t take = remain < avail ? remain : avail;
        memcpy(p->buffer + p->position, data + written, take);
        p->position += take;
        written += take;
        // If buffer is full, flush to underlying
        if (p->position == p->buffer_size) {
            if (bufio_flush(b, e) != 0)
                return -1;
        }
    }
    return (ssize_t)written;
}

static void bufio_close(struct bufio *b) {
    if (!b)
        return;
    if (b->priv) {
        char *e = NULL;
        bufio_flush(b, &e); // best-effort flush
        struct bufio_priv *p = (struct bufio_priv *)b->priv;
        if (p->underlying) {
            if (p->underlying->close) {
                // close() will free both the stream and its priv
                p->underlying->close(p->underlying);
            } else {
                // No close function, we need to free manually
                if (p->underlying->priv) {
                    FREE(p->underlying->priv);
                    p->underlying->priv = NULL;
                }
                FREE(p->underlying);
            }
            p->underlying = NULL;
        }
        if (p->buffer) {
            FREE(p->buffer);
            p->buffer = NULL;
        }
        FREE(p);
        b->priv = NULL;
    }
    FREE(b);
}

static struct stream *stream_open_from_file(const char *filename, enum flintdb_open_mode mode, char **e) {
    if (!filename)
        return NULL;
    // When opening for FLINTDB_RDWR in our usage, we generally want to create a new file for output
    // and truncate any existing file. Using O_WRONLY avoids requiring the file to exist for read.
    int flags = (mode == FLINTDB_RDONLY) ? O_RDONLY : (O_WRONLY | O_CREAT | O_TRUNC);
#ifdef _WIN32
    int fd = open(filename, flags | O_BINARY, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
#else
    int fd = open(filename, flags, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
#endif
    if (fd < 0) {
        if (e) THROW(e, "open failed: %s (%s)", filename, strerror(errno));
        return NULL;
    }

    struct stream *s = (struct stream *)CALLOC(1, sizeof(struct stream));
    struct stream_priv *p = (struct stream_priv *)CALLOC(1, sizeof(struct stream_priv));
    if (!s || !p) {
        if (fd >= 0)
            close(fd);
        if (s)
            FREE(s);
        if (p)
            FREE(p);
        if (e) {
            THROW(e, "Out of memory");
        }
        return NULL;
    }
    p->fd = fd;
    p->mode = mode;
    s->priv = p;
    s->read = &stream_read;
    s->write = &stream_write;
    s->close = &stream_close;
    return s;

EXCEPTION:
    if (fd >= 0)
        close(fd);
    return NULL;
}

// Parse env bytes with optional K/M/G suffix
static size_t parse_env_bytes_local(const char *name, size_t defval) {
    const char *s = getenv(name);
    if (!s || !*s) return defval;
    char *end = NULL;
    long long v = strtoll(s, &end, 10);
    if (v <= 0) return defval;
    while (end && *end && isspace((unsigned char)*end)) end++;
    if (end && *end) {
        if (*end == 'K' || *end == 'k') v *= 1024LL;
        else if (*end == 'M' || *end == 'm') v *= (1024LL*1024LL);
        else if (*end == 'G' || *end == 'g') v *= (1024LL*1024LL*1024LL);
    }
    if (v <= 0) return defval;
    return (size_t)v;
}

struct stream *stream_open_from_gzfile(const char *filename, enum flintdb_open_mode mode, char **e) {
    if (!filename)
        return NULL;
    const char *m = (mode == FLINTDB_RDONLY) ? "rb" : "wb";
    gzFile gz = gzopen(filename, m);
    if (!gz) {
        if (e) {
            THROW(e, "gzopen failed: %s", filename);
        }
        return NULL;
    }
    // Use a larger gzip buffer for faster sequential reads/writes (override with GZ_BUFFER_BYTES)
    size_t gzbuf = parse_env_bytes_local("GZ_BUFFER_BYTES", (size_t)(1<<20));
    if (gzbuf < (1<<16)) gzbuf = (1<<16); // minimum 64KB
    gzbuffer(gz, (unsigned int)gzbuf);

    struct stream *s = (struct stream *)CALLOC(1, sizeof(struct stream));
    struct gzstream_priv *p = (struct gzstream_priv *)CALLOC(1, sizeof(struct gzstream_priv));
    if (!s || !p) {
        if (gz)
            gzclose(gz);
        if (s)
            FREE(s);
        if (p)
            FREE(p);
        if (e) {
            THROW(e, "Out of memory");
        }
        return NULL;
    }
    p->gz = gz;
    p->mode = mode;
    s->priv = p;
    s->read = &stream_gzip_read;
    s->write = &stream_gzip_write;
    s->close = &stream_gzip_close;
    return s;

EXCEPTION:
    if (gz)
        gzclose(gz);
    return NULL;
}

struct bufio *bufio_wrap_stream(struct stream *s, size_t buffer_size, char **e) {
    if (!s || buffer_size == 0)
        buffer_size = 1 << 16; // default 64KB
    struct bufio *b = (struct bufio *)CALLOC(1, sizeof(struct bufio));
    struct bufio_priv *p = (struct bufio_priv *)CALLOC(1, sizeof(struct bufio_priv));
    if (!b || !p) {
        if (b)
            FREE(b);
        if (p)
            FREE(p);
        if (e)
            THROW(e, "Out of memory");
        return NULL;
    }
    p->underlying = s;
    p->buffer_size = buffer_size;
    p->buffer = (char *)MALLOC(buffer_size);
    if (!p->buffer) {
        FREE(p);
        FREE(b);
        if (e) {
            THROW(e, "Out of memory");
        }
        return NULL;
    }
    p->position = 0;
    p->limit = 0;
    // inherit mode from underlying if possible; if unknown, default to FLINTDB_RDWR
    p->mode = FLINTDB_RDWR;
    b->priv = p;
    b->read = &bufio_read;
    b->write = &bufio_write;
    b->readline = &bufio_readline;
    b->writeline = &bufio_writeline;
    b->flush = &bufio_flush;
    b->close = &bufio_close;
    return b;

EXCEPTION:
    if (p) {
        if (p->buffer)
            FREE(p->buffer);
        FREE(p);
    }
    if (b)
        FREE(b);
    return NULL;
}


struct stream *file_stream_open(const char *filename, enum flintdb_open_mode mode, char **e) {
    if (!filename)
        return NULL;
    if (suffix(filename, ".gz") || suffix(filename, ".gzip")) {
        return stream_open_from_gzfile(filename, mode, e);
    } else {
        return stream_open_from_file(filename, mode, e);
    }
}

struct bufio *file_bufio_open(const char *filename, enum flintdb_open_mode mode, size_t buffer_size, char **e) {
    struct stream *s = file_stream_open(filename, mode, e);
    if (!s)
        return NULL;
    struct bufio *b = bufio_wrap_stream(s, buffer_size, e);
    if (!b) {
        s->close(s);
        return NULL;
    }
    return b;
}

// Wrap existing fd as buffered stream (for stdout/stderr)
struct bufio *bufio_wrap_fd(int fd, enum flintdb_open_mode mode, size_t buffer_size, char **e) {
    struct stream *s = NULL;
    struct stream_priv *p = NULL;
    struct bufio *b = NULL;

    s = (struct stream *)CALLOC(1, sizeof(struct stream));
    if (!s) THROW(e, "Out of memory");

    p = (struct stream_priv *)CALLOC(1, sizeof(struct stream_priv));
    if (!p) THROW(e, "Out of memory");

    p->fd = fd;  // Don't dup, caller manages fd lifetime
    p->mode = mode;

    s->read = stream_read;
    s->write = stream_write;
    s->close = NULL; // don't close underlying fd
    s->priv = p;

    b = bufio_wrap_stream(s, buffer_size, e);
    // if (b) b->close = bufio_wrap_fd_close;
    return b;

EXCEPTION:
    if (s) FREE(s);
    if (p) FREE(p);
    if (b) FREE(b);
    return NULL;
}