
#ifndef FLINTDB_IOSTREAM_H
#define FLINTDB_IOSTREAM_H

#include "types.h"
#include "flintdb.h"
#include "buffer.h"


struct stream {
    // Read up to size bytes into data. Returns number of bytes read, 0 on EOF, or -1 on error (sets e)
    ssize_t (*read)(struct stream *s, char *data, size_t size, char **e);
    // Write up to size bytes from data. Returns number of bytes written or -1 on error (sets e)
    ssize_t (*write)(struct stream *s, const char *data, size_t size, char **e);
    void (*close)(struct stream *s);

    void *priv;
};

struct bufio {
    // Buffered read/write wrapper around an underlying stream
    ssize_t (*read)(struct bufio *b, char *data, size_t size, char **e);
    ssize_t (*write)(struct bufio *b, const char *data, size_t size, char **e);

    ssize_t (*readline)(struct bufio *b, char *data, size_t size, char **e);
    ssize_t (*writeline)(struct bufio *b, const char *data, size_t size, char **e);
    int (*flush)(struct bufio *b, char **e);
    void (*close)(struct bufio *b);

    void *priv;
};

// Opens a file stream, automatically choosing gzip or plain file based on filename suffix
struct stream * file_stream_open(const char *filename, enum flintdb_open_mode mode, char **e); 
struct bufio *  file_bufio_open(const char *filename, enum flintdb_open_mode mode, size_t buffer_size, char **e);
struct bufio *  bufio_wrap_stream(struct stream *s, size_t buffer_size, char **e);

// Wrap an existing file descriptor as a buffered stream (for stdout/stderr)
struct bufio *  bufio_wrap_fd(int fd, enum flintdb_open_mode mode, size_t buffer_size, char **e);

#endif