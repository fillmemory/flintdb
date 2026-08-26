/**
 * Command Line Interface for FlintDB
 */

// Include standard headers first (before any project headers)
// to ensure _GNU_SOURCE takes effect
#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#endif

// Now include project headers (runtime_win32.h will handle POSIX declarations)
#include "iostream.h"
#include "flintdb.h"
#include "runtime.h"
// #include "allocator.h"

#ifndef VERSION
#define VERSION "0.0.1"
#endif

#ifndef PRODUCT_NAME
#define PRODUCT_NAME "FlintDB"
#endif

#ifndef BUILD_TIME
#define BUILD_TIME "unknown"
#endif

#ifndef GIT_REVISION
#define GIT_REVISION "unknown"
#endif

#define CLI_BUFIO_OUTPUT_MAX 8192 // 8KB buffer for CLI output
// Pretty print structures
#define MAX_PRETTY_ROWS 10000
#define MAX_PRETTY_COLS 100

#define REPL_META_NONE 0
#define REPL_META_EXIT 1
#define REPL_META_HELP 2

// REPL: SIGINT cancels the current input instead of exiting
static volatile sig_atomic_t cli_interrupted = 0;
static volatile sig_atomic_t cli_repl_active = 0;

// Signal handler for graceful shutdown
static void signal_handler(int signum) {
    if (cli_repl_active && signum == SIGINT) {
        cli_interrupted = 1;
        {
            ssize_t n = write(STDERR_FILENO, "^C\n", 3);
            (void)n;
        }
        return;
    }
    flintdb_cleanup(NULL);
    exit(signum == SIGINT ? 130 : 1);
}

// SQL statement iterator
struct flintdb_sql_iterator {
    const char *sql; // SQL string (for string mode)
    FILE *file;      // File pointer (for file mode)
    size_t pos;
    size_t sql_len;
    char *current_stmt;
    size_t stmt_capacity;
    int owns_sql;      // 1 if iterator allocated sql, 0 otherwise
    int from_file;     // 1 if reading from file stream
    char *file_buffer; // Buffer for file reading
    size_t file_buf_size;
    size_t file_buf_pos;
    size_t file_buf_len;
};

static struct flintdb_sql_iterator *sql_iterator_new(const char *sql, char **e);
static struct flintdb_sql_iterator *sql_iterator_new_from_file(const char *filepath, char **e);
static char *sql_iterator_next(struct flintdb_sql_iterator *iter, char **e);
static void sql_iterator_free(struct flintdb_sql_iterator *iter);

// Forward declarations
static i64 execute_cli(FILE *out, int argc, char *argv[], char **e);
static i64 run_repl(FILE *out, int pretty, int status, int head, int rownum, char **e);
static int execute_one_statement(struct bufio *bufout, const char *stmt, int stmt_idx, int pretty, int status,
                                 int head, int rownum, struct flintdb_transaction **transaction, i64 *affected,
                                 char **e);
static void usage(const char *progname);

// Utility functions
static void format_number(char *buf, size_t size, i64 num);

// Buffered output helpers (avoid fprintf overhead)
static inline int bufio_print(struct bufio *b, const char *s, char **e) {
    if (!b || !s)
        return 0;
    size_t len = strlen(s);
    ssize_t n = b->write(b, s, len, e);
    return (n == (ssize_t)len) ? 0 : -1;
}

static inline int bufio_print_tab(struct bufio *b, char **e) {
    return bufio_print(b, "\t", e);
}

static inline int bufio_print_newline(struct bufio *b, char **e) {
    return bufio_print(b, "\n", e);
}

struct pretty_table {
    char ***rows; // Array of row arrays of strings
    int row_count;
    int col_count;
    int *col_widths; // Width of each column
    int capacity;
};

static struct pretty_table *pretty_table_new(int col_count);
static void pretty_table_free(struct pretty_table *table);
static void pretty_table_add_row(struct pretty_table *table, char **row_data, int col_count);
static void pretty_table_print(struct pretty_table *table, struct bufio *bufout, char **e);

extern int webui_run(int argc, char **argv, char **e); // in webui.c

/**
 * Main entry point for the CLI application
 */
int main(int argc, char *argv[]) {
    char *e = NULL;

#ifdef _WIN32
    // Keep byte output stable (avoid CRLF translation / codepage surprises).
    _setmode(_fileno(stdout), _O_BINARY);
    _setmode(_fileno(stderr), _O_BINARY);

    // If attached to a real Windows console, switch console code pages to UTF-8.
    // (When output is redirected, this is unnecessary.)
    {
        DWORD console_mode = 0;
        HANDLE hOut = GetStdHandle(STD_OUTPUT_HANDLE);
        if (hOut != INVALID_HANDLE_VALUE && GetConsoleMode(hOut, &console_mode)) {
            SetConsoleOutputCP(CP_UTF8);
        }
        HANDLE hIn = GetStdHandle(STD_INPUT_HANDLE);
        if (hIn != INVALID_HANDLE_VALUE && GetConsoleMode(hIn, &console_mode)) {
            SetConsoleCP(CP_UTF8);
        }
    }
#endif

    // Register signal handlers for graceful shutdown.
    // No SA_RESTART so REPL fgets() returns EINTR on Ctrl-C.
#ifdef _WIN32
    signal(SIGINT, signal_handler);  // Ctrl-C
    signal(SIGTERM, signal_handler); // kill command
#else
    {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = signal_handler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);
    }
#endif

    // Check for web UI mode before normal CLI execution
    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "-webui", 7) == 0) {
            // Pass remaining args (including -webui) to webui_run
            int rc = webui_run(argc - i, argv + i, &e);
            flintdb_cleanup(NULL);
            if (e) {
                fprintf(stderr, "Error: %s\n", e);
                return 1;
            }
            return rc;
        }
    }
    i64 result = execute_cli(stdout, argc, argv, &e);

    if (e) {
        fprintf(stderr, "Error: %s\n", e);
        return 1;
    }

    flintdb_cleanup(&e);
    if (e) {
        fprintf(stderr, "Error during cleanup: %s\n", e);
        return 1;
    }

    return (result < 0) ? 1 : 0;
}

/**
 * Display usage information
 */
static void usage(const char *progname) {
    const char *CMD = progname ? progname : "./bin/db";
    printf("Usage: \"%s\" [options]\n\n", CMD);
    printf("With no SQL and a terminal, starts an interactive prompt.\n\n");
    printf(" options:\n");
    printf(" \t<SQL>     \tSELECT|INSERT|DELETE|UPDATE|DESC|META|SHOW\n");
    printf(" \t-pretty   \tpretty print when sql is SELECT\n");
    printf(" \t-status   \tprint the executed status\n");
    printf(" \t-log      \tenable detailed logging\n");
    printf(" \t-nohead   \tignore header when printing rows\n");
    printf(" \t-rownum   \tshow row number when printing rows\n");
    printf(" \t-sql <SQL>\tspecify SQL statement\n");
    printf(" \t-f <file> \texecute SQL from file\n");
    printf(" \t-webui    \tstart embedded HTTP Web UI (port 3334 or -port=)\n");
    printf(" \t-version \tshow version information\n");
    printf(" \t-help     \tshow this help\n\n");
    printf(" examples:\n");
    printf("\t%s\n", CMD);
    printf("\t%s \"SELECT * FROM temp/tpch_lineitem"TABLE_NAME_SUFFIX" USE INDEX(PRIMARY DESC) WHERE l_orderkey > 1 LIMIT 0, 10\" -rownum -pretty\n", CMD);
    printf("\t%s \"SELECT * FROM temp/tpch_lineitem.tsv.gz WHERE l_orderkey > 1 LIMIT 0, 10\"\n", CMD);
    printf("\t%s \"SELECT * FROM temp/file"TABLE_NAME_SUFFIX" INTO temp/output.tsv.gz\"\n", CMD);
    printf("\t%s \"SELECT * FROM temp/file"TABLE_NAME_SUFFIX" INTO temp/output.csv.gz\"\n", CMD);
    printf("\t%s \"INSERT INTO temp/file"TABLE_NAME_SUFFIX" FROM temp/input.tsv.gz\"\n", CMD);
    printf("\t%s \"REPLACE INTO temp/file"TABLE_NAME_SUFFIX" FROM temp/input.tsv.gz\"\n", CMD);
    printf("\t%s \"UPDATE temp/file"TABLE_NAME_SUFFIX" SET B = 'abc', C = 2 WHERE A = 1\"\n", CMD);
    printf("\t%s \"DELETE FROM temp/file"TABLE_NAME_SUFFIX" WHERE A = 1\"\n", CMD);
    printf("\t%s \"SHOW TABLES WHERE temp\"\n", CMD);
    printf("\t%s \"SHOW TABLES WHERE temp OPTION -R\"\n", CMD);
    printf("\t%s \"DESC temp/file"TABLE_NAME_SUFFIX"\"\n", CMD);
    printf("\t%s \"META temp/file"TABLE_NAME_SUFFIX"\"\n", CMD);
    printf("\t%s \"BEGIN TRANSACTION temp/table"TABLE_NAME_SUFFIX"\"\n", CMD);
    printf("\n");
    printf("Development build: not all features are implemented yet.\n");
    printf("\n");
}

static int cmd_eq(const char *a, const char *b) {
    if (!a || !b)
        return 0;
    for (; *a && *b; a++, b++) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b))
            return 0;
    }
    return *a == '\0' && *b == '\0';
}

static int buffer_is_blank(const char *s) {
    if (!s)
        return 1;
    while (*s) {
        if (!isspace((unsigned char)*s))
            return 0;
        s++;
    }
    return 1;
}

static int repl_meta_kind(const char *s) {
    char tmp[64];
    size_t n = 0;

    if (!s)
        return REPL_META_NONE;
    while (*s && isspace((unsigned char)*s))
        s++;
    while (s[n] && n + 1 < sizeof(tmp)) {
        tmp[n] = s[n];
        n++;
    }
    if (s[n])
        return REPL_META_NONE;
    tmp[n] = '\0';
    while (n > 0 && isspace((unsigned char)tmp[n - 1]))
        tmp[--n] = '\0';
    if (n > 0 && tmp[n - 1] == ';') {
        tmp[--n] = '\0';
        while (n > 0 && isspace((unsigned char)tmp[n - 1]))
            tmp[--n] = '\0';
    }
    if (n == 0)
        return REPL_META_NONE;
    if (cmd_eq(tmp, "exit") || cmd_eq(tmp, "quit") || cmd_eq(tmp, "\\q") || cmd_eq(tmp, "\\exit"))
        return REPL_META_EXIT;
    if (cmd_eq(tmp, "help") || cmd_eq(tmp, "\\h") || cmd_eq(tmp, "\\help"))
        return REPL_META_HELP;
    return REPL_META_NONE;
}

/* 1 if s has a ';' outside quotes/comments. stmt_len is bytes before that ';'. */
static int sql_has_complete_stmt(const char *s, size_t *stmt_len) {
    char quote = 0;
    char comment_end = 0;
    char prev = 0;
    const char *p;

    if (!s)
        return 0;
    for (p = s; *p; p++) {
        char ch = *p;
        char next = p[1];

        if (quote == 0 && comment_end == 0) {
            if (ch == '-' && next == '-') {
                comment_end = '\n';
                prev = '-';
                p++;
                continue;
            }
            if (ch == '/' && next == '*') {
                comment_end = '*';
                prev = '*';
                p++;
                continue;
            }
        }
        if (comment_end == '\n') {
            if (ch == '\n')
                comment_end = 0;
            prev = ch;
            continue;
        }
        if (comment_end == '*') {
            if (ch == '*' && next == '/') {
                comment_end = 0;
                prev = '/';
                p++;
                continue;
            }
            prev = ch;
            continue;
        }
        if (quote != 0) {
            if (prev != '\\' && ch == quote)
                quote = 0;
            prev = ch;
            continue;
        }
        if (ch == '\'' || ch == '"' || ch == '`') {
            quote = ch;
            prev = ch;
            continue;
        }
        if (ch == ';') {
            if (stmt_len)
                *stmt_len = (size_t)(p - s);
            return 1;
        }
        prev = ch;
    }
    return 0;
}

static int sql_buffer_append(char **buf, size_t *len, size_t *cap, const char *line, char **e) {
    size_t n = line ? strlen(line) : 0;
    size_t need = *len + n + 2; /* newline + NUL */

    if (need > *cap) {
        size_t ncap = *cap ? *cap : 256;
        char *p;
        while (ncap < need)
            ncap *= 2;
        p = REALLOC(*buf, ncap);
        if (!p)
            THROW(e, "Out of memory");
        *buf = p;
        *cap = ncap;
    }
    if (n)
        memcpy(*buf + *len, line, n);
    *len += n;
    (*buf)[(*len)++] = '\n';
    (*buf)[*len] = '\0';
    return 0;

EXCEPTION:
    return -1;
}

static char *sql_buffer_take_stmt(char **buf, size_t *len, size_t *cap, char **e) {
    size_t stmt_len = 0;
    char *stmt;
    size_t rest_off;
    size_t rest;

    (void)cap;
    if (!buf || !*buf || !sql_has_complete_stmt(*buf, &stmt_len))
        return NULL;
    stmt = MALLOC(stmt_len + 1);
    if (!stmt)
        THROW(e, "Out of memory");
    memcpy(stmt, *buf, stmt_len);
    stmt[stmt_len] = '\0';
    rest_off = stmt_len + 1; /* skip ';' */
    rest = *len > rest_off ? *len - rest_off : 0;
    memmove(*buf, *buf + rest_off, rest);
    (*buf)[rest] = '\0';
    *len = rest;
    return stmt;

EXCEPTION:
    return NULL;
}

static char *repl_read_line(char **e) {
    char chunk[1024];
    char *line = NULL;
    size_t len = 0;
    size_t cap = 0;

    for (;;) {
        size_t n;
        size_t need;
        char *p;

        if (!fgets(chunk, sizeof(chunk), stdin)) {
            if (cli_interrupted) {
                FREE(line);
                return NULL;
            }
            if (ferror(stdin) && errno == EINTR) {
                clearerr(stdin);
                if (cli_interrupted) {
                    FREE(line);
                    return NULL;
                }
                continue;
            }
            return line; /* EOF: NULL if nothing read, else last partial line */
        }
        n = strlen(chunk);
        need = len + n + 1;
        if (need > cap) {
            size_t ncap = cap ? cap : 256;
            while (ncap < need)
                ncap *= 2;
            p = REALLOC(line, ncap);
            if (!p) {
                FREE(line);
                THROW(e, "Out of memory");
            }
            line = p;
            cap = ncap;
        }
        memcpy(line + len, chunk, n + 1);
        len += n;
        if (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
                line[--len] = '\0';
            return line;
        }
    }

EXCEPTION:
    return NULL;
}

static void repl_print_help(struct bufio *bufout, char **e) {
    bufio_print(bufout, "Enter SQL statements terminated by a semicolon (;).\n", e);
    bufio_print(bufout, "Commands:\n", e);
    bufio_print(bufout, "  help, \\h          Show this help\n", e);
    bufio_print(bufout, "  exit, quit, \\q    Exit\n", e);
    bufio_print(bufout, "\n", e);
    bufio_print(bufout, "SQL: SELECT, INSERT, REPLACE, UPDATE, DELETE, DESC, META, SHOW,\n", e);
    bufio_print(bufout, "     CREATE, DROP, ALTER TABLE, BEGIN TRANSACTION\n", e);
}

static void repl_reset_accum(char **accum, size_t *len, size_t *cap) {
    FREE(*accum);
    *accum = NULL;
    *len = 0;
    *cap = 0;
}

/**
 * Execute one SQL statement and print the result.
 * Returns 0 on success, 1 on SQL error (printed, e cleared), -1 on fatal (e set).
 */
static int execute_one_statement(struct bufio *bufout, const char *stmt, int stmt_idx, int pretty, int status,
                                 int head, int rownum, struct flintdb_transaction **transaction, i64 *affected,
                                 char **e) {
    struct flintdb_sql_result *result = NULL;
    struct pretty_table *table = NULL;
    char buf[65536];
    size_t buf_len = sizeof(buf);
    char num_buf[64], time_buf[64], ops_buf[64];
    i64 row_count = 0;

    if (affected)
        *affected = 0;

    STOPWATCH_START(watch);
    result = flintdb_sql_exec(stmt, transaction ? *transaction : NULL, e);
    time_dur(time_elapsed(&watch), time_buf, sizeof(time_buf));
    snprintf(ops_buf, sizeof(ops_buf), "%.0f", result ? time_ops(result->affected, &watch) : 0);

    if (e && *e) {
        if (stmt_idx >= 0)
            snprintf(buf, sizeof(buf), "Error in statement %d: %s\n", stmt_idx + 1, *e);
        else
            snprintf(buf, sizeof(buf), "Error: %s\n", *e);
        bufio_print(bufout, buf, e);
        *e = NULL;
        if (result) {
            result->close(result);
            result = NULL;
        }
        return 1;
    }
    if (!result) {
        if (stmt_idx >= 0)
            snprintf(buf, sizeof(buf), "Error in statement %d: Failed to execute SQL\n", stmt_idx + 1);
        else
            snprintf(buf, sizeof(buf), "Error: Failed to execute SQL\n");
        bufio_print(bufout, buf, e);
        return 1;
    }

    if (transaction)
        *transaction = result->transaction;

    if (result->row_cursor) {
        if (result->column_count == 0 || result->column_names == NULL) {
            fprintf(stderr, "Warning: No column information in result\n");
            result->close(result);
            return 0;
        }

        if (pretty) {
            table = pretty_table_new(result->column_count);
            pretty_table_add_row(table, result->column_names, result->column_count);
        } else if (head) {
            for (int i = 0; i < result->column_count; i++) {
                if (i > 0)
                    bufio_print_tab(bufout, e);
                bufio_print(bufout, result->column_names[i] ? result->column_names[i] : "", e);
            }
            bufio_print_newline(bufout, e);
        }

        {
            struct flintdb_row *r;
            while ((r = result->row_cursor->next(result->row_cursor, e)) != NULL) {
                if (e && *e)
                    THROW_S(e);
                row_count++;

                if (pretty) {
                    char **row_data = (char **)CALLOC(result->column_count, sizeof(char *));
                    if (!row_data)
                        THROW(e, "Out of memory");
                    for (int i = 0; i < result->column_count; i++) {
                        struct flintdb_variant *v = r->get(r, i, e);
                        if (e && *e) {
                            for (int j = 0; j < i; j++)
                                FREE(row_data[j]);
                            FREE(row_data);
                            THROW_S(e);
                        }
                        if (v) {
                            flintdb_variant_to_string(v, buf, buf_len);
                            row_data[i] = STRDUP(buf);
                        } else {
                            row_data[i] = STRDUP("\\N");
                        }
                    }
                    pretty_table_add_row(table, row_data, result->column_count);
                    for (int i = 0; i < result->column_count; i++)
                        FREE(row_data[i]);
                    FREE(row_data);
                } else {
                    if (rownum) {
                        char rownum_buf[32];
                        snprintf(rownum_buf, sizeof(rownum_buf), "%lld", (long long)row_count);
                        bufio_print(bufout, rownum_buf, e);
                        bufio_print_tab(bufout, e);
                    }

                    for (int i = 0; i < result->column_count; i++) {
                        if (i > 0)
                            bufio_print_tab(bufout, e);
                        struct flintdb_variant *v = r->get(r, i, e);
                        if (e && *e)
                            THROW_S(e);
                        if (v) {
                            flintdb_variant_to_string(v, buf, buf_len);
                            bufio_print(bufout, buf, e);
                        } else {
                            bufio_print(bufout, "\\N", e);
                        }
                    }
                    bufio_print_newline(bufout, e);
                }
            }
        }

        if (pretty && table) {
            pretty_table_print(table, bufout, e);
            if (e && *e)
                THROW_S(e);
            pretty_table_free(table);
            table = NULL;
        }

        if (status || pretty) {
            format_number(num_buf, sizeof(num_buf), row_count);
            snprintf(buf, sizeof(buf), "%s rows, %s\n", num_buf, time_buf);
            bufio_print(bufout, buf, e);
        }

        if (affected)
            *affected = row_count;
    } else {
        i64 n = result->affected;
        if (status) {
            format_number(num_buf, sizeof(num_buf), n);
            if (n < 2)
                snprintf(buf, sizeof(buf), "%s rows affected, %s\n", num_buf, time_buf);
            else
                snprintf(buf, sizeof(buf), "%s rows affected, %s, %sops\n", num_buf, time_buf, ops_buf);
            bufio_print(bufout, buf, e);
        }
        if (affected)
            *affected = n;
    }

    result->close(result);
    return 0;

EXCEPTION:
    if (table)
        pretty_table_free(table);
    if (result)
        result->close(result);
    return -1;
}

static i64 run_repl(FILE *out, int pretty, int status, int head, int rownum, char **e) {
    struct bufio *bufout = NULL;
    struct flintdb_transaction *transaction = NULL;
    char *accum = NULL;
    size_t accum_len = 0;
    size_t accum_cap = 0;
    i64 total_affected = 0;
    char banner[256];

    cli_repl_active = 1;
    cli_interrupted = 0;

    if (out == stdout || out == stderr) {
        bufout = bufio_wrap_fd(fileno(out), FLINTDB_RDWR, CLI_BUFIO_OUTPUT_MAX, e);
        if (e && *e)
            THROW_S(e);
    }

    snprintf(banner, sizeof(banner), "%s version %s (build: %s, git: %s)\n", PRODUCT_NAME, VERSION, BUILD_TIME,
             GIT_REVISION);
    bufio_print(bufout, banner, e);
    bufio_print(bufout, "Type 'help' or '\\h'. Statements end with ';'. Exit with 'exit' or Ctrl-D.\n\n", e);
    if (bufout)
        bufout->flush(bufout, e);

    for (;;) {
        char *line;
        int meta;

        bufio_print(bufout, buffer_is_blank(accum) ? "flintdb> " : "    -> ", e);
        if (bufout)
            bufout->flush(bufout, e);

        line = repl_read_line(e);
        if (e && *e)
            THROW_S(e);

        if (cli_interrupted) {
            cli_interrupted = 0;
            FREE(line);
            repl_reset_accum(&accum, &accum_len, &accum_cap);
            clearerr(stdin);
            continue;
        }

        if (!line) {
            bufio_print(bufout, "\nBye\n", e);
            if (bufout)
                bufout->flush(bufout, e);
            break;
        }

        if (buffer_is_blank(accum)) {
            meta = repl_meta_kind(line);
            if (meta == REPL_META_EXIT) {
                FREE(line);
                bufio_print(bufout, "Bye\n", e);
                if (bufout)
                    bufout->flush(bufout, e);
                break;
            }
            if (meta == REPL_META_HELP) {
                FREE(line);
                repl_print_help(bufout, e);
                if (bufout)
                    bufout->flush(bufout, e);
                continue;
            }
            if (buffer_is_blank(line)) {
                FREE(line);
                continue;
            }
        }

        if (sql_buffer_append(&accum, &accum_len, &accum_cap, line, e) != 0) {
            FREE(line);
            THROW_S(e);
        }
        FREE(line);

        {
            char *stmt;
            while ((stmt = sql_buffer_take_stmt(&accum, &accum_len, &accum_cap, e)) != NULL) {
                char *t = stmt;
                while (*t && isspace((unsigned char)*t))
                    t++;
                if (*t) {
                    i64 n = 0;
                    int er = execute_one_statement(bufout, t, -1, pretty, status, head, rownum, &transaction, &n, e);
                    if (er < 0) {
                        FREE(stmt);
                        THROW_S(e);
                    }
                    if (er == 0)
                        total_affected += n;
                    if (bufout)
                        bufout->flush(bufout, e);
                }
                FREE(stmt);
            }
            if (e && *e)
                THROW_S(e);
        }

        meta = repl_meta_kind(accum);
        if (meta == REPL_META_EXIT) {
            bufio_print(bufout, "Bye\n", e);
            if (bufout)
                bufout->flush(bufout, e);
            break;
        }
        if (meta == REPL_META_HELP) {
            repl_print_help(bufout, e);
            if (bufout)
                bufout->flush(bufout, e);
            repl_reset_accum(&accum, &accum_len, &accum_cap);
        }
    }

    cli_repl_active = 0;
    if (transaction)
        transaction->close(transaction);
    FREE(accum);
    if (bufout)
        bufout->close(bufout);
    return total_affected;

EXCEPTION:
    cli_repl_active = 0;
    if (transaction)
        transaction->close(transaction);
    FREE(accum);
    if (bufout)
        bufout->close(bufout);
    return -1;
}

/**
 * Execute CLI commands
 */
static i64 execute_cli(FILE *out, int argc, char *argv[], char **e) {
    char *sql = NULL;
    char *sql_file = NULL;
    int pretty = 0;
    int status = 0;
    int head = 1;
    int rownum = 0;
    struct bufio *bufout = NULL;
    struct flintdb_sql_iterator *iter = NULL;
    struct flintdb_transaction *transaction = NULL;
    i64 affected = 0;

    for (int i = 1; i < argc; i++) {
        const char *s = argv[i];
        if (strcmp(s, "-help") == 0) {
            usage(argv[0]);
            return 0;
        } else if (strcmp(s, "-version") == 0) {
            printf("%s version %s (build: %s, git: %s)\n", PRODUCT_NAME, VERSION, BUILD_TIME, GIT_REVISION);
            return 0;
        } else if (strcmp(s, "-pretty") == 0) {
            pretty = 1;
        } else if (strcmp(s, "-status") == 0) {
            status = 1;
        } else if (strcmp(s, "-nohead") == 0) {
            head = 0;
        } else if (strcmp(s, "-rownum") == 0) {
            rownum = 1;
        } else if (strcmp(s, "-sql") == 0) {
            if (i + 1 < argc) {
                sql = argv[++i];
            } else {
                THROW(e, "-sql requires an argument");
            }
        } else if (strcmp(s, "-f") == 0) {
            if (i + 1 < argc) {
                sql_file = argv[++i];
            } else {
                THROW(e, "-f requires a file path");
            }
        } else if (strcmp(s, "-log") == 0) {
            // TODO: Enable detailed logging
        } else if (s[0] == '-') {
            fprintf(stderr, "Warning: Unknown option '%s' - ignoring\n", s);
        } else {
            if (sql == NULL && sql_file == NULL) {
                sql = (char *)s;
            }
        }
    }

    if (sql == NULL && sql_file == NULL) {
        if (isatty(STDIN_FILENO)) {
            pretty = 1;
            status = 1;
            return run_repl(out, pretty, status, head, rownum, e);
        }
        sql_file = "-";
    }

    if (sql != NULL && sql_file != NULL) {
        THROW(e, "Cannot specify both -sql and -f options");
    }

    if (out == stdout || out == stderr) {
        int fd = fileno(out);
        bufout = bufio_wrap_fd(fd, FLINTDB_RDWR, CLI_BUFIO_OUTPUT_MAX, e);
        if (e && *e)
            THROW_S(e);
    }

    if (sql_file) {
        iter = sql_iterator_new_from_file(sql_file, e);
    } else {
        iter = sql_iterator_new(sql, e);
    }
    if (e && *e)
        THROW_S(e);

    {
        i64 total_affected = 0;
        int has_error = 0;
        int stmt_idx = 0;
        char *stmt = NULL;

        while ((stmt = sql_iterator_next(iter, e)) != NULL) {
            int er;
            i64 n = 0;

            if (e && *e) {
                FREE(stmt);
                THROW_S(e);
            }

            if (stmt_idx > 0 && status)
                bufio_print_newline(bufout, e);

            er = execute_one_statement(bufout, stmt, stmt_idx, pretty, status, head, rownum, &transaction, &n, e);
            FREE(stmt);
            if (er < 0)
                THROW_S(e);
            if (er > 0)
                has_error = 1;
            else
                total_affected += n;
            stmt_idx++;
        }

        affected = has_error ? -1 : total_affected;
    }

    if (transaction)
        transaction->close(transaction);
    if (iter)
        sql_iterator_free(iter);
    if (bufout)
        bufout->close(bufout);

    return affected;

EXCEPTION:
    if (transaction)
        transaction->close(transaction);
    if (iter)
        sql_iterator_free(iter);
    if (bufout)
        bufout->close(bufout);

    return -1;
}

// ============================================================================
// Pretty Print Implementation
// ============================================================================

/* Decode one UTF-8 code point. Returns bytes consumed (>= 1 if *s != 0). */
static int utf8_next_cp(const char *s, unsigned int *cp) {
    const unsigned char *p = (const unsigned char *)s;
    unsigned char c = p[0];
    if (c < 0x80) {
        *cp = c;
        return 1;
    }
    if ((c & 0xE0) == 0xC0 && (p[1] & 0xC0) == 0x80) {
        *cp = ((unsigned int)(c & 0x1F) << 6) | (unsigned int)(p[1] & 0x3F);
        return 2;
    }
    if ((c & 0xF0) == 0xE0 && (p[1] & 0xC0) == 0x80 && (p[2] & 0xC0) == 0x80) {
        *cp = ((unsigned int)(c & 0x0F) << 12) | ((unsigned int)(p[1] & 0x3F) << 6) | (unsigned int)(p[2] & 0x3F);
        return 3;
    }
    if ((c & 0xF8) == 0xF0 && (p[1] & 0xC0) == 0x80 && (p[2] & 0xC0) == 0x80 && (p[3] & 0xC0) == 0x80) {
        *cp = ((unsigned int)(c & 0x07) << 18) | ((unsigned int)(p[1] & 0x3F) << 12) |
              ((unsigned int)(p[2] & 0x3F) << 6) | (unsigned int)(p[3] & 0x3F);
        return 4;
    }
    *cp = c; /* invalid lead byte */
    return 1;
}

/* Terminal cell width of a code point (Unicode TR11 East Asian Width). */
static int unicode_display_width(unsigned int cp) {
    if (cp == 0 || cp < 0x20 || (cp >= 0x7F && cp < 0xA0))
        return 0;
    /* Combining marks, variation selectors, zero-width format */
    if ((cp >= 0x0300 && cp <= 0x036F) || (cp >= 0x0483 && cp <= 0x0489) ||
        (cp >= 0x0591 && cp <= 0x05BD) || cp == 0x05BF || (cp >= 0x05C1 && cp <= 0x05C2) ||
        (cp >= 0x05C4 && cp <= 0x05C5) || cp == 0x05C7 || (cp >= 0x0610 && cp <= 0x061A) ||
        (cp >= 0x064B && cp <= 0x065F) || cp == 0x0670 || (cp >= 0x06D6 && cp <= 0x06DC) ||
        (cp >= 0x06DF && cp <= 0x06E4) || (cp >= 0x06E7 && cp <= 0x06E8) ||
        (cp >= 0x06EA && cp <= 0x06ED) || (cp >= 0x1AB0 && cp <= 0x1ACE) ||
        (cp >= 0x1DC0 && cp <= 0x1DFF) || (cp >= 0x20D0 && cp <= 0x20F0) ||
        (cp >= 0xFE00 && cp <= 0xFE0F) || (cp >= 0xFE20 && cp <= 0xFE2F) ||
        (cp >= 0xE0100 && cp <= 0xE01EF) || cp == 0x200B || cp == 0x200C || cp == 0x200D ||
        cp == 0x2060 || cp == 0xFEFF)
        return 0;
    /* Wide / Fullwidth: Hangul, CJK, kana, fullwidth forms, emoji, CJK Ext B+ */
    if ((cp >= 0x1100 && cp <= 0x115F) || /* Hangul Jamo */
        cp == 0x2329 || cp == 0x232A ||
        (cp >= 0x2E80 && cp <= 0xA4CF && cp != 0x303F) || /* CJK radicals .. Yi */
        (cp >= 0xA960 && cp <= 0xA97C) ||                 /* Hangul Jamo Extended-A */
        (cp >= 0xAC00 && cp <= 0xD7FF) ||                 /* Hangul Syllables + Jamo Ext-B */
        (cp >= 0xF900 && cp <= 0xFAFF) ||                 /* CJK Compatibility Ideographs */
        (cp >= 0xFE10 && cp <= 0xFE19) ||                 /* Vertical forms */
        (cp >= 0xFE30 && cp <= 0xFE6F) ||                 /* CJK Compatibility Forms */
        (cp >= 0xFF00 && cp <= 0xFF60) ||                 /* Fullwidth Forms */
        (cp >= 0xFFE0 && cp <= 0xFFE6) ||
        (cp >= 0x1F300 && cp <= 0x1F64F) || /* Emoji */
        (cp >= 0x1F680 && cp <= 0x1F6FF) || (cp >= 0x1F900 && cp <= 0x1F9FF) ||
        (cp >= 0x1FA00 && cp <= 0x1FAFF) || (cp >= 0x20000 && cp <= 0x2FFFD) ||
        (cp >= 0x30000 && cp <= 0x3FFFD))
        return 2;
    return 1;
}

static int string_display_width(const char *s) {
    int width = 0;
    if (!s)
        return 0;
    while (*s) {
        unsigned int cp;
        int n = utf8_next_cp(s, &cp);
        width += unicode_display_width(cp);
        s += n;
    }
    return width;
}

static struct pretty_table *pretty_table_new(int col_count) {
    struct pretty_table *table = CALLOC(1, sizeof(struct pretty_table));
    table->col_count = col_count;
    table->capacity = 100;
    table->row_count = 0;
    table->rows = CALLOC(table->capacity, sizeof(char **));
    table->col_widths = CALLOC(col_count, sizeof(int));
    return table;
}

static void pretty_table_free(struct pretty_table *table) {
    if (!table)
        return;

    for (int i = 0; i < table->row_count; i++) {
        if (table->rows[i]) {
            for (int j = 0; j < table->col_count; j++) {
                FREE(table->rows[i][j]);
            }
            FREE(table->rows[i]);
        }
    }
    FREE(table->rows);
    FREE(table->col_widths);
    FREE(table);
}

static void pretty_table_add_row(struct pretty_table *table, char **row_data, int col_count) {
    if (table->row_count >= MAX_PRETTY_ROWS) {
        return; // Skip if too many rows
    }

    // Expand capacity if needed
    if (table->row_count >= table->capacity) {
        int new_capacity = table->capacity * 2;
        if (new_capacity > MAX_PRETTY_ROWS)
            new_capacity = MAX_PRETTY_ROWS;
        table->rows = REALLOC(table->rows, new_capacity * sizeof(char **));
        table->capacity = new_capacity;
    }

    // Allocate and copy row
    char **row = CALLOC(table->col_count, sizeof(char *));
    for (int i = 0; i < col_count && i < table->col_count; i++) {
        if (row_data[i]) {
            row[i] = STRDUP(row_data[i]);
            int width = string_display_width(row[i]);
            if (width > table->col_widths[i]) {
                table->col_widths[i] = width;
            }
        } else {
            row[i] = STRDUP("\\N");
            if (2 > table->col_widths[i]) {
                table->col_widths[i] = 2;
            }
        }
    }

    table->rows[table->row_count++] = row;
}

static void pretty_table_print_border(struct pretty_table *table, struct bufio *bufout, char **e) {
    for (int i = 0; i < table->col_count; i++) {
        if (i > 0)
            bufio_print(bufout, "+", e);
        for (int j = 0; j < table->col_widths[i]; j++) {
            bufio_print(bufout, "-", e);
        }
    }
    bufio_print_newline(bufout, e);
}

static void pretty_table_flintdb_print_row(struct pretty_table *table, struct bufio *bufout, int row_idx, char **e) {
    char **row = table->rows[row_idx];

    for (int i = 0; i < table->col_count; i++) {
        if (i > 0)
            bufio_print(bufout, "|", e);

        const char *cell = row[i] ? row[i] : "\\N";
        int display_width = string_display_width(cell);
        int padding = table->col_widths[i] - display_width;
        if (padding < 0)
            padding = 0;

        bufio_print(bufout, cell, e);
        for (int p = 0; p < padding; p++) {
            bufio_print(bufout, " ", e);
        }
    }
    bufio_print_newline(bufout, e);
}

static void pretty_table_print(struct pretty_table *table, struct bufio *bufout, char **e) {
    if (table->row_count == 0)
        return;

    // Print top border
    pretty_table_print_border(table, bufout, e);
    if (e && *e)
        return;

    // Print header (first row)
    if (table->row_count > 0) {
        pretty_table_flintdb_print_row(table, bufout, 0, e);
        if (e && *e)
            return;
        pretty_table_print_border(table, bufout, e);
        if (e && *e)
            return;
    }

    // Print data rows
    for (int i = 1; i < table->row_count; i++) {
        pretty_table_flintdb_print_row(table, bufout, i, e);
        if (e && *e)
            return;
    }

    // Print bottom border
    if (table->row_count > 1) {
        pretty_table_print_border(table, bufout, e);
    }
}

// Utility functions

static void format_number(char *buf, size_t size, i64 num) {
    if (num < 1000) {
        snprintf(buf, size, "%lld", (long long)num);
    } else if (num < 1000000) {
        snprintf(buf, size, "%lld,%03lld",
                 (long long)(num / 1000), (long long)(num % 1000));
    } else if (num < 1000000000) {
        snprintf(buf, size, "%lld,%03lld,%03lld",
                 (long long)(num / 1000000),
                 (long long)((num / 1000) % 1000),
                 (long long)(num % 1000));
    } else {
        snprintf(buf, size, "%lld,%03lld,%03lld,%03lld",
                 (long long)(num / 1000000000),
                 (long long)((num / 1000000) % 1000),
                 (long long)((num / 1000) % 1000),
                 (long long)(num % 1000));
    }
}

/**
 * Create a new SQL statement iterator
 *
 * @param sql SQL string containing one or more statements
 * @param e Error pointer
 * @return Iterator instance (caller must free with sql_iterator_free)
 */
static struct flintdb_sql_iterator *sql_iterator_new(const char *sql, char **e) {
    if (!sql) {
        THROW(e, "Invalid SQL string");
    }

    struct flintdb_sql_iterator *iter = CALLOC(1, sizeof(struct flintdb_sql_iterator));
    if (!iter)
        THROW(e, "Out of memory");

    iter->sql = sql;
    iter->pos = 0;
    iter->sql_len = strlen(sql);
    iter->stmt_capacity = 4096; // Initial 4KB buffer
    iter->current_stmt = MALLOC(iter->stmt_capacity);
    iter->owns_sql = 0; // Doesn't own the SQL string

    if (!iter->current_stmt) {
        FREE(iter);
        THROW(e, "Out of memory");
    }

    return iter;

EXCEPTION:
    return NULL;
}

/**
 * Create a new SQL statement iterator from file
 * Reads file as stream without loading entire content into memory
 *
 * @param filepath Path to SQL file
 * @param e Error pointer
 * @return Iterator instance (caller must free with sql_iterator_free)
 */
static struct flintdb_sql_iterator *sql_iterator_new_from_file(const char *filepath, char **e) {
    if (!filepath) {
        THROW(e, "Invalid file path");
    }

    // Open file for streaming, or use stdin if filepath is "-"
    FILE *f;
    if (strcmp(filepath, "-") == 0) {
        f = stdin;
    } else {
        f = fopen(filepath, "rb");
        if (!f) {
            THROW(e, "Cannot open file");
        }
    }

    // Create iterator
    struct flintdb_sql_iterator *iter = CALLOC(1, sizeof(struct flintdb_sql_iterator));
    if (!iter) {
        fclose(f);
        THROW(e, "Out of memory");
    }

    iter->file = f;
    iter->from_file = 1;
    iter->pos = 0;
    iter->sql_len = 0; // Unknown for stream
    iter->stmt_capacity = 4096;
    iter->current_stmt = MALLOC(iter->stmt_capacity);
    iter->owns_sql = 0;

    // Allocate file read buffer (64KB for efficient I/O)
    iter->file_buf_size = 65536;
    iter->file_buffer = MALLOC(iter->file_buf_size);
    iter->file_buf_pos = 0;
    iter->file_buf_len = 0;

    if (!iter->current_stmt || !iter->file_buffer) {
        if (iter->current_stmt)
            FREE(iter->current_stmt);
        if (iter->file_buffer)
            FREE(iter->file_buffer);
        FREE(iter);
        fclose(f);
        THROW(e, "Out of memory");
    }

    return iter;

EXCEPTION:
    return NULL;
}

/**
 * Get next SQL statement from iterator
 * Returns NULL when no more statements
 *
 * @param iter SQL iterator
 * @param e Error pointer
 * @return SQL statement string (caller must FREE) or NULL if done
 */
static char *sql_iterator_next(struct flintdb_sql_iterator *iter, char **e) {
    if (!iter)
        return NULL;

    size_t cur_len = 0;
    char quote = 0;       // 0=none, '\'', '"', '`'
    char comment_end = 0; // 0=none, '\n'=single-line, '*'=multi-line
    char prev = 0;
    char ch;
    int has_char = 0;

#define GET_NEXT_CHAR(out)                                                                                                                                \
    (iter->from_file ? ((iter->file_buf_pos >= iter->file_buf_len) ? ((iter->file_buf_len = fread(iter->file_buffer, 1, iter->file_buf_size, iter->file), \
                                                                       iter->file_buf_pos = 0,                                                            \
                                                                       iter->file_buf_len == 0)                                                           \
                                                                          ? 0                                                                             \
                                                                          : (*(out) = iter->file_buffer[iter->file_buf_pos++], 1))                        \
                                                                   : (*(out) = iter->file_buffer[iter->file_buf_pos++], 1))                               \
                     : (iter->pos >= iter->sql_len ? 0 : (*(out) = iter->sql[iter->pos++], 1)))

#define PEEK_NEXT_CHAR(out)                                                                                                                               \
    (iter->from_file ? ((iter->file_buf_pos >= iter->file_buf_len) ? ((iter->file_buf_len = fread(iter->file_buffer, 1, iter->file_buf_size, iter->file), \
                                                                       iter->file_buf_pos = 0,                                                            \
                                                                       iter->file_buf_len == 0)                                                           \
                                                                          ? 0                                                                             \
                                                                          : (*(out) = iter->file_buffer[iter->file_buf_pos], 1))                          \
                                                                   : (*(out) = iter->file_buffer[iter->file_buf_pos], 1))                                 \
                     : (iter->pos >= iter->sql_len ? 0 : (*(out) = iter->sql[iter->pos], 1)))

    while ((has_char = GET_NEXT_CHAR(&ch)) != 0) {
        char next_ch;

        // Check for comment start (only when not in quote)
        if (quote == 0 && comment_end == 0) {
            // Single-line comment: --
            if (ch == '-' && PEEK_NEXT_CHAR(&next_ch) && next_ch == '-') {
                GET_NEXT_CHAR(&next_ch); // consume second dash
                comment_end = '\n';
                continue;
            }
            // Multi-line comment: /*
            else if (ch == '/' && PEEK_NEXT_CHAR(&next_ch) && next_ch == '*') {
                GET_NEXT_CHAR(&next_ch); // consume asterisk
                comment_end = '*';
                continue;
            }
        }

        // Inside comment - check for end
        if (comment_end != 0) {
            if (comment_end == '\n') {
                if (ch == '\n') {
                    comment_end = 0;
                    // Expand buffer if needed
                    if (cur_len + 1 >= iter->stmt_capacity) {
                        iter->stmt_capacity *= 2;
                        char *new_buf = REALLOC(iter->current_stmt, iter->stmt_capacity);
                        if (!new_buf)
                            THROW(e, "Out of memory");
                        iter->current_stmt = new_buf;
                    }
                    iter->current_stmt[cur_len++] = ' '; // preserve whitespace
                }
            } else if (comment_end == '*') {
                // Multi-line comment ends with */
                if (ch == '*' && PEEK_NEXT_CHAR(&next_ch) && next_ch == '/') {
                    GET_NEXT_CHAR(&next_ch); // consume slash
                    comment_end = 0;
                    // Expand buffer if needed
                    if (cur_len + 1 >= iter->stmt_capacity) {
                        iter->stmt_capacity *= 2;
                        char *new_buf = REALLOC(iter->current_stmt, iter->stmt_capacity);
                        if (!new_buf)
                            THROW(e, "Out of memory");
                        iter->current_stmt = new_buf;
                    }
                    iter->current_stmt[cur_len++] = ' '; // preserve whitespace
                    prev = ch;
                    continue;
                }
            }
            prev = ch;
            continue;
        }

        // Track quotes
        if (quote != 0) {
            // Inside quoted string
            if (prev != '\\' && ch == quote) {
                // End of quoted string
                quote = 0;
            }
            // Expand buffer if needed
            if (cur_len + 1 >= iter->stmt_capacity) {
                iter->stmt_capacity *= 2;
                char *new_buf = REALLOC(iter->current_stmt, iter->stmt_capacity);
                if (!new_buf)
                    THROW(e, "Out of memory");
                iter->current_stmt = new_buf;
            }
            iter->current_stmt[cur_len++] = ch;
        } else if (ch == '\'' || ch == '"' || ch == '`') {
            // Start of quoted string
            quote = ch;
            // Expand buffer if needed
            if (cur_len + 1 >= iter->stmt_capacity) {
                iter->stmt_capacity *= 2;
                char *new_buf = REALLOC(iter->current_stmt, iter->stmt_capacity);
                if (!new_buf)
                    THROW(e, "Out of memory");
                iter->current_stmt = new_buf;
            }
            iter->current_stmt[cur_len++] = ch;
        } else if (ch == ';') {
            // Statement separator - finish current statement
            break;
        } else {
            // Regular character
            // Expand buffer if needed
            if (cur_len + 1 >= iter->stmt_capacity) {
                iter->stmt_capacity *= 2;
                char *new_buf = REALLOC(iter->current_stmt, iter->stmt_capacity);
                if (!new_buf)
                    THROW(e, "Out of memory");
                iter->current_stmt = new_buf;
            }
            iter->current_stmt[cur_len++] = ch;
        }

        prev = ch;
    }

#undef GET_NEXT_CHAR
#undef PEEK_NEXT_CHAR

    // Null-terminate and trim whitespace
    iter->current_stmt[cur_len] = '\0';

    char *stmt = iter->current_stmt;
    while (*stmt && (*stmt == ' ' || *stmt == '\t' || *stmt == '\n' || *stmt == '\r'))
        stmt++;

    if (*stmt) {
        // Remove trailing whitespace
        char *end = stmt + strlen(stmt) - 1;
        while (end > stmt && (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r')) {
            *end = '\0';
            end--;
        }

        return STRDUP(stmt);
    }

    // Empty statement - try next if more data available
    if (cur_len == 0 && has_char == 0) {
        // Check if we can read more
        if (iter->from_file && !feof(iter->file)) {
            return sql_iterator_next(iter, e);
        } else if (!iter->from_file && iter->pos < iter->sql_len) {
            return sql_iterator_next(iter, e);
        }
    }

    return NULL;

EXCEPTION:
    return NULL;
}

/**
 * Free SQL iterator
 */
static void sql_iterator_free(struct flintdb_sql_iterator *iter) {
    if (!iter)
        return;
    if (iter->current_stmt)
        FREE(iter->current_stmt);
    if (iter->file_buffer)
        FREE(iter->file_buffer);
    if (iter->from_file && iter->file && iter->file != stdin)
        fclose(iter->file);
    if (iter->owns_sql && iter->sql)
        FREE((void *)iter->sql);
    FREE(iter);
}
