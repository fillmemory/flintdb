/**
 * Command Line Interface for FlintDB
 */

// Include standard headers first (before any project headers)
// to ensure _GNU_SOURCE takes effect
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

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

#define CLI_BUFIO_OUTPUT_MAX 8192 // 8KB buffer for CLI output
// Pretty print structures
#define MAX_PRETTY_ROWS 10000
#define MAX_PRETTY_COLS 100

extern void print_memory_leak_info();                                           // in debug.c
extern int variant_to_string_fast(const struct flintdb_variant *v, char *out, u32 len); // in variant.c
extern void sql_exec_cleanup();
extern void plugin_manager_cleanup();                                            // in plugin.c

// Signal handler for graceful shutdown
static void signal_handler(int signum) {
    // fprintf(stderr, "\nReceived signal %d, cleaning up...\n", signum);
    sql_exec_cleanup();
    plugin_manager_cleanup();
    print_memory_leak_info();
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

    // Register signal handlers for graceful shutdown
    signal(SIGINT, signal_handler);  // Ctrl-C
    signal(SIGTERM, signal_handler); // kill command

    // Check for web UI mode before normal CLI execution
    for (int i = 1; i < argc; i++) {
        if (strncmp(argv[i], "-webui", 7) == 0) {
            // Pass remaining args (including -webui) to webui_run
            int rc = webui_run(argc - i, argv + i, &e);
            if (e) {
                fprintf(stderr, "Error: %s\n", e);
                return 1;
            }
            print_memory_leak_info();
            return rc;
        }
    }
    i64 result = execute_cli(stdout, argc, argv, &e);

    if (e) {
        fprintf(stderr, "Error: %s\n", e);
        return 1;
    }

    sql_exec_cleanup();

#ifdef MTRACE // Memory tracing enabled for leak detection
    pthread_exit(NULL); // Clean up threads
    print_memory_leak_info();
#endif

    return (result < 0) ? 1 : 0;
}

/**
 * Display usage information
 */
static void usage(const char *progname) {
    const char *CMD = progname ? progname : "./bin/db";
    printf("Usage: \"%s\" [options]\n\n", CMD);
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
    printf("\t%s \"BEGIN TRANSACTION "TABLE_NAME_SUFFIX"\"\n", CMD);
    printf("\n");
    printf("Development build: not all features are implemented yet.\n");
    printf("\n");
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
    /* Declare result early to avoid uninitialized warning on THROW goto */
    struct flintdb_sql_result *result = NULL;
    struct bufio *bufout = NULL; // Buffered output wrapper
    struct flintdb_sql_iterator *iter = NULL;
    struct flintdb_transaction *transaction = NULL;
    i64 affected = 0;

    char buf[256]; // General purpose buffer
    size_t buf_len = sizeof(buf);
    char num_buf[64], time_buf[64]; // For formatting numbers and time

    // Parse arguments
    for (int i = 1; i < argc; i++) {
        const char *s = argv[i];
        if (strcmp(s, "-help") == 0) {
            usage(argv[0]);
            return 0;
        } else if (strcmp(s, "-version") == 0) {
            printf("%s version %s (build: %s)\n", PRODUCT_NAME, VERSION, BUILD_TIME);
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
        } else {
            // Treat as SQL if no explicit -sql flag
            if (sql == NULL && sql_file == NULL) {
                sql = (char *)s;
            }
        }
    }

    if (argc <= 1) {
        usage(argv[0]);
        return 0;
    }

    if (sql == NULL && sql_file == NULL) {
        THROW(e, "SQL statement or file must be specified");
    }

    if (sql != NULL && sql_file != NULL) {
        THROW(e, "Cannot specify both -sql and -f options");
    }

    // Wrap stdout with buffered writer (avoid fprintf per-call overhead)
    // Using custom bufio provides ~10x better throughput than line-buffered stdio
    if (out == stdout || out == stderr) {
        int fd = fileno(out);
        bufout = bufio_wrap_fd(fd, FLINTDB_RDWR, CLI_BUFIO_OUTPUT_MAX, e); // 64KB buffer
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

    i64 total_affected = 0;
    int has_error = 0;
    int stmt_idx = 0;
    char *stmt = NULL;

    while ((stmt = sql_iterator_next(iter, e)) != NULL) {
        if (e && *e) {
            FREE(stmt);
            THROW_S(e);
        }

        if (stmt_idx > 0 && status) {
            bufio_print_newline(bufout, e); // blank line between statements
        }

        // Execute SQL
        STOPWATCH_START(watch);
        result = flintdb_sql_exec(stmt, transaction, e);
        transaction = result->transaction; // keep transaction for next statement if any
        time_dur(time_elapsed(&watch), time_buf, sizeof(time_buf));

        FREE(stmt); // Free statement after execution

        if (e && *e) {
            snprintf(buf, sizeof(buf), "Error in statement %d: %s\n", stmt_idx + 1, *e);
            bufio_print(bufout, buf, e);
            *e = NULL; // Clear error to continue
            has_error = 1;
            stmt_idx++;
            continue;
        }
        if (!result) {
            snprintf(buf, sizeof(buf), "Error in statement %d: Failed to execute SQL\n", stmt_idx + 1);
            bufio_print(bufout, buf, e);
            has_error = 1;
            stmt_idx++;
            continue;
        }

        // Handle different result types
        if (result->row_cursor) {
            // SELECT query with row cursor
            struct pretty_table *table = NULL;

            // Check if we have column information
            if (result->column_count == 0 || result->column_names == NULL) {
                // No column information available
                fprintf(stderr, "Warning: No column information in result\n");
                affected = 0;
                goto DONE;
            }

            if (pretty) {
                table = pretty_table_new(result->column_count);
                // Add header row
                pretty_table_add_row(table, result->column_names, result->column_count);
            } else if (head) {
                // Print TSV header
                for (int i = 0; i < result->column_count; i++) {
                    if (i > 0)
                        bufio_print_tab(bufout, e);
                    bufio_print(bufout, result->column_names[i] ? result->column_names[i] : "", e);
                }
                bufio_print_newline(bufout, e);
            }

            // Iterate through rows
            i64 row_count = 0;
            struct flintdb_row *r;

            while ((r = result->row_cursor->next(result->row_cursor, e)) != NULL) {
                if (e && *e)
                    THROW_S(e);
                row_count++;

                if (pretty) {
                    // For pretty mode we still need to collect strings (width calculation)
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
                            variant_to_string_fast(v, buf, buf_len); // variant_to_string_fast(v, buf, buf_len);
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
                            variant_to_string_fast(v, buf, buf_len); // variant_to_string_fast(v, buf, buf_len);
                            bufio_print(bufout, buf, e);
                        } else {
                            bufio_print(bufout, "\\N", e);
                        }
                    }
                    bufio_print_newline(bufout, e);
                }
            }

            // Print pretty table if enabled
            if (pretty && table) {
                pretty_table_print(table, bufout, e);
                if (e && *e)
                    THROW_S(e);
                pretty_table_free(table);
            }

            // Print statistics if requested
            if (status || pretty) {
                format_number(num_buf, sizeof(num_buf), row_count);
                snprintf(buf, sizeof(buf), "%s rows, %s\n", num_buf, time_buf);
                bufio_print(bufout, buf, e);
            }

            // Ensure all buffered output is written before closing
            affected = row_count;
        } else {
            // Non-SELECT query (INSERT, UPDATE, DELETE, etc.)
            affected = result->affected;

            if (status) {
                format_number(num_buf, sizeof(num_buf), affected);
                snprintf(buf, sizeof(buf), "%s rows affected, %s\n", num_buf, time_buf);
                bufio_print(bufout, buf, e);
            }
        }

        total_affected += affected;
        if (result) {
            result->close(result);
            result = NULL;
        }
        stmt_idx++;
    } // end while loop

    if (has_error) {
        affected = -1;
    } else {
        affected = total_affected;
    }

    // LOG("Total affected rows: %lld", (long long)total_affected);
DONE:
    if (transaction) 
        transaction->close(transaction);
    if (iter)
        sql_iterator_free(iter);
    if (result)
        result->close(result);
    if (bufout) 
        bufout->close(bufout); // flush and close

    return affected;

EXCEPTION:
    if (transaction) 
        transaction->close(transaction);
    if (iter)
        sql_iterator_free(iter);
    if (result)
        result->close(result);
    if (bufout) 
        bufout->close(bufout); // flush and close
    
    return -1;
}

// ============================================================================
// Pretty Print Implementation
// ============================================================================

static int utf8_char_width(const char *s) {
    unsigned char c = (unsigned char)*s;
    if (c >= 0x80) {
        // Multibyte UTF-8 character - assume width 2 for CJK characters
        if ((c & 0xE0) == 0xC0)
            return 2; // 2-byte UTF-8
        if ((c & 0xF0) == 0xE0) {
            // 3-byte UTF-8 - check if it's CJK range
            // CJK Unified Ideographs (U+4E00 to U+9FFF)
            if (c == 0xE4 || c == 0xE5 || c == 0xE6 || c == 0xE7 || c == 0xE8 || c == 0xE9) {
                return 2; // Wide character
            }
            return 1;
        }
        if ((c & 0xF8) == 0xF0)
            return 2; // 4-byte UTF-8
        return 2;
    }
    return 1; // ASCII character
}

static int string_display_width(const char *s) {
    int width = 0;
    while (*s) {
        int char_width = utf8_char_width(s);
        width += char_width;

        // Move to next character
        unsigned char c = (unsigned char)*s;
        if (c < 0x80)
            s++;
        else if ((c & 0xE0) == 0xC0)
            s += 2;
        else if ((c & 0xF0) == 0xE0)
            s += 3;
        else if ((c & 0xF8) == 0xF0)
            s += 4;
        else
            s++;
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
            row[i] = strdup(row_data[i]);
            int width = string_display_width(row[i]);
            if (width > table->col_widths[i]) {
                table->col_widths[i] = width;
            }
        } else {
            row[i] = strdup("\\N");
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

        // Print cell with padding
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

    // Open file for streaming
    FILE *f = fopen(filepath, "rb");
    if (!f) {
        THROW(e, "Cannot open file");
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
    if (iter->from_file && iter->file)
        fclose(iter->file);
    if (iter->owns_sql && iter->sql)
        FREE((void *)iter->sql);
    FREE(iter);
}
