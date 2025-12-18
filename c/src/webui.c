/**
 * webui.c - Minimal HTTP interface
 * Inspired by java/src/webui/.../WebUI.java
 * 
 * Features:
 *  - GET /      -> serve webui.html (fallback to embedded HTML)
 *  - POST /     -> JSON body {"q":"SQL"} executes SQL and returns JSON array
 *                  Format: [ ["col1","col2",...], ["v1","v2",...], ... ]
 *                  Non-SELECT: ["N rows affected"]
 *
 * Limitations:
 *  - Single-threaded, sequential request handling
 *  - Very naive JSON parsing (only extracts first "q":"..." occurrence)
 *  - No chunked encoding; must send Content-Length
 */
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
// #pragma comment(lib, "ws2_32.lib")
// MSYS2/Cygwin use POSIX close(), native Windows uses closesocket()
#if defined(_POSIX_) || defined(__MSYS__) || defined(__CYGWIN__)
#define CLOSE_SOCKET(fd) close(fd)
#else
#define CLOSE_SOCKET(fd) closesocket(fd)
#endif
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#define CLOSE_SOCKET(fd) close(fd)
#endif

#include "allocator.h"
#include "flintdb.h"
#include "runtime.h"
#include "sql.h"

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define WEBUI_HTML "webui/webui.html"
#define WEBUI_DEFAULT_PORT 3334
// #define WEBUI_OPEN_BROWSER 1

#ifdef EMBED_HTML
// Embedded HTML (fallback when file not found)
static const char EMBEDDED_HTML[] = 
#include "webui_embedded.h"
;
static const size_t EMBEDDED_HTML_SIZE = sizeof(EMBEDDED_HTML) - 1;
#endif

static char *load_file(const char *path, size_t *out_size) {
    struct stat st;
    if (stat(path, &st) == 0 && S_ISREG(st.st_mode)) {
        FILE *f = fopen(path, "rb");
        if (!f)
            return NULL;
        char *buf = (char *)MALLOC(st.st_size + 1);
        size_t n = fread(buf, 1, st.st_size, f);
        fclose(f);
        buf[n] = '\0';
        if (out_size)
            *out_size = n;
        return buf;
    }
    return NULL;
}

static void http_write(int fd, const char *data, size_t len) {
    while (len > 0) {
        ssize_t w = send(fd, data, len, 0);
        if (w < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        data += w;
        len -= (size_t)w;
    }
}

static void respond_404(int fd) {
    const char *body = "Not Found";
    char hdr[256];
    snprintf(hdr, sizeof(hdr), "HTTP/1.1 404 Not Found\r\nContent-Length: %zu\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n", strlen(body));
    http_write(fd, hdr, strlen(hdr));
    http_write(fd, body, strlen(body));
}

// ============================= cJSON dynamic loader =============================
// We attempt to load cJSON at runtime using dlopen so that FlintDB does not have
// a hard build-time dependency. Build libcjson as a shared library, e.g.:
//   git clone https://github.com/DaveGamble/cJSON.git
//   cd cJSON; cc -fPIC -c cJSON.c -O2; cc -shared -o libcjson.so cJSON.o
//   (macOS) cc -dynamiclib -o libcjson.dylib cJSON.o
// Place the resulting .so/.dylib somewhere in DYLD_LIBRARY_PATH / LD_LIBRARY_PATH
// or alongside the executable. We only use a few functions.

typedef struct cJSON cJSON; // Opaque forward declaration (we only use function APIs)
static cJSON *(*p_cJSON_Parse)(const char *value) = NULL;
static void (*p_cJSON_Delete)(cJSON *item) = NULL;
static cJSON *(*p_cJSON_GetObjectItemCaseSensitive)(const cJSON *const object, const char *const name) = NULL;
static char *(*p_cJSON_GetStringValue)(const cJSON *const item) = NULL;
static cJSON *(*p_cJSON_CreateObject)(void) = NULL;
static cJSON *(*p_cJSON_CreateArray)(void) = NULL;
static cJSON *(*p_cJSON_CreateString)(const char *string) = NULL;
static cJSON *(*p_cJSON_CreateNumber)(double num) = NULL;
static int (*p_cJSON_AddItemToObject)(cJSON *object, const char *string, cJSON *item) = NULL;
static int (*p_cJSON_AddItemToArray)(cJSON *array, cJSON *item) = NULL;
static char *(*p_cJSON_PrintUnformatted)(const cJSON *item) = NULL;
static void (*p_cJSON_free)(void *ptr) = NULL;

static void *cjson_handle = NULL;

static int cjson_try_load() {
    if (cjson_handle)
        return 1; // already loaded
    // Allow explicit override via environment variable CJSON_LIB
    const char *override = getenv("CJSON_LIB");
    if (override && *override) {
        cjson_handle = dlopen(override, RTLD_LAZY | RTLD_LOCAL);
        if (!cjson_handle)
            fprintf(stderr, "cJSON dlopen failed for %s: %s\n", override, dlerror());
    }
    // Common names and Homebrew cellars (Intel /usr/local, Apple Silicon /opt/homebrew)
    // MSYS2: /mingw64/bin/libcjson-1.dll
    const char *candidates[] = {
        "libcjson.dylib", "libcjson.so", "libcjson.dll", "libcjson-1.dll",
        "./libcjson.dylib", "./libcjson.so", "./libcjson.dll", "./libcjson-1.dll",
        "/usr/local/lib/libcjson.dylib", "/usr/local/lib/libcjson.so",
        "/opt/homebrew/lib/libcjson.dylib", "/opt/homebrew/lib/libcjson.so",
        "/mingw64/bin/libcjson-1.dll", "/mingw64/bin/libcjson.dll",
        "C:/msys64/mingw64/bin/libcjson-1.dll", "C:/msys64/mingw64/bin/libcjson.dll",
        NULL};
    if (!cjson_handle) {
        for (int i = 0; candidates[i]; i++) {
            cjson_handle = dlopen(candidates[i], RTLD_LAZY | RTLD_LOCAL);
            if (cjson_handle)
                break;
        }
    }
    if (!cjson_handle) {
        fprintf(stderr, "ERROR: cJSON library not found. Please install cJSON:\n");
        fprintf(stderr, "  macOS: brew install cjson\n");
        fprintf(stderr, "  Linux: apt-get install libcjson-dev or yum install cjson-devel\n");
        fprintf(stderr, "  Windows: Copy cjson.dll to lib/ directory or system PATH\n");
        fprintf(stderr, "           Download from: https://github.com/DaveGamble/cJSON/releases\n");
        fprintf(stderr, "  Windows (MSYS2/Dev): pacman -S mingw-w64-x86_64-cjson\n");
        fprintf(stderr, "  Or set CJSON_LIB environment variable to the library path\n");
        return 0;
    }
    p_cJSON_Parse = (cJSON * (*)(const char *)) dlsym(cjson_handle, "cJSON_Parse");
    p_cJSON_Delete = (void (*)(cJSON *))dlsym(cjson_handle, "cJSON_Delete");
    p_cJSON_GetObjectItemCaseSensitive = (cJSON * (*)(const cJSON *, const char *)) dlsym(cjson_handle, "cJSON_GetObjectItemCaseSensitive");
    p_cJSON_GetStringValue = (char *(*)(const cJSON *))dlsym(cjson_handle, "cJSON_GetStringValue");
    p_cJSON_CreateObject = (cJSON * (*)(void)) dlsym(cjson_handle, "cJSON_CreateObject");
    p_cJSON_CreateArray = (cJSON * (*)(void)) dlsym(cjson_handle, "cJSON_CreateArray");
    p_cJSON_CreateString = (cJSON * (*)(const char *)) dlsym(cjson_handle, "cJSON_CreateString");
    p_cJSON_CreateNumber = (cJSON * (*)(double)) dlsym(cjson_handle, "cJSON_CreateNumber");
    p_cJSON_AddItemToObject = (int (*)(cJSON *, const char *, cJSON *))dlsym(cjson_handle, "cJSON_AddItemToObject");
    p_cJSON_AddItemToArray = (int (*)(cJSON *, cJSON *))dlsym(cjson_handle, "cJSON_AddItemToArray");
    p_cJSON_PrintUnformatted = (char *(*)(const cJSON *))dlsym(cjson_handle, "cJSON_PrintUnformatted");
    p_cJSON_free = (void (*)(void *))dlsym(cjson_handle, "cJSON_free");
    if (!p_cJSON_Parse || !p_cJSON_Delete || !p_cJSON_GetObjectItemCaseSensitive || !p_cJSON_GetStringValue ||
        !p_cJSON_CreateObject || !p_cJSON_CreateArray || !p_cJSON_CreateString || !p_cJSON_CreateNumber ||
        !p_cJSON_AddItemToObject || !p_cJSON_AddItemToArray || !p_cJSON_PrintUnformatted || !p_cJSON_free) {
        fprintf(stderr, "ERROR: Failed to load required cJSON functions\n");
        dlclose(cjson_handle);
        cjson_handle = NULL;
        return 0;
    }
    return 1;
}

static char *extract_q_cjson(const char *body) {
    cJSON *root = p_cJSON_Parse(body);
    if (!root)
        return NULL;
    cJSON *qitem = p_cJSON_GetObjectItemCaseSensitive(root, "q");
    if (!qitem) {
        p_cJSON_Delete(root);
        return NULL;
    }
    char *val = p_cJSON_GetStringValue(qitem);
    if (!val) {
        p_cJSON_Delete(root);
        return NULL;
    }
    // Copy value so we can free the cJSON tree
    char *out = STRDUP(val);
    p_cJSON_Delete(root);
    return out;
}

// Time helpers and JSON builders
static i64 time_us() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (i64)tv.tv_sec * 1000000 + tv.tv_usec;
}

static void log_access(const char *remote_addr, const char *method, const char *path, int status, i64 elapsed_us, const char *q) {
    time_t now = time(NULL);
    struct tm tm;
    localtime_r(&now, &tm);
    char timestamp[32];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &tm);
    printf("%s %s\t%s %s\t%d\t%.3fms\t%s\n", timestamp, remote_addr, method, path, status, elapsed_us / 1000.0, q==NULL? "" : q);
}

static char *build_json_object(struct flintdb_sql_result*result, i64 elapsed_us, i64 *out_len) {
    // Use cJSON for building - Java format: [[header], [row1], [row2], ...]
    cJSON *root = p_cJSON_CreateArray();
    if (result->row_cursor) {
        // First row: column headers
        cJSON *header_arr = p_cJSON_CreateArray();
        for (int i = 0; i < result->column_count; i++) {
            p_cJSON_AddItemToArray(header_arr, p_cJSON_CreateString(result->column_names[i] ? result->column_names[i] : ""));
        }
        p_cJSON_AddItemToArray(root, header_arr);

        // Data rows
        struct flintdb_row *r;
        char buf[65536];
        size_t buf_len = sizeof(buf);

        while ((r = result->row_cursor->next(result->row_cursor, NULL)) != NULL) {
            cJSON *row_arr = p_cJSON_CreateArray();
            for (int i = 0; i < result->column_count; i++) {
                struct flintdb_variant *v = r->get(r, i, NULL);
                if (v) {
                    flintdb_variant_to_string(v, buf, buf_len);
                    p_cJSON_AddItemToArray(row_arr, p_cJSON_CreateString(buf));
                } else {
                    p_cJSON_AddItemToArray(row_arr, p_cJSON_CreateString("\\N"));
                }
            }
            p_cJSON_AddItemToArray(root, row_arr);
        }
    } else {
        // Non-SELECT: [[""],["N rows affected"]]
        char msg[64];
        snprintf(msg, sizeof(msg), "%lld rows affected", (long long)result->affected);
        cJSON *header_arr = p_cJSON_CreateArray();
        p_cJSON_AddItemToArray(header_arr, p_cJSON_CreateString(""));
        p_cJSON_AddItemToArray(root, header_arr);
        cJSON *row_arr = p_cJSON_CreateArray();
        p_cJSON_AddItemToArray(row_arr, p_cJSON_CreateString(msg));
        p_cJSON_AddItemToArray(root, row_arr);
    }

    char *json_str = p_cJSON_PrintUnformatted(root);
    p_cJSON_Delete(root);

    if (out_len && json_str) {
        *out_len = (i64)strlen(json_str);
    }
    char *result_str = json_str ? STRDUP(json_str) : STRDUP("[]");
    if (json_str)
        p_cJSON_free(json_str);
    return result_str;
}

static char *build_json_error(const char *msg, i64 *out_len) {
    // Java format: [["ERROR"],["message"]]
    cJSON *root = p_cJSON_CreateArray();
    cJSON *header_arr = p_cJSON_CreateArray();
    p_cJSON_AddItemToArray(header_arr, p_cJSON_CreateString("ERROR"));
    p_cJSON_AddItemToArray(root, header_arr);
    cJSON *row_arr = p_cJSON_CreateArray();
    p_cJSON_AddItemToArray(row_arr, p_cJSON_CreateString(msg ? msg : "unknown"));
    p_cJSON_AddItemToArray(root, row_arr);
    char *json_str = p_cJSON_PrintUnformatted(root);
    p_cJSON_Delete(root);
    if (out_len && json_str)
        *out_len = (i64)strlen(json_str);
    char *result_str = json_str ? STRDUP(json_str) : STRDUP("[]");
    if (json_str)
        p_cJSON_free(json_str);
    return result_str;
}

int webui_run(int argc, char **argv, char **e) {
#if defined(_WIN32) && !defined(_POSIX_)
    // Initialize Winsock (only for native Windows, not MSYS2/Cygwin)
    WSADATA wsaData;
    int wsaerr = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (wsaerr != 0) {
        fprintf(stderr, "WSAStartup failed with error: %d\n", wsaerr);
        return 1;
    }
#endif

    // Ignore SIGHUP to run as daemon
    signal(SIGHUP, SIG_IGN);
    
    // Check cJSON availability at startup
    if (!cjson_try_load()) {
        fprintf(stderr, "Failed to load cJSON library. Cannot start WebUI.\n");
        return 1;
    }
    
    int port = WEBUI_DEFAULT_PORT;
    if (argc > 1) {
        for (int i = 1; i < argc; i++) {
            if (strncmp(argv[i], "-port=", 6) == 0) {
                port = atoi(argv[i] + 6);
            }
        }
    }
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        fprintf(stderr, "socket error: %s\n", strerror(errno));
        return 1;
    }
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char *)&opt, sizeof(opt));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((unsigned short)port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "bind error: %s\n", strerror(errno));
        CLOSE_SOCKET(fd);
        return 1;
    }
    if (listen(fd, 16) < 0) {
        fprintf(stderr, "listen error: %s\n", strerror(errno));
        CLOSE_SOCKET(fd);
        return 1;
    }
    // Debug: list received args
    /* Uncomment for troubleshooting
    for (int i=0;i<argc;i++) fprintf(stderr, "webui arg[%d]=%s\n", i, argv[i]);
    */

    char url[64];
    snprintf(url, sizeof(url), "http://localhost:%d", port);
    printf("%s\n", url);
#if WEBUI_OPEN_BROWSER == 1 && __APPLE__
    // fire and forget browser open (best-effort)
    if (fork() == 0) {
        execlp("open", "open", url, (char *)NULL);
        _exit(0);
    }
#endif
    for (;;) {
        struct sockaddr_in cli;
        socklen_t clilen = sizeof(cli);
        int cfd = accept(fd, (struct sockaddr *)&cli, &clilen);
        if (cfd < 0) {
            if (errno == EINTR)
                continue;
            fprintf(stderr, "accept: %s\n", strerror(errno));
            break;
        }
        char remote_addr[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &cli.sin_addr, remote_addr, sizeof(remote_addr));
        char req[8192];
        ssize_t r = recv(cfd, req, sizeof(req) - 1, 0);
        if (r <= 0) {
            CLOSE_SOCKET(cfd);
            continue;
        }
        req[r] = '\0';
        const char *method = (strncmp(req, "GET", 3) == 0) ? "GET" : (strncmp(req, "POST", 4) == 0 ? "POST" : "");
        if (!*method) {
            respond_404(cfd);
            CLOSE_SOCKET(cfd);
            continue;
        }
        if (strcmp(method, "GET") == 0) {
            i64 start_us = time_us();
            size_t fsz = 0;
            char *html = load_file(WEBUI_HTML, &fsz);
            int is_embedded = 0;
            if (!html) {
#ifdef EMBED_HTML
                // Fallback to embedded HTML
                fprintf(stderr, "File %s not found, using embedded HTML\n", WEBUI_HTML);
                html = (char *)EMBEDDED_HTML;
                fsz = EMBEDDED_HTML_SIZE;
                is_embedded = 1;
#else
                fprintf(stderr, "Failed to load %s (embedded HTML not enabled)\n", WEBUI_HTML);
                respond_404(cfd);
                CLOSE_SOCKET(cfd);
                continue;
#endif
            }
            char hdr[256];
            snprintf(hdr, sizeof(hdr), "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=UTF-8\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n", fsz);
            http_write(cfd, hdr, strlen(hdr));
            http_write(cfd, html, fsz);
            if (!is_embedded)
                FREE(html);
            i64 elapsed_us = time_us() - start_us;
            log_access(remote_addr, "GET", "/", 200, elapsed_us, NULL);
        } else { // POST
            // find content-length
            long clen = 0;
            const char *cl = strcasestr(req, "Content-Length:");
            if (cl) {
                clen = strtol(cl + 15, NULL, 10);
            }
            // if body not fully read and length known, read remaining
            char *body_start = strstr(req, "\r\n\r\n");
            char *body = NULL;
            size_t have = 0;
            if (body_start) {
                body_start += 4;
                have = (size_t)(r - (body_start - req));
                if (clen > 0 && (long)have < clen) {
                    body = (char *)MALLOC(clen + 1);
                    memcpy(body, body_start, have);
                    size_t remain = (size_t)clen - have;
                    size_t off = have;
                    while (remain > 0) {
                        ssize_t rr = recv(cfd, body + off, remain, 0);
                        if (rr <= 0)
                            break;
                        remain -= (size_t)rr;
                        off += (size_t)rr;
                    }
                    body[clen] = '\0';
                } else {
                    body = (char *)MALLOC(have + 1);
                    memcpy(body, body_start, have);
                    body[have] = '\0';
                }
            }
            if (!body) {
                respond_404(cfd);
                CLOSE_SOCKET(cfd);
                continue;
            }
            // Prefer cJSON parsing if available; fallback to manual parser
            char *q = extract_q_cjson(body);
            if (!q) {
                const char *msg = "[\"invalid request\"]";
                char hdr[256];
                snprintf(hdr, sizeof(hdr), "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n", strlen(msg));
                http_write(cfd, hdr, strlen(hdr));
                http_write(cfd, msg, strlen(msg));
                FREE(body);
                CLOSE_SOCKET(cfd);
                continue;
            }
            i64 start_us = time_us();
            struct flintdb_sql_result*res = flintdb_sql_exec(q, NULL, e);
            i64 elapsed_us = time_us() - start_us;
            char *json = NULL;
            i64 jlen = 0;
            if (res && (!e || !*e)) {
                json = build_json_object(res, elapsed_us, &jlen);
            } else {
                const char *err = (e && *e) ? *e : "execution error";
                json = build_json_error(err, &jlen);
            }
            if (res)
                res->close(res);
            char hdr[256];
            snprintf(hdr, sizeof(hdr), "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=UTF-8\r\nContent-Length: %lld\r\nConnection: close\r\n\r\n", (long long)jlen);
            http_write(cfd, hdr, strlen(hdr));
            http_write(cfd, json, (size_t)jlen);
            FREE(json);
            FREE(q);
            FREE(body);
            i64 post_elapsed_us = time_us() - start_us;
            log_access(remote_addr, "POST", "/", 200, post_elapsed_us, q);
        }
        CLOSE_SOCKET(cfd);
    }
    CLOSE_SOCKET(fd);

#if defined(_WIN32) && !defined(_POSIX_)
    WSACleanup();
#endif
    return 0;
}