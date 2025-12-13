#include "../../src/flintdb.h"
#include "../../src/plugin.h"
#include <string.h>

// Forward declaration from jsonlfile.c
extern struct flintdb_genericfile *jsonlfile_open(const char *file, enum flintdb_open_mode mode, const struct flintdb_meta *meta, char **e);

// JSONL plugin implementation for FlintDB plugin system

static const char *jsonl_extensions[] = {
    ".jsonl",
    ".ndjson",
    ".jsonl.gz",
    ".ndjson.gz",
    NULL
};

static struct flintdb_genericfile *jsonl_plugin_open(const char *file, enum flintdb_open_mode mode, const struct flintdb_meta *meta, char **e) {
    // Forward to jsonlfile_open implementation
    return jsonlfile_open(file, mode, meta, e);
}

static int jsonl_plugin_init(char **e) {
    // Any initialization needed for JSONL plugin
    return 0;
}

static void jsonl_plugin_cleanup(void) {
    // Any cleanup needed for JSONL plugin
}

static struct plugin_interface jsonl_interface = {
    .name = "jsonl",
    .version = "1.0.0",
    .extensions = jsonl_extensions,
    .open = jsonl_plugin_open,
    .close = NULL,  // Uses genericfile_close
    .init = jsonl_plugin_init,
    .cleanup = jsonl_plugin_cleanup,
};

// Export plugin interface
struct plugin_interface *FlintDB_plugin_interface(void) {
    return &jsonl_interface;
}
