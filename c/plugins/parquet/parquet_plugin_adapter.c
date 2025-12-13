#include "../../src/flintdb.h"
#include "../../src/plugin.h"
#include <string.h>

// Forward declaration from parquetfile.c (now in same directory)
extern struct flintdb_genericfile *parquetfile_open(const char *file, enum flintdb_open_mode mode, const struct flintdb_meta *meta, char **e);

// Parquet plugin implementation for FlintDB plugin system

static const char *parquet_extensions[] = {
    ".parquet",
    NULL
};

static struct flintdb_genericfile *parquet_plugin_open(const char *file, enum flintdb_open_mode mode, const struct flintdb_meta *meta, char **e) {
    // Forward to parquetfile_open implementation (from parquetfile.c in same directory)
    return parquetfile_open(file, mode, meta, e);
}

static int parquet_plugin_init(char **e) {
    // Any initialization needed for parquet plugin
    return 0;
}

static void parquet_plugin_cleanup(void) {
    // Any cleanup needed for parquet plugin
}

static struct plugin_interface parquet_interface = {
    .name = "parquet",
    .version = "1.0.0",
    .extensions = parquet_extensions,
    .open = parquet_plugin_open,
    .close = NULL,  // Uses genericfile_close
    .init = parquet_plugin_init,
    .cleanup = parquet_plugin_cleanup,
};

// Export plugin interface
struct plugin_interface *FlintDB_plugin_interface(void) {
    return &parquet_interface;
}
