#ifndef FLINTDB_PLUGIN_H
#define FLINTDB_PLUGIN_H

#include "flintdb.h"
#include <dlfcn.h>
#include <limits.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#ifdef __cplusplus
extern "C" {
#endif

// Plugin interface definition
struct plugin_interface {
    const char *name;           // Plugin name (e.g., "parquet")
    const char *version;        // Plugin version
    const char **extensions;    // Supported file extensions (NULL-terminated array)
    
    // Function pointers for file operations
    struct flintdb_genericfile *(*open)(const char *file, enum flintdb_open_mode mode, const struct flintdb_meta *meta, char **e);
    void (*close)(struct flintdb_genericfile *f);
    
    // Optional: plugin initialization/cleanup
    int (*init)(char **e);
    void (*cleanup)(void);
};

// Plugin handle structure
struct plugin_handle {
    void *dl_handle;                       // dlopen handle
    struct plugin_interface *iface;        // Plugin interface
    char path[PATH_MAX];                   // Plugin library path
};

// Plugin manager functions
int plugin_manager_init(char **e);
void plugin_manager_cleanup();
struct plugin_handle *plugin_load(const char *plugin_path, char **e);
void plugin_unload(struct plugin_handle *handle);
struct plugin_interface *plugin_find_by_extension(const char *extension, char **e);
struct plugin_interface *plugin_find_by_suffix(const char *filename, char **e);
int plugin_scan_directory(const char *dir, char **e);

#ifdef __cplusplus
}
#endif

#endif // FLINTDB_PLUGIN_H
