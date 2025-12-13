#include "plugin.h"
#include "runtime.h"
#include "internal.h"
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#define MAX_PLUGINS 32

static struct {
    struct plugin_handle *handles[MAX_PLUGINS];
    int count;
    int initialized;
} plugin_registry = {0};

int plugin_manager_init(char **e) {
    if (plugin_registry.initialized)
        return 0;
    
    memset(&plugin_registry, 0, sizeof(plugin_registry));
    plugin_registry.initialized = 1;
    
    // Scan default plugin directories
    // 1. Try ./lib directory (relative to current working directory)
    char plugin_dir[PATH_MAX] = {0};
    char *cwd = getcwd(plugin_dir, sizeof(plugin_dir) - 5); // Reserve space for "/lib"
    if (cwd) {
        size_t len = strlen(plugin_dir);
        if (len > 0 && len < sizeof(plugin_dir) - 5) {
            plugin_dir[len] = '/';
            plugin_dir[len + 1] = 'l';
            plugin_dir[len + 2] = 'i';
            plugin_dir[len + 3] = 'b';
            plugin_dir[len + 4] = '\0';
            plugin_scan_directory(plugin_dir, NULL); // Ignore errors
        }
    }
    
    // 2. Try environment variable FLINTDB_PLUGIN_PATH
    const char *env_path = getenv("FLINTDB_PLUGIN_PATH");
    if (env_path && *env_path) {
        plugin_scan_directory(env_path, NULL); // Ignore errors
    }
    
    DEBUG("plugin_manager_init: loaded %d plugins", plugin_registry.count);
    return 0;
}

void plugin_manager_cleanup() {
    if (!plugin_registry.initialized)
        return;
    
    for (int i = 0; i < plugin_registry.count; i++) {
        if (plugin_registry.handles[i]) {
            plugin_unload(plugin_registry.handles[i]);
            plugin_registry.handles[i] = NULL;
        }
    }
    plugin_registry.count = 0;
    plugin_registry.initialized = 0;
}

struct plugin_handle *plugin_load(const char *plugin_path, char **e) {
    if (!plugin_path || !*plugin_path)
        THROW(e, "plugin_path is empty");
    
    // Check if already loaded
    for (int i = 0; i < plugin_registry.count; i++) {
        if (plugin_registry.handles[i] && 
            strcmp(plugin_registry.handles[i]->path, plugin_path) == 0) {
            return plugin_registry.handles[i];
        }
    }
    
    // Check registry limit
    if (plugin_registry.count >= MAX_PLUGINS)
        THROW(e, "Maximum number of plugins reached (%d)", MAX_PLUGINS);
    
    struct plugin_handle *handle = CALLOC(1, sizeof(struct plugin_handle));
    if (!handle)
        THROW(e, "Failed to allocate plugin handle");
    
    // Load dynamic library
    handle->dl_handle = dlopen(plugin_path, RTLD_LAZY | RTLD_LOCAL);
    if (!handle->dl_handle) {
        FREE(handle);
        THROW(e, "Failed to load plugin '%s': %s", plugin_path, dlerror());
    }
    
    // Find plugin_interface symbol
    struct plugin_interface *(*get_interface)(void) = 
        (struct plugin_interface *(*)(void))dlsym(handle->dl_handle, "FLINTDB_PLUGIN_interface");
    
    if (!get_interface) {
        dlclose(handle->dl_handle);
        FREE(handle);
        THROW(e, "Plugin '%s' does not export 'FLINTDB_PLUGIN_interface' symbol", plugin_path);
    }
    
    handle->iface = get_interface();
    if (!handle->iface) {
        dlclose(handle->dl_handle);
        FREE(handle);
        THROW(e, "Plugin '%s' returned NULL interface", plugin_path);
    }
    
    strncpy(handle->path, plugin_path, PATH_MAX - 1);
    
    // Call plugin init if available
    if (handle->iface->init) {
        if (handle->iface->init(e) != 0) {
            dlclose(handle->dl_handle);
            FREE(handle);
            THROW_S(e);
        }
    }
    
    // Register plugin
    plugin_registry.handles[plugin_registry.count++] = handle;
    
    DEBUG("plugin_load: loaded plugin '%s' (version %s)", 
          handle->iface->name, handle->iface->version);
    
    return handle;
    
EXCEPTION:
    return NULL;
}

void plugin_unload(struct plugin_handle *handle) {
    if (!handle)
        return;
    
    // Call plugin cleanup if available
    if (handle->iface && handle->iface->cleanup) {
        handle->iface->cleanup();
    }
    
    // Close dynamic library
    if (handle->dl_handle) {
        dlclose(handle->dl_handle);
        handle->dl_handle = NULL;
    }
    
    FREE(handle);
}

struct plugin_interface *plugin_find_by_extension(const char *extension, char **e) {
    if (!extension || !*extension)
        return NULL;
    
    // Ensure plugin manager is initialized
    if (!plugin_registry.initialized)
        plugin_manager_init(e);
    
    // Normalize extension (remove leading dot if present)
    const char *ext = (extension[0] == '.') ? extension + 1 : extension;
    
    // Search through registered plugins
    for (int i = 0; i < plugin_registry.count; i++) {
        struct plugin_handle *h = plugin_registry.handles[i];
        if (!h || !h->iface || !h->iface->extensions)
            continue;
        
        for (const char **exts = h->iface->extensions; *exts; exts++) {
            const char *plugin_ext = (*exts[0] == '.') ? *exts + 1 : *exts;
            
            // Simple extension match (e.g., ".gz" matches "file.gz")
            if (strcasecmp(plugin_ext, ext) == 0) {
                DEBUG("plugin_find_by_extension: found plugin '%s' for extension '.%s'",
                      h->iface->name, ext);
                return h->iface;
            }
        }
    }
    
    return NULL;
}

// Find plugin by filename suffix (supports multi-part extensions like .json.gz)
struct plugin_interface *plugin_find_by_suffix(const char *filename, char **e) {
    if (!filename || !*filename)
        return NULL;
    
    // Ensure plugin manager is initialized
    if (!plugin_registry.initialized)
        plugin_manager_init(e);
    
    // Search through registered plugins
    for (int i = 0; i < plugin_registry.count; i++) {
        struct plugin_handle *h = plugin_registry.handles[i];
        if (!h || !h->iface || !h->iface->extensions)
            continue;
        
        for (const char **exts = h->iface->extensions; *exts; exts++) {
            // Use suffix() for multi-part extension matching
            if (suffix(filename, *exts)) {
                DEBUG("plugin_find_by_suffix: found plugin '%s' for file '%s'",
                      h->iface->name, filename);
                return h->iface;
            }
        }
    }
    
    return NULL;
}

int plugin_scan_directory(const char *dir, char **e) {
    if (!dir || !*dir)
        return 0;
    
    DIR *d = opendir(dir);
    if (!d) {
        // Not an error if directory doesn't exist
        return 0;
    }
    
    int loaded = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        // Look for shared library files
        const char *name = de->d_name;
        size_t len = strlen(name);
        
        // Skip hidden files and non-libraries
        if (name[0] == '.')
            continue;
        
        // Check for library extensions (.so, .dylib, .dll)
        int is_lib = 0;
        if (len > 3 && strcmp(name + len - 3, ".so") == 0)
            is_lib = 1;
        else if (len > 6 && strcmp(name + len - 6, ".dylib") == 0)
            is_lib = 1;
        else if (len > 4 && strcmp(name + len - 4, ".dll") == 0)
            is_lib = 1;
        
        if (!is_lib)
            continue;
        
        // Look for flintdb plugin naming pattern: libflintdb_*.{so,dylib,dll}
        if (strncmp(name, "libflintdb_", 10) != 0)
            continue;
        
        char plugin_path[PATH_MAX];
        snprintf(plugin_path, sizeof(plugin_path), "%s%c%s", dir, PATH_CHAR, name);
        
        // Try to load plugin (errors are logged but not fatal)
        char *err = NULL;
        if (plugin_load(plugin_path, &err)) {
            loaded++;
        } else {
            DEBUG("plugin_scan_directory: failed to load '%s': %s", 
                  plugin_path, err ? err : "unknown error");
            // Do NOT free err: it points to a thread-local buffer (see THROW in runtime.h)
            // Just ignore or reset the pointer.
            err = NULL;
        }
    }
    
    closedir(d);
    DEBUG("plugin_scan_directory: loaded %d plugins from '%s'", loaded, dir);
    return loaded;
}
