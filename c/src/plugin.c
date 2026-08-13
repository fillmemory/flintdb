#include "plugin.h"
#include "runtime.h"
#include "internal.h"
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

#define MAX_PLUGINS 32
#define MAX_PLUGIN_SEARCH_DIRS 24

#ifdef _WIN32
#define PLUGIN_PATH_LIST_SEP ';'
#else
#define PLUGIN_PATH_LIST_SEP ':'
#endif

static struct {
    struct plugin_handle *handles[MAX_PLUGINS];
    int count;
    int initialized;
} plugin_registry = {0};

static int plugin_path_join(char *dst, size_t dstsz, const char *a, const char *b) {
    if (!dst || dstsz == 0 || !a || !*a)
        return 0;
    if (!b || !*b) {
        snprintf(dst, dstsz, "%s", a);
        return dst[0] != '\0';
    }
    size_t alen = strlen(a);
    int need_sep = (a[alen - 1] != '/' && a[alen - 1] != '\\');
    int n;
    if (need_sep)
        n = snprintf(dst, dstsz, "%s%c%s", a, PATH_CHAR, b);
    else
        n = snprintf(dst, dstsz, "%s%s", a, b);
    return n > 0 && (size_t)n < dstsz;
}

static int plugin_add_dir(char dirs[][PATH_MAX], int *n, int maxn, const char *dir) {
    if (!dir || !*dir || !n || *n >= maxn)
        return 0;

    char resolved[PATH_MAX];
    const char *p = dir;
#ifndef _WIN32
    if (realpath(dir, resolved))
        p = resolved;
#endif
    for (int i = 0; i < *n; i++) {
        if (strcmp(dirs[i], p) == 0)
            return 0;
    }
    snprintf(dirs[*n], PATH_MAX, "%s", p);
    (*n)++;
    return 1;
}

static void plugin_add_neighbors(char dirs[][PATH_MAX], int *n, int maxn, const char *base) {
    char buf[PATH_MAX];
    char parent[PATH_MAX];

    if (!base || !*base)
        return;

    plugin_add_dir(dirs, n, maxn, base);
    if (plugin_path_join(buf, sizeof(buf), base, "lib"))
        plugin_add_dir(dirs, n, maxn, buf);

    getdir(base, parent);
    if (parent[0] && plugin_path_join(buf, sizeof(buf), parent, "lib"))
        plugin_add_dir(dirs, n, maxn, buf);
}

static void plugin_add_path_list(char dirs[][PATH_MAX], int *n, int maxn, const char *list) {
    if (!list || !*list)
        return;

    char *copy = STRDUP(list);
    if (!copy)
        return;

    char *cur = copy;
    while (*cur) {
        char *sep = strchr(cur, PLUGIN_PATH_LIST_SEP);
        if (sep)
            *sep = '\0';
        if (*cur)
            plugin_add_dir(dirs, n, maxn, cur);
        if (!sep)
            break;
        cur = sep + 1;
    }
    FREE(copy);
}

// Directory of the loaded image that contains plugin_manager_init
// (libflintdb when the CLI is dynamically linked, or the CLI itself if static).
static int plugin_module_dir(char *out, size_t outsz) {
    char file[PATH_MAX];

#if defined(_WIN32)
    HMODULE hm = NULL;
    if (!GetModuleHandleExA(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
                                GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                            (LPCSTR)(void *)&plugin_manager_init, &hm) ||
        !hm)
        return 0;
    DWORD n = GetModuleFileNameA(hm, file, (DWORD)sizeof(file));
    if (n == 0 || n >= sizeof(file))
        return 0;
#else
    Dl_info info;
    memset(&info, 0, sizeof(info));
    if (!dladdr((void *)&plugin_manager_init, &info) || !info.dli_fname || !info.dli_fname[0])
        return 0;
    if (!realpath(info.dli_fname, file))
        snprintf(file, sizeof(file), "%s", info.dli_fname);
#endif

    char dir[PATH_MAX];
    getdir(file, dir);
    if (!dir[0])
        return 0;
    snprintf(out, outsz, "%s", dir);
    return 1;
}

static int plugin_exe_dir(char *out, size_t outsz) {
    char file[PATH_MAX];

#if defined(_WIN32)
    DWORD n = GetModuleFileNameA(NULL, file, (DWORD)sizeof(file));
    if (n == 0 || n >= sizeof(file))
        return 0;
#elif defined(__APPLE__)
    uint32_t size = sizeof(file);
    if (_NSGetExecutablePath(file, &size) != 0)
        return 0;
    char resolved[PATH_MAX];
    if (realpath(file, resolved))
        snprintf(file, sizeof(file), "%s", resolved);
#else
    ssize_t n = readlink("/proc/self/exe", file, sizeof(file) - 1);
    if (n <= 0)
        return 0;
    file[n] = '\0';
#endif

    char dir[PATH_MAX];
    getdir(file, dir);
    if (!dir[0])
        return 0;
    snprintf(out, outsz, "%s", dir);
    return 1;
}

// Collect unique plugin directories. Typical layouts:
//   <prefix>/bin/flintdb
//   <prefix>/lib/libflintdb.{so,dylib}
//   <prefix>/lib/libflintdb_parquet.{so,dylib}
static int plugin_collect_search_dirs(char dirs[][PATH_MAX], int maxn) {
    int n = 0;
    char buf[PATH_MAX];

    // 1. FLINTDB_PLUGIN_PATH (PATH-style, ':' on Unix / ';' on Windows)
    plugin_add_path_list(dirs, &n, maxn, getenv("FLINTDB_PLUGIN_PATH"));

    // 2. Directory of libflintdb (and lib/, ../lib next to it)
    if (plugin_module_dir(buf, sizeof(buf)))
        plugin_add_neighbors(dirs, &n, maxn, buf);

    // 3. Directory of the running executable (and lib/, ../lib)
    if (plugin_exe_dir(buf, sizeof(buf)))
        plugin_add_neighbors(dirs, &n, maxn, buf);

    // 4. CWD-relative (development: run from c/ with ./lib)
    if (getcwd(buf, sizeof(buf)))
        plugin_add_neighbors(dirs, &n, maxn, buf);

    // 5. System install locations
    plugin_add_dir(dirs, &n, maxn, "/usr/local/lib/flintdb");
    plugin_add_dir(dirs, &n, maxn, "/opt/flintdb/lib");
    plugin_add_dir(dirs, &n, maxn, "/usr/lib/flintdb");

    return n;
}

int plugin_manager_init(char **e) {
    if (plugin_registry.initialized)
        return 0;

    (void)e;
    memset(&plugin_registry, 0, sizeof(plugin_registry));
    plugin_registry.initialized = 1;

    char dirs[MAX_PLUGIN_SEARCH_DIRS][PATH_MAX];
    int ndirs = plugin_collect_search_dirs(dirs, MAX_PLUGIN_SEARCH_DIRS);
    for (int i = 0; i < ndirs; i++) {
        DEBUG("plugin_manager_init: scanning '%s'", dirs[i]);
        plugin_scan_directory(dirs[i], NULL);
    }

    DEBUG("plugin_manager_init: loaded %d plugins from %d directories",
          plugin_registry.count, ndirs);
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
        (struct plugin_interface *(*)(void))dlsym(handle->dl_handle, "FlintDB_plugin_interface");
    
    if (!get_interface) {
        dlclose(handle->dl_handle);
        FREE(handle);
        THROW(e, "Plugin '%s' does not export 'FlintDB_plugin_interface' symbol", plugin_path);
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
