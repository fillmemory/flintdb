
#include "flintdb.h"
#include "runtime.h"
#include <stdio.h>
#include <stdlib.h>


extern void print_memory_leak_info(); // in allocator.c
extern void sql_exec_cleanup();
extern void plugin_manager_cleanup();                                            // in plugin.c
extern void sql_pool_cleanup();                                                  // in sql.c
extern void variant_strpool_cleanup();                                           // in variant.c
extern void variant_tempstr_cleanup();                                           // in variant.c

// Static flags
static int cleanup_registered = 0;
static int cleanup_executed = 0;

// Wrapper function for atexit (no parameters)
static void flintdb_cleanup_atexit(void) {
    if (cleanup_executed) return; // Prevent duplicate cleanup
    char *e = NULL;
    flintdb_cleanup(&e);
    if (e) {
        WARN("FlintDB cleanup error: %s", e);
    }
}

// Register cleanup function to be called automatically at exit
__attribute__((constructor))
static void flintdb_init(void) {
    if (!cleanup_registered) {
        atexit(flintdb_cleanup_atexit);
        cleanup_registered = 1;
    }
}

// Called when shared library is unloaded (dlclose) or process exits
__attribute__((destructor))
static void flintdb_fini(void) {
    flintdb_cleanup_atexit();
}

void flintdb_cleanup(char **e) {
    if (cleanup_executed) {
        DEBUG("FlintDB cleanup already executed, skipping");
        return;
    }
    cleanup_executed = 1;

    DEBUG("FlintDB cleanup");

    plugin_manager_cleanup();
    sql_pool_cleanup();
    variant_strpool_cleanup();
    variant_tempstr_cleanup();
    sql_exec_cleanup();

    DEBUG("FlintDB cleanup completed");

#ifdef MTRACE // Memory tracing enabled for leak detection
    // pthread_exit(NULL); // Clean up threads
    print_memory_leak_info();
#endif
}