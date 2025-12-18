
#include "flintdb.h"
#include "runtime.h"
#include <stdio.h>


extern void print_memory_leak_info(); // in allocator.c
extern void sql_exec_cleanup();
extern void plugin_manager_cleanup();                                            // in plugin.c
extern void sql_pool_cleanup();                                                  // in sql.c
extern void variant_strpool_cleanup();                                           // in variant.c

void flintdb_cleanup(char **e) {
    DEBUG("FlintDB cleanup");

    plugin_manager_cleanup();
    sql_pool_cleanup();
    variant_strpool_cleanup();
    sql_exec_cleanup();

    DEBUG("FlintDB cleanup completed");

#ifdef MTRACE // Memory tracing enabled for leak detection
    // pthread_exit(NULL); // Clean up threads
    print_memory_leak_info();
#endif
}