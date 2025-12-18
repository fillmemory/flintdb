
#include "flintdb.h"
#include "runtime.h"

extern void sql_exec_cleanup();
extern void plugin_manager_cleanup();                                            // in plugin.c
extern void sql_pool_cleanup();                                                  // in sql.c
extern void variant_strpool_cleanup();                                           // in variant.c

void flintdb_cleanup(char **e) {
    plugin_manager_cleanup();
    sql_pool_cleanup();
    variant_strpool_cleanup();
    sql_exec_cleanup();
}