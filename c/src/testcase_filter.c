#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <regex.h>
#include <ctype.h>

#include "flintdb.h"
#include "runtime.h"
#include "allocator.h"
#include "buffer.h"
#include "list.h"
#include "sql.h"
#include "internal.h"
#include "filter.h"



#ifdef TESTCASE_FILTER_EX
// ./testcase.sh TESTCASE_FILTER_EX --mtrace

extern void print_memory_leak_info(); // in debug.c

// 

void row_dealloc(valtype v) {
    struct flintdb_row *r = (struct flintdb_row *)v;
    r->free(r);
}

int main(int argc, char *argv[]) {
    char *e = NULL;

    const char *meta_sql = "CREATE TABLE tpch_lineitem ( "
"l_orderkey    UINT, "
"l_quantity    UINT, "
"l_comment      STRING(44), "
" "
"PRIMARY KEY (l_orderkey, l_quantity), "
"KEY IX_QUANTITY (l_quantity) "
")";

    struct flintdb_meta meta;
    struct flintdb_sql *q = NULL;
    q = flintdb_sql_parse(meta_sql, &e);

    if (!q) THROW(&e, "sql_parse");
    if (flintdb_sql_to_meta(q, &meta, &e) < 0) THROW(&e, "sql_to_meta");
    flintdb_sql_free(q);

    struct list *rows = arraylist_new(12);
    for(int i=0; i<10; i++) {
        struct flintdb_row *r = flintdb_row_new(&meta, &e);
        rows->add(rows, (valtype)r, row_dealloc, &e);
        
        struct flintdb_variant l_orderkey; 
        flintdb_variant_init(&l_orderkey);
        flintdb_variant_u32_set(&l_orderkey, i + 1000);
        r->set(r, 0, &l_orderkey, &e);
        flintdb_variant_free(&l_orderkey);

        struct flintdb_variant l_quantity; 
        flintdb_variant_init(&l_quantity);
        flintdb_variant_u32_set(&l_quantity, i);
        r->set(r, 1, &l_quantity, &e);
        flintdb_variant_free(&l_quantity);
        
        struct flintdb_variant l_comment; 
        flintdb_variant_init(&l_comment);
        char comment[256] = {0, };
        snprintf(comment, 255, "comment - %d", i);
        flintdb_variant_string_set(&l_comment, comment, (u32)strlen(comment));
        r->set(r, 2, &l_comment, &e);
        flintdb_variant_free(&l_comment); // free temporary owned string buffer
    }
    
    // Add rows with NULL values for testing
    // row[10]: orderkey=1010, quantity=NULL, comment=NULL
    {
        struct flintdb_row *r = flintdb_row_new(&meta, &e);
        rows->add(rows, (valtype)r, row_dealloc, &e);
        
        struct flintdb_variant l_orderkey; 
        flintdb_variant_init(&l_orderkey);
        flintdb_variant_u32_set(&l_orderkey, 1010);
        r->set(r, 0, &l_orderkey, &e);
        flintdb_variant_free(&l_orderkey);
        
        // l_quantity and l_comment remain NULL (NIL type)
    }
    
    // row[11]: orderkey=1011, quantity=11, comment=NULL
    {
        struct flintdb_row *r = flintdb_row_new(&meta, &e);
        rows->add(rows, (valtype)r, row_dealloc, &e);
        
        struct flintdb_variant l_orderkey; 
        flintdb_variant_init(&l_orderkey);
        flintdb_variant_u32_set(&l_orderkey, 1011);
        r->set(r, 0, &l_orderkey, &e);
        flintdb_variant_free(&l_orderkey);
        
        struct flintdb_variant l_quantity; 
        flintdb_variant_init(&l_quantity);
        flintdb_variant_u32_set(&l_quantity, 11);
        r->set(r, 1, &l_quantity, &e);
        flintdb_variant_free(&l_quantity);
        
        // l_comment remains NULL
    }

    // version 1: l-value must be a expression, r-value must be a constant
    struct {
        const char *where;
        int expected_rows[13];  // matched row indices (-1 terminated, 13 to hold 0-11 + -1)
    } testcases[] = {
        {"l_orderkey <= 1002", {0, 1, 2, -1}},
        {"l_orderkey = 1001 AND l_quantity = 1", {1, -1}},
        {"l_orderkey = 1001 AnD l_quantity = 1", {1, -1}},
        {"l_orderkey = 1002 AND l_quantity = 2", {2, -1}},
        {"l_orderkey = 1002 AND l_quantity <> 1", {2, -1}},
        {"l_orderkey = 1002 AND l_quantity != 1", {2, -1}}, // "<>, !="  => allowed
        {"l_orderkey > 1003 AND l_quantity < 6", {4, 5, -1}},
        {"l_comment != NULL", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1}}, // "IS NOT NULL" => now allowed
        {"(l_orderkey = 1001 OR l_orderkey = 1002) AND l_comment = 'comment - 1' ", {1, -1}},
        {"l_comment like 'comment%' ", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1}},
        {"l_comment like '%comment%' ", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1}},
        {"l_comment like '*comment*' ", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1}},
        {"l_comment like '%- 5%' ", {5, -1}},
        {"l_comment like '*- 7*' ", {7, -1}},
        {"l_comment like '%9' ", {9, -1}},
        {"l_comment like '*3' ", {3, -1}},
        // NULL comparison tests
        {"l_quantity = NULL", {10, -1}},
        {"l_comment = NULL", {10, 11, -1}},
        {"l_quantity != NULL", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, -1}},
        {"l_orderkey = 1010 AND l_quantity = NULL", {10, -1}},
        {"l_orderkey = 1011 AND l_comment = NULL", {11, -1}},
    };
    int n = sizeof(testcases) / sizeof(testcases[0]);

    for(int i=0; i<n; i++) {
        printf("---------------------------------------------------------------------------------------------------------------------------\n");
        const char *s = testcases[i].where;
        DEBUG("where[%d]: %s", i, s);

        struct filter *f = filter_compile(s, &meta, &e);
        DEBUG("struct filter: %p", f);
        if (e) THROW_S(e);

        if (f) {
            int matched_rows[12] = {0};
            int match_count = 0;
            
            for(int j=0; j<rows->count(rows); j++) {
                struct flintdb_row *r = (struct flintdb_row *)rows->get(rows, j, &e);
                
                int d = filter_compare(f, r, &e);
                if (d == 0) {
                    matched_rows[match_count++] = j;
                    DEBUG("  row[%d] matched", j);
                }
            }
            
            if (match_count == 0) {
                DEBUG("  no rows matched");
            }
            
            // Verify results
            int expected_count = 0;
            while (testcases[i].expected_rows[expected_count] != -1) {
                expected_count++;
            }
            
            if (match_count != expected_count) {
                WARN("  FAILED: expected %d matches, got %d", expected_count, match_count);
            } else {
                int all_match = 1;
                for (int k = 0; k < match_count; k++) {
                    if (matched_rows[k] != testcases[i].expected_rows[k]) {
                        all_match = 0;
                        break;
                    }
                }
                if (all_match) {
                    DEBUG("  PASSED");
                } else {
                    WARN("  FAILED: matched rows differ from expected");
                }
            }
        } else {
            WARN("  FAILED: failed to compile filter");
        }

        filter_free(f);
    }

    // Test filter_split functionality
    printf("===========================================================================================================\n");
    printf("Testing filter_split (B+Tree index optimization)\n");
    printf("===========================================================================================================\n");
    
    struct {
        const char *where;
        const char *index_name;       // index to use (NULL for PRIMARY KEY)
        const char *expected_first;   // indexable part (NULL if none)
        const char *expected_second;  // non-indexable part (NULL if none)
    } split_tests[] = {
        // PRIMARY KEY (l_orderkey, l_quantity)
        {"l_orderkey = 1001", NULL, "indexable", NULL},
        {"l_orderkey = 1001 AND l_quantity = 1", NULL, "indexable", NULL},
        {"l_comment = 'test'", NULL, NULL, "non-indexable"},
        {"l_orderkey = 1001 AND l_comment = 'test'", NULL, "indexable", "non-indexable"},
        {"l_orderkey >= 1000 AND l_quantity < 5 AND l_comment like '%test%'", NULL, "indexable", "non-indexable"},
        {"l_orderkey = 1001 OR l_comment = 'test'", NULL, NULL, "non-indexable"},
        
        // IX_QUANTITY index (l_quantity)
        {"l_quantity < 5", "IX_QUANTITY", "indexable", NULL},
        {"l_quantity < 5 AND l_comment = 'test'", "IX_QUANTITY", "indexable", "non-indexable"},
        {"l_orderkey = 1001", "IX_QUANTITY", NULL, "non-indexable"},  // l_orderkey not in IX_QUANTITY
    };
    
    int split_test_count = sizeof(split_tests) / sizeof(split_tests[0]);
    
    for (int i = 0; i < split_test_count; i++) {
        printf("---------------------------------------------------------------------------------------------------------------------------\n");
        const char *where = split_tests[i].where;
        
        // Find target index
        struct flintdb_index *target_index = NULL;
        if (split_tests[i].index_name) {
            // Find index by name
            for (int j = 0; j < meta.indexes.length; j++) {
                if (strcmp(meta.indexes.a[j].name, split_tests[i].index_name) == 0) {
                    target_index = &meta.indexes.a[j];
                    break;
                }
            }
        } else {
            // Find PRIMARY KEY
            for (int j = 0; j < meta.indexes.length; j++) {
                if (strcasecmp(meta.indexes.a[j].type, "PRIMARY") == 0) {
                    target_index = &meta.indexes.a[j];
                    break;
                }
            }
        }
        
        if (!target_index) {
            WARN("  FAILED: target index not found");
            continue;
        }
        
        const char *index_name = target_index->name;
        DEBUG("split_test[%d]: %s (index: %s)", i, where, index_name);
        
        struct filter *f = filter_compile(where, &meta, &e);
        if (e) THROW_S(e);
        
        if (!f) {
            WARN("  FAILED: filter_compile returned NULL");
            continue;
        }
        
        struct filter_layers *layers = filter_split(f, &meta, target_index, &e);
        if (e) THROW_S(e);
        
        if (!layers) {
            WARN("  FAILED: filter_split returned NULL");
            filter_free(f);
            continue;
        }
        
        // Check first layer (indexable)
        int first_ok = 1;
        if (split_tests[i].expected_first) {
            if (!layers->first) {
                WARN("  FAILED: expected indexable first layer, got NULL");
                first_ok = 0;
            } else {
                DEBUG("  first layer: exists (indexable)");
            }
        } else {
            if (layers->first) {
                WARN("  FAILED: expected NULL first layer, got filter");
                first_ok = 0;
            } else {
                DEBUG("  first layer: NULL (as expected)");
            }
        }
        
        // Check second layer (non-indexable)
        int second_ok = 1;
        if (split_tests[i].expected_second) {
            if (!layers->second) {
                WARN("  FAILED: expected non-indexable second layer, got NULL");
                second_ok = 0;
            } else {
                DEBUG("  second layer: exists (non-indexable)");
            }
        } else {
            if (layers->second) {
                WARN("  FAILED: expected NULL second layer, got filter");
                second_ok = 0;
            } else {
                DEBUG("  second layer: NULL (as expected)");
            }
        }
        
        if (first_ok && second_ok) {
            DEBUG("  PASSED");
        }
        
        filter_layers_free(layers);
        filter_free(f);
    }

    // Test error cases: invalid syntax and operators
    printf("===========================================================================================================\n");
    printf("Testing error cases (invalid syntax and operators)\n");
    printf("===========================================================================================================\n");
    
    struct {
        const char *where;
        const char *expected_error_pattern; // substring that should appear in error message
    } error_tests[] = {
        // Unsupported SQL operators (should give helpful error messages)
        {"l_orderkey BETWEEN 1 AND 5", "BETWEEN operator is not supported"},     // BETWEEN not supported
        {"l_orderkey IN (1, 2, 3)", "IN operator is not supported"},             // IN not supported
        {"NOT l_orderkey = 1", "unknown column"},                                // NOT parsed as column name
        {"l_orderkey IS NULL", "IS operator is not supported"},                  // IS not supported
        {"l_orderkey IS NOT NULL", "IS operator is not supported"},              // IS NOT not supported
        
        // Invalid operators - detected as invalid operators now
        {"l_orderkey == 1", "invalid value format"},                // == is parsed as value
        {"l_orderkey := 1", "invalid operator"},                    // := is invalid operator
        {"l_orderkey <=> 1", "invalid value format"},               // <=> is parsed as value
        
        // Missing operands
        {"l_orderkey =", "unexpected end of input"},                // missing right operand
        {"= 1", "expected column name"},                            // missing left operand
        {"l_orderkey", "invalid operator"},                         // missing operator - more accurate!
        
        // Invalid column names
        {"unknown_column = 1", "unknown column"},                   // non-existent column
        {"123column = 1", "unknown column"},                        // parser accepts it, but unknown
        
        // Unclosed parentheses
        {"(l_orderkey = 1", "missing closing parenthesis"},         // missing ) - error
        // Note: "l_orderkey = 1)" succeeds - parser ignores trailing )
        
        // Missing logical operands
        {"l_orderkey = 1 AND", "unexpected end of input"},          // AND without right operand
        {"l_orderkey = 1 OR", "unexpected end of input"},           // OR without right operand
        {"AND l_orderkey = 1", "unknown column"},                   // AND parsed as column name
        
        // Invalid value formats
        {"l_orderkey = 'unclosed", "unterminated string literal"},  // unclosed string
        {"l_comment = ", "unexpected end of input"},                // missing value
        
        // Empty or null input (should handle gracefully)
        {"", NULL},                                                 // empty string (returns NULL filter, no error)
    };
    
    int error_test_count = sizeof(error_tests) / sizeof(error_tests[0]);
    int error_tests_passed = 0;
    int error_tests_failed = 0;
    
    for (int i = 0; i < error_test_count; i++) {
        printf("---------------------------------------------------------------------------------------------------------------------------\n");
        const char *where = error_tests[i].where;
        const char *expected_pattern = error_tests[i].expected_error_pattern;
        
        DEBUG("error_test[%d]: '%s' (expect: %s)", i, where, expected_pattern ? expected_pattern : "NULL filter");
        
        char *test_e = NULL;
        struct filter *f = filter_compile(where, &meta, &test_e);
        
        if (expected_pattern == NULL) {
            // Expecting NULL filter without error
            if (f == NULL && test_e == NULL) {
                DEBUG("  PASSED: got NULL filter as expected");
                error_tests_passed++;
            } else if (f == NULL && test_e != NULL) {
                WARN("  FAILED: expected NULL filter without error, but got error: %s", test_e);
                error_tests_failed++;
            } else {
                WARN("  FAILED: expected NULL filter, but got valid filter");
                filter_free(f);
                error_tests_failed++;
            }
        } else {
            // Expecting error
            if (test_e && strstr(test_e, expected_pattern)) {
                DEBUG("  PASSED: got expected error: %s", test_e);
                error_tests_passed++;
            } else if (test_e) {
                WARN("  FAILED: got error '%s', but expected pattern '%s'", test_e, expected_pattern);
                error_tests_failed++;
            } else {
                WARN("  FAILED: expected error with pattern '%s', but compilation succeeded", expected_pattern);
                if (f) filter_free(f);
                error_tests_failed++;
            }
        }
    }
    
    printf("===========================================================================================================\n");
    LOG("Error test summary: %d passed, %d failed out of %d total", error_tests_passed, error_tests_failed, error_test_count);
    printf("===========================================================================================================\n");


    // 
    rows->free(rows);
    flintdb_meta_close(&meta);

    pthread_exit(NULL); // to clean up memory properly
    print_memory_leak_info();
    return 0;

    EXCEPTION:
    if (e) WARN("exc: %s", e);
    if (rows) rows->free(rows);
    flintdb_meta_close(&meta);

    pthread_exit(NULL);
    print_memory_leak_info();
    return 0;
}

#endif