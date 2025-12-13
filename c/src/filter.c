#include "filter.h"
#include "runtime.h"
#include "list.h"
#include "internal.h"

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <limits.h>

// Limit implementations

static int nolimit_remains(struct limit *l) {
    return 1; // always remains
}

static int nolimit_skip(struct limit *l) {
    return 0; // never skip
}

FLINTDB_API struct limit NOLIMIT = { .priv = { .offset = 0, .limit = -1, .n = 0, .o = 0 }, .remains = nolimit_remains, .skip = nolimit_skip };

static int maxlimit_remains(struct limit *l) {
    return l->priv.n-- > 0;
}

static int maxlimit_skip(struct limit *l) {
    return (l->priv.o > 0 ? l->priv.o-- : l->priv.o) > 0;

}

struct limit maxlimit(int offset, int limit) {
    // TODO: test again
    int actual_limit = (limit == -1 ? INT_MAX : limit);
    struct limit l = {
        .priv = { .offset = offset, .limit = limit, .n = actual_limit, .o = offset },
        .remains = maxlimit_remains,
        .skip = maxlimit_skip,
    };
    return l;
}

struct limit limit_parse(const char *s) {
    if (s == NULL || *s == '\0' || strcmp(s, "nolimit") == 0) 
        return NOLIMIT;
    
    // parse "offset,limit" or  "limit"
    int offset = 0;
    int limit = 0;
    if (sscanf(s, "%d,%d", &offset, &limit) == 2) {
        if (limit < 0) limit = -1;
        if (offset < 0) offset = 0;
        return maxlimit(offset, limit);
    } else if (sscanf(s, "%d", &limit) == 1)    {
        if (limit < 0) limit = -1;
        return maxlimit(0, limit);
    } else {
        return NOLIMIT; // default
    }
}

/**
 * @brief Compare row value with filter value for B+Tree search
 * This follows Java Filter.compile() logic: compare(RV, LV) where RV is filter value, LV is row value
 * 
 * @param op arithmetic operator
 * @param key index of column in row 
 * @param left row to compare
 * @param right value to compare against (RV in Java code)
 * @return int -1 if RV < LV, 0 if match, 1 if RV > LV (for B+Tree binary search)
 */
int filter_row_compare(enum arithmetic_operator op, int key, struct flintdb_row *left, struct flintdb_variant *right) {
    struct flintdb_variant *l = left->get(left, key, NULL);  // LV (left value from row)
    struct flintdb_variant *r = right;                        // RV (right value from filter)
    
    // Special handling for NULL comparisons
    // For arithmetic operators (except EQUAL/NOT_EQUAL), NULL should not match
    if (op != EQUAL && op != NOT_EQUAL) {
        if (l->type == VARIANT_NULL || r->type == VARIANT_NULL) {
            return 1;  // No match for NULL in arithmetic comparisons
        }
    }
    
    int cmp = flintdb_variant_compare(r, l);  // compare(RV, LV) like Java

    switch (op) {
    case BAD_OPERATOR:
        // This should never happen - bad operator should be caught during parsing
        return 1;  // No match
        
    case EQUAL: 
        // For B+Tree range scan with EQUAL operator:
        // Returns compare(RV, LV) where RV is filter value, LV is row value
        // This matches Java implementation: return compare(RV, o.get(key));
        return cmp; // -1: RV < LV, 0: RV == LV, 1: RV > LV

    case LESSER_EQUAL:  // LV <= RV  =>  compare(RV, LV) >= 0
        return cmp >= 0 ? 0 : -1;
        
    case LESSER:  // LV < RV  =>  compare(RV, LV) > 0
        return cmp > 0 ? 0 : -1;

    case GREATER_EQUAL:  // LV >= RV  =>  -compare(RV, LV) >= 0  =>  compare(RV, LV) <= 0
        return -cmp >= 0 ? 0 : 1;
        
    case GREATER:  // LV > RV  =>  -compare(RV, LV) > 0  =>  compare(RV, LV) < 0
        return -cmp > 0 ? 0 : 1;

    case NOT_EQUAL: 
        return cmp != 0 ? 0 : -1;

    case LIKE: {
        // LIKE implementation (supports % and * wildcards)
        if (l->type != VARIANT_STRING || r->type != VARIANT_STRING) return 1;
        
        const char *str = flintdb_variant_string_get(l);
        const char *pattern = flintdb_variant_string_get(r);
        
        if (!str || !pattern) return 1;
        
        int pattern_len = strlen(pattern);
        if (pattern_len == 0) {
            return strlen(str) == 0 ? 0 : 1;
        }
        
        // Determine wildcard character (% or *)
        char wildcard = '%';
        if (strchr(pattern, '*') != NULL) {
            wildcard = '*';
        }
        
        // Check for prefix% or prefix* pattern
        if (pattern[pattern_len - 1] == wildcard) {
            // Check for %substring% or *substring* pattern
            if (pattern[0] == wildcard && pattern_len > 1) {
                // Contains pattern: %substring% or *substring*
                char substring[256];
                int sub_len = pattern_len - 2;
                if (sub_len > 0 && sub_len < 256) {
                    strncpy(substring, pattern + 1, sub_len);
                    substring[sub_len] = '\0';
                    const char *found = strstr(str, substring);
                    return found ? 0 : 1;
                }
                return 1;
            }
            // Prefix pattern: prefix% or prefix*
            int result = strncmp(str, pattern, pattern_len - 1);
            return result == 0 ? 0 : (result < 0 ? -1 : 1);
        }
        
        // Check for %suffix or *suffix pattern
        if (pattern[0] == wildcard && pattern_len > 1) {
            const char *suffix = pattern + 1;
            int suffix_len = pattern_len - 1;
            int str_len = strlen(str);
            if (str_len >= suffix_len) {
                int result = strcmp(str + str_len - suffix_len, suffix);
                return result == 0 ? 0 : (result < 0 ? -1 : 1);
            }
            return 1;
        }
        
        // Exact match (no wildcards)
        int result = strcmp(str, pattern);
        return result == 0 ? 0 : (result < 0 ? -1 : 1);
    }
    }
    return 1;
}

int filter_compare(struct filter *filter, struct flintdb_row *r, char **e) {
    if (!filter) return 1;
    if (!r) THROW(e, "row is NULL");
    
    if (filter->type == FILTER_CONDITION) {
        // evaluate single condition
        struct filter_condition *cond = &filter->data.cond;
        return filter_row_compare(cond->op, cond->column_index, r, cond->value);
    } else if (filter->type == FILTER_LOGICAL) {
        // evaluate logical operation
        struct list *filters = filter->data.logical.filters;
        enum logical_operator op = filter->data.logical.op;
        
        if (!filters || filters->count(filters) == 0) return 1;
        
        if (op == AND) {
            // all conditions must match (return 0)
            for (int i = 0; i < filters->count(filters); i++) {
                struct filter *f = (struct filter *)filters->get(filters, i, NULL);
                int result = filter_compare(f, r, e);
                if (e && *e) THROW_S(e);
                if (result != 0) return result; // return first non-zero (non-match)
            }
            return 0; // all matched
        } else if (op == OR) {
            // at least one condition must match (return 0)
            for (int i = 0; i < filters->count(filters); i++) {
                struct filter *f = (struct filter *)filters->get(filters, i, NULL);
                int result = filter_compare(f, r, e);
                if (e && *e) THROW_S(e);
                if (result == 0) return 0; // found a match
            }
            return 1; // no matches
        }
    }
    
    return 1;

    EXCEPTION:
    return 1;
}

// helper functions for parsing
static inline void skip_whitespace(const char **s) {
    while (**s && (**s == ' ' || **s == '\t' || **s == '\n' || **s == '\r')) (*s)++;
}

/**
 * @brief Parse column name (L-Value)
 * Extracts identifier: alphanumeric characters and underscore
 * 
 * @param s Input string pointer (advanced after parsing)
 * @param out Output buffer for column name
 * @param max_len Maximum length of output buffer
 * @param e Error message output
 * @return int 1 if successful, 0 if failed
 */
static int parse_column_name(const char **s, char *out, int max_len, char **e) {
    skip_whitespace(s);
    int i = 0;
    while (**s && (isalnum((unsigned char)**s) || **s == '_') && i < max_len - 1) {
        out[i++] = **s;
        (*s)++;
    }
    out[i] = '\0';
    
    if (i == 0) {
        THROW(e, "expected column name");
    }
    if (i >= max_len - 1) {
        THROW(e, "column name too long");
    }
    return 1;
    
    EXCEPTION:
    return 0;
}

/**
 * @brief Parse arithmetic/comparison operator
 * Supports: =, <=, <, >=, >, <>, !=, LIKE
 * 
 * @param s Input string pointer (advanced after parsing)
 * @param e Error message output
 * @return enum arithmetic_operator Operator type, or BAD_OPERATOR if not found/unsupported
 */
static enum arithmetic_operator parse_operator(const char **s, char **e) {
    skip_whitespace(s);
    
    // Check for unsupported SQL keywords first (case insensitive)
    // Note: We need to ensure these are complete words, not prefixes of identifiers
    if (strncasecmp(*s, "BETWEEN", 7) == 0 && ((*s)[7] == '\0' || (!isalnum((unsigned char)(*s)[7]) && (*s)[7] != '_'))) {
        THROW(e, "BETWEEN operator is not supported. Use 'column >= value1 AND column <= value2' instead");
    }
    if (strncasecmp(*s, "IN", 2) == 0 && ((*s)[2] == '\0' || (!isalnum((unsigned char)(*s)[2]) && (*s)[2] != '_'))) {
        THROW(e, "IN operator is not supported. Use 'column = value1 OR column = value2' instead");
    }
    if (strncasecmp(*s, "NOT", 3) == 0 && ((*s)[3] == '\0' || (!isalnum((unsigned char)(*s)[3]) && (*s)[3] != '_'))) {
        THROW(e, "NOT operator is not supported");
    }
    if (strncasecmp(*s, "IS", 2) == 0 && ((*s)[2] == '\0' || (!isalnum((unsigned char)(*s)[2]) && (*s)[2] != '_'))) {
        THROW(e, "IS operator is not supported. Use '=' for equality or check for NULL values");
    }
    
    if (strncmp(*s, "<=", 2) == 0) { *s += 2; return LESSER_EQUAL; }
    if (strncmp(*s, ">=", 2) == 0) { *s += 2; return GREATER_EQUAL; }
    if (strncmp(*s, "<>", 2) == 0) { *s += 2; return NOT_EQUAL; }
    if (strncmp(*s, "!=", 2) == 0) { *s += 2; return NOT_EQUAL; }
    if (**s == '<') { (*s)++; return LESSER; }
    if (**s == '>') { (*s)++; return GREATER; }
    if (**s == '=') { (*s)++; return EQUAL; }
    
    // check for LIKE (case insensitive)
    skip_whitespace(s);
    if (strncasecmp(*s, "like", 4) == 0) {
        *s += 4;
        return LIKE;
    }
    
    THROW(e, "invalid operator");
    
    EXCEPTION:
    return BAD_OPERATOR;
}

/**
 * @brief Parse value (R-Value) with type casting
 * Parses quoted string, NULL, or numeric value
 * Automatically casts to target column type if meta information is available
 * 
 * @param s Input string pointer (advanced after parsing)
 * @param v Output variant to store parsed value
 * @param meta Table metadata for type casting
 * @param column_index Column index for determining target type
 * @param e Error message output
 * @return int 1 if successful, 0 if failed
 */
static int parse_value(const char **s, struct flintdb_variant *v, struct flintdb_meta *meta, int column_index, char **e) {
    skip_whitespace(s);
    
    if (!**s) {
        THROW(e, "unexpected end of input, expected value");
    }
    
    // Get column type for casting
    enum flintdb_variant_type  target_type = VARIANT_NULL;
    if (meta && column_index >= 0 && column_index < meta->columns.length) {
        target_type = meta->columns.a[column_index].type;
    }
    
    // string value (quoted)
    if (**s == '\'' || **s == '"') {
        char quote = **s;
        (*s)++;
        char buffer[256] = {0};
        int i = 0;
        while (**s && **s != quote && i < 255) {
            buffer[i++] = **s;
            (*s)++;
        }
        if (**s != quote) {
            THROW(e, "unterminated string literal");
        }
        if (**s == quote) (*s)++;
        
        // If target type is known and numeric, try to parse as number
        if (target_type == VARIANT_UINT32 || target_type == VARIANT_INT32 || target_type == VARIANT_INT64 || 
            target_type == VARIANT_UINT8 || target_type == VARIANT_INT8 || target_type == VARIANT_UINT16 || target_type == VARIANT_INT16) {
            i64 num;
            if (parse_i64(buffer, i, &num) == 0) {
                flintdb_variant_i64_set(v, num);
                return 1;
            }
        } else if (target_type == VARIANT_DOUBLE || target_type == VARIANT_FLOAT) {
            char *end;
            double d = strtod(buffer, &end);
            if (end != buffer) {
                flintdb_variant_f64_set(v, d);
                return 1;
            }
        }
        
        // Default: store as string
        flintdb_variant_string_set(v, buffer, i);
        return 1;
    }
    
    // NULL value
    if (strncasecmp(*s, "NULL", 4) == 0) {
        *s += 4;
        flintdb_variant_null_set(v);
        return 1;
    }
    
    // numeric value
    char *end;
    double d = strtod(*s, &end);
    if (end != *s) {
        *s = end;
        
        // Cast to target type if known
        if (target_type == VARIANT_UINT32 || target_type == VARIANT_UINT8 || target_type == VARIANT_UINT16) {
            flintdb_variant_u32_set(v, (u32)d);
        } else if (target_type == VARIANT_INT32 || target_type == VARIANT_INT8 || target_type == VARIANT_INT16) {
            flintdb_variant_i32_set(v, (i32)d);
        } else if (target_type == VARIANT_INT64) {
            flintdb_variant_i64_set(v, (i64)d);
        } else if (target_type == VARIANT_DOUBLE || target_type == VARIANT_FLOAT) {
            flintdb_variant_f64_set(v, d);
        } else {
            // Default: check if it's an integer
            if (d == (int)d) {
                flintdb_variant_u32_set(v, (u32)d);
            } else {
                flintdb_variant_f64_set(v, d);
            }
        }
        return 1;
    }
    
    THROW(e, "invalid value format");
    
    EXCEPTION:
    return 0;
}

static void filter_dealloc(valtype v) {
    struct filter *f = (struct filter *)v;
    if (!f) return;
    
    if (f->type == FILTER_CONDITION) {
        if (f->data.cond.value) {
            flintdb_variant_free(f->data.cond.value);
            FREE(f->data.cond.value);
            f->data.cond.value = NULL;
        }
    } else if (f->type == FILTER_LOGICAL) {
        if (f->data.logical.filters) {
            f->data.logical.filters->free(f->data.logical.filters);
            f->data.logical.filters = NULL;
        }
    }
    FREE(f);
}

// 

/**
 * @brief Check if a filter condition can use a specific B+Tree index
 * Checks if all filter conditions are on columns that are part of the given index
 * 
 * @param f Filter to check
 * @param meta Table metadata
 * @param target_index Index to check against
 * @return int 1 if indexable, 0 otherwise
 */
static int is_indexable(struct filter *f, struct flintdb_meta *meta, struct flintdb_index *target_index) {
    if (!f || !meta || !target_index) return 0;
    
    if (f->type == FILTER_CONDITION) {
        int col_index = f->data.cond.column_index;
        
        // Check if column is part of the target index
        const char *col_name = meta->columns.a[col_index].name;
        for (int i = 0; i < target_index->keys.length; i++) {
            if (strcmp(target_index->keys.a[i], col_name) == 0) {
                return 1;
            }
        }
        return 0;
    }
    
    // Logical filters need recursive check
    if (f->type == FILTER_LOGICAL) {
        struct list *filters = f->data.logical.filters;
        if (!filters) return 0;
        
        // For AND: all conditions must be indexable
        if (f->data.logical.op == AND) {
            for (int i = 0; i < filters->count(filters); i++) {
                struct filter *sub = (struct filter *)filters->get(filters, i, NULL);
                if (!is_indexable(sub, meta, target_index)) {
                    return 0;
                }
            }
            return 1;
        }
        
        // For OR: cannot use index efficiently (would need multiple searches)
        return 0;
    }
    
    return 0;
}

/**
 * @brief Clone a filter structure (deep copy)
 * 
 * @param f Filter to clone
 * @param e Error message output
 * @return struct filter* Cloned filter, or NULL if failed
 */
static struct filter *filter_clone(struct filter *f, char **e) {
    if (!f) return NULL;
    
    struct filter *clone = CALLOC(1, sizeof(struct filter));
    clone->type = f->type;
    
    if (f->type == FILTER_CONDITION) {
        clone->data.cond.op = f->data.cond.op;
        clone->data.cond.column_index = f->data.cond.column_index;
        
        // Clone value
        clone->data.cond.value = CALLOC(1, sizeof(struct flintdb_variant));
        flintdb_variant_init(clone->data.cond.value);
        flintdb_variant_copy(clone->data.cond.value, f->data.cond.value);
        
    } else if (f->type == FILTER_LOGICAL) {
        clone->data.logical.op = f->data.logical.op;
        clone->data.logical.filters = arraylist_new(2);
        
        // Clone all sub-filters
        struct list *src_list = f->data.logical.filters;
        for (int i = 0; i < src_list->count(src_list); i++) {
            struct filter *sub = (struct filter *)src_list->get(src_list, i, NULL);
            struct filter *sub_clone = filter_clone(sub, e);
            if (!sub_clone) {
                filter_dealloc((valtype)clone);
                return NULL;
            }
            clone->data.logical.filters->add(clone->data.logical.filters, (valtype)sub_clone, filter_dealloc, NULL);
        }
    }
    
    return clone;
}

/**
 * @brief Split filter into indexable and non-indexable parts
 * Optimizes B+Tree searches by separating conditions that can use a specific index
 * 
 * For example with PRIMARY KEY (l_orderkey, l_quantity):
 * - "l_orderkey = 1001 AND l_comment = 'test'" 
 *   → first: "l_orderkey = 1001", second: "l_comment = 'test'"
 * 
 * - "l_orderkey = 1001 AND l_quantity < 5"
 *   → first: "l_orderkey = 1001 AND l_quantity < 5", second: NULL
 * 
 * - "l_comment = 'test'"
 *   → first: NULL, second: "l_comment = 'test'"
 * 
 * @param f Original filter
 * @param meta Table metadata
 * @param target_index Index to use for splitting
 * @param e Error message output
 * @return struct filter_layers* Split filter layers, or NULL if failed
 */
struct filter_layers *filter_split(struct filter *f, struct flintdb_meta *meta, struct flintdb_index *target_index, char **e) {
    if (!f || !meta || !target_index) return NULL;
    
    struct filter_layers *layers = CALLOC(1, sizeof(struct filter_layers));
    
    // Simple case: entire filter is indexable
    if (is_indexable(f, meta, target_index)) {
        layers->first = filter_clone(f, e);
        layers->second = NULL;
        return layers;
    }
    
    // Simple case: entire filter is not indexable
    if (f->type == FILTER_CONDITION) {
        layers->first = NULL;
        layers->second = filter_clone(f, e);
        return layers;
    }
    
    // Complex case: logical filter with mixed indexable/non-indexable conditions
    if (f->type == FILTER_LOGICAL && f->data.logical.op == AND) {
        struct list *src_list = f->data.logical.filters;
        struct list *indexable_list = arraylist_new(2);
        struct list *nonindexable_list = arraylist_new(2);
        
        for (int i = 0; i < src_list->count(src_list); i++) {
            struct filter *sub = (struct filter *)src_list->get(src_list, i, NULL);
            if (is_indexable(sub, meta, target_index)) {
                struct filter *clone = filter_clone(sub, e);
                indexable_list->add(indexable_list, (valtype)clone, NULL, NULL);
            } else {
                struct filter *clone = filter_clone(sub, e);
                nonindexable_list->add(nonindexable_list, (valtype)clone, NULL, NULL);
            }
        }
        
        // Build first layer (indexable)
        if (indexable_list->count(indexable_list) == 0) {
            // No indexable conditions
            indexable_list->free(indexable_list);
        } else if (indexable_list->count(indexable_list) == 1) {
            // Single condition - transfer ownership directly
            layers->first = (struct filter *)indexable_list->get(indexable_list, 0, NULL);
            indexable_list->free(indexable_list);
        } else {
            // Multiple indexable conditions - need to register dealloc for list management
            // Re-register with dealloc for proper cleanup when list is freed
            for (int i = 0; i < indexable_list->count(indexable_list); i++) {
                // Update entry's dealloc function
                struct entry {
                    valtype item;
                    void (*dealloc)(valtype);
                } *ent = (struct entry *)indexable_list->a[i];
                ent->dealloc = filter_dealloc;
            }
            
            layers->first = CALLOC(1, sizeof(struct filter));
            layers->first->type = FILTER_LOGICAL;
            layers->first->data.logical.op = AND;
            layers->first->data.logical.filters = indexable_list;
            // indexable_list ownership transferred to layers->first
        }
        
        // Build second layer (non-indexable)
        if (nonindexable_list->count(nonindexable_list) == 0) {
            // No non-indexable conditions
            nonindexable_list->free(nonindexable_list);
        } else if (nonindexable_list->count(nonindexable_list) == 1) {
            // Single condition - transfer ownership directly
            layers->second = (struct filter *)nonindexable_list->get(nonindexable_list, 0, NULL);
            nonindexable_list->free(nonindexable_list);
        } else {
            // Multiple non-indexable conditions - need to register dealloc for list management
            // Re-register with dealloc for proper cleanup when list is freed
            for (int i = 0; i < nonindexable_list->count(nonindexable_list); i++) {
                // Update entry's dealloc function
                struct entry {
                    valtype item;
                    void (*dealloc)(valtype);
                } *ent = (struct entry *)nonindexable_list->a[i];
                ent->dealloc = filter_dealloc;
            }
            
            layers->second = CALLOC(1, sizeof(struct filter));
            layers->second->type = FILTER_LOGICAL;
            layers->second->data.logical.op = AND;
            layers->second->data.logical.filters = nonindexable_list;
            // nonindexable_list ownership transferred to layers->second
        }
        
        return layers;
    }
    
    // OR filters or other complex cases: cannot split efficiently
    layers->first = NULL;
    layers->second = filter_clone(f, e);
    return layers;
}

/**
 * @brief Free filter_layers structure
 * 
 * @param layers Filter layers to free
 */
void filter_layers_free(struct filter_layers *layers) {
    if (!layers) return;
    
    if (layers->first) filter_dealloc((valtype)layers->first);
    if (layers->second) filter_dealloc((valtype)layers->second);
    FREE(layers);
}



/**
 * @brief Parse a single condition: column_name operator value
 * This parses the complete L-Value OP R-Value expression
 * Example: "l_orderkey <= 1002"
 * 
 * @param s Input string pointer (advanced after parsing)
 * @param meta Table metadata for column lookup and type casting
 * @param e Error message output
 * @return struct filter* Filter condition, or NULL if failed
 */
static struct filter *parse_condition(const char **s, struct flintdb_meta *meta, char **e) {
    char column_name[256] = {0};
    if (!parse_column_name(s, column_name, sizeof(column_name), e)) {
        return NULL;
    }
    
    // find column index using column_at
    int column_index = flintdb_column_at(meta, column_name);
    if (column_index < 0) {
        THROW(e, "unknown column '%s'", column_name);
    }
    
    enum arithmetic_operator op = parse_operator(s, e);
    if (op == BAD_OPERATOR) {
        return NULL;
    }
    
    struct flintdb_variant *value = CALLOC(1, sizeof(struct flintdb_variant));
    flintdb_variant_init(value);
    if (!parse_value(s, value, meta, column_index, e)) {
        flintdb_variant_free(value);
        FREE(value);
        return NULL;
    }
    
    struct filter *f = CALLOC(1, sizeof(struct filter));
    f->type = FILTER_CONDITION;
    f->data.cond.op = op;
    f->data.cond.column_index = column_index;
    f->data.cond.value = value;
    
    return f;
    
    EXCEPTION:
    return NULL;
}

static struct filter *parse_expression(const char **s, struct flintdb_meta *meta, char **e);

/**
 * @brief Parse primary expression (atomic unit in the grammar)
 * Handles two cases:
 * 1. Parenthesized expression: '(' expression ')'
 * 2. Single condition: column_name operator value
 * 
 * This is NOT just L-Value parsing - it parses a complete filter unit.
 * 
 * @param s Input string pointer (advanced after parsing)
 * @param meta Table metadata
 * @param e Error message output
 * @return struct filter* Filter object, or NULL if failed
 */
static struct filter *parse_primary(const char **s, struct flintdb_meta *meta, char **e) {
    skip_whitespace(s);
    struct filter *f = NULL;
    
    if (!**s) {
        THROW(e, "unexpected end of input");
    }
    
    // handle parentheses
    if (**s == '(') {
        (*s)++;
        f = parse_expression(s, meta, e);
        if (!f) return NULL;
        
        skip_whitespace(s);
        if (**s != ')') {
            THROW(e, "missing closing parenthesis ')'");
        }
        (*s)++;
        return f;
    }
    
    // parse condition
    return parse_condition(s, meta, e);
    
    EXCEPTION:
    if (f) filter_dealloc((valtype)f);
    return NULL;
}

/**
 * @brief Parse expression with logical operators (AND/OR)
 * Handles chains of conditions connected by AND/OR operators
 * Uses iterative approach to build left-associative tree
 * 
 * Examples:
 * - "A AND B"          → LOGICAL(AND, [A, B])
 * - "A AND B OR C"     → LOGICAL(OR, [LOGICAL(AND, [A, B]), C])
 * - "A OR B AND C"     → LOGICAL(AND, [LOGICAL(OR, [A, B]), C])
 * 
 * Note: No operator precedence - evaluates left-to-right
 * Use parentheses for explicit precedence: "(A OR B) AND C"
 * 
 * @param s Input string pointer (advanced after parsing)
 * @param meta Table metadata
 * @param e Error message output
 * @return struct filter* Filter tree, or NULL if failed
 */
static struct filter *parse_expression(const char **s, struct flintdb_meta *meta, char **e) {
    struct filter *left = parse_primary(s, meta, e);
    if (!left) {
        return NULL;
    }
    
    skip_whitespace(s);
    
    // check for logical operators (iteratively to handle multiple conditions)
    while (**s) {
        enum logical_operator logical_op = -1;
        if (strncasecmp(*s, "AND", 3) == 0 && ((*s)[3] == '\0' || !isalnum((unsigned char)(*s)[3]))) {
            *s += 3;
            logical_op = AND;
        } else if (strncasecmp(*s, "OR", 2) == 0 && ((*s)[2] == '\0' || !isalnum((unsigned char)(*s)[2]))) {
            *s += 2;
            logical_op = OR;
        } else {
            break; // no logical operator found
        }
        
        skip_whitespace(s);
        
        struct filter *right = parse_primary(s, meta, e);
        if (!right) {
            if (e && !*e) {
                if (logical_op == AND) {
                    THROW(e, "expected condition after AND");
                } else {
                    THROW(e, "expected condition after OR");
                }
            }
            filter_dealloc((valtype)left);
            return NULL;
        }
        
        struct filter *f = CALLOC(1, sizeof(struct filter));
        f->type = FILTER_LOGICAL;
        f->data.logical.op = logical_op;
        f->data.logical.filters = arraylist_new(2);
        f->data.logical.filters->add(f->data.logical.filters, (valtype)left, filter_dealloc, NULL);
        f->data.logical.filters->add(f->data.logical.filters, (valtype)right, filter_dealloc, NULL);
        
        left = f; // continue with combined filter
        skip_whitespace(s);
    }
    
    return left;
    
    EXCEPTION:
    if (left) filter_dealloc((valtype)left);
    return NULL;
}

/**
 * @brief Compile SQL WHERE clause string into filter tree
 * Main entry point for filter compilation
 * 
 * Usage example with B+Tree:
 * ```c
 * // Option 1: Let compiler choose best index
 * int best_idx = filter_best_index_get("l_orderkey >= 1000", "l_orderkey", &meta, &e);
 * struct filter *f = filter_compile("l_orderkey >= 1000 AND l_quantity < 50", &meta, best_idx, &e);
 * 
 * // Option 2: Use specific index
 * struct filter *f = filter_compile("l_orderkey >= 1000", &meta, &e);  
 * 
 * // Option 3: No index optimization
 * struct filter *f = filter_compile("l_comment LIKE '%test%'", &meta, &e);
 * 
 * int cmp = filter_compare(f, row, &e);
 * // cmp: -1 (go left), 0 (match), 1 (go right) for B+Tree navigation
 * ```
 * 
 * @param where WHERE clause string (without "WHERE" keyword)
 * @param meta Table metadata for column lookup and type information
 * @param e Error message output
 * @return struct filter* Compiled filter tree, or NULL if failed
 * @note filter_split() to separate indexable and non-indexable parts
 */
struct filter * filter_compile(const char *where, struct flintdb_meta *meta, char **e) {
    if (!where || where[0] == '\0') return NULL; 
    if (!meta) THROW(e, "meta is NULL");
    
    const char *p = where;
    struct filter *f = parse_expression(&p, meta, e);
    
    // Store index hint in filter (for future optimization)
    // Currently the index is used by the caller (e.g., table_scan) to choose search strategy
    // The filter itself doesn't need to know which index to use during compilation
    // TODO: Add index hint to filter structure if needed for advanced optimizations
    
    return f;

    EXCEPTION:
    return NULL;
}

void filter_free(struct filter *filter) {
    if (!filter) 
        return;
    filter_dealloc((valtype)filter);
}

/**
 * @brief Find the best index for given WHERE and ORDER BY clauses
 * 
 * Selection priority:
 * 1. Indexes that match both WHERE and ORDER BY columns
 * 2. Indexes that match WHERE columns
 * 3. Indexes that match ORDER BY columns
 * 4. Return -1 if no suitable index found
 * 
 * @param where WHERE clause string (without "WHERE" keyword)
 * @param orderby ORDER BY clause string (without "ORDER BY" keyword)
 * @param meta Table metadata for column lookup and type information
 * @param e Error message output
 * @return int Best index to use (0-based), or -1 if none found
 */
int filter_best_index_get(const char *where, const char *orderby, struct flintdb_meta *meta, char **e) {
    struct filter *filter = NULL;

    if (!meta) THROW(e, "meta is NULL");
    
    // No WHERE and no ORDER BY: use primary index if available
    if (!strempty(where) && !strempty(orderby)) {
        return meta->indexes.length > 0 ? 0 : -1;
    }
    
    // Parse WHERE clause to get filter tree
    if (!strempty(where)) {
        filter = filter_compile(where, meta, e);
        if (e && *e) {
            if (filter) filter_free(filter);
            THROW_S(e);
        }
    }
    
    // Parse ORDER BY clause to extract column names
    char orderby_columns[8][256] = {0};
    int orderby_count = 0;
    if (!strempty(orderby)) {
        const char *p = orderby;
        while (*p && orderby_count < 8) {
            skip_whitespace(&p);
            int i = 0;
            while (*p && (isalnum((unsigned char)*p) || *p == '_') && i < 255) {
                orderby_columns[orderby_count][i++] = *p;
                p++;
            }
            if (i > 0) {
                orderby_columns[orderby_count][i] = '\0';
                orderby_count++;
            }
            // skip optional ASC/DESC
            skip_whitespace(&p);
            if (strncasecmp(p, "ASC", 3) == 0 || strncasecmp(p, "DESC", 4) == 0) {
                while (*p && *p != ',') p++;
            }
            if (*p == ',') p++;
        }
    }
    
    int best_index = -1;
    int best_score = 0;
    
    // Evaluate each index
    for (int idx = 0; idx < meta->indexes.length; idx++) {
        struct flintdb_index *index = &meta->indexes.a[idx];
        int score = 0;
        
        // Check WHERE clause compatibility
        int where_match = 0;
        if (filter) {
            where_match = is_indexable(filter, meta, index);
            if (where_match) {
                score += 100; // WHERE match is most important
            }
        }
        
        // Check ORDER BY compatibility
        int orderby_match = 0;
        if (orderby_count > 0 && index->keys.length > 0) {
            // Check if ORDER BY columns match index prefix
            orderby_match = 1;
            int check_count = (orderby_count < index->keys.length) ? orderby_count : index->keys.length;
            for (int i = 0; i < check_count; i++) {
                if (orderby_columns[i][0] == '\0' || strcmp(orderby_columns[i], index->keys.a[i]) != 0) {
                    orderby_match = 0;
                    break;
                }
            }
            if (orderby_match && orderby_count <= index->keys.length) {
                score += 50; // ORDER BY match is secondary
                score += orderby_count; // Prefer indexes matching more ORDER BY columns
            }
        }
        
        // Additional scoring: prefer indexes with fewer total columns (more specific)
        if (score > 0) {
            score += (10 - index->keys.length); // Slight preference for narrower indexes
        }
        
        // Update best index
        if (score > best_score) {
            best_score = score;
            best_index = idx;
        }
    }
    
    if (filter) filter_free(filter);
    return best_index;
    
    EXCEPTION:
    if (filter) filter_free(filter);
    return -1;
}