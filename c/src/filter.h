#ifndef FLINTDB_FILTER_H
#define FLINTDB_FILTER_H

#include "flintdb.h"
#include "types.h"


/**
 * @brief SQL WHERE clause parsing structure
 * 
 * Parsing Grammar:
 * ================
 * expression  ::= primary (AND primary | OR primary)*
 * primary     ::= '(' expression ')' | condition
 * condition   ::= column_name operator value
 * operator    ::= '=' | '<=' | '<' | '>=' | '>' | '<>' | '!=' | 'LIKE'
 * value       ::= quoted_string | NULL | number
 * 
 * Example parsing flow:
 * ---------------------
 * Input: "l_orderkey = 1001 AND l_quantity > 5"
 * 
 * filter_compile
 *   └─> parse_expression
 *         ├─> parse_primary (left)
 *         │     └─> parse_condition: "l_orderkey = 1001"
 *         │           ├─> parse_column_name: "l_orderkey"
 *         │           ├─> parse_operator: "="
 *         │           └─> parse_value: "1001"
 *         │
 *         ├─> logical operator: AND
 *         │
 *         └─> parse_primary (right)
 *               └─> parse_condition: "l_quantity > 5"
 *                     ├─> parse_column_name: "l_quantity"
 *                     ├─> parse_operator: ">"
 *                     └─> parse_value: "5"
 * 
 * Result: FILTER_LOGICAL { AND, [condition1, condition2] }
 */

enum arithmetic_operator {
    BAD_OPERATOR = -1,  // Error indicator for invalid/unsupported operators
    EQUAL = 0,
    LESSER_EQUAL,
    LESSER,
    GREATER_EQUAL,
    GREATER,
    NOT_EQUAL,
    LIKE,
};

enum logical_operator {
    AND = 0,
    OR,
    // IN, // reserved for future
    // NOT_IN, // reserved for future
    // BETWEEN, // reserved for future 
};

enum filter_type {
    FILTER_CONDITION = 0,  // single condition (column op value)
    FILTER_LOGICAL,        // logical operation (AND/OR)
};

struct filter_condition {
    enum arithmetic_operator op;
    int column_index;
    struct flintdb_variant *value;
};

struct filter {
    enum filter_type type;
    union {
        struct filter_condition cond;  // for FILTER_CONDITION
        struct {
            enum logical_operator op;
            struct list *filters;  // list of struct filter*
        } logical;
    } data;
};

struct filter_layers {
    struct filter *first; // indexable filter for B+Tree search
    struct filter *second; // remaining filter for post-filtering
};

struct filter * filter_compile(const char *s, struct flintdb_meta *meta, char **e);
void filter_free(struct filter *filter);
struct filter_layers *filter_split(struct filter *f, struct flintdb_meta *meta, struct flintdb_index *target_index, char **e);
void filter_layers_free(struct filter_layers *layers);

int filter_compare(struct filter *filter, struct flintdb_row *r, char **e);
int filter_best_index_get(const char *where, const char *orderby, struct flintdb_meta *meta, char **e);


/**
 * @brief Limit structure for limiting query results
 * 
 * Example parsing flow:
 * ---------------------
 * Input: "LIMIT 0, 10"
 * Input: "LIMIT 10"
 */

struct limit {
    struct {
        int offset;
        int limit;
        int n;
        int o;
    } priv;
    
    int (*remains)(struct limit *l);
    int (*skip)(struct limit *l);
};

extern FLINTDB_API struct limit NOLIMIT;
FLINTDB_API struct limit maxlimit(int offset, int limit);
FLINTDB_API struct limit limit_parse(const char *s);

#endif // FLINTDB_FILTER_H