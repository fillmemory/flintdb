#ifndef FLINTDB_SQL_BIND_H
#define FLINTDB_SQL_BIND_H

#include "sql.h"
#include "filter.h"

/**
 * Typed view of a parsed SQL statement.
 *
 * sql.c still extracts clauses as strings; this layer interprets those
 * strings once so sql_exec.c does not re-parse COUNT(, GROUP BY, ORDER BY, etc.
 */

enum sql_stmt_kind {
    SQL_STMT_UNKNOWN = 0,
    SQL_STMT_SELECT,
    SQL_STMT_INSERT,
    SQL_STMT_REPLACE,
    SQL_STMT_UPDATE,
    SQL_STMT_DELETE,
    SQL_STMT_CREATE,
    SQL_STMT_DROP,
    SQL_STMT_ALTER,
    SQL_STMT_DESCRIBE,
    SQL_STMT_META,
    SQL_STMT_SHOW,
    SQL_STMT_BEGIN,
    SQL_STMT_COMMIT,
    SQL_STMT_ROLLBACK,
};

enum sql_item_kind {
    SQL_ITEM_STAR = 0,
    SQL_ITEM_COLUMN,
    SQL_ITEM_AGG,
};

enum sql_agg_fn {
    SQL_AGG_NONE = 0,
    SQL_AGG_COUNT,
    SQL_AGG_SUM,
    SQL_AGG_AVG,
    SQL_AGG_MIN,
    SQL_AGG_MAX,
    SQL_AGG_FIRST,
    SQL_AGG_LAST,
    SQL_AGG_DISTINCT_COUNT,
    SQL_AGG_DISTINCT_HLL_COUNT,
    SQL_AGG_UNKNOWN,
};

struct sql_select_item {
    enum sql_item_kind kind;
    enum sql_agg_fn agg;
    char expr[SQL_OBJECT_STRING_LIMIT];   /* original SELECT text */
    char name[MAX_COLUMN_NAME_LIMIT];     /* column or aggregate argument */
    char alias[MAX_COLUMN_NAME_LIMIT];    /* empty if none */
    int col_idx;                          /* resolved against meta, else -1 */
};

struct sql_order_item {
    char name[MAX_COLUMN_NAME_LIMIT];
    i8 descending;
    int col_idx; /* resolved against the meta passed to sql_bound_resolve_order */
};

struct sql_bound {
    enum sql_stmt_kind stmt;
    i8 distinct;
    i8 has_into;
    i8 has_from;
    i8 has_where;
    i8 has_groupby;
    i8 has_having;
    i8 has_orderby;
    i8 has_limit;
    i8 is_show_tables;
    i8 is_star;
    i8 has_aggregate;
    i8 is_fast_count; /* COUNT(*)/COUNT(1)/COUNT(0), no where/group/order/distinct */

    int item_count;
    struct sql_select_item *items;

    int groupby_count;
    char groupby_name[MAX_COLUMNS_LIMIT][MAX_COLUMN_NAME_LIMIT];
    int groupby_idx[MAX_COLUMNS_LIMIT];

    int order_count;
    struct sql_order_item order[MAX_COLUMNS_LIMIT];

    struct limit limit;
};

/* GROUP BY n / ORDER BY n (1-based SELECT-list ordinals) are rewritten in sql_bound_new. */
struct sql_bound *sql_bound_new(const struct flintdb_sql *q, char **e);
void sql_bound_free(struct sql_bound *b);

/* Fill col_idx / groupby_idx from table or file meta. */
int sql_bound_resolve(struct sql_bound *b, const struct flintdb_meta *meta, char **e);

/* Fill order[].col_idx from result (or source) meta. */
int sql_bound_resolve_order(struct sql_bound *b, const struct flintdb_meta *meta, char **e);

/* Output column label: alias, else expression (agg), else column name. */
const char *sql_select_item_label(const struct sql_select_item *it);

/*
 * Build groupby + aggregate_func arrays for aggregate_new().
 * On success the arrays are owned by the caller (then by aggregate_new).
 */
int sql_bound_aggregates(const struct sql_bound *b, const struct flintdb_meta *meta,
                         struct flintdb_aggregate_groupby ***groupbys_out, int *groupby_n,
                         struct flintdb_aggregate_func ***funcs_out, int *func_n, char **e);

#endif /* FLINTDB_SQL_BIND_H */
