#include "sql_bind.h"
#include "allocator.h"
#include "runtime.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "internal.h"

static int equals_ic(const char *a, const char *b) {
    if (!a || !b)
        return 0;
    for (; *a && *b; a++, b++) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b))
            return 0;
    }
    return *a == '\0' && *b == '\0';
}

static enum sql_stmt_kind stmt_kind_of(const char *s) {
    if (!s || !*s)
        return SQL_STMT_UNKNOWN;
    if (equals_ic(s, "SELECT"))
        return SQL_STMT_SELECT;
    if (equals_ic(s, "INSERT"))
        return SQL_STMT_INSERT;
    if (equals_ic(s, "REPLACE"))
        return SQL_STMT_REPLACE;
    if (equals_ic(s, "UPDATE"))
        return SQL_STMT_UPDATE;
    if (equals_ic(s, "DELETE"))
        return SQL_STMT_DELETE;
    if (equals_ic(s, "CREATE"))
        return SQL_STMT_CREATE;
    if (equals_ic(s, "DROP"))
        return SQL_STMT_DROP;
    if (equals_ic(s, "ALTER"))
        return SQL_STMT_ALTER;
    if (equals_ic(s, "DESCRIBE") || equals_ic(s, "DESC"))
        return SQL_STMT_DESCRIBE;
    if (equals_ic(s, "META"))
        return SQL_STMT_META;
    if (equals_ic(s, "SHOW"))
        return SQL_STMT_SHOW;
    if (equals_ic(s, "BEGIN"))
        return SQL_STMT_BEGIN;
    if (equals_ic(s, "COMMIT"))
        return SQL_STMT_COMMIT;
    if (equals_ic(s, "ROLLBACK"))
        return SQL_STMT_ROLLBACK;
    return SQL_STMT_UNKNOWN;
}

static enum sql_agg_fn agg_fn_of(const char *name) {
    if (!name || !*name)
        return SQL_AGG_UNKNOWN;
    if (equals_ic(name, "COUNT"))
        return SQL_AGG_COUNT;
    if (equals_ic(name, "SUM"))
        return SQL_AGG_SUM;
    if (equals_ic(name, "AVG"))
        return SQL_AGG_AVG;
    if (equals_ic(name, "MIN"))
        return SQL_AGG_MIN;
    if (equals_ic(name, "MAX"))
        return SQL_AGG_MAX;
    if (equals_ic(name, "FIRST"))
        return SQL_AGG_FIRST;
    if (equals_ic(name, "LAST"))
        return SQL_AGG_LAST;
    if (equals_ic(name, "DISTINCT_COUNT"))
        return SQL_AGG_DISTINCT_COUNT;
    if (equals_ic(name, "DISTINCT_HLL_COUNT") || equals_ic(name, "HLL_COUNT"))
        return SQL_AGG_DISTINCT_HLL_COUNT;
    return SQL_AGG_UNKNOWN;
}

static const char *agg_fn_name(enum sql_agg_fn fn) {
    switch (fn) {
    case SQL_AGG_COUNT:
        return "COUNT";
    case SQL_AGG_SUM:
        return "SUM";
    case SQL_AGG_AVG:
        return "AVG";
    case SQL_AGG_MIN:
        return "MIN";
    case SQL_AGG_MAX:
        return "MAX";
    case SQL_AGG_FIRST:
        return "FIRST";
    case SQL_AGG_LAST:
        return "LAST";
    case SQL_AGG_DISTINCT_COUNT:
        return "DISTINCT_COUNT";
    case SQL_AGG_DISTINCT_HLL_COUNT:
        return "DISTINCT_HLL_COUNT";
    default:
        return "UNKNOWN";
    }
}

/* Drop a trailing alias (and optional AS) from expr into out. */
static void item_body(const char *expr, const char *alias, char *out, size_t cap) {
    s_copy(out, cap, expr);
    trim(out);
    if (!alias || !alias[0])
        return;
    size_t n = strlen(out);
    size_t alen = strlen(alias);
    if (n <= alen)
        return;
    if (strcasecmp(out + n - alen, alias) != 0)
        return;
    char prev = out[n - alen - 1];
    if (prev != ' ' && prev != '\t' && prev != ')')
        return;
    out[n - alen] = '\0';
    trim(out);
    n = strlen(out);
    if (n >= 2) {
        char *p = out + n;
        while (p > out && (p[-1] == ' ' || p[-1] == '\t'))
            p--;
        if (p - out >= 2 && (p[-2] == 'A' || p[-2] == 'a') && (p[-1] == 'S' || p[-1] == 's')) {
            if (p - out == 2 || p[-3] == ' ' || p[-3] == '\t') {
                p[-2] = '\0';
                trim(out);
            }
        }
    }
}

static int parse_select_item(const char *expr, struct sql_select_item *item, char **e) {
    (void)e;
    memset(item, 0, sizeof(*item));
    item->col_idx = -1;
    item->agg = SQL_AGG_NONE;
    s_copy(item->expr, sizeof(item->expr), expr);
    trim(item->expr);

    sql_extract_alias(expr, item->alias, sizeof(item->alias));

    char body[SQL_OBJECT_STRING_LIMIT];
    item_body(expr, item->alias, body, sizeof(body));

    if (strcmp(body, "*") == 0) {
        item->kind = SQL_ITEM_STAR;
        s_copy(item->name, sizeof(item->name), "*");
        return 0;
    }

    const char *open_paren = strchr(body, '(');
    const char *close_paren = strrchr(body, ')');
    if (open_paren && close_paren && close_paren > open_paren) {
        char func_name[64];
        int fname_len = (int)(open_paren - body);
        if (fname_len < 0)
            fname_len = 0;
        if (fname_len >= (int)sizeof(func_name))
            fname_len = (int)sizeof(func_name) - 1;
        memcpy(func_name, body, (size_t)fname_len);
        func_name[fname_len] = '\0';
        trim(func_name);

        int col_len = (int)(close_paren - open_paren - 1);
        char arg[MAX_COLUMN_NAME_LIMIT];
        if (col_len < 0)
            col_len = 0;
        if (col_len >= MAX_COLUMN_NAME_LIMIT)
            col_len = MAX_COLUMN_NAME_LIMIT - 1;
        memcpy(arg, open_paren + 1, (size_t)col_len);
        arg[col_len] = '\0';
        trim(arg);

        item->kind = SQL_ITEM_AGG;
        item->agg = agg_fn_of(func_name);
        s_copy(item->name, sizeof(item->name), arg);
        if (!item->alias[0])
            s_copy(item->alias, sizeof(item->alias), item->expr);
        return 0;
    }

    item->kind = SQL_ITEM_COLUMN;
    s_copy(item->name, sizeof(item->name), body);
    return 0;
}

static int is_count_star_arg(const char *arg) {
    return arg && (strcmp(arg, "*") == 0 || strcmp(arg, "1") == 0 || strcmp(arg, "0") == 0);
}

static enum flintdb_variant_type column_type(const struct flintdb_meta *meta, const char *name) {
    if (!meta || !name)
        return VARIANT_NULL;
    for (int j = 0; j < meta->columns.length; j++) {
        if (strcmp(meta->columns.a[j].name, name) == 0)
            return meta->columns.a[j].type;
    }
    return VARIANT_NULL;
}

static int parse_positive_ordinal(const char *s, int *out) {
    if (!s || !*s || !out)
        return 0;
    const char *p = s;
    while (*p == ' ' || *p == '\t')
        p++;
    if (!*p)
        return 0;
    for (const char *q = p; *q; q++) {
        if (*q < '0' || *q > '9')
            return 0;
    }
    char *end = NULL;
    long v = strtol(p, &end, 10);
    if (!end || *end != '\0' || v < 1 || v > 100000)
        return 0;
    *out = (int)v;
    return 1;
}

static int is_group_key(const struct sql_bound *b, const struct sql_select_item *it) {
    if (!b || !it)
        return 0;
    for (int i = 0; i < b->groupby_count; i++) {
        const char *g = b->groupby_name[i];
        if (it->name[0] && strcmp(g, it->name) == 0)
            return 1;
        if (it->alias[0] && strcmp(g, it->alias) == 0)
            return 1;
        if (it->expr[0] && strcmp(g, it->expr) == 0)
            return 1;
    }
    return 0;
}

static int resolve_select_ordinals(struct sql_bound *b, char **e) {
    for (int i = 0; i < b->groupby_count; i++) {
        int ord = 0;
        if (!parse_positive_ordinal(b->groupby_name[i], &ord))
            continue;
        if (b->is_star)
            THROW(e, "GROUP BY %d is not supported with SELECT *", ord);
        if (ord > b->item_count)
            THROW(e, "GROUP BY %d is out of range (SELECT has %d item(s))", ord, b->item_count);
        const struct sql_select_item *it = &b->items[ord - 1];
        if (it->kind == SQL_ITEM_STAR)
            THROW(e, "GROUP BY %d is not supported with SELECT *", ord);
        if (it->kind != SQL_ITEM_COLUMN)
            THROW(e, "GROUP BY %d refers to an aggregate, not a column", ord);
        s_copy(b->groupby_name[i], sizeof(b->groupby_name[i]), it->name);
    }
    for (int i = 0; i < b->order_count; i++) {
        int ord = 0;
        if (!parse_positive_ordinal(b->order[i].name, &ord))
            continue;
        if (b->is_star)
            continue; /* resolved later against table meta */
        if (ord > b->item_count)
            THROW(e, "ORDER BY %d is out of range (SELECT has %d item(s))", ord, b->item_count);
        s_copy(b->order[i].name, sizeof(b->order[i].name), sql_select_item_label(&b->items[ord - 1]));
    }
    return 0;
EXCEPTION:
    return -1;
}

const char *sql_select_item_label(const struct sql_select_item *it) {
    if (!it)
        return "";
    if (it->alias[0])
        return it->alias;
    if (it->kind == SQL_ITEM_STAR)
        return "*";
    return it->name;
}

struct sql_bound *sql_bound_new(const struct flintdb_sql *q, char **e) {
    struct sql_bound *b = NULL;
    if (!q)
        THROW(e, "SQL query is NULL");

    b = (struct sql_bound *)CALLOC(1, sizeof(struct sql_bound));
    if (!b)
        THROW(e, ERR_OUT_OF_MEMORY);

    b->stmt = stmt_kind_of(q->statement);
    b->distinct = q->distinct;
    b->has_into = !strempty(q->into);
    b->has_from = !strempty(q->from);
    b->has_where = !strempty(q->where);
    b->has_groupby = !strempty(q->groupby);
    b->has_having = !strempty(q->having);
    b->has_orderby = !strempty(q->orderby);
    b->has_limit = !strempty(q->limit);
    b->is_show_tables = (b->stmt == SQL_STMT_SHOW && q->object && strncasecmp(q->object, "TABLES", 6) == 0);
    b->limit = b->has_limit ? limit_parse(q->limit) : NOLIMIT;

    if (q->columns.length > 0) {
        b->items = (struct sql_select_item *)CALLOC((size_t)q->columns.length, sizeof(struct sql_select_item));
        if (!b->items)
            THROW(e, ERR_OUT_OF_MEMORY);
        b->item_count = q->columns.length;
        for (int i = 0; i < q->columns.length; i++) {
            if (parse_select_item(q->columns.name[i], &b->items[i], e) != 0)
                THROW_S(e);
            if (b->items[i].kind == SQL_ITEM_AGG)
                b->has_aggregate = 1;
        }
    }

    b->is_star = (b->item_count == 1 && b->items && b->items[0].kind == SQL_ITEM_STAR);

    b->groupby_count = sql_parse_groupby_columns(q->groupby, b->groupby_name);
    for (int i = 0; i < b->groupby_count; i++)
        b->groupby_idx[i] = -1;

    if (b->has_orderby) {
        char cols[MAX_COLUMNS_LIMIT][MAX_COLUMN_NAME_LIMIT];
        i8 desc[MAX_COLUMNS_LIMIT];
        int n = 0;
        sql_parse_orderby_clause(q->orderby, cols, desc, &n);
        b->order_count = n;
        for (int i = 0; i < n; i++) {
            s_copy(b->order[i].name, sizeof(b->order[i].name), cols[i]);
            b->order[i].descending = desc[i];
            b->order[i].col_idx = -1;
        }
    }

    b->is_fast_count = 0;
    if (b->stmt == SQL_STMT_SELECT && b->item_count == 1 && b->items &&
        b->items[0].kind == SQL_ITEM_AGG && b->items[0].agg == SQL_AGG_COUNT &&
        is_count_star_arg(b->items[0].name) && !b->has_where && !b->has_groupby &&
        !b->has_orderby && !b->distinct) {
        b->is_fast_count = 1;
    }

    if (resolve_select_ordinals(b, e) != 0)
        THROW_S(e);

    return b;

EXCEPTION:
    sql_bound_free(b);
    return NULL;
}

void sql_bound_free(struct sql_bound *b) {
    if (!b)
        return;
    if (b->items)
        FREE(b->items);
    FREE(b);
}

int sql_bound_resolve(struct sql_bound *b, const struct flintdb_meta *meta, char **e) {
    if (!b)
        THROW(e, "bound query is NULL");
    if (!meta)
        return 0;

    for (int i = 0; i < b->item_count; i++) {
        struct sql_select_item *it = &b->items[i];
        it->col_idx = -1;
        if (it->kind != SQL_ITEM_COLUMN)
            continue;
        int idx = flintdb_column_at((struct flintdb_meta *)meta, it->name);
        if (idx < 0)
            THROW(e, "Column not found: %s", it->name);
        it->col_idx = idx;
    }

    for (int i = 0; i < b->groupby_count; i++) {
        b->groupby_idx[i] = flintdb_column_at((struct flintdb_meta *)meta, b->groupby_name[i]);
    }
    return 0;

EXCEPTION:
    return -1;
}

int sql_bound_resolve_order(struct sql_bound *b, const struct flintdb_meta *meta, char **e) {
    if (!b)
        THROW(e, "bound query is NULL");
    if (!b->has_orderby)
        return 0;
    if (!meta)
        THROW(e, "ORDER BY requires result metadata");
    if (b->order_count <= 0)
        THROW(e, "Failed to parse ORDER BY clause");
    for (int i = 0; i < b->order_count; i++) {
        int ord = 0;
        if (parse_positive_ordinal(b->order[i].name, &ord)) {
            if (ord > meta->columns.length)
                THROW(e, "ORDER BY %d is out of range (%d column(s))", ord, meta->columns.length);
            b->order[i].col_idx = ord - 1;
            continue;
        }
        int idx = flintdb_column_at((struct flintdb_meta *)meta, b->order[i].name);
        if (idx < 0)
            THROW(e, "ORDER BY column not found: %s", b->order[i].name);
        b->order[i].col_idx = idx;
    }
    return 0;

EXCEPTION:
    return -1;
}

static struct flintdb_aggregate_func *agg_func_new(enum sql_agg_fn fn, const char *col, const char *alias, char **e) {
    struct flintdb_aggregate_condition cond = {0};
    switch (fn) {
    case SQL_AGG_COUNT:
        return flintdb_func_count(col, alias, VARIANT_NULL, cond, e);
    case SQL_AGG_SUM:
        return flintdb_func_sum(col, alias, VARIANT_NULL, cond, e);
    case SQL_AGG_AVG:
        return flintdb_func_avg(col, alias, VARIANT_NULL, cond, e);
    case SQL_AGG_MIN:
        return flintdb_func_min(col, alias, VARIANT_NULL, cond, e);
    case SQL_AGG_MAX:
        return flintdb_func_max(col, alias, VARIANT_NULL, cond, e);
    case SQL_AGG_FIRST:
        return flintdb_func_first(col, alias, VARIANT_NULL, cond, e);
    case SQL_AGG_LAST:
        return flintdb_func_last(col, alias, VARIANT_NULL, cond, e);
    case SQL_AGG_DISTINCT_COUNT:
        return flintdb_func_distinct_count(col, alias, VARIANT_NULL, cond, e);
    case SQL_AGG_DISTINCT_HLL_COUNT:
        return flintdb_func_distinct_hll_count(col, alias, VARIANT_NULL, cond, e);
    default:
        THROW(e, ERR_UNKNOWN_AGGREGATE_FUNCTION ": %s", agg_fn_name(fn));
    }
EXCEPTION:
    return NULL;
}

int sql_bound_aggregates(const struct sql_bound *b, const struct flintdb_meta *meta,
                         struct flintdb_aggregate_groupby ***groupbys_out, int *groupby_n,
                         struct flintdb_aggregate_func ***funcs_out, int *func_n, char **e) {
    struct flintdb_aggregate_groupby **groupbys = NULL;
    struct flintdb_aggregate_func **funcs = NULL;
    int aggr_count = 0;

    if (!b || !groupbys_out || !groupby_n || !funcs_out || !func_n)
        THROW(e, "invalid arguments");
    if (b->is_star)
        THROW(e, "SELECT * not supported with GROUP BY or aggregate functions");

    *groupbys_out = NULL;
    *funcs_out = NULL;
    *groupby_n = 0;
    *func_n = 0;

    if (b->groupby_count > 0) {
        groupbys = (struct flintdb_aggregate_groupby **)CALLOC((size_t)b->groupby_count,
                                                              sizeof(struct flintdb_aggregate_groupby *));
        if (!groupbys)
            THROW(e, ERR_OUT_OF_MEMORY " allocating groupbys");
        for (int i = 0; i < b->groupby_count; i++) {
            enum flintdb_variant_type col_type = column_type(meta, b->groupby_name[i]);
            if (col_type == VARIANT_NULL && meta == NULL)
                col_type = VARIANT_STRING;
            groupbys[i] = groupby_new(b->groupby_name[i], b->groupby_name[i], col_type, e);
            if (e && *e)
                THROW_S(e);
        }
    }

    funcs = (struct flintdb_aggregate_func **)CALLOC(b->item_count > 0 ? (size_t)b->item_count : 1,
                                                    sizeof(struct flintdb_aggregate_func *));
    if (!funcs)
        THROW(e, ERR_OUT_OF_MEMORY " allocating funcs");

    for (int i = 0; i < b->item_count; i++) {
        const struct sql_select_item *it = &b->items[i];
        if (it->kind == SQL_ITEM_COLUMN && is_group_key(b, it))
            continue;
        if (it->kind == SQL_ITEM_STAR)
            THROW(e, "SELECT * not supported with GROUP BY or aggregate functions");
        if (it->kind != SQL_ITEM_AGG)
            THROW(e, "Malformed aggregate expression: %s", it->expr);
        if (it->agg == SQL_AGG_UNKNOWN)
            THROW(e, ERR_UNKNOWN_AGGREGATE_FUNCTION ": %s", it->expr);

        funcs[aggr_count] = agg_func_new(it->agg, it->name, sql_select_item_label(it), e);
        if (e && *e)
            THROW_S(e);
        aggr_count++;
    }

    if (aggr_count == 0)
        THROW(e, "No aggregate functions found");

    *groupbys_out = groupbys;
    *groupby_n = b->groupby_count;
    *funcs_out = funcs;
    *func_n = aggr_count;
    return 0;

EXCEPTION:
    if (groupbys) {
        int n = b ? b->groupby_count : 0;
        for (int i = 0; i < n; i++) {
            if (groupbys[i] && groupbys[i]->free)
                groupbys[i]->free(groupbys[i]);
        }
        FREE(groupbys);
    }
    if (funcs) {
        for (int i = 0; i < aggr_count; i++) {
            if (funcs[i] && funcs[i]->free)
                funcs[i]->free(funcs[i]);
        }
        FREE(funcs);
    }
    return -1;
}
