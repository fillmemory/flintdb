

#include "flintdb.h"
#include "allocator.h"
#include "list.h"
#include "runtime.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>

struct entry {
    valtype item;
    void (*dealloc)(valtype);
};

static valtype arraylist_get(struct list *me, int index, char **e) {
    if (!me) THROW(e, "List is NULL");
    if (index < 0 || index >= me->length) THROW(e, "Invalid index[%d]", index);
    struct entry *ent = (struct entry *)me->a[index];
    return ent ? ent->item : VALUETYPE_NULL;

    EXCEPTION:
    return VALUETYPE_NULL;
}

static int arraylist_add(struct list *me, valtype item, void (*dealloc)(valtype), char **e) {
    void **new_a = NULL;
    struct entry *ent = NULL;
    if (!me) THROW(e, "List is NULL");

    if (me->length >= me->capacity) {
        // Resize (ensure capacity grows from a sane minimum and avoid zero/overflow)
        int new_capacity = me->capacity > 0 ? me->capacity * 2 : 8;
        if (new_capacity < me->length + 1) {
            // handle potential integer overflow or pathological cases
            new_capacity = me->length + 1;
        }
        new_a = (void **)REALLOC(me->a, sizeof(void *) * new_capacity);
        if (!new_a) THROW(e, "Out of memory");
        me->a = new_a;
        me->capacity = new_capacity;
    }

    ent = (struct entry *)CALLOC(1, sizeof(struct entry));
    if (!ent) THROW(e, "Out of memory");

    ent->item = item;
    ent->dealloc = dealloc;
    me->a[me->length] = ent;
    me->length++;
    return me->length - 1;

    EXCEPTION:
    if (new_a) FREE(new_a);
    if (ent) FREE(ent);
    return -1;
}

static int arraylist_remove(struct list *me, int index) {
    if (!me || index < 0 || index >= me->length) {
        return -1;
    }
    struct entry *ent = (struct entry *)me->a[index];
    if (ent) {
        if (ent->dealloc) {
            ent->dealloc(ent->item);
        }
        FREE(ent);
    }
    // Shift elements left
    for (int i = index; i < me->length - 1; i++) {
        me->a[i] = me->a[i + 1];
    }
    me->a[me->length - 1] = NULL;
    me->length--;
    return 0;
}

static void arraylist_clear(struct list *me) {
    if (!me || !me->a) return;
    for (int i = 0; i < me->length; i++) {
        if (me->a[i]) {
            struct entry *ent = (struct entry *)me->a[i];
            if (ent->dealloc) {
                ent->dealloc(ent->item);
            }
            FREE(ent);
            me->a[i] = NULL;
        }
    }
    me->length = 0;
}

static int arraylist_index_of(struct list *me, const void *item, int (*cmpr)(const void *, const void *)) {
    if (!me || !cmpr) return -1;
    for (int i = 0; i < me->length; i++) {
        struct entry *ent = (struct entry *)me->a[i];
        if (ent && cmpr((const void *)ent->item, item) == 0) {
            return i;
        }
    }
    return -1;
}

static int arraylist_count(struct list *me) {
    if (!me) return 0;
    return me->length;
}

static void arraylist_free(struct list *me) {
    if (!me) return;
    arraylist_clear(me);
    FREE(me->a);
    FREE(me);
}

struct list * arraylist_new(int capacity) {
    struct list *me = (struct list *)CALLOC(1, sizeof(struct list));
    if (!me) return NULL;

    // Normalize capacity to a sane minimum to avoid zero/negative sizes
    if (capacity < 1) capacity = 8;
    me->a = (void **)CALLOC(1, sizeof(void *) * capacity);
    if (!me->a) {
        FREE(me);
        return NULL;
    }

    me->length = 0;
    me->capacity = capacity;

    me->free = arraylist_free;
    me->clear = arraylist_clear;
    me->count = arraylist_count;
    me->get = arraylist_get;
    me->add = arraylist_add;
    me->remove = arraylist_remove;
    me->index_of = arraylist_index_of;

    return me;
}


static int arraylist_FLINTDB_RDONLY_add(struct list *me, valtype item, void (*dealloc)(valtype), char **e) {
    if (!me) THROW(e, "List is NULL");
    THROW(e, "List is read-only");
    return -1;

    EXCEPTION:
    return -1;
}

static int arraylist_FLINTDB_RDONLY_remove(struct list *me, int index) {
    if (!me) return -1;
    return -1; // read-only
}

static void arraylist_FLINTDB_RDONLY_clear(struct list *me) {
}

struct list * arraylist_strings_wrap(int argc, const char **argv, char **e) {
    struct list *me = arraylist_new(argc > 0 ? argc : 0);
    if (!me) THROW(e, "Out of memory");

    for (int i = 0; i < argc; i++) {
        arraylist_add(me, (valtype)argv[i], NULL, e);
        if (e && *e) THROW_S(e);
    }

    me->add = arraylist_FLINTDB_RDONLY_add; // make read-only
    me->remove = arraylist_FLINTDB_RDONLY_remove; // make read-only
    me->clear = arraylist_FLINTDB_RDONLY_clear; 

    return me;

    EXCEPTION:
    if (me) me->free(me);
    return NULL;
}

struct list * arraylist_string_split(const char *string, const char *token, char **e) {
    struct list *me = NULL;
    char *copy = NULL;

    if (!string) THROW(e, "Input string is NULL");
    if (!token) THROW(e, "Token is NULL");

    me = arraylist_new(16);
    if (!me) THROW(e, "Out of memory");

    size_t token_len = strlen(token);

    // Fast path: empty token -> return whole string as single item
    if (token_len == 0) {
        char *tok_copy = STRDUP(string);
        if (!tok_copy) THROW(e, "Out of memory");
        arraylist_add(me, (valtype)tok_copy, arraylist_string_dealloc, e);
        if (e && *e) THROW_S(e);
        return me;
    }

    // Single-character delimiter: keep existing strtok_r semantics (collapses repeats)
    if (token_len == 1) {
        copy = STRDUP(string);
        if (!copy) THROW(e, "Out of memory");

        char *saveptr = NULL;
        char *tok = strtok_r(copy, token, &saveptr);
        while (tok) {
            if (*tok != '\0') { // skip empty segments to mimic strtok behavior
                char *tok_copy = STRDUP(tok);
                if (!tok_copy) THROW(e, "Out of memory");
                arraylist_add(me, (valtype)tok_copy, arraylist_string_dealloc, e);
                if (e && *e) THROW_S(e);
            }
            tok = strtok_r(NULL, token, &saveptr);
        }

        FREE(copy);
        copy = NULL;
        return me;
    }

    // Multi-character delimiter: split using strstr, skip empty segments (similar to strtok)
    const char *start = string;
    const char *pos = strstr(start, token);
    int found_any = 0;
    while (pos) {
        size_t seg_len = (size_t)(pos - start);
        if (seg_len > 0) {
            char *seg = (char *)MALLOC(seg_len + 1);
            if (!seg) THROW(e, "Out of memory");
            memcpy(seg, start, seg_len);
            seg[seg_len] = '\0';
            arraylist_add(me, (valtype)seg, arraylist_string_dealloc, e);
            if (e && *e) THROW_S(e);
            found_any = 1;
        }
        // advance past the token
        start = pos + token_len;
        pos = strstr(start, token);
    }

    // Remainder after the last token
    if (*start != '\0') {
        char *seg = STRDUP(start);
        if (!seg) THROW(e, "Out of memory");
        arraylist_add(me, (valtype)seg, arraylist_string_dealloc, e);
        if (e && *e) THROW_S(e);
        found_any = 1;
    }

    // If token wasn't found at all and string wasn't empty, return the whole string
    if (!found_any && *string != '\0') {
        char *seg = STRDUP(string);
        if (!seg) THROW(e, "Out of memory");
        arraylist_add(me, (valtype)seg, arraylist_string_dealloc, e);
        if (e && *e) THROW_S(e);
    }

    return me;

    EXCEPTION:
    if (copy) FREE(copy);
    if (me) me->free(me);
    return NULL;
}

void arraylist_string_dealloc(valtype item) {
    char *s = (char *)item;
    if (s) FREE(s);
}
