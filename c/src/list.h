/**
 * @file list.h
 * @brief  Dynamic array (list) data structure interface
 * @note This data structure is intentionally designed to handle only positive numbers, strings, and pointer types.
 */
#ifndef FLINTDB_LIST_H
#define FLINTDB_LIST_H

#include "types.h"

struct list {
    void **a;       // array of pointers
    int length;     // current length
    int capacity;   // allocated capacity

    void (*free)(struct list *me);
    void (*clear)(struct list *me);
    int (*count)(struct list *me);
    valtype (*get)(struct list *me, int index, char **e);
    int (*add)(struct list *me, valtype item, void (*dealloc)(valtype), char **e);
    int (*remove)(struct list *me, int index);
    int (*index_of)(struct list *me, const void *item, int (*cmpr)(const void *, const void *));
};

struct list * arraylist_new(int capacity);
struct list * arraylist_strings_wrap(int argc, const char **argv, char **e); // wraps string array into list of string pointers, no copy
struct list * arraylist_string_split(const char *string, const char *token, char **e); // splits string by token into list of string pointers, copies
void arraylist_string_dealloc(valtype item); // dealloc function for string items

#endif // FLINTDB_LIST_H
