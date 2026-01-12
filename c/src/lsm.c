#include "lsm.h"
#include "allocator.h"
#include "rbtree.h"
#include "runtime.h"
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <sys/stat.h>

#define SSTABLE_SUFFIX ".sst"
#define MAX_SSTABLES 1024
#define COMPACTION_THRESHOLD 10

struct lsm_entry {
    i64 key;
    i64 offset;
};

static int lsm_compact(struct lsm_tree *me, char **e);

struct lsm_tree {
    char path[PATH_MAX];
    struct rbtree *memtable; // TODO: SkipList
    size_t mem_max_entries;
    enum flintdb_open_mode mode;

    char *sst_paths[MAX_SSTABLES];
    FILE *sst_files[MAX_SSTABLES];
    i64 sst_counts[MAX_SSTABLES];
    int sst_count;
};

static int sst_scan(struct lsm_tree *me, char **e) {
    char dir_path[PATH_MAX];
    getdir(me->path, dir_path);
    if (strempty(dir_path))
        strcpy(dir_path, ".");

    DIR *d = opendir(dir_path);
    if (!d)
        return 0;

    struct dirent *dir;
    char name_prefix[PATH_MAX];
    getname(me->path, name_prefix);

    while ((dir = readdir(d)) != NULL) {
        if (suffix(dir->d_name, SSTABLE_SUFFIX) && strstr(dir->d_name, name_prefix) == dir->d_name) {
            if (me->sst_count < MAX_SSTABLES) {
                char full_path[PATH_MAX];
                snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, dir->d_name);
                me->sst_paths[me->sst_count] = strdup(full_path);
                me->sst_files[me->sst_count] = fopen(full_path, "rb");
                if (me->sst_files[me->sst_count]) {
                    fread(&me->sst_counts[me->sst_count], sizeof(i64), 1, me->sst_files[me->sst_count]);
                }
                me->sst_count++;
            }
        }
    }
    closedir(d);

    // Sort SSTables by path descending
    for (int i = 0; i < me->sst_count - 1; i++) {
        for (int j = i + 1; j < me->sst_count; j++) {
            if (strcmp(me->sst_paths[i], me->sst_paths[j]) < 0) {
                char *tmp_p = me->sst_paths[i];
                me->sst_paths[i] = me->sst_paths[j];
                me->sst_paths[j] = tmp_p;

                FILE *tmp_f = me->sst_files[i];
                me->sst_files[i] = me->sst_files[j];
                me->sst_files[j] = tmp_f;

                i64 tmp_c = me->sst_counts[i];
                me->sst_counts[i] = me->sst_counts[j];
                me->sst_counts[j] = tmp_c;
            }
        }
    }

    return 0;
}

struct lsm_tree *lsm_open(const char *path, enum flintdb_open_mode mode, size_t mem_max_bytes, char **e) {
    struct lsm_tree *me = CALLOC(1, sizeof(struct lsm_tree));
    if (!me)
        THROW(e, "LSM: Failed to allocate memory");

    strncpy_safe(me->path, path, sizeof(me->path));
    me->mode = mode;
    me->memtable = rbtree_new(NULL); // use default i64 compare

    // Estimate max entries based on bytes. Each entry in rbtree is ~64 bytes (node + pool logic)
    // For simplicity, let's treat mem_max_bytes as entry count limit * 16 if it's small,
    // or divide by node size if it's large.
    if (mem_max_bytes < 1000 * 1000) {
        me->mem_max_entries = mem_max_bytes / 16;
    } else {
        me->mem_max_entries = mem_max_bytes / (sizeof(struct rbnode) + 16);
    }

    if (me->mem_max_entries < 100)
        me->mem_max_entries = 1000;

    sst_scan(me, e);

    return me;

EXCEPTION:
    if (me)
        lsm_close(me);
    return NULL;
}

static void rb_flush_walk(struct rbnode *node, FILE *fp) {
    if (!node)
        return;
    rb_flush_walk(node->left, fp);
    struct lsm_entry entry = {.key = node->key, .offset = node->val};
    fwrite(&entry, sizeof(entry), 1, fp);
    rb_flush_walk(node->right, fp);
}

static int lsm_flush(struct lsm_tree *me, char **e) {
    i64 count = me->memtable->count_get(me->memtable);
    if (count == 0)
        return 0;

    char sst_path[PATH_MAX];
    int next_id = 1;
    if (me->sst_count > 0) {
        // Parse ID from newest SSTable name: prefix.00005.sst
        const char *p = strstr(me->sst_paths[0], SSTABLE_SUFFIX);
        if (p && p - me->sst_paths[0] > 6) { // Ensure there's enough space for "00001"
            // Find the start of the number (5 chars before suffix)
            const char *num_start = p - 5;
            // Check if it's actually a number
            if (num_start >= me->sst_paths[0] && isdigit(num_start[0])) {
                next_id = atoi(num_start) + 1;
            }
        }
    }

    snprintf(sst_path, sizeof(sst_path), "%s.%05d%s", me->path, next_id, SSTABLE_SUFFIX);
    DEBUG("LSM: Flushing %lld entries to %s", count, sst_path);

    FILE *fp = fopen(sst_path, "wb");
    if (!fp)
        THROW(e, "LSM: Failed to open SSTable for writing: %s", strerror(errno));

    // Header: count
    fwrite(&count, sizeof(count), 1, fp);
    // Data: in-order walk of rbtree
    rb_flush_walk(me->memtable->root, fp);

    // Re-open for reading
    FILE *read_fp = freopen(sst_path, "rb", fp);
    if (!read_fp) {
        fclose(fp); // Close the original fp if freopen fails
        THROW(e, "LSM: Failed to re-open SSTable for reading: %s", strerror(errno));
    }

    // Update SSTable list (insert at beginning so newest is always first)
    if (me->sst_count < MAX_SSTABLES) {
        memmove(&me->sst_paths[1], &me->sst_paths[0], sizeof(char *) * me->sst_count);
        memmove(&me->sst_files[1], &me->sst_files[0], sizeof(FILE *) * me->sst_count);
        memmove(&me->sst_counts[1], &me->sst_counts[0], sizeof(i64) * me->sst_count);
        me->sst_paths[0] = strdup(sst_path);
        me->sst_files[0] = read_fp;
        me->sst_counts[0] = count;
        me->sst_count++;
    } else {
        fclose(read_fp); // Close the file if it's not added to the list
    }

    me->memtable->clear(me->memtable);

    // Check if we need compaction
    if (me->sst_count >= COMPACTION_THRESHOLD) {
        lsm_compact(me, e);
    }

    return 0;

EXCEPTION:
    return -1;
}

static int lsm_compact(struct lsm_tree *me, char **e) {
    if (me->sst_count < 2)
        return 0;

    DEBUG("LSM: Compacting %d SSTables", me->sst_count);

    // In a production LSM, we'd use a priority queue to merge multiple sorted streams.
    // For this implementation, let's just merge all existing SSTables into one mega-SSTable
    // with the ID of the newest one.

    char merged_path[PATH_MAX];
    snprintf(merged_path, sizeof(merged_path), "%s.merged%s", me->path, SSTABLE_SUFFIX);

    FILE *out = fopen(merged_path, "wb");
    if (!out)
        THROW(e, "LSM: Compaction failed to open target: %s", strerror(errno));

    // Placeholder for total count, will update later
    i64 total_count = 0;
    fwrite(&total_count, sizeof(total_count), 1, out);

    // Merge logic: newest keys win. Since SSTables are newest-first in me->sst_files,
    // we should actually process them oldest to newest if we want newer ones to overwrite.
    // However, binary search in SSTables already finds the newest first.
    // A simple merge will just append everything, but we should handle duplicate keys.

    // For a truly simple but effective compaction:
    // 1. Read all entries into a large buffer (or use a K-way merge)
    // 2. Sort/Deduplicate
    // 3. Write

    // Better: K-way merge.
    FILE *inputs[MAX_SSTABLES];
    i64 counts[MAX_SSTABLES];
    i64 positions[MAX_SSTABLES];
    struct lsm_entry entries[MAX_SSTABLES];
    int active_inputs = 0;

    for (int i = 0; i < me->sst_count; i++) {
        inputs[active_inputs] = fopen(me->sst_paths[i], "rb"); // Use sst_paths here
        if (inputs[active_inputs]) {
            if (fread(&counts[active_inputs], sizeof(i64), 1, inputs[active_inputs]) == 1) {
                if (fread(&entries[active_inputs], sizeof(struct lsm_entry), 1, inputs[active_inputs]) == 1) {
                    positions[active_inputs] = 0;
                    active_inputs++;
                } else {
                    fclose(inputs[active_inputs]);
                }
            } else {
                fclose(inputs[active_inputs]);
            }
        }
    }

    i64 last_key = -1;
    while (active_inputs > 0) {
        // Find smallest key among active inputs.
        // NOTE: if multiple inputs have same smallest key, pick from LOWEST index
        // (because sst_files is NEWEST-first, me->sst_files[0] Is newest).
        int best = -1;
        for (int i = 0; i < active_inputs; i++) {
            if (best == -1 || entries[i].key < entries[best].key) {
                best = i;
            } else if (entries[i].key == entries[best].key) {
                // Duplicate key: newer one wins. sst_files[0] is newest.
                // If i < best, i is newer.
                if (i < best)
                    best = i;

                // Advance the losing input
                positions[best]++;
                if (positions[best] >= counts[best]) {
                    fclose(inputs[best]);
                    active_inputs--;
                    if (best < active_inputs) {
                        inputs[best] = inputs[active_inputs];
                        counts[best] = counts[active_inputs];
                        positions[best] = positions[active_inputs];
                        entries[best] = entries[active_inputs];
                    }
                    // Re-evaluate current best because indices shifted
                    best = i;
                } else {
                    fread(&entries[best], sizeof(struct lsm_entry), 1, inputs[best]);
                }
            }
        }

        if (best == -1)
            break;

        // Write best entry if not same as last (should be handled by duplicate logic above though)
        if (entries[best].key != last_key) {
            if (entries[best].offset != -2) { // Skip tombstones
                fwrite(&entries[best], sizeof(struct lsm_entry), 1, out);
                total_count++;
            }
            last_key = entries[best].key;
        }

        // Advance best input
        positions[best]++;
        if (positions[best] >= counts[best]) {
            fclose(inputs[best]);
            active_inputs--;
            if (best < active_inputs) {
                inputs[best] = inputs[active_inputs];
                counts[best] = counts[active_inputs];
                positions[best] = positions[active_inputs];
                entries[best] = entries[active_inputs];
            }
        } else {
            fread(&entries[best], sizeof(struct lsm_entry), 1, inputs[best]);
        }
    }

    fseek(out, 0, SEEK_SET);
    fwrite(&total_count, sizeof(total_count), 1, out);
    fclose(out);

    // Replace old SSTables with the merged one
    for (int i = 0; i < me->sst_count; i++) {
        fclose(me->sst_files[i]); // Close the file pointer
        remove(me->sst_paths[i]); // Remove the file from disk
        free(me->sst_paths[i]);   // Free the path string
    }

    // Final merged filename
    char final_sst[PATH_MAX];
    snprintf(final_sst, sizeof(final_sst), "%s.00001%s", me->path, SSTABLE_SUFFIX);
    rename(merged_path, final_sst);

    me->sst_paths[0] = strdup(final_sst);
    me->sst_files[0] = fopen(final_sst, "rb");
    if (me->sst_files[0]) {
        fread(&me->sst_counts[0], sizeof(i64), 1, me->sst_files[0]);
    }
    me->sst_count = 1;

    return 0;

EXCEPTION:
    if (out)
        fclose(out);
    return -1;
}

int lsm_put(struct lsm_tree *me, i64 key, i64 offset, char **e) {
    if (me->memtable->count_get(me->memtable) >= (i64)me->mem_max_entries) {
        if (lsm_flush(me, e) < 0)
            return -1;
    }

    me->memtable->put(me->memtable, key, offset, NULL);
    return 0;
}

i64 lsm_get(struct lsm_tree *me, i64 key, char **e) {
    // 1. Check MemTable
    struct rbnode *node = me->memtable->get(me->memtable, key);
    if (node)
        return node->val;

    // 2. Check SSTables (newest first)
    for (int i = 0; i < me->sst_count; i++) {
        FILE *fp = me->sst_files[i];
        if (!fp)
            continue;

        i64 count = me->sst_counts[i];
        if (count == 0)
            continue;

        // Binary search in file
        i64 low = 0;
        i64 high = count - 1;
        i64 result = -1;

        while (low <= high) {
            i64 mid = low + (high - low) / 2;
            fseek(fp, sizeof(i64) + mid * sizeof(struct lsm_entry), SEEK_SET);
            struct lsm_entry entry;
            if (fread(&entry, sizeof(entry), 1, fp) != 1)
                break;

            if (entry.key == key) {
                result = entry.offset;
                break;
            }
            if (entry.key < key)
                low = mid + 1;
            else
                high = mid - 1;
        }

        if (result != -1)
            return result;
    }

    return -1;
}

int lsm_delete(struct lsm_tree *me, i64 key, char **e) {
    // LSM deletion is usually a tombstone. For our i64->i64 mapping, we can use offset=-2 as tombstone.
    return lsm_put(me, key, -2, e);
}

void lsm_close(struct lsm_tree *me) {
    if (!me)
        return;
    if (me->mode == FLINTDB_RDWR) {
        lsm_flush(me, NULL);
    }
    if (me->memtable)
        me->memtable->free(me->memtable);
    for (int i = 0; i < me->sst_count; i++) {
        if (me->sst_files[i])
            fclose(me->sst_files[i]);
        if (me->sst_paths[i])
            free(me->sst_paths[i]);
    }
    FREE(me);
}
