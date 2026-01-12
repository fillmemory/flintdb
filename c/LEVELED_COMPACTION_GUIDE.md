# LSM Leveled Compaction - Technical Implementation Guide

## 1. Core Data Structures

### SSTable Metadata
```c
struct sstable_meta {
    char path[PATH_MAX];     // Full file path
    FILE *file;               // Open file handle (for reads)
    i64 count;                // Number of entries
    i64 min_key;              // Minimum key in this SSTable
    i64 max_key;              // Maximum key in this SSTable
    int level;                // Which level (0-6)
};
```

### LSM Tree
```c
#define MAX_LEVEL 7
#define L0_COMPACTION_TRIGGER 4
#define LEVEL_SIZE_MULTIPLIER 10

struct lsm_tree {
    char path[PATH_MAX];
    struct rbtree *memtable;
    size_t mem_max_entries;
    enum flintdb_open_mode mode;

    // Level organization
    struct sstable_meta **levels[MAX_LEVEL];  // levels[i][j] = j-th SSTable in level i
    int level_counts[MAX_LEVEL];               // Number of SSTables per level
    int level_capacities[MAX_LEVEL];           // Allocated capacity per level
};
```

## 2. SSTable File Format

### Header (3 * sizeof(i64) = 24 bytes)
```
Offset 0:  count (i64)     - Number of entries
Offset 8:  min_key (i64)   - Minimum key
Offset 16: max_key (i64)   - Maximum key
```

### Data (count * sizeof(struct lsm_entry))
```c
struct lsm_entry {
    i64 key;
    i64 offset;    // Value or -2 for tombstone
};
```

### Filename Convention
```
L0: base_L0_00001.sst, base_L0_00002.sst, ...
L1: base_L1_00001.sst, base_L1_00002.sst, ...
L2: base_L2_00001.sst, ...
```

## 3. Level Organization Rules

### Level Sizes
```
L0: Up to 4 SSTables (flush한 그대로, 키 범위 중첩 허용)
L1: ~10 SSTables, 각 10MB = 100MB total
L2: ~100 SSTables, 각 10MB = 1GB total
L3: ~1000 SSTables, 각 10MB = 10GB total
...
```

### Key Invariants
- **L0**: Keys may overlap between SSTables (newest has priority)
- **L1+**: Non-overlapping key ranges per level
- **Size**: Level n+1 is ~10x larger than level n

## 4. Core Algorithms

### 4.1 MemTable Flush → L0
```c
static int lsm_flush(struct lsm_tree *me, char **e) {
    // 1. Generate filename: base_L0_XXXXX.sst
    // 2. Open file for writing
    // 3. Write header (count, min_key, max_key)
    // 4. In-order walk of RB-tree, write entries
    //    - Track min_key, max_key during walk
    // 5. Update header with actual min/max
    // 6. Close write file, reopen for read
    // 7. Add to me->levels[0]
    // 8. Clear memtable
    // 9. Check: if (level_counts[0] >= L0_COMPACTION_TRIGGER) 
    //       -> trigger L0→L1 compaction
}
```

### 4.2 L0 → L1 Compaction
```c
static int compact_l0_to_l1(struct lsm_tree *me, char **e) {
    // 1. Collect all L0 SSTables
    // 2. Find L1 SSTables with overlapping key ranges
    //    - Check: sst->min_key <= l0_max && sst->max_key >= l0_min
    // 3. K-way merge all selected SSTables:
    //    - Open all files
    //    - Heap/priority queue for merge
    //    - Newest key wins (L0 > L1)
    //    - Skip tombstones (-2)
    // 4. Write merged output to new L1 SSTables
    //    - Split 10MB per file
    //    - Track min/max for each new SSTable
    // 5. Delete old L0 and L1 SSTables (files + metadata)
    // 6. Update levels[0], levels[1]
    // 7. Check: if (L1 size > target) -> trigger L1→L2
}
```

### 4.3 Ln → Ln+1 Compaction (n ≥ 1)
```c
static int compact_level(struct lsm_tree *me, int level, char **e) {
    // 1. Pick 1 SSTable from level n (round-robin or oldest)
    // 2. Find overlapping SSTables in level n+1
    // 3. Merge (2-way or K-way)
    //    - Level n SSTable wins on duplicates
    //    - Skip tombstones
    // 4. Write output to level n+1 (split into 10MB files)
    // 5. Delete old SSTables from both levels
    // 6. Check: if (level n+1 size > target) -> trigger n+1→n+2
}
```

### 4.4 lsm_get - Level-Aware Search
```c
i64 lsm_get(struct lsm_tree *me, i64 key, char **e) {
    // 1. Check memtable
    struct rbnode *node = me->memtable->get(me->memtable, key);
    if (node) return node->val;

    // 2. Check L0 (SSTables may overlap, check newest first)
    for (int i = me->level_counts[0] - 1; i >= 0; i--) {
        struct sstable_meta *sst = me->levels[0][i];
        if (key < sst->min_key || key > sst->max_key)
            continue;
        i64 result = binary_search_sstable(sst->file, sst->count, key);
        if (result != -1) return result;
    }

    // 3. Check L1+ (non-overlapping, binary search by range)
    for (int level = 1; level < MAX_LEVEL; level++) {
        for (int i = 0; i < me->level_counts[level]; i++) {
            struct sstable_meta *sst = me->levels[level][i];
            if (key >= sst->min_key && key <= sst->max_key) {
                i64 result = binary_search_sstable(sst->file, sst->count, key);
                if (result != -1) return result;
                break; // Non-overlapping, so stop after checking one SSTable
            }
        }
    }

    return -1; // Not found
}
```

### 4.5 Binary Search in SSTable
```c
static i64 binary_search_sstable(FILE *fp, i64 count, i64 key) {
    i64 low = 0, high = count - 1;
    while (low <= high) {
        i64 mid = low + (high - low) / 2;
        // Seek: skip header (24 bytes) + mid * entry_size
        fseek(fp, 24 + mid * sizeof(struct lsm_entry), SEEK_SET);
        struct lsm_entry entry;
        if (fread(&entry, sizeof(entry), 1, fp) != 1)
            return -1;
        
        if (entry.key == key)
            return entry.offset;
        if (entry.key < key)
            low = mid + 1;
        else
            high = mid - 1;
    }
    return -1;
}
```

## 5. K-Way Merge Implementation

### Min-Heap Structure
```c
struct merge_cursor {
    FILE *file;
    i64 count;
    i64 position;
    struct lsm_entry current;
    int level;      // For priority (lower level = higher priority)
    int sst_index;
};

struct merge_heap {
    struct merge_cursor *cursors[MAX_SSTABLES_PER_LEVEL * 2];
    int size;
};
```

### Merge Loop
```c
static int k_way_merge(struct merge_cursor *cursors, int n, FILE *out, char **e) {
    struct merge_heap heap;
    heap_init(&heap, cursors, n);
    
    i64 last_key = LLONG_MIN;
    i64 out_count = 0;
    
    while (heap.size > 0) {
        struct merge_cursor *best = heap_pop(&heap);
        
        // Write if new key and not tombstone
        if (best->current.key != last_key && best->current.offset != -2) {
            fwrite(&best->current, sizeof(struct lsm_entry), 1, out);
            out_count++;
            last_key = best->current.key;
        }
        
        // Advance cursor
        if (best->position < best->count) {
            fread(&best->current, sizeof(struct lsm_entry), 1, best->file);
            best->position++;
            heap_push(&heap, best);
        } else {
            fclose(best->file);
        }
    }
    
    return out_count;
}
```

## 6. Performance Optimizations

### Bloom Filter (Optional)
```c
struct sstable_meta {
    // ... existing fields ...
    struct bloom_filter *bloom;  // Skip SSTable if key not in bloom
};

// Before binary search:
if (sst->bloom && !bloom_may_contain(sst->bloom, key))
    continue;  // Skip this SSTable
```

### Write Buffer
```c
#define WRITE_BUFFER_SIZE (1024 * 1024)  // 1MB
char write_buffer[WRITE_BUFFER_SIZE];
setvbuf(fp, write_buffer, _IOFBF, WRITE_BUFFER_SIZE);
```

## 7. Edge Cases & Error Handling

### Compaction Failures
- If compaction fails mid-way, keep old SSTables
- Only delete after new SSTables are successfully written
- Use temp files: `base_L1_XXXXX.sst.tmp`

### File Handle Limits
- Close SSTables not recently used (LRU cache)
- Reopen on demand for reads

### Concurrent Access
- Current implementation: single-threaded
- For multi-thread: Add RW locks per level

## 8. Disk Space Management

### Temporary Space Requirements
```
L0→L1: ~40MB (4 * 10MB L0 + overlapping L1)
Ln→Ln+1: ~20MB (1 SSTable from Ln + overlapping from Ln+1)
```

### Cleanup Sequence
```
1. Write new SSTables with .tmp extension
2. fsync() to ensure durability
3. Rename .tmp → .sst (atomic)
4. Update in-memory metadata
5. Delete old SSTables
6. fsync() directory
```

## 9. Testing Strategy

### Unit Tests
- `test_l0_flush`: Verify min/max tracking
- `test_binary_search`: Verify SSTable search
- `test_k_way_merge`: Verify merge correctness
- `test_l0_to_l1`: Verify L0→L1 compaction

### Integration Tests
- Insert 200M records over simulated "days"
- Verify level stabilization
- Check disk space < 2x data size

### Edge Case Tests
- Duplicate keys across levels
- Tombstones
- Empty SSTables
- MAX_LEVEL overflow

## 10. Implementation Checklist

### Phase 1: Core Structure
- [ ] Define `struct sstable_meta`
- [ ] Update `struct lsm_tree` with levels
- [ ] Update `lsm_open` to initialize levels
- [ ] Update `sst_scan` to parse level from filename

### Phase 2: L0 Operations
- [ ] Implement `lsm_flush` with min/max tracking
- [ ] Write SSTables with new header format (count, min, max)
- [ ] Test: Flush 1000 entries, verify L0 file

### Phase 3: Read Path
- [ ] Implement `binary_search_sstable`
- [ ] Update `lsm_get` for level-aware search
- [ ] Test: Insert + read 10K entries

### Phase 4: Compaction
- [ ] Implement `k_way_merge` helper
- [ ] Implement `compact_l0_to_l1`
- [ ] Implement `compact_level(n)` for Ln→Ln+1
- [ ] Test: Force compaction, verify output

### Phase 5: Cleanup
- [ ] Update `lsm_close` for level cleanup
- [ ] Add file handle management (LRU)
- [ ] Test: Open/close 100 times, verify no leaks

### Phase 6: Production
- [ ] Add logging for compaction events
- [ ] Monitor level sizes
- [ ] Benchmark with 200M records/day workload

## 11. Quick Reference

### Size Calculations
```
Entry size: 16 bytes (i64 key + i64 offset)
10MB SSTable: ~655,360 entries
100MB MemTable: ~6.5M entries
200M entries/day ÷ 6.5M = ~31 flushes/day → 31 L0 SSTables/day
31 ÷ 4 (L0 trigger) = ~8 L0→L1 compactions/day
```

### File Operations
```c
// Open for buffered read
FILE *fp = fopen(path, "rb");
setvbuf(fp, NULL, _IOFBF, 1024*1024);

// Atomic rename
rename("base_L1_00001.sst.tmp", "base_L1_00001.sst");

// Sync directory (Linux)
int dirfd = open(".", O_RDONLY);
fsync(dirfd);
close(dirfd);
```

### Debug Tips
```c
// Log compaction events
DEBUG("Compacting L%d: %d SSTables → L%d", level, count, level+1);

// Verify invariants
assert(me->levels[level][i]->min_key <= me->levels[level][i]->max_key);
assert(level > 0 || me->level_counts[level] <= L0_COMPACTION_TRIGGER);
```
