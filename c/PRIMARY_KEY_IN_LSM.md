# LSM with struct flintdb_row as Primary Keys

## Current Implementation (Simple i64)

### Current types.h
```c
typedef u64 keytype;  // Simple 64-bit integer
typedef u64 valtype;
```

### Current LSM usage
```c
me->memtable = rbtree_new(NULL);  // NULL = default u64 comparator
lsm_put(me->lsm_index, i64_key, i64_offset, &e);
```

## table.c Pattern (Custom Row Comparison)

### How table.c Uses RB-Tree with Rows

**1. Custom Comparator Function**
```c
// table.c line 1260
static int row_compare_get(void *o, const void *a, i64 b) {
    struct sorter *s = (struct sorter*)o;
    const struct flintdb_row *r1 = (const struct flintdb_row *)a;
    const struct flintdb_row *r2 = table_read_unlocked(t, b, NULL);
    
    // Compare by PRIMARY KEY columns
    for(int i=0; i<s->keys.length; i++) {
        int key = s->keys.a[i];
        struct flintdb_variant *v1 = r1->get(r1, key, &e);
        struct flintdb_variant *v2 = r2->get(r2, key, &e);
        cmp = flintdb_variant_compare(v1, v2);
        if (cmp != 0) break;
    }
    return cmp;
}
```

**2. Tree Usage**
```c
// Insert
primary->tree.put(&primary->tree, rowid, e);

// Search with custom comparator
i64 rowid = primary->tree.compare_get(&primary->tree, primary, r, row_compare_get, e);
```

## Applying to LSM

### Option 1: Keep i64, Store Row Metadata in vLog (RECOMMENDED)

**현재 구조 유지 + vLog에 전체 row 저장**

```c
// LSM: i64 hash → i64 vLog offset
lsm_put(me->lsm_index, hash_of_primary_key, vlog_offset, &e);

// vLog에 저장되는 데이터
struct vlog_record {
    u32 magic;
    u32 klen;               // PRIMARY KEY 길이
    char primary_key[klen]; // 실제 PRIMARY KEY (복합키 가능)
    u32 vlen;               // Value 길이
    char value[vlen];       // 실제 데이터
    u32 checksum;
};

// Get 시 collision 체크
struct buffer *wisckey_get(struct wisckey *me, const char *pk, char **e) {
    i64 hash = hash_primary_key(pk);
    i64 offset = lsm_get(me->lsm_index, hash, e);
    
    // vLog에서 읽기
    struct vlog_record *rec = read_vlog(offset);
    
    // Hash collision 체크
    if (strcmp(rec->primary_key, pk) != 0)
        return NULL;  // Collision
    
    return rec->value;
}
```

**장점:**
- LSM은 단순 i64 유지 (빠름)
- RB-Tree 변경 불필요
- Leveled Compaction 그대로 사용 가능

**단점:**
- Hash collision 가능 (매우 드묾)
- vLog에서 추가 읽기 필요

---

### Option 2: Use struct flintdb_row as MemTable Key (COMPLEX)

**MemTable만 row 직접 저장, SSTable은 serialize**

#### Step 1: Modify types.h
```c
// types.h
typedef struct {
    void *data;      // Pointer to flintdb_row or serialized key
    size_t len;      // Length of key
    int is_row;      // 1 = flintdb_row*, 0 = serialized
} keytype;

typedef i64 valtype;  // vLog offset
```

#### Step 2: Custom Comparator
```c
// lsm.c
static i32 row_comparator(keytype a, keytype b) {
    if (a.is_row && b.is_row) {
        // Both are in-memory rows
        struct flintdb_row *r1 = (struct flintdb_row *)a.data;
        struct flintdb_row *r2 = (struct flintdb_row *)b.data;
        return compare_rows_by_primary_key(r1, r2);
    } else if (!a.is_row && !b.is_row) {
        // Both are serialized (from SSTable)
        return memcmp(a.data, b.data, MIN(a.len, b.len));
    } else {
        // Mixed: serialize row if needed
        // ...
    }
}

static i32 compare_rows_by_primary_key(struct flintdb_row *r1, struct flintdb_row *r2) {
    // Compare PRIMARY KEY columns
    for (int i = 0; i < num_pk_cols; i++) {
        struct flintdb_variant *v1 = r1->get(r1, pk_cols[i], NULL);
        struct flintdb_variant *v2 = r2->get(r2, pk_cols[i], NULL);
        int cmp = flintdb_variant_compare(v1, v2);
        if (cmp != 0) return cmp;
    }
    return 0;
}
```

#### Step 3: LSM Initialization
```c
struct lsm_tree *lsm_open(const char *path, enum flintdb_open_mode mode, 
                          size_t mem_max_bytes, int *pk_cols, int num_pk_cols, char **e) {
    struct lsm_tree *me = CALLOC(1, sizeof(struct lsm_tree));
    
    // Store PK column info for comparator
    me->pk_cols = pk_cols;
    me->num_pk_cols = num_pk_cols;
    
    // Create RB-Tree with custom comparator
    me->memtable = rbtree_new(row_comparator);
    
    // ...
}
```

#### Step 4: Put/Get
```c
int lsm_put(struct lsm_tree *me, struct flintdb_row *row, i64 vlog_offset, char **e) {
    keytype key = {
        .data = row,
        .len = 0,  // Not needed for in-memory
        .is_row = 1
    };
    
    me->memtable->put(me->memtable, key, vlog_offset, NULL);
    return 0;
}

i64 lsm_get(struct lsm_tree *me, struct flintdb_row *query_row, char **e) {
    keytype key = {
        .data = query_row,
        .len = 0,
        .is_row = 1
    };
    
    struct rbnode *node = me->memtable->get(me->memtable, key);
    if (node) return node->val;
    
    // Search SSTables with serialized key
    // ...
}
```

#### Step 5: Flush (Serialize Rows)
```c
static int lsm_flush(struct lsm_tree *me, char **e) {
    // Walk MemTable, serialize each row's PRIMARY KEY
    FILE *fp = fopen(sst_path, "wb");
    
    // For each entry in memtable:
    //   1. Extract PRIMARY KEY from flintdb_row
    //   2. Serialize to bytes
    //   3. Write: [serialized_pk_len][serialized_pk][vlog_offset]
    
    // ...
}
```

**장점:**
- 정확한 PRIMARY KEY 비교 (no hashing)
- 복합 키 완전 지원

**단점:**
- 매우 복잡한 구현
- SSTable에서 deserialize 필요
- Leveled Compaction 모두 수정 필요
- 성능 오버헤드

---

## Recommendation for 200M Records/Day

### **Use Option 1 (Hash-based with Collision Check)**

**이유:**
1. **성능**: LSM은 i64만 처리 (Leveled Compaction 효율적)
2. **단순성**: 기존 구조 그대로, 검증된 코드
3. **충돌 희박**: 64-bit hash로 충분히 낮은 collision rate
4. **확장성**: 200M/day 환경에 적합

**구현 예시:**
```c
// Application layer
void insert_record(struct wisckey *wk, struct flintdb_row *row, char **e) {
    // 1. Extract PRIMARY KEY
    char pk_buffer[1024];
    serialize_primary_key(row, pk_buffer);
    
    // 2. Hash PRIMARY KEY
    i64 hash = murmur3_hash(pk_buffer, strlen(pk_buffer));
    
    // 3. Serialize entire row to value buffer
    struct buffer *value = serialize_row(row);
    
    // 4. Store in WiscKey
    wisckey_put(wk, hash, value, e);
    value->free(value);
}

void *get_record(struct wisckey *wk, const char *pk, char **e) {
    i64 hash = murmur3_hash(pk, strlen(pk));
    struct buffer *value = wisckey_get(wk, hash, e);
    
    if (value) {
        // Deserialize and verify PRIMARY KEY
        struct flintdb_row *row = deserialize_row(value);
        char actual_pk[1024];
        serialize_primary_key(row, actual_pk);
        
        if (strcmp(actual_pk, pk) != 0) {
            // Hash collision detected
            row->free(row);
            value->free(value);
            return NULL;
        }
        
        value->free(value);
        return row;
    }
    return NULL;
}
```

## Summary Matrix

| Aspect | Option 1 (Hash) | Option 2 (Direct Row) |
|--------|----------------|----------------------|
| LSM Complexity | ✓ Simple | ✗ Very Complex |
| Performance | ✓✓ Fast | ✗ Slower |
| PK Accuracy | ✓ 99.9999% | ✓✓ 100% |
| Leveled Compact | ✓ No change | ✗ Major rewrite |
| Disk Format | ✓ Simple | ✗ Complex |
| Implementation Time | 1-2 days | 2-3 weeks |

**For your use case: Use Option 1 (Hash-based)**
