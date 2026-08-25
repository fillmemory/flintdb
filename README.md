# FlintDB

Embedded, file-based data engine. Open a table or a TSV/CSV file, scan with a cursor, write rows. No server process.

Two implementations share the same design:

| Tree | Role |
|------|------|
| `java/` | Original implementation (JDBC sources, pipeline XML, Android JAR) |
| `c/` | Native library + CLI (`libflintdb`, `bin/flintdb`). Usually the faster path for bulk work |
| `zig/` | Zig build wrapper around the C core |
| `webui/` | Shared HTML for the embedded debugger |

The API is the production interface. SQL is a convenience layer for the CLI and Web UI, not a full SQL engine.

This project is developed for study and personal use.

## Platforms

| | Status |
|--|--------|
| Linux, macOS | Primary |
| Windows | MSYS2 / MinGW (`c/build.sh --win64` to cross-compile) |
| iOS | C core as XCFramework (`c/build_ios.sh`). No CLI or Web UI |
| Android | Java 11 subset JAR (`java/build-android.sh`) |

Compilers: gcc, clang, mingw, zig. Java 17 (Java 11 for the Android JAR). Gradle 8.10.2.

## Features

- **Tables** — native `.flintdb` files with B+ Tree indexes
- **Files** — TSV/CSV (gzip/zip), plus JSONL and Parquet via plugins
- **Cursors** — streaming reads without loading the whole file
- **Aggregates** — COUNT, SUM, AVG, MIN, MAX, FIRST, LAST, distinct count, HyperLogLog
- **WAL** — `OFF` / `LOG` / `TRUNCATE`, optional zlib compression, recovery on open
- **In-memory tables** — `STORAGE=MEMORY`
- **CLI + Web UI** — inspect files without writing a program

Java extras: JDBC as a `GenericFile` source (`java/jdbc.md`), pipeline XML, `src/rc` JSONL/Parquet.

C extras: plugin DLLs (`c/plugins/jsonl`, `c/plugins/parquet`), embedded Web UI on port 3334 (needs cJSON).

## File formats

FROM / INTO is a path. Format is inferred from the suffix.

| Format | Notes |
|--------|--------|
| `.flintdb` | Binary table, B+ Tree, WAL |
| `.tsv` / `.csv` | Built-in, `.gz` / `.zip` ok |
| `.jsonl` | C plugin; Java under `src/rc` |
| `.parquet` | C plugin (Arrow); Java under `src/rc` |
| `jdbc:…` | Java only |

## SQL

Not ANSI SQL. No JOIN. SELECT items are columns and aggregates, not arbitrary expressions.

Works today:

```sql
SELECT * FROM data.flintdb WHERE id > 10 LIMIT 10
SELECT name, COUNT(*) FROM data.flintdb GROUP BY 1 ORDER BY 2 DESC
INSERT INTO data.flintdb FROM input.tsv.gz
SELECT * FROM data.flintdb INTO output.csv.gz
UPDATE data.flintdb SET salary = 65000 WHERE id = 2
DELETE FROM data.flintdb WHERE id = 2
DESC data.flintdb
SHOW TABLES WHERE temp
BEGIN TRANSACTION data.flintdb
```

Also: `REPLACE`, `CREATE` / `DROP` / `ALTER TABLE`, `DISTINCT`, `HAVING`, `USE INDEX`, 1-based `GROUP BY n` / `ORDER BY n`.

C parse still extracts clause strings; `sql_bind.c` turns them into a typed plan, then `sql_exec.c` runs one `cursor_row` pipeline for binary tables and generic files.

## Data types

`INT8`/`UINT8`, `INT16`/`UINT16`, `INT32`/`UINT32`, `INT64`, `FLOAT`, `DOUBLE`, `DECIMAL`, `STRING`, `BYTES`, `DATE`, `TIME`, `UUID`, `IPV6`.

## WAL

Set on `CREATE TABLE` (e.g. `WAL=TRUNCATE`).

| Mode | Behavior |
|------|----------|
| `OFF` | No log |
| `LOG` | Append-only |
| `TRUNCATE` | Truncate after checkpoint |

Crash recovery runs on table open. Checkpoint interval, batch size, and compression threshold are configurable.

## Thread safety

Table writes use a C11 (C) / `AtomicInteger` (Java) spinlock. The C buffer pool uses a mutex; the SQL parser is thread-local. For higher write throughput, partition by key, hash, or time.

## Quick start

```bash
cd c && ./build.sh
./bin/flintdb "SELECT * FROM data.flintdb LIMIT 10" -pretty
./bin/flintdb "INSERT INTO data.flintdb FROM input.tsv.gz"
./bin/flintdb -webui          # C, port 3334 (cJSON)
```

Java Web UI: `java/bin/webui` (port 3333).

Tutorials (API, not SQL):

- Java: `java/tutorial/java/`
- C: `c/tutorial/c/`
- C++: `c/tutorial/cpp/`
- Go: `c/tutorial/go/`
- Rust: `c/tutorial/rust/`
- Zig: `c/tutorial/zig/`
- Python: `c/tutorial/python3/`
- Swift: `c/tutorial/swift/`

FFI tutorials are examples; they do not cover the full API.

## Building

```bash
# Java
cd java && ./build.sh            # jar; add --test / --javadoc / --pack

# C
cd c && ./build.sh               # lib + CLI
./build.sh -all                  # + jsonl/parquet plugins
./build.sh --win64               # MinGW cross-compile
./build_ios.sh                   # XCFramework

# Zig (wraps C)
cd zig
zig build
zig build --release=fast
# zlib required. Windows: -Dtarget=x86_64-windows-gnu -Dmingw_sysroot=...
```

C deps: a C compiler, zlib. Optional: cJSON (Web UI), Apache Arrow (Parquet plugin), jemalloc.

## Performance

TPC-H lineitem, 6,001,215 rows, macOS, WAL=OFF, direct memory (C pointers / Java `ByteBuffer.allocateDirect`).

**Intel Core i7-8700B @ 3.20GHz**

| | Cache (nodes) | Time |
|--|---------------|------|
| C | 256K | 41s |
| Java | 50K | 54s |
| Java | 1M | 61s |

**Apple M1**

| | Cache (nodes) | Time |
|--|---------------|------|
| C | 256K | 22s |
| Java | 50K | 38s |

C is faster here (about 24% on Intel, 42% on M1). A smaller Java cache often wins because of GC. C’s hash table does not auto-resize; 256K is the practical minimum for this bulk insert (50K can fault). Java `LinkedHashMap` resizes, so 50K is usable.

More numbers: `c/성능 테스트 결과.md`.

## License

Apache License 2.0 — see `LICENSE`.

## Contributing

Issues and suggestions welcome: API shape vs. speed, C plugins, Java `src/rc` features.
