# FlintDB

Lightweight, embedded, file-based data engine focused on direct API access with minimal dependencies.

FlintDB is available in two implementations:
- **Java** (`java/`) - Primary implementation with full features
- **C** (`c/`) - Native port with core features (not all Java features are implemented)

**Development Model**: Features are first implemented in Java, then selectively ported to C based on performance and use case requirements.

## Supported Platforms

### Operating Systems
- Linux
- macOS
- Windows (MSYS2)
- iOS
- Android (planned)

### Compilers
- clang
- gcc
- mingw gcc
- zig
- Java 17 (minimum Java 11 for Android)
  - Built and tested with Gradle 8.10.2

## File Format Support

FlintDB provides native support for multiple file formats with automatic type inference and schema detection:

### Supported Formats

* **TSV/CSV** - Tab/comma-separated values with automatic type detection
  - Compressed file support (`.gz`, `.zip`)
  - Streaming processing for large files
  - Configurable delimiters and quote characters
  
* **Parquet** - Apache Parquet columnar format
  - Efficient columnar storage and compression
  - Schema evolution support
  - Java: Available under `src/rc` (release candidate)
  - C: Available as plugin (`plugins/parquet`)

* **JSONL** - JSON Lines format (newline-delimited JSON)
  - Schema inference from JSON structure
  - Streaming JSON processing
  - Java: Available under `src/rc` (release candidate)
  - C: Available as plugin (`plugins/jsonl`)

* **Binary** - Native FlintDB binary format (`FORMAT_BIN`)
  - Optimized for direct memory access
  - B+ Tree indexed storage
  - Primary format for production use

**Use Case**: These formats enable seamless data ingestion from various sources without manual schema definition or ETL pipelines.

## Core Philosophy

**Direct Programmatic Access First**
- Zero SQL parsing overhead for production workloads
- Direct memory access patterns optimized for single-thread performance
- SQL support provided only for development/debugging

**Simple & Focused**
- No external database server required
- File-based storage with B+ Tree indexes
- Streaming/cursor access for memory-efficient processing

## Key Features

### Common Features (Java & C)

* **Direct API Access**: Type-safe row operations without SQL overhead
* **Binary Table Storage**: Efficient `FORMAT_BIN` format with B+ Tree indexes
* **Streaming Processing**: Memory-efficient cursor iteration for large datasets
* **Aggregation**: Built-in functions (COUNT, SUM, AVG, MIN, MAX) with HyperLogLog for approximate counts
* **In-memory Tables**: Memory-only storage without file persistence (`Storage.TYPE_MEMORY`/`TYPE_MEMORY`)
* **Write-Ahead Logging (WAL)**: Transaction support with crash recovery
* **Development Tools**: Optional CLI and Web UI for debugging

### Java-Specific Features

* AtomicInteger-based locking for concurrent access
* Release candidate features under `src/rc`:
  - JSONLFile: JSON Lines read/write with schema inference
  - ParquetFile: Apache Parquet integration
* Minimal dependencies (Gson for WebUI only)

### C-Specific Features

* Native performance with minimal overhead
* Plugin system for extensibility:
  - JSONL plugin: JSON Lines support
  - Parquet plugin: Apache Parquet support
* Embedded Web UI (port 3334 default, requires cJSON)
* Standard C library with optional zlib for compression

**Note**: C implementation is a port of the Java version and may not include all Java features.

## Data Types

Supported types: DATE, TIME, STRING, INT/INT16/INT64, UINT16, DECIMAL, DOUBLE, FLOAT, UUID, IPV6, BYTES, and more integer variants (INT8/UINT8, etc.)

## Thread Safety

### C Implementation

Full multi-thread support with atomic spinlock:
- **Table Operations**: Atomic spinlock (C11 stdatomic) for thread-safe write operations
- **Buffer Pool**: Thread-safe buffer pooling using pthread mutex
- **SQL Parser**: Thread-local storage (TLS) for parser instances per thread
- **Reads & Writes**: Both fully concurrent and thread-safe across multiple threads

### Java Implementation

Full multi-thread support with atomic spinlock:
- **Table Operations**: AtomicInteger-based spinlock for thread-safe write operations
- **Reads & Writes**: Both fully concurrent and thread-safe across multiple threads

**Note**: Spinlock provides fine-grained concurrency control for both read and write operations. For higher throughput, consider partitioning tables by key range, hash, or time dimension.

## Write-Ahead Logging (WAL)

WAL support is now available for crash recovery and transaction management.

### WAL Modes

* **OFF**: No WAL 
* **LOG**: Standard WAL mode with append-only logging
* **TRUNCATE**: Auto-truncate WAL file after checkpoint

### Features

* **Crash Recovery**: Automatic recovery on table open
* **Batch Logging**: Efficient batch write operations
* **Compression Support**: Optional zlib compression for log entries
* **Metadata Logging**: Logs transaction metadata for fast replay
* **Auto-checkpoint**: Configurable checkpoint intervals

## Quick Start

For detailed code examples and tutorials, see:
- **Java**: `java/tutorial/` directory
- **C**: `c/tutorial/c/` directory  
- **Go**: `c/tutorial/go/` directory
- **Rust**: `c/tutorial/rust/` directory
- **Zig**: `c/tutorial/zig/` directory

## SQL Support (Development/Debug Only)

> **Note**: The SQL implementation is planned to be replaced in the future to include better expression support and error handling.

```bash
# CLI for quick inspection
./bin/flintdb "SELECT * FROM data.flintdb LIMIT 10" -pretty

# Web UI for interactive debugging
# Java (port 3333)
./bin/webui

# C (port 3334, requires cJSON)
./bin/flintdb -webui
```

## Building

### Java (gradle)
```bash
cd java
./build.sh
# Output: build/flintdb-{version}.jar
```

### C (make)
```bash
cd c
./build.sh
# Output: bin/flintdb
```

### C (zig build)
```bash
cd zig
zig build                    # Debug
zig build --release=fast     # Release (Equivalent to Makefile -O3)

# Cross-compile
# (zlib is required by c/src/compress.c, so the target must provide zlib headers + library)

# Windows cross-compile (provide a sysroot that has include/zlib.h and lib/libz.*)
zig build -Dtarget=x86_64-windows-gnu -Dmingw_sysroot=/path/to/x86_64-w64-mingw32 --release=fast

# Generic sysroot (any target)
zig build -Dtarget=<triple> -Dsysroot=/path/to/sysroot --release=fast
```

## Use Cases

**Data Processing & Transformation**
- Daily batch processing with memory-efficient streaming
- ETL pipelines without external dependencies
- Log file analysis and aggregation
- Data format conversion (TSV/CSV/JSONL/Parquet)

**ML/DL Data Preparation**
- Training data format conversion and preprocessing
- Log data transformation for model input
- Feature extraction from raw logs
- Efficient streaming of large training datasets

**Embedded Applications**
- Embedded storage for applications without server dependencies
- Local data caching and intermediate results
- Offline-first applications with file-based storage

**Development & Debugging**
- Quick data verification during development
- SQL-based prototyping and exploration
- Testing and debugging with Web UI

## Performance

### TPCH Lineitem Benchmark

**Test Configuration**:
- Dataset: TPC-H lineitem (6,001,215 rows)
- OS: macOS
- Storage: WAL compression enabled

#### Intel Core i7-8700B @ 3.20GHz (6 cores)

| Implementation | Cache Limit (Nodes) | Time | Performance |
|----------------|---------------------|------|-------------|
| **C** | 256K | **41s** | Baseline |
| **Java** | 50K | 54s | C is 24% faster |
| **Java** | 1M | 61s | Larger cache slower (GC overhead) |

#### Apple M1 Chip

| Implementation | Cache Limit (Nodes) | Time | Performance |
|----------------|---------------------|------|-------------|
| **C** | 256K | **22s** | Baseline |
| **Java** | 50K | 38s | C is 42% faster |

**Key Findings**:
- **C is consistently faster than Java** (24% on Intel, 42% on M1)
- **Smaller cache often performs better** due to:
  - Reduced GC pressure (Java)
  - Better CPU cache locality
  - Lower memory overhead
- **C requires minimum 256K cache** due to:
  - Fixed-size hash table (no auto-resize)
  - Backward-shift deletion instability with frequent evictions
  - 50K cache causes segfault during bulk inserts
- **Java's LinkedHashMap** automatically resizes, making small cache sizes viable

**Note**: Both implementations use direct memory access (C pointers, Java ByteBuffer.allocateDirect)

**Test Environment**: macOS, TPC-H lineitem dataset, WAL=OFF

## Development Context

This project is being developed for **educational purposes and personal study**, not as a full-time engagement.

This project heavily utilizes **GitHub Copilot** for code development and verification.

## License

Apache License 2.0 – see `LICENSE`

## Contributing

Issues and suggestions welcome. Focus areas:
- API ergonomics and performance trade-offs
- Plugin development (C implementation)
- Release candidate feature feedback (Java implementation)
