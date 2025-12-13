# FlintDB JSONL Plugin

This plugin enables FlintDB to read JSONL (JSON Lines / Newline Delimited JSON) files.

## Features

- Read `.jsonl` and `.ndjson` files
- Automatic schema inference from first JSON object
- Support for nested objects and arrays (converted to JSON strings)
- Type inference: bool, int, double, string

## Requirements

- cJSON library (`brew install cjson` on macOS)

## Building

```bash
# Test if cJSON is available
make test-cjson

# Build the plugin
make

# Install to lib directory
make install
```

## Usage

Once installed in `lib/libflintdb_jsonl.dylib`, FlintDB will automatically use this plugin for `.jsonl` and `.ndjson` files:

```bash
./bin/FlintDB "SELECT * FROM data.jsonl LIMIT 10"
```

## JSONL File Format

JSONL files contain one JSON object per line:

```json
{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
{"id": 3, "name": "Charlie", "age": 35}
```

## Limitations

- Currently read-only (write support coming soon)
- Nested objects/arrays are converted to JSON strings
- No efficient row counting (requires full file scan)
- Schema is inferred from first object only
