# Parquet Plugin for FlintDB

## 개요

FlintDB의 Parquet 파일 지원을 위한 플러그인입니다. Apache Arrow C++ 라이브러리를 사용하여 Parquet 파일을 읽고 쓸 수 있습니다.

## 왜 플러그인으로 분리했나요?

1. **의존성 관리**: Apache Arrow C++는 큰 라이브러리이며, 모든 사용자가 필요로 하지 않습니다
2. **프로젝트 단순성**: 메인 프로젝트는 C로 유지하고, Parquet 지원만 C++로 분리
3. **선택적 설치**: Parquet이 필요한 사용자만 플러그인을 빌드/설치
4. **동적 로딩**: dlopen을 통해 런타임에 플러그인을 로드하므로 메인 바이너리는 영향 없음

## 설치

### 1. Apache Arrow 설치

#### macOS
```bash
brew install apache-arrow
```

#### Linux (Ubuntu/Debian)
```bash
sudo apt-get update
sudo apt-get install libarrow-dev libparquet-dev
```

#### Linux (CentOS/RHEL)
```bash
sudo yum install epel-release
sudo yum install arrow-devel parquet-devel
```

### 2. 플러그인 빌드

```bash
cd c/plugins/parquet
./build.sh
```

빌드가 성공하면 다음 위치에 공유 라이브러리가 생성됩니다:
- macOS: `c/lib/libflintdb_parquet.dylib`
- Linux: `c/lib/libflintdb_parquet.so`

## 사용법

플러그인이 빌드되면 FlintDB는 `.parquet` 파일을 자동으로 인식하고 플러그인을 로드합니다.

### C API 예제

```c
#include "flintdb.h"

int main() {
    char* error = NULL;
    
    // Parquet 파일 열기 (플러그인 자동 로드)
    struct flintdb_meta meta = flintdb_meta_new("test", &error);
    meta_add_column(&meta, "id", VARIANT_INT64, 0);
    meta_add_column(&meta, "name", VARIANT_STRING, 100);
    meta_add_column(&meta, "value", VARIANT_DOUBLE, 0);
    
    struct flintdb_genericfile* f = parquetfile_open(
        "data.parquet", 
        FLINTDB_RDWR, 
        &meta, 
        &error
    );
    
    if (!f) {
        fprintf(stderr, "Error: %s\n", error);
        return 1;
    }
    
    // 데이터 쓰기
    struct flintdb_row* row = flintdb_row_new(&meta, &error);
    struct flintdb_variant v;
    
    v.type = VARIANT_INT64; v.value.i = 1;
    row->set(row, 0, &v, &error);
    
    v.type = VARIANT_STRING;
    flintdb_variant_string_set(&v, "Alice", 5);
    row->set(row, 1, &v, &error);
    
    v.type = VARIANT_DOUBLE; v.value.f = 123.45;
    row->set(row, 2, &v, &error);
    
    f->write(f, row, &error);
    
    // 정리
    row->free(row);
    f->close(f);
    flintdb_meta_close(&meta);
    
    return 0;
}
```

## 플러그인 동작 방식

1. **파일 열기**: `parquetfile_open()` 호출시
2. **플러그인 검색**: 다음 경로에서 `libflintdb_parquet.{dylib,so}` 검색
   - `./lib`
   - `../lib`
   - `./c/lib`
   - `/usr/local/lib/FlintDB`
   - `/opt/FlintDB/lib`
3. **심볼 로드**: dlopen으로 플러그인 로드 및 함수 포인터 바인딩
4. **Arrow C Data Interface**: ABI 안정적인 C 인터페이스를 통한 데이터 교환

## 플러그인 API

플러그인은 다음 C 함수들을 export합니다:

### Reader
- `void* FlintDB_parquet_reader_open(const char* path, char** error)`
- `void FlintDB_parquet_reader_close(void* reader)`
- `int FlintDB_parquet_reader_get_stream(void* reader, struct ArrowArrayStream* out)`
- `int64_t FlintDB_parquet_reader_num_rows(void* reader)`

### Writer
- `void* FlintDB_parquet_writer_open(const char* path, struct ArrowSchema* schema, char** error)`
- `void FlintDB_parquet_writer_close(void* writer)`
- `int FlintDB_parquet_writer_write_batch(void* writer, struct ArrowArray* batch)`

### Schema Builder
- `void* FlintDB_parquet_schema_builder_new(void)`
- `void FlintDB_parquet_schema_builder_free(void* builder)`
- `int FlintDB_parquet_schema_builder_add_column(void* builder, const char* name, const char* arrow_type)`
- `struct ArrowSchema* FlintDB_parquet_schema_builder_build(void* builder)`

## 타입 매핑

| FlintDB Type | Arrow Format | Arrow Type |
|-------------|--------------|------------|
| INT8        | c            | int8       |
| UINT8       | C            | uint8      |
| INT16       | s            | int16      |
| UINT16      | S            | uint16     |
| INT32       | i            | int32      |
| UINT32      | I            | uint32     |
| INT64       | l            | int64      |
| FLOAT       | f            | float32    |
| DOUBLE      | g            | float64    |
| STRING      | u            | utf8       |
| BYTES       | z            | binary     |
| DATE        | tdD          | date32     |
| TIME        | tts          | time64     |

## 트러블슈팅

### "Failed to load FlintDB Parquet plugin"
- 플러그인이 빌드되지 않았습니다: `cd c/plugins/parquet && ./build.sh`
- Apache Arrow가 설치되지 않았습니다: `brew install apache-arrow` (macOS)

### "Failed to load required symbols"
- 플러그인 버전이 맞지 않습니다. 재빌드하세요: `cd c/plugins/parquet && make clean && ./build.sh`

### Arrow 라이브러리를 찾을 수 없음
```bash
# macOS
export ARROW_HOME=$(brew --prefix apache-arrow)

# Linux (수동 설치한 경우)
export ARROW_HOME=/path/to/arrow
export LD_LIBRARY_PATH=$ARROW_HOME/lib:$LD_LIBRARY_PATH
```

## 개발자 정보

### 플러그인 빌드 시스템
- `Makefile`: GNU Make 빌드 파일
- `build.sh`: 편리한 빌드 스크립트 (Arrow 자동 검색)
- 자동 의존성 검사 및 에러 메시지

### 테스트
```bash
cd c/plugins/parquet
make clean && make
./test_plugin  # 기본 테스트
./test_plugin output.parquet  # 쓰기 테스트
```

### 플러그인 구조
```
c/plugins/parquet/
├── README.md              # 상세 문서
├── parquet_plugin.h       # C API 헤더
├── parquet_plugin.cpp     # C++ 구현 (Arrow 래퍼)
├── Makefile              # 빌드 파일
├── build.sh              # 빌드 스크립트
├── test_plugin.c         # 테스트 프로그램
└── .gitignore
```

## 라이센스

FlintDB와 동일한 라이센스를 따릅니다.
