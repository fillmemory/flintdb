# Transformer Programmatic API

## Overview

`Transformer`는 이제 XML 설정 파일 뿐만 아니라 Java 코드에서 직접 API를 호출하여 사용할 수 있습니다.

## Usage

### 1. XML 방식 (기존)

```bash
./bin/transform.sh transformer.xml
```

### 2. Programmatic API (신규)

```java
import flint.db.transformer.Transformer;

Transformer transformer = Transformer.createBuilder("my-transform")
    .maxThreads(4)
    .maxErrors(10)
    .input()
        .directory("./data/input")
        .pattern(".*\\.csv")
        .formatType("csv")
        .done()
    .output()
        .directory("./data/output")
        .filename("{basename}.parquet")
        .column("id", "long").from("user_id").done()
        .column("name", "string").bytes(100).from("user_name").done()
        .done()
    .build();

transformer.run();
```

## API Reference

### Builder Methods

#### Transform Configuration
- `createBuilder(String name)` - Create a new builder
- `maxThreads(int threads)` - Set maximum parallel threads (default: 1)
- `maxErrors(int errors)` - Set maximum allowed errors (default: 10)

#### Variables
- `variable(String name, String value)` - Add string variable
- `dateVariable(String name, String format, int offsetDays)` - Add date variable

#### Input Configuration
- `input()` - Start input configuration
  - `directory(String dir)` - Input directory path
  - `pattern(String pattern)` - File name regex pattern
  - `formatType(String type)` - Format type (csv, tsv, tsv.gz, parquet, etc.)
  - `maxDepth(int depth)` - Max directory traversal depth
  - `where(String clause)` - SQL-like filter clause
  - `done()` - Finish input configuration

#### Output Configuration
- `output()` - Start output configuration
  - `directory(String dir)` - Output directory path
  - `filename(String filename)` - Output filename pattern (supports {basename}, {var})
  - `skipIfExists(boolean skip)` - Skip if output file exists
  - `overwriteIfExists(boolean overwrite)` - Overwrite existing files
  - `sortBy(String... columns)` - Sort output by columns
  - `column(String name, String type)` - Add output column
    - `bytes(int bytes)` - Column size in bytes
    - `precision(int precision)` - Decimal precision
    - `from(String... sources)` - Source column names
    - `transform(String func)` - Transform function (e.g., "hash")
    - `notNull(boolean notNull)` - Not null constraint
    - `defaultValue(Object value)` - Default value
    - `done()` - Finish column configuration
  - `done()` - Finish output configuration

#### Custom Handlers
- `handler(Class<? extends RowHandler> handlerClass, Map<String, String> params)` - Add custom row handler

### Column Types

- `string` - Variable length string
- `int` - 32-bit integer
- `long` - 64-bit integer
- `float` - 32-bit floating point
- `double` - 64-bit floating point
- `decimal` - High precision decimal
- `date` - Date/timestamp
- `bytes` - Binary data

## Examples

### Example 1: Simple CSV to Parquet

```java
Transformer.createBuilder("csv-to-parquet")
    .input()
        .directory("./data")
        .pattern(".*\\.csv")
        .formatType("csv")
        .done()
    .output()
        .directory("./output")
        .filename("{basename}.parquet")
        .column("id", "long").from("user_id").done()
        .column("name", "string").bytes(100).from("user_name").done()
        .done()
    .build()
    .run();
```

### Example 2: With Date Variables

```java
Transformer.createBuilder("daily-logs")
    .dateVariable("date", "yyyyMMdd", -1)  // yesterday
    .input()
        .directory("./logs/{date}")
        .pattern("app_.*\\.log")
        .formatType("tsv")
        .where("level = 'ERROR'")
        .done()
    .output()
        .directory("./reports")
        .filename("errors_{date}.parquet")
        .column("timestamp", "string").bytes(30).from("timestamp").done()
        .column("message", "string").bytes(500).from("message").done()
        .done()
    .build()
    .run();
```

### Example 3: With Sorting and Transform

```java
Transformer.createBuilder("user-data")
    .maxThreads(8)
    .input()
        .directory("./raw")
        .pattern("users_.*\\.tsv")
        .formatType("tsv")
        .done()
    .output()
        .directory("./processed")
        .filename("users.flintdb")
        .sortBy("user_id", "created_at")
        .column("user_id", "long").from("id").done()
        .column("email_hash", "long").from("email").transform("hash").done()
        .column("created_at", "string").bytes(30).from("timestamp").done()
        .done()
    .build()
    .run();
```

### Example 4: Custom Row Handler

```java
public class CustomHandler implements Transformer.RowHandler {
    @Override
    public void handle(Row in, Map<String, String> vars, Map<String, Object> out) {
        // Add computed fields
        out.put("full_name", in.get("first_name") + " " + in.get("last_name"));
    }
}

Transformer.createBuilder("with-handler")
    .input()
        .directory("./data")
        .pattern(".*\\.csv")
        .done()
    .output()
        .directory("./output")
        .filename("output.parquet")
        .column("full_name", "string").bytes(200).done()
        .done()
    .handler(CustomHandler.class, Map.of("param1", "value1"))
    .build()
    .run();
```

## Benefits of Programmatic API

1. **Type Safety** - Compile-time checking vs XML runtime parsing
2. **IDE Support** - Auto-completion and inline documentation
3. **Dynamic Configuration** - Build configs programmatically based on runtime conditions
4. **Testing** - Easier to unit test transformation logic
5. **Refactoring** - Easier to refactor compared to XML strings
6. **Integration** - Better integration with existing Java applications

## Comparison

### XML Configuration
```xml
<transform name="example" max-threads="4">
  <input type="filesystem">
    <directory>./data</directory>
    <pattern>.*\.csv</pattern>
  </input>
  <output>
    <directory>./output</directory>
    <filename>{basename}.parquet</filename>
    <meta>
      <columns>
        <column name="id" type="long" from="user_id"/>
        <column name="name" type="string" bytes="100" from="user_name"/>
      </columns>
    </meta>
  </output>
</transform>
```

### Programmatic API
```java
Transformer.createBuilder("example")
    .maxThreads(4)
    .input()
        .directory("./data")
        .pattern(".*\\.csv")
        .done()
    .output()
        .directory("./output")
        .filename("{basename}.parquet")
        .column("id", "long").from("user_id").done()
        .column("name", "string").bytes(100).from("user_name").done()
        .done()
    .build()
    .run();
```

## See Also

- [TRANSFORMER.md](./TRANSFORMER.md) - XML configuration documentation
- [TransformerBuilderExample.java](../src/test/java/flint/db/transformer/TransformerBuilderExample.java) - More examples
