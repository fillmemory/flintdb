# JDBC Integration Guide

FlintDB now supports JDBC connections as data sources, allowing you to query relational databases using the same SQL syntax used for files.

## Features

- **Unified Interface**: Access JDBC databases and files through the same `GenericFile` interface
- **Connection Aliases**: Define reusable connection configurations
- **Transparent Integration**: Use JDBC sources in SELECT, INSERT FROM, and other operations
- **Type Mapping**: Automatic conversion between JDBC and FlintDB column types

## Quick Start

### 1. Direct JDBC URI

Use JDBC URIs directly in SQL statements:

```bash
# Query from MySQL
./bin/flintdb "SELECT * FROM jdbc:mysql://localhost:3306/mydb?table=users LIMIT 10" -pretty

# Query from H2
./bin/flintdb "SELECT * FROM jdbc:h2:mem:test?table=customers WHERE age > 18"

# Export JDBC to file
./bin/flintdb "INSERT INTO output.tsv FROM jdbc:postgresql://localhost/db?table=orders"

# Import file to JDBC
./bin/flintdb "INSERT INTO jdbc:mysql://localhost:3306/mydb?table=users FROM input.csv"
```

### 2. Using Connection Aliases

Create a configuration file to avoid repeating connection strings.

#### Create `~/.flintdb/jdbc.properties`:

```properties
# MySQL connection
mydb = jdbc:mysql://localhost:3306/mydb?user=admin&password=secret&table={table}

# H2 in-memory database
testdb = jdbc:h2:mem:test?table={table}

# PostgreSQL production
prod = jdbc:postgresql://prod.server.com:5432/production?user=app&password=xxx&table={table}
```

#### Use aliases in SQL:

```bash
# @ prefix notation
./bin/flintdb "SELECT * FROM @mydb:users LIMIT 10" -pretty

# Dot notation (simpler)
./bin/flintdb "SELECT * FROM mydb.customers WHERE age > 18"

# Insert operations
./bin/flintdb "INSERT INTO output.tsv FROM @prod:orders"
./bin/flintdb "INSERT INTO data.flintdb FROM testdb.products"
```

## Configuration

### Configuration File Locations

The system looks for `jdbc.properties` in the following order:

1. Current directory: `./jdbc.properties`
2. User home: `~/.flintdb/jdbc.properties`
3. System property: `-Dflintdb.jdbc.config=/path/to/jdbc.properties`

### Configuration Format

```properties
# Format: alias = jdbc_uri_template

# Use {table} as placeholder for table name
alias_name = jdbc:vendor://host:port/database?user=xxx&password=yyy&table={table}

# Examples for different databases:

# MySQL
mysql_local = jdbc:mysql://localhost:3306/mydb?user=root&password=pass&table={table}

# MariaDB
mariadb = jdbc:mariadb://localhost:3306/analytics?user=reader&password=xxx&table={table}

# H2 Database
h2_mem = jdbc:h2:mem:test?table={table}
h2_file = jdbc:h2:file:/tmp/testdb?table={table}

# PostgreSQL
postgres = jdbc:postgresql://localhost:5432/mydb?user=postgres&password=xxx&table={table}

# Oracle
oracle = jdbc:oracle:thin:@localhost:1521:ORCL?user=system&password=xxx&table={table}

# SQL Server
sqlserver = jdbc:sqlserver://localhost:1433;databaseName=mydb;user=sa;password=xxx;table={table}
```

### Example Configuration File

See [`jdbc.properties.example`](jdbc.properties.example) for a complete template.

## Usage Examples

### SELECT Queries

```bash
# Simple select
./bin/flintdb "SELECT * FROM @mydb:users"

# With WHERE clause
./bin/flintdb "SELECT * FROM mydb.orders WHERE status = 'active'"

# With aggregation
./bin/flintdb "SELECT COUNT(*), AVG(price) FROM prod.products"

# With LIMIT
./bin/flintdb "SELECT * FROM testdb.logs LIMIT 100"

# Pretty print
./bin/flintdb "SELECT * FROM @mydb:users LIMIT 10" -pretty -rownum

# SELECT INTO (export query results)
./bin/flintdb "SELECT * FROM @mydb:users INTO users_backup.tsv"
./bin/flintdb "SELECT name, email FROM mydb.customers WHERE age > 18 INTO active_customers.csv"
./bin/flintdb "SELECT * FROM @prod:orders WHERE date > '2024-01-01' INTO recent_orders.flintdb"
```

### Data Export (JDBC → File)

```bash
# To TSV
./bin/flintdb "INSERT INTO output.tsv FROM @mydb:users"

# To compressed TSV
./bin/flintdb "INSERT INTO output.tsv.gz FROM prod.logs"

# To FlintDB binary format
./bin/flintdb "INSERT INTO data.flintdb FROM @mydb:orders"

# To Parquet
./bin/flintdb "INSERT INTO archive.parquet FROM prod.events"

# To JSONL
./bin/flintdb "INSERT INTO export.jsonl FROM testdb.products"
```

### Data Import (File → JDBC)

```bash
# From CSV
./bin/flintdb "INSERT INTO @mydb:users FROM input.csv"

# From TSV
./bin/flintdb "INSERT INTO testdb.temp FROM data.tsv"

# From FlintDB
./bin/flintdb "INSERT INTO @prod:staging FROM local.flintdb"
```

### Mixed Operations

```bash
# JDBC to File with SELECT INTO
./bin/flintdb "SELECT * FROM @mydb:users INTO backup.tsv"
./bin/flintdb "SELECT * FROM @prod:logs WHERE level='ERROR' INTO errors.jsonl"

# File to JDBC with INSERT FROM
./bin/flintdb "INSERT INTO @mydb:users FROM input.csv"

# JDBC to JDBC (between different databases)
./bin/flintdb "INSERT INTO @prod:archive FROM @staging:current"
./bin/flintdb "SELECT * FROM @staging:temp INTO @prod:permanent"

# Complex filtering and transformation
./bin/flintdb "SELECT id, name, price*1.1 as new_price FROM @mydb:products WHERE stock > 0 INTO updated_prices.parquet"
```

## Supported Databases

| Database   | JDBC Driver          | URI Format                                    |
|------------|----------------------|-----------------------------------------------|
| MySQL      | `com.mysql.jdbc.Driver` | `jdbc:mysql://host:3306/db?table={table}`     |
| MariaDB    | `org.mariadb.jdbc.Driver` | `jdbc:mariadb://host:3306/db?table={table}`   |
| PostgreSQL | `org.postgresql.Driver` | `jdbc:postgresql://host:5432/db?table={table}` |
| H2         | `org.h2.Driver`      | `jdbc:h2:mem:test?table={table}`              |
| Oracle     | `oracle.jdbc.driver.OracleDriver` | `jdbc:oracle:thin:@host:1521:SID?table={table}` |
| SQL Server | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | `jdbc:sqlserver://host:1433;databaseName=db;table={table}` |

### JDBC Drivers

Ensure the appropriate JDBC driver JAR is in the classpath:

```bash
# Add driver to classpath
export CLASSPATH="$CLASSPATH:lib/mysql-connector-java.jar"

# Or specify when running
java -cp "flintdb.jar:lib/mysql-connector-java.jar" flint.db.CLI "SELECT * FROM @mydb:users"
```

## Type Mapping

JDBC types are automatically mapped to FlintDB column types:

| JDBC Type           | FlintDB Type      |
|---------------------|------------------|
| VARCHAR, CHAR       | STRING           |
| INT, INTEGER        | INT              |
| BIGINT              | DECIMAL          |
| SMALLINT            | INT16            |
| TINYINT             | INT8             |
| DOUBLE, FLOAT       | DOUBLE           |
| DECIMAL, NUMERIC    | DECIMAL          |
| DATE                | DATE             |
| TIMESTAMP, DATETIME | TIME             |
| BOOLEAN             | INT8 (0 or 1)    |

## API Usage

### Java API

```java
import flint.db.GenericFile;
import flint.db.JdbcTable;
import flint.db.JdbcConfig;
import java.net.URI;

// Direct JDBC URI
try (GenericFile gf = GenericFile.open("jdbc:mysql://localhost:3306/mydb?table=users")) {
    Meta meta = gf.meta();
    Cursor<Row> cursor = gf.find();
    while (cursor.hasNext()) {
        Row row = cursor.next();
        // Process row...
    }
}

// Using URI object
URI uri = new URI("jdbc:h2:mem:test?table=customers");
try (GenericFile gf = GenericFile.open(uri)) {
    // Query data...
}

// Register alias programmatically
JdbcConfig.register("mydb", "jdbc:mysql://localhost:3306/mydb?table={table}");

// Resolve alias
String resolved = JdbcConfig.resolve("@mydb:users");
// Returns: jdbc:mysql://localhost:3306/mydb?table=users
```

### Direct JdbcTable API

```java
import flint.db.JdbcTable;
import java.net.URI;

// Open with explicit table name
URI uri = new URI("jdbc:mysql://localhost:3306/mydb");
try (JdbcTable jdbc = JdbcTable.open(uri, "admin", "secret", "users")) {
    Meta meta = jdbc.meta();
    
    // Execute custom SQL
    Cursor<Row> cursor = jdbc.find("SELECT * FROM users WHERE age > 18");
    
    // Apply updates (if supported)
    int affected = jdbc.apply("UPDATE users SET status = ? WHERE id = ?", "active", 123);
}

// Open with table in URI
URI uri2 = new URI("jdbc:mysql://localhost:3306/mydb?table=users");
try (JdbcTable jdbc = JdbcTable.open(uri2)) {
    // Query with GenericFile interface
    Cursor<Row> cursor = jdbc.find();
}
```

## Implementation Details

### Architecture

```
┌─────────────────┐
│   CLI / SQL     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌──────────────┐
│   JdbcConfig    │────▶│ jdbc.properties│
│  (Alias Resolver)│     └──────────────┘
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   SQLExec       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌──────────────┐
│  GenericFile    │────▶│  JdbcTable   │
│   (Interface)   │     │ (implements) │
└─────────────────┘     └──────┬───────┘
                               │
                               ▼
                        ┌──────────────┐
                        │ JDBC Driver  │
                        │ (MySQL, H2,  │
                        │  PostgreSQL) │
                        └──────────────┘
```

### Key Classes

- **`GenericFile`**: Interface for all data sources (files and JDBC)
- **`JdbcTable`**: JDBC implementation of GenericFile
- **`JdbcConfig`**: Configuration and alias management
- **`SQLExec`**: SQL execution engine with JDBC support

### Limitations

- **Read-mostly**: Current implementation is optimized for SELECT operations
- **Single Table**: Each JDBC connection references one table at a time
- **No Joins**: Multi-table joins must be done at the JDBC level (in the SQL query)
- **Transaction Support**: Limited to auto-commit mode

### Future Enhancements

- [ ] Write operations (INSERT, UPDATE, DELETE) to JDBC
- [ ] Transaction support
- [ ] Connection pooling
- [ ] Prepared statement caching
- [ ] Multi-table JDBC queries
- [ ] SSL/TLS connection support
- [ ] Authentication via external credential store

## Troubleshooting

### Driver Not Found

```
Error: Unknown JDBC URL (cannot detect driver): jdbc:mysql://...
```

**Solution**: Add the JDBC driver JAR to classpath or copy to `lib/` directory.

### Connection Refused

```
Error: Failed to open JDBC connection: jdbc:mysql://localhost:3306/mydb
```

**Solutions**:
- Check if the database server is running
- Verify host, port, and database name
- Check firewall settings
- Verify credentials

### Table Not Found

```
Error: JDBC URI must include 'table' query parameter
```

**Solution**: Add `?table=tablename` to the JDBC URI or use an alias with `{table}` placeholder.

### Configuration Not Loaded

**Solution**: Ensure `jdbc.properties` exists in one of the searched locations:
```bash
# Check file exists
ls -la ~/.flintdb/jdbc.properties
ls -la ./jdbc.properties

# Set explicit path
export JAVA_OPTS="-Dflintdb.jdbc.config=/path/to/jdbc.properties"
```

## Security Considerations

### Password Protection

1. **Use configuration files** instead of command-line arguments (avoid shell history)
2. **Set proper file permissions**:
   ```bash
   chmod 600 ~/.flintdb/jdbc.properties
   ```
3. **Use environment variables** for sensitive values:
   ```properties
   mydb = jdbc:mysql://localhost:3306/mydb?user=${DB_USER}&password=${DB_PASS}&table={table}
   ```

### Network Security

- Use SSL/TLS for production connections
- Limit database user permissions (read-only when possible)
- Use firewall rules to restrict database access
- Consider SSH tunneling for remote connections:
  ```bash
  ssh -L 3306:localhost:3306 user@remote-server
  ```

## Performance Tips

1. **Use LIMIT** for large tables:
   ```bash
   ./bin/flintdb "SELECT * FROM @prod:logs LIMIT 1000"
   ```

2. **Filter at source** with WHERE clauses:
   ```bash
   ./bin/flintdb "SELECT * FROM @mydb:orders WHERE created_at > '2024-01-01'"
   ```

3. **Export to binary formats** for better compression:
   ```bash
   ./bin/flintdb "INSERT INTO data.flintdb FROM @prod:logs"
   ./bin/flintdb "INSERT INTO archive.parquet FROM @prod:events"
   ```

4. **Use connection pooling** for repeated queries (future feature)

## Examples

See the [examples](examples/) directory for complete examples:

- `jdbc-mysql-example.sh` - MySQL integration example
- `jdbc-export.sh` - Data export from JDBC to files
- `jdbc-pipeline.sh` - Multi-stage data pipeline with JDBC

## Contributing

Contributions are welcome! Areas for improvement:

- Additional JDBC driver support
- Write operation implementation
- Performance optimizations
- Documentation improvements

## License

Same as FlintDB main project.
