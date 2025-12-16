package flint.db;

import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;


/**
 * SQL Execution Engine
 * 
 * Handles SQL statement parsing and execution for FlintDB.
 * Supports SELECT, INSERT, INSERT FROM, UPDATE, DELETE, DROP, DESCRIBE, META, and SHOW TABLES operations.
 * Works with both binary table files (.flintdb) and generic text formats (TSV, CSV, Parquet, JSONL).
 * 
 * Based on sql_exec.c implementation.
 */
public final class SQLExec {

    // Table pool for connection reuse (similar to C implementation)
    private static final Map<String, PooledTable> tablePool = new ConcurrentHashMap<>();
    
    static class PooledTable {
        final String path;
        Table table;
        int mode;
        int refCount;
        long lastUsed;
        final ReentrantLock lock;
        
        PooledTable(String path, Table table, int mode) {
            this.path = path;
            this.table = table;
            this.mode = mode;
            this.refCount = 1;
            this.lastUsed = System.currentTimeMillis();
            this.lock = new ReentrantLock();
        }
    }

    /**
     * Execute SQL statement and return result
     * 
     * @param sql SQL statement
     * @return SQLResult object containing affected rows or result cursor
     * @throws DatabaseException on SQL execution error
     */
    public static SQLResult execute(final String sql) throws DatabaseException {
        return execute(sql, null);
    }

    /**
     * Execute SQL statement with existing transaction context and return result
     * 
     * @param sql SQL statement
     * @param transaction Current transaction context (null if no active transaction)
     * @return SQLResult object containing affected rows or result cursor
     * @throws DatabaseException on SQL execution error
     */
    public static SQLResult execute(final String sql, Transaction transaction) throws DatabaseException {
        if (sql == null || sql.trim().isEmpty()) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION,"SQL statement is null or empty");
        }

        SQL q = SQL.parse(sql);
        //System.err.println("[SQLExec.execute] SQL: " + sql.substring(0, Math.min(100, sql.length())));

        // SHOW TABLES doesn't operate on a single table file
        if (q.statement().toUpperCase().startsWith("SHOW") 
            && q.object() != null 
            && q.object().toUpperCase().startsWith("TABLES")) {
            return showTables(q);
        }

        String statement = q.statement().toUpperCase();
        //System.err.println("[SQLExec.execute] Statement: " + statement + ", Table: " + q.table());
        
        // Handle transaction commands (BEGIN TRANSACTION, COMMIT, ROLLBACK)
        if ("BEGIN".equals(statement)) {
            // System.err.println("[SQLExec.execute] Calling beginTransaction for table: " + q.table());
            return beginTransaction(q);
        } else if ("COMMIT".equals(statement)) {
            // System.err.println("[SQLExec.execute] Calling commitTransaction");
            //return commitTransaction(q);
        } else if ("ROLLBACK".equals(statement)) {
            // System.err.println("[SQLExec.execute] Calling rollbackTransaction");
            return rollbackTransaction(q);
        }
        
        String table = q.table();
        if (table == null || table.isEmpty()) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION,"Table name is required");
        }
        
        // Resolve JDBC aliases (e.g., @mydb:users -> jdbc:mysql://...?table=users)
        table = JdbcConfig.resolve(table);
        
        String fmt = format(table);
        if (fmt == null) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "Unsupported file format for table: " + table);
        }

        long affected = 0;

        if ("SELECT".equals(statement) && q.into() == null) {
            // JDBC doesn't need file existence check
            if (!"jdbc".equals(fmt)) {
                File file = new File(table);
                if (!file.exists())
                    throw new DatabaseException(ErrorCode.INVALID_OPERATION, "Table file does not exist: " + table);
                if (file.isDirectory())
                    throw new DatabaseException(ErrorCode.INVALID_OPERATION, "Table path is a directory, not a file: " + table);
            }

            if (fmt.equals(Meta.PRODUCT_NAME_LC))
                return select(q);
            else if (fmt.equals("jdbc"))
                return selectJDBC(q);
            else
                return selectGF(q);
        } else if ("SELECT".equals(statement) && q.into() != null) {
            affected = selectInto(q);
        } else if (("INSERT".equals(statement) || "REPLACE".equals(statement)) && q.from() == null) {
            if (!fmt.equals(Meta.PRODUCT_NAME_LC) && !fmt.equals("jdbc"))
                throw new DatabaseException(ErrorCode.INVALID_OPERATION, "INSERT operation not supported for read-only file formats: " + table);

            if (fmt.equals("jdbc"))
                affected = insertJDBC(q);
            else
                affected = insert(q);
        } else if (("INSERT".equals(statement) || "REPLACE".equals(statement)) && q.from() != null) {
            affected = insertFrom(q);

        } else if ("UPDATE".equals(statement)) {
            if (!fmt.equals(Meta.PRODUCT_NAME_LC) && !fmt.equals("jdbc"))
                throw new DatabaseException(ErrorCode.INVALID_OPERATION, "UPDATE operation not supported for read-only file formats: " + table);
            
            if (fmt.equals("jdbc"))
                affected = updateJDBC(q);
            else
                affected = update(q);
            
        } else if ("DELETE".equals(statement)) {
            if (!fmt.equals(Meta.PRODUCT_NAME_LC) && !fmt.equals("jdbc"))
                throw new DatabaseException(ErrorCode.INVALID_OPERATION, "DELETE operation not supported for read-only file formats: " + table);
            
            if (fmt.equals("jdbc"))
                affected = deleteJDBC(q);
            else
                affected = delete(q);
            
        } else if ("CREATE".equals(statement)) {
            affected = createTable(q);
            
        } else if ("ALTER".equals(statement)) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION,"ALTER TABLE is not supported yet");
            
        } else if ("DROP".equals(statement)) {
            try {
                File file = new File(table);
                if (fmt.equals(Meta.PRODUCT_NAME_LC)) {
                    Table.drop(file);
                } else {
                    GenericFile.drop(file);
                }
                affected = 1;
            } catch (IOException e) {
                throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to drop table: " + table, e);
            }
            
        } else if ("DESCRIBE".equals(statement) || "DESC".equals(statement)) {
            return describeTable(q);

        } else if ("META".equals(statement)) {
            return describeTableMeta(q);
            
        } else {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "Unsupported SQL statement: " + q.statement());
        }

        return new SQLResult(affected);
    }

    /**
     * Detect file format from filename
     */
    static String format(final File f) {
        String n = f.getName().toLowerCase();
        if (n.endsWith(".tsv") || n.endsWith(".tsv.gz") || n.endsWith(".tsv.zip") || n.endsWith(".tbl") || n.endsWith(".tbl.gz"))
            return "tsv";
        if (n.endsWith(".csv") || n.endsWith(".csv.gz") || n.endsWith(".csv.zip"))
            return "csv";
        if (n.endsWith(".parquet"))
            return "parquet";
        if (n.endsWith(".jsonl") || n.endsWith(".jsonl.gz") || n.endsWith(".jsonl.zip"))
            return "jsonl";
        if (n.endsWith(".union"))
            return "union";
        if (n.endsWith(Meta.TABLE_NAME_SUFFIX) || n.endsWith(".flintdb"))
            return Meta.PRODUCT_NAME_LC;

        String fmt = System.getProperty("FLINTDB_FILEFORMAT_GZ", "").toLowerCase();
        if (("csv".equals(fmt) || "tsv".equals(fmt) || "jsonl".equals(fmt)) && (n.endsWith(".gz") || n.endsWith(".zip")))
            return fmt;
        return null;
    }

    /**
     * Detect format from path string (supports JDBC URIs)
     */
    static String format(final String path) {
        if (path == null || path.isEmpty())
            return null;
        if (path.startsWith("jdbc:"))
            return "jdbc";
        return format(new File(path));
    }

    /**
     * Open or borrow table from pool (for binary tables)
     */
    static Table borrowTable(String path) throws IOException {
        return borrowTable(path, Table.OPEN_WRITE);
    }

    /**
     * Open or borrow table from pool with requested mode.
     *
     * - READ requests may reuse an existing WRITE table.
     * - WRITE requests will upgrade an existing READ table by reopening it.
     */
    static Table borrowTable(String path, int mode) throws IOException {
        synchronized (tablePool) {
            PooledTable pt = tablePool.get(path);
            if (pt != null) {
                // Try to acquire lock (will succeed if same thread holds it - reentrant)
                pt.lock.lock();
                try {
                    boolean needWrite = (Table.OPEN_WRITE & mode) > 0;
                    boolean hasWrite = (Table.OPEN_WRITE & pt.mode) > 0;
                    if (needWrite && !hasWrite) {
                        try {
                            pt.table.close();
                        } catch (Exception ignore) {
                            // ignore close errors during upgrade
                        }
                        pt.table = Table.open(new File(path), Table.OPEN_WRITE);
                        pt.mode = Table.OPEN_WRITE;
                    }
                    pt.refCount++;
                    pt.lastUsed = System.currentTimeMillis();
                    return pt.table;
                } catch (Exception e) {
                    pt.lock.unlock();
                    throw e;
                }
            }

            Table t = Table.open(new File(path), mode);
            pt = new PooledTable(path, t, mode);
            pt.lock.lock(); // Lock for first borrow
            tablePool.put(path, pt);
            return t;
        }
    }

    /**
     * Return table to pool
     */
    static void returnTable(String path) throws IOException {
        synchronized (tablePool) {
            PooledTable pt = tablePool.get(path);
            if (pt != null) {
                pt.refCount--;
                pt.lastUsed = System.currentTimeMillis();
                if (pt.refCount == 0) {
                    try {
                        pt.table.close();
                        tablePool.remove(path);
                    } finally {
                        pt.lock.unlock(); // Release lock when closing
                    }
                } else {
                    pt.lock.unlock(); // Release one lock level
                }
            }
        }
    }

    /**
     * INSERT INTO table
     */
    static long insert(SQL q) throws DatabaseException {
        String[] columns = q.columns();
        String[] values = q.values();
        boolean upsert = "REPLACE".equalsIgnoreCase(q.statement());
        
        if (values == null) {
            throw new DatabaseException(ErrorCode.COLUMN_MISMATCH, "No values specified for INSERT operation");
        }

        Table table = null;
        try {
            table = borrowTable(q.table());
            Meta meta = table.meta();
            
            Row row;
            
            // Support two modes:
            // 1. INSERT INTO table (col1, col2, ...) VALUES (val1, val2, ...)
            // 2. INSERT INTO table VALUES (val1, val2, ...) - values for all columns in order
            
            if (columns == null || columns.length == 0) {
                // Mode 2: No columns specified, values must match all table columns in order
                if (values.length != meta.columns().length) {
                    throw new DatabaseException(ErrorCode.COLUMN_MISMATCH, "Number of values (" + values.length + 
                        ") does not match number of table columns (" + meta.columns().length + ")");
                }
                
                row = Row.create(meta);
                
                // Set values in order for all columns
                for (int i = 0; i < meta.columns().length; i++) {
                    String colValue = values[i];
                    
                    if (colValue == null || "NULL".equalsIgnoreCase(colValue)) {
                        row.set(i, null);
                    } else {
                        row.set(i, colValue);
                    }
                }
            } else {
                // Mode 1: Columns specified, map values to specified columns
                if (columns.length != values.length) {
                    throw new DatabaseException(ErrorCode.COLUMN_MISMATCH, "Number of values (" + values.length + 
                        ") does not match number of columns (" + columns.length + ")");
                }
                
                row = Row.create(meta);
                
                for (int i = 0; i < columns.length; i++) {
                    String colName = columns[i];
                    String colValue = values[i];
                    
                    if (colValue == null || "NULL".equalsIgnoreCase(colValue)) {
                        row.set(colName, null);
                    } else {
                        row.set(colName, colValue);
                    }
                }
            }
            
            // Check if there's an active transaction for this table
            Transaction tx = activeTransactions.get().get(q.table());
            
            long rowid;
            if (tx != null) {
                rowid = table.apply(row, upsert, tx);
            } else {
                rowid = table.apply(row, upsert);
            }
            
            if (rowid < 0) {
                throw new DatabaseException(ErrorCode.TRANSACTION_FAILED, "Failed to insert row");
            }
            
            return 1;
        } catch (DatabaseException e) {
            throw e;
        } catch (IOException e) {
            // If the cause is a DatabaseException, throw it directly to preserve error code
            if (e.getCause() instanceof DatabaseException) {
                throw (DatabaseException) e.getCause();
            }
            throw new DatabaseException(ErrorCode.TRANSACTION_FAILED, "INSERT failed: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.TRANSACTION_FAILED, "INSERT failed: " + e.getMessage(), e);
        } finally {
            if (table != null) {
                try {
                    returnTable(q.table());
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }

    /**
     * SELECT ... INTO target
     * 
     * Executes a SELECT query and writes the result to a target file.
     * Supports:
     * - SELECT * FROM source INTO target (copies all columns)
     * - SELECT col1, col2 FROM source INTO target (copies specified columns)
     * - SELECT col1, col2 FROM source WHERE condition INTO target
     * 
     * Target can be any writable format: .flintdb, .tsv, .tsv.gz, .csv, .parquet, .jsonl
     */
    static long selectInto(SQL q) throws DatabaseException {
        final boolean upsert = true; // always upsert for SELECT ... INTO
        String into = JdbcConfig.resolve(q.into());
        long affected = 0;
        
        if (into == null || into.isEmpty()) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "INTO clause is required for SELECT INTO operation");
        }
        
        // Check target format
        File targetFile = new File(into);
        String targetFmt = format(targetFile);
        
        if (targetFmt == null) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "Unsupported target file format: " + into);
        }
        
        if (targetFile.exists()) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "Target file already exists: " + into);
        }
        
        SQLResult srcResult = null;
        GenericFile gf = null;
        Table table = null;
        
        try {
            // Execute SELECT query (without INTO clause to avoid recursion)
            SQL selectQuery = SQL.parse(buildSelectWithoutInto(q));
            
            // Get source format and execute appropriate select
            String source = JdbcConfig.resolve(q.table());
            String sourceFmt = format(source);
            
            if (Meta.PRODUCT_NAME_LC.equals(sourceFmt)) {
                srcResult = select(selectQuery);
            } else if ("jdbc".equals(sourceFmt)) {
                srcResult = selectJDBC(selectQuery);
            } else {
                srcResult = selectGF(selectQuery);
            }
            
            if (srcResult.getCursor() == null) {
                throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to execute SELECT query");
            }
            
            // Get first row to determine metadata
            Row firstRow = srcResult.getCursor().next();
            if (firstRow == null) {
                // Empty result - create empty target file
                Meta emptyMeta = new Meta(targetFile.getName());
                Column[] columns = new Column[srcResult.getColumns().length];
                for (int i = 0; i < columns.length; i++) {
                    columns[i] = new Column(srcResult.getColumns()[i], Column.TYPE_STRING, (short) 256, (short) 0, false, null, null);
                }
                emptyMeta.columns(columns);
                
                if (Meta.PRODUCT_NAME_LC.equals(targetFmt)) {
                    // Create .desc file and empty binary table
                    emptyMeta.indexes(new Index[] {
                        new Table.PrimaryKey(new String[] { columns[0].name() })
                    });
                    Meta.make(targetFile, emptyMeta);
                } else {
                    gf = GenericFile.create(targetFile, columns);
                    gf.close();
                }
                return 0;
            }
            
            Meta sourceMeta = firstRow.meta();
            
            // Create target with source metadata
            if (Meta.PRODUCT_NAME_LC.equals(targetFmt)) {
                // Create .desc file first
                Meta targetMeta = new Meta(targetFile.getName());
                targetMeta.columns(sourceMeta.columns());
                
                // For binary tables, try to preserve indexes from source if available
                if (sourceMeta.indexes() != null && sourceMeta.indexes().length > 0) {
                    targetMeta.indexes(sourceMeta.indexes());
                } else {
                    // Create a default primary key on first column
                    targetMeta.indexes(new Index[] {
                        new Table.PrimaryKey(new String[] { sourceMeta.columns()[0].name() })
                    });
                }
                
                Meta.make(targetFile, targetMeta);
                table = borrowTable(into);
                
                // Write first row
                long rowid = table.apply(firstRow, upsert);
                if (rowid < 0) {
                    throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to write row to target: " + into);
                }
                affected++;
                
                // Write remaining rows
                Row row;
                while ((row = srcResult.getCursor().next()) != null) {
                    rowid = table.apply(row, upsert);
                    if (rowid < 0) {
                        throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to write row to target: " + into);
                    }
                    affected++;
                }
            } else {
                // Generic file target
                gf = GenericFile.create(targetFile, sourceMeta.columns());
                
                // Write first row
                long ok = gf.write(firstRow);
                if (ok < 0) {
                    throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to write row to target: " + into);
                }
                affected++;
                
                // Write remaining rows
                Row row;
                while ((row = srcResult.getCursor().next()) != null) {
                    ok = gf.write(row);
                    if (ok < 0) {
                        throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to write row to target: " + into);
                    }
                    affected++;
                }
            }
            
            return affected;
            
        } catch (DatabaseException e) {
            throw e;
        } catch (IOException e) {
            // If the cause is a DatabaseException, throw it directly to preserve error code
            if (e.getCause() instanceof DatabaseException) {
                throw (DatabaseException) e.getCause();
            }
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "SELECT INTO failed: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "SELECT INTO failed: " + e.getMessage(), e);
        } finally {
            if (srcResult != null && srcResult.getCursor() != null) {
                try {
                    srcResult.getCursor().close();
                } catch (Exception e) {
                    // Ignore
                }
            }
            if (gf != null) {
                try {
                    gf.close();
                } catch (Exception e) {
                    // Ignore
                }
            }
            if (table != null) {
                try {
                    returnTable(into);
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }
    
    /**
     * Build SELECT statement without INTO clause
     */
    static String buildSelectWithoutInto(SQL q) {
        StringBuilder sb = new StringBuilder("SELECT ");
        
        if (q.distinct()) {
            sb.append("DISTINCT ");
        }
        
        // Columns
        String[] columns = q.columns();
        if (columns != null && columns.length > 0) {
            for (int i = 0; i < columns.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(columns[i]);
            }
        } else {
            sb.append("*");
        }
        
        // FROM
        sb.append(" FROM ").append(q.table());
        
        // USE INDEX
        if (q.index() != null && !q.index().isEmpty()) {
            sb.append(" USE INDEX(").append(q.index()).append(")");
        }
        
        // WHERE
        if (q.where() != null && !q.where().isEmpty()) {
            sb.append(" WHERE ").append(q.where());
        }
        
        // GROUP BY
        if (q.groupby() != null && !q.groupby().isEmpty()) {
            sb.append(" GROUP BY ").append(q.groupby());
        }
        
        // ORDER BY
        if (q.orderby() != null && !q.orderby().isEmpty()) {
            sb.append(" ORDER BY ").append(q.orderby());
        }
        
        // LIMIT
        if (q.limit() != null && !q.limit().isEmpty()) {
            sb.append(" LIMIT ").append(q.limit());
        }
        
        return sb.toString();
    }

    /**
     * INSERT INTO table FROM source
     * 
     * Supports:
     * - INSERT INTO target FROM source (copies all columns in order)
     * - INSERT INTO target (col1, col2) FROM source (maps source columns to specified target columns)
     */
    static long insertFrom(SQL q) throws DatabaseException {
        final boolean upsert = "REPLACE".equalsIgnoreCase(q.statement());
        String target = JdbcConfig.resolve(q.table());
        String from = JdbcConfig.resolve(q.from());
        long affected = 0;
        
        if (from == null || from.isEmpty()) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "FROM clause is required for INSERT FROM operation");
        }
        
        // Check if source is JDBC or file
        boolean isJdbcSource = from.startsWith("jdbc:");
        File targetFile = new File(target);
        
        if (!isJdbcSource) {
            File sourceFile = new File(from);
            if (!sourceFile.exists()) {
                throw new DatabaseException(ErrorCode.INVALID_OPERATION,"Source file for INSERT ... FROM does not exist: " + from);
            }
        }
        
        String targetFmt = format(targetFile);
        if (targetFile.exists() && !Meta.PRODUCT_NAME_LC.equals(targetFmt)) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "INSERT ... FROM operation not supported for read-only file formats: " + target);
        }
        
        // Load target metadata
        File descFile = new File(target + Meta.META_NAME_SUFFIX);
        Meta targetMeta;
        try {
            targetMeta = Meta.read(descFile);
        } catch (IOException e) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "Failed to read target table metadata: " + descFile, e);
        }
        
        if (targetMeta.columns() == null || targetMeta.columns().length == 0) {
            throw new DatabaseException(ErrorCode.COLUMN_MISMATCH, "No columns found in metadata for target table: " + descFile);
        }
        
        // Binary tables require at least one index (primary key)
        if (Meta.PRODUCT_NAME_LC.equals(targetFmt) && (targetMeta.indexes() == null || targetMeta.indexes().length == 0)) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "INSERT ... FROM requires target table to have at least one index: " + target);
        }
        
        // Build column mapping if columns were specified
        int[] colMapping = null;
        String selectSql;
        
        if (q.columns() != null && q.columns().length > 0) {
            colMapping = new int[q.columns().length];
            
            // Map each specified target column to its index in the target meta
            for (int i = 0; i < q.columns().length; i++) {
                int targetIdx = targetMeta.column(q.columns()[i]);
                if (targetIdx < 0) {
                    throw new DatabaseException(ErrorCode.COLUMN_MISMATCH, "Column not found in target table: " + q.columns()[i]);
                }
                colMapping[i] = targetIdx;
            }
            
            // Build SELECT with only the specified columns from source
            StringBuilder sb = new StringBuilder("SELECT ");
            for (int i = 0; i < q.columns().length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(q.columns()[i]);
            }
            sb.append(" FROM ").append(from);
            selectSql = sb.toString();
        } else {
            // No columns specified - select all
            selectSql = "SELECT * FROM " + from;
        }
        if (q.where() != null && !q.where().isEmpty()) 
            selectSql += " WHERE " + q.where();
        if (q.orderby() != null && !q.orderby().isEmpty()) 
            selectSql += " ORDER BY " + q.orderby();
        if (q.limit() != null && !q.limit().isEmpty()) 
            selectSql += " LIMIT " + q.limit();

        // System.out.println("INSERT FROM SQL: " + selectSql);

        SQLResult srcResult = null;
        Table table = null;
        GenericFile gf = null;
        
        try {
            srcResult = execute(selectSql);
            
            if (srcResult.getCursor() == null) {
                throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to read source data from file: " + from);
            }
            
            // When columns are specified, the source SELECT already projected the right columns
            // No need to check column count mismatch - it's already matched by the SELECT projection
            
            // Open target for writing
            if (Meta.PRODUCT_NAME_LC.equals(targetFmt)) {
                table = borrowTable(target);
                
                // Commit transaction periodically for large bulk inserts to enable WAL checkpointing
                // This prevents WAL file from growing too large and improves performance
                // Can be configured via FLINTDB_BULK_INSERT_COMMIT_INTERVAL environment variable
                final int BULK_INSERT_COMMIT_INTERVAL = Integer.parseInt(
                    System.getenv().getOrDefault("FLINTDB_BULK_INSERT_COMMIT_INTERVAL", "10000")
                );
                Transaction tx = table.begin();
                int rowsInCurrentTx = 0;
                
                // Insert rows from source
                final Column[] targetCols = targetMeta.columns();
                final Object[] array = new Object[targetCols.length]; // reusable array
                Row srcRow;
                while ((srcRow = srcResult.getCursor().next()) != null) {
                    
                    if (colMapping != null) {
                        // Map selected columns from source to target positions
                        // ultra fast path: direct array access + cast
                        final Object[] srcArray = srcRow.array();
                        for (int i = 0; i < colMapping.length; i++) {
                            final int targetIdx = colMapping[i];
                            array[targetIdx] = Row.cast(srcArray[i], targetCols[targetIdx].type(), targetCols[targetIdx].precision());
                        }
                    } else {
                        // No column mapping: cast entire row to target meta
                        // ultra fast path: direct array access + cast
                        final Object[] srcArray = srcRow.array();
                        for (int i = 0; i < targetCols.length; i++) {
                            array[i] = Row.cast(srcArray[i], targetCols[i].type(), targetCols[i].precision());
                        }
                    }
                    
                    final long rowid = table.apply(new RowImpl(targetMeta, array), upsert, tx);
                    if (rowid < 0) {
                        throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to insert row into target table: " + target);
                    }
                    affected++;
                    rowsInCurrentTx++;
                    
                    // Periodically commit and restart transaction for bulk inserts
                    if (rowsInCurrentTx >= BULK_INSERT_COMMIT_INTERVAL) {
                        tx.commit();
                        tx = table.begin();
                        rowsInCurrentTx = 0;
                    }
                }
                
                // Commit remaining rows
                if (tx != null) {
                    tx.commit();
                }
            } else {
                // Generic file target - create it with target meta columns
                gf = GenericFile.create(targetFile, targetMeta.columns());
                
                // Insert rows from source
                final Column[] targetCols = targetMeta.columns();
                final Object[] array = new Object[targetCols.length]; // reusable array
                Row srcRow;
                while ((srcRow = srcResult.getCursor().next()) != null) {
                    
                    if (colMapping != null) {
                        // Map selected columns from source to target positions
                        // ultra fast path: direct array access + cast
                        final Object[] srcArray = srcRow.array();
                        for (int i = 0; i < colMapping.length; i++) {
                            final int targetIdx = colMapping[i];
                            array[targetIdx] = Row.cast(srcArray[i], targetCols[targetIdx].type(), targetCols[targetIdx].precision());
                        }
                    } else {
                        // No column mapping: cast entire row to target meta
                        // ultra fast path: direct array access + cast
                        final Object[] srcArray = srcRow.array();
                        for (int i = 0; i < targetCols.length; i++) {
                            array[i] = Row.cast(srcArray[i], targetCols[i].type(), targetCols[i].precision());
                        }
                    }
                    
                    final long ok = gf.write(new RowImpl(targetMeta, array));
                    if (ok < 0) {
                        throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to insert row into target generic file: " + target);
                    }
                    affected++;
                }
            }
            
            return affected;
        } catch (DatabaseException e) {
            throw e;
        } catch (IOException e) {
            // If the cause is a DatabaseException, throw it directly to preserve error code
            if (e.getCause() instanceof DatabaseException) {
                throw (DatabaseException) e.getCause();
            }
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "INSERT FROM failed: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "INSERT FROM failed: " + e.getMessage(), e);
        } finally {
            if (srcResult != null && srcResult.getCursor() != null) {
                try {
                    srcResult.getCursor().close();
                } catch (Exception e) {
                    // Ignore
                }
            }
            if (gf != null) {
                try {
                    gf.close();
                } catch (Exception e) {
                    // Ignore
                }
            }
            if (table != null) {
                try {
                    returnTable(target);
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }

    /**
     * UPDATE table
     */
    static long update(SQL q) throws DatabaseException {
        if (q.where() == null || q.where().trim().isEmpty()) {
            throw new DatabaseException(ErrorCode.COLUMN_MISMATCH, "UPDATE operation requires a WHERE clause to prevent full table updates");
        }
        
        String[] columns = q.columns();
        String[] values = q.values();
        
        if (columns == null || values == null || columns.length == 0) {
            throw new DatabaseException(ErrorCode.COLUMN_MISMATCH, "No columns/values specified for UPDATE operation");
        }
        
        if (columns.length != values.length) {
            throw new DatabaseException(ErrorCode.COLUMN_MISMATCH, "Number of columns does not match number of values");
        }

        Table table = null;
        int affected = 0;
        
        try {
            table = borrowTable(q.table());
            Meta meta = table.meta();
            
            // Check if there's an active transaction for this table
            Transaction tx = activeTransactions.get().get(q.table());
            System.err.println("[update] Table: " + q.table() + ", table instance: " + System.identityHashCode(table) + ", activeTransactions size: " + activeTransactions.get().size() + ", tx: " + (tx != null ? tx.id() : "null"));
            
            String where = buildIndexableWhere(meta, q);
            System.err.println("[update] WHERE clause: " + where);
            try(Cursor<Long> cursor = table.find(where)){
                System.err.println("[update] Got cursor from table.find()");
                int foundCount = 0;
                for (long rowid; (rowid = cursor.next()) > -1;) {
                    foundCount++;
                    System.err.println("[update] Reading rowid: " + rowid + " (found #" + foundCount + ")");
                    Row readRow = table.read(rowid);
                    System.err.println("[update] Read result: " + (readRow != null ? "Row found" : "NULL"));
                    if (readRow == null) {
                        System.err.println("[update] WARNING: table.read(" + rowid + ") returned null, skipping");
                        continue;
                    }
                    Row row = readRow.copy();
                    
                    for (int i = 0; i < columns.length; i++) {
                        String colName = columns[i];
                        String colValue = values[i];
                        
                        if (colValue == null || "NULL".equalsIgnoreCase(colValue)) {
                            row.set(colName, null);
                        } else {
                            row.set(colName, colValue);
                        }
                    }
                    
                    if (tx != null) {
                        table.apply(rowid, row, tx);
                    } else {
                        table.apply(rowid, row);
                    }
                    affected++;
                }
            }
            
            return affected;
            
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "UPDATE failed: " + e.getMessage(), e);
        } finally {
            if (table != null) {
                try {
                    returnTable(q.table());
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }

    /**
     * DELETE FROM table
     */
    static long delete(SQL q) throws DatabaseException {
        if (q.where() == null || q.where().trim().isEmpty()) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "DELETE operation requires a WHERE clause to prevent full table deletions");
        }

        Table table = null;
        int affected = 0;
        
        try {
            table = borrowTable(q.table());
            Meta meta = table.meta();
            
            // Check if there's an active transaction for this table
            Transaction tx = activeTransactions.get().get(q.table());
            
            String where = buildIndexableWhere(meta, q);
            try(Cursor<Long> cursor = table.find(where)){
                for (long rowid; (rowid = cursor.next()) > -1;) {
                    long ok;
                    if (tx != null) {
                        ok = table.delete(rowid, tx);
                    } else {
                        ok = table.delete(rowid);
                    }
                    if (ok < 0) {
                        throw new DatabaseException(ErrorCode.NOT_FOUND, "Failed to delete row with ID: " + rowid);
                    }
                    affected++;
                }
            }
            
            return affected;
            
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "DELETE failed: " + e.getMessage(), e);
        } finally {
            if (table != null) {
                try {
                    returnTable(q.table());
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }

    /**
     * Build WHERE clause with index hint if applicable
     */
    static String buildIndexableWhere(Meta meta, SQL q) {
        StringBuilder sb = new StringBuilder();
        
        if (q.index() != null && !q.index().trim().isEmpty()) {
            sb.append("USE INDEX(").append(q.index()).append(")");
            if (q.where() != null && !q.where().isEmpty()) {
                sb.append(" ");
            }
        }
        
        if (q.where() != null && !q.where().isEmpty()) {
            if (sb.length() > 0 && !sb.toString().endsWith(" ")) {
                sb.append(" ");
            }
            sb.append("WHERE ").append(q.where());
        }
        
        return sb.toString();
    }

    /**
     * SELECT from binary table
     */
    static SQLResult select(SQL q) throws DatabaseException {
        Table table = null;
        
        try {
            table = borrowTable(q.table(), Table.OPEN_WRITE);
            Meta meta = table.meta();
            
            // Fast path: SELECT COUNT(*) with no WHERE/GROUP/ORDER/DISTINCT
            if (isFastCount(q)) {
                return selectFastCount(q, table);
            }
            
            String where = buildIndexableWhere(meta, q);
            Cursor<Long> cursor = table.find(where);
            
            // Check for GROUP BY or aggregate functions
            if (hasGroupByOrAggregate(q)) {
                // Wrap Cursor<Long> as Cursor<Row>
                Cursor<Row> rowCursor = new TableIdToCursor(table, cursor);
                return selectGroupBy(q, meta, rowCursor, q.table());
            }
            
            // Check for ORDER BY
            if (q.orderby() != null && !q.orderby().trim().isEmpty()) {
                // Wrap Cursor<Long> as Cursor<Row>
                String[] outputCols = expandWildcard(q.columns(), meta);
                Cursor<Row> rowCursor = new TableRowCursor(table, q.table(), cursor, outputCols, Filter.NOLIMIT, q.distinct());
                return selectOrderBy(q, meta, rowCursor, outputCols);
            }
            
            // Simple SELECT with optional DISTINCT and LIMIT
            return selectSimple(q, table, cursor);
            
        } catch (Exception e) {
            if (table != null) {
                try {
                    returnTable(q.table());
                } catch (IOException ex) {
                    // Ignore
                }
            }
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "SELECT failed: " + e.getMessage(), e);
        }
    }

    /**
     * SELECT from JDBC
     */
    static SQLResult selectJDBC(SQL q) throws DatabaseException {
        JdbcTable jdbcTable = null;
        
        try {
            String table = q.table();
            java.net.URI uri = new java.net.URI(table);
            jdbcTable = JdbcTable.open(uri);
            
            // Get actual table name from URI
            String ssp = uri.getSchemeSpecificPart();
            String queryString = null;
            if (ssp != null) {
                int qmark = ssp.indexOf('?');
                if (qmark >= 0) {
                    queryString = ssp.substring(qmark + 1);
                }
            }
            
            String tableName = JdbcTable.split(queryString).get("table");
            if (tableName == null || tableName.isEmpty()) {
                throw new DatabaseException(ErrorCode.INVALID_OPERATION,"JDBC URI must include 'table' query parameter");
            }
            
            // Build proper SQL query for JDBC
            StringBuilder sql = new StringBuilder("SELECT ");
            
            // Columns
            String[] columns = q.columns();
            if (columns != null && columns.length > 0) {
                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(columns[i]);
                }
            } else {
                sql.append("*");
            }
            
            sql.append(" FROM ").append(tableName);
            
            // WHERE
            if (q.where() != null && !q.where().isEmpty()) {
                sql.append(" WHERE ").append(q.where());
            }
            
            // GROUP BY
            if (q.groupby() != null && !q.groupby().isEmpty()) {
                sql.append(" GROUP BY ").append(q.groupby());
            }
            
            // ORDER BY
            if (q.orderby() != null && !q.orderby().isEmpty()) {
                sql.append(" ORDER BY ").append(q.orderby());
            }
            
            // LIMIT
            if (q.limit() != null && !q.limit().isEmpty()) {
                sql.append(" LIMIT ").append(q.limit());
            }
            
            Cursor<Row> cursor = jdbcTable.executeQuery(sql.toString());
            
            // For JDBC, use the columns from the query directly
            // (not from table metadata, since GROUP BY/aggregates change the result columns)
            String[] outputCols;
            if (columns != null && columns.length > 0 && !"*".equals(columns[0])) {
                // Use the columns from the query
                outputCols = columns;
            } else {
                // For SELECT *, get metadata from table
                Meta meta = jdbcTable.meta();
                outputCols = expandWildcard(new String[]{"*"}, meta);
            }
            
            return new SQLResult(outputCols, cursor);
            
        } catch (Exception e) {
            if (jdbcTable != null) {
                try {
                    jdbcTable.close();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "SELECT from JDBC failed: " + e.getMessage(), e);
        }
    }

    /**
     * INSERT into JDBC
     */
    static long insertJDBC(SQL q) throws DatabaseException {
        JdbcTable jdbcTable = null;
        
        try {
            String table = q.table();
            java.net.URI uri = new java.net.URI(table);
            jdbcTable = JdbcTable.open(uri);
            
            // Get actual table name from URI
            String ssp = uri.getSchemeSpecificPart();
            String queryString = null;
            if (ssp != null) {
                int qmark = ssp.indexOf('?');
                if (qmark >= 0) {
                    queryString = ssp.substring(qmark + 1);
                }
            }
            
            String tableName = JdbcTable.split(queryString).get("table");
            if (tableName == null || tableName.isEmpty()) {
                throw new DatabaseException(ErrorCode.INVALID_OPERATION,"JDBC URI must include 'table' query parameter");
            }
            
            // Build proper INSERT SQL for JDBC
            StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName);
            
            String[] columns = q.columns();
            String[] values = q.values();
            
            if (values == null) {
                throw new DatabaseException(ErrorCode.COLUMN_MISMATCH,"No values specified for INSERT operation");
            }
            
            // Columns
            if (columns != null && columns.length > 0) {
                sql.append(" (");
                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(columns[i]);
                }
                sql.append(")");
            }
            
            // Values
            sql.append(" VALUES (");
            for (int i = 0; i < values.length; i++) {
                if (i > 0) sql.append(", ");
                
                String value = values[i];
                if (value == null || "NULL".equalsIgnoreCase(value)) {
                    sql.append("NULL");
                } else if (value.startsWith("'") && value.endsWith("'")) {
                    // Already quoted string
                    sql.append(value);
                } else {
                    // Try to determine if it's a number
                    try {
                        Double.parseDouble(value);
                        sql.append(value);
                    } catch (NumberFormatException e) {
                        // Not a number, quote it
                        sql.append("'").append(value.replace("'", "''")).append("'");
                    }
                }
            }
            sql.append(")");
            
            int affected = jdbcTable.apply(sql.toString());
            jdbcTable.close();
            
            return affected;
            
        } catch (Exception e) {
            if (jdbcTable != null) {
                try {
                    jdbcTable.close();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "INSERT into JDBC failed: " + e.getMessage(), e);
        }
    }

    /**
     * UPDATE JDBC table
     */
    static long updateJDBC(SQL q) throws DatabaseException {
        JdbcTable jdbcTable = null;
        
        try {
            String table = q.table();
            java.net.URI uri = new java.net.URI(table);
            jdbcTable = JdbcTable.open(uri);
            
            // Get actual table name from URI
            String ssp = uri.getSchemeSpecificPart();
            String queryString = null;
            if (ssp != null) {
                int qmark = ssp.indexOf('?');
                if (qmark >= 0) {
                    queryString = ssp.substring(qmark + 1);
                }
            }
            
            String tableName = JdbcTable.split(queryString).get("table");
            if (tableName == null || tableName.isEmpty()) {
                throw new DatabaseException(ErrorCode.INVALID_OPERATION,"JDBC URI must include 'table' query parameter");
            }
            
            // Build proper UPDATE SQL for JDBC
            StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
            
            String[] columns = q.columns();
            String[] values = q.values();
            
            if (columns == null || values == null || columns.length == 0) {
                throw new DatabaseException(ErrorCode.COLUMN_MISMATCH,"No columns/values specified for UPDATE operation");
            }
            
            // SET clause
            for (int i = 0; i < columns.length; i++) {
                if (i > 0) sql.append(", ");
                sql.append(columns[i]).append(" = ");
                
                String value = values[i];
                if (value == null || "NULL".equalsIgnoreCase(value)) {
                    sql.append("NULL");
                } else if (value.startsWith("'") && value.endsWith("'")) {
                    sql.append(value);
                } else {
                    try {
                        Double.parseDouble(value);
                        sql.append(value);
                    } catch (NumberFormatException e) {
                        sql.append("'").append(value.replace("'", "''")).append("'");
                    }
                }
            }
            
            // WHERE clause
            if (q.where() != null && !q.where().isEmpty()) {
                sql.append(" WHERE ").append(q.where());
            }
            
            int affected = jdbcTable.apply(sql.toString());
            jdbcTable.close();
            
            return affected;
            
        } catch (Exception e) {
            if (jdbcTable != null) {
                try {
                    jdbcTable.close();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "UPDATE JDBC failed: " + e.getMessage(), e);
        }
    }

    /**
     * DELETE from JDBC table
     */
    static long deleteJDBC(SQL q) throws DatabaseException {
        JdbcTable jdbcTable = null;
        
        try {
            String table = q.table();
            java.net.URI uri = new java.net.URI(table);
            jdbcTable = JdbcTable.open(uri);
            
            // Get actual table name from URI
            String ssp = uri.getSchemeSpecificPart();
            String queryString = null;
            if (ssp != null) {
                int qmark = ssp.indexOf('?');
                if (qmark >= 0) {
                    queryString = ssp.substring(qmark + 1);
                }
            }
            
            String tableName = JdbcTable.split(queryString).get("table");
            if (tableName == null || tableName.isEmpty()) {
                throw new DatabaseException(ErrorCode.INVALID_OPERATION,"JDBC URI must include 'table' query parameter");
            }
            
            // Build proper DELETE SQL for JDBC
            StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName);
            
            // WHERE clause
            if (q.where() != null && !q.where().isEmpty()) {
                sql.append(" WHERE ").append(q.where());
            }
            
            int affected = jdbcTable.apply(sql.toString());
            jdbcTable.close();
            
            return affected;
            
        } catch (Exception e) {
            if (jdbcTable != null) {
                try {
                    jdbcTable.close();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "DELETE from JDBC failed: " + e.getMessage(), e);
        }
    }

    /**
     * SELECT from generic file (TSV, CSV, Parquet, JSONL)
     */
    static SQLResult selectGF(SQL q) throws DatabaseException {
        GenericFile gf = null;
        
        try {
            String table = q.table();
            File file = new File(table);
            gf = GenericFile.open(file);
            
            // Fast path: SELECT COUNT(*) for some formats
            if (isFastCount(q)) {
                // Try fast count if supported
                SQLResult fast = selectGFFastCount(q, gf);
                if (fast != null) {
                    return fast;
                }
            }
            
            // Build WHERE clause with keyword (consistent with buildIndexableWhere for Table)
            String where = "";
            if (q.where() != null && !q.where().isEmpty()) {
                where = "WHERE " + q.where();
            }
            
            Cursor<Row> cursor = gf.find(where);
            
            // Check for GROUP BY or aggregate functions
            if (hasGroupByOrAggregate(q)) {
                return selectGroupBy(q, gf.meta(), cursor, null);
            }
            
            // Check for ORDER BY
            if (q.orderby() != null && !q.orderby().trim().isEmpty()) {
                Meta meta = gf.meta();
                String[] outputCols = expandWildcard(q.columns(), meta);
                Cursor<Row> projCursor = new ProjectionCursor(cursor, gf, outputCols, Filter.NOLIMIT, q.distinct());
                return selectOrderBy(q, meta, projCursor, outputCols);
            }
            
            // Simple SELECT with optional DISTINCT and LIMIT
            return selectGFSimple(q, gf, cursor);
            
        } catch (Exception e) {
            if (gf != null) {
                try {
                    gf.close();
                } catch (Exception ex) {
                    // Ignore
                }
            }
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "SELECT failed: " + e.getMessage(), e);
        }
    }

    /**
     * Check if query is SELECT COUNT(*) without WHERE/GROUP/ORDER/DISTINCT
     */
    static boolean isFastCount(SQL q) {
        String[] cols = q.columns();
        if (cols == null || cols.length != 1) return false;
        
        String col = cols[0].toUpperCase().replaceAll("\\s+", "");
        if (!col.matches("COUNT\\([*10]\\).*")) return false;
        
        return (q.where() == null || q.where().trim().isEmpty())
            && (q.groupby() == null || q.groupby().trim().isEmpty())
            && (q.orderby() == null || q.orderby().trim().isEmpty())
            && !q.distinct();
    }

    /**
     * Fast COUNT(*) for binary tables
     */
    static SQLResult selectFastCount(SQL q, Table table) throws Exception {
        long rows = table.rows();
        
        // Apply LIMIT if specified
        Filter.Limit limit = Filter.MaxLimit.parse(q.limit());
        if (!limit.remains()) {
            rows = 0;
        } else {
            // Skip offset rows
            int skipped = 0;
            while (limit.skip() && skipped < rows) {
                skipped++;
            }
            rows = Math.max(0, rows - skipped);
        }
        
        String alias = SQL.extractAlias(q.columns()[0]);
        if (alias == null) alias = "COUNT(*)";
        
        List<Row> resultRows = new ArrayList<>();
        if (rows >= 0) {
            Meta resultMeta = new Meta("count");
            resultMeta.columns(new Column[]{
                new Column(alias, Column.TYPE_INT64, (short) 8, (short) 0, false, null, null)
            });
            Row row = Row.create(resultMeta);
            row.set(0, rows);
            resultRows.add(row);
        }
        
        Cursor<Row> cursor = new ListCursor(resultRows);
        String[] columnNames = new String[]{alias};
        
        returnTable(q.table());
        
        return new SQLResult(columnNames, cursor);
    }

    /**
     * Fast COUNT(*) for generic files
     */
    static SQLResult selectGFFastCount(SQL q, GenericFile gf) throws Exception {
        // For now, just return null (not implemented for all formats)
        return null;
    }

    /**
     * Check if query has GROUP BY or aggregate functions
     */
    static boolean hasGroupByOrAggregate(SQL q) {
        if (q.groupby() != null && !q.groupby().trim().isEmpty()) {
            return true;
        }
        
        String[] cols = q.columns();
        if (cols == null) return false;
        
        for (String col : cols) {
            String upper = col.toUpperCase().replaceAll("\\s+", "");
            if (upper.contains("COUNT(") || upper.contains("SUM(") || upper.contains("AVG(") 
                || upper.contains("MIN(") || upper.contains("MAX(") || upper.contains("FIRST(") 
                || upper.contains("LAST(")) {
                return true;
            }
        }
        
        return false;
    }

    /**
     * Simple SELECT (no GROUP BY, no ORDER BY)
     */
    static SQLResult selectSimple(SQL q, Table table, Cursor<Long> idCursor) throws Exception {
        Meta meta = table.meta();
        String[] cols = q.columns();
        
        // Expand * wildcard
        String[] outputCols;
        if (cols.length == 1 && "*".equals(cols[0])) {
            Column[] metaCols = meta.columns();
            outputCols = new String[metaCols.length];
            for (int i = 0; i < metaCols.length; i++) {
                outputCols[i] = metaCols[i].name();
            }
        } else {
            outputCols = cols;
        }
        
        Filter.Limit limit = Filter.MaxLimit.parse(q.limit());
        Cursor<Row> cursor = new TableRowCursor(table, q.table(), idCursor, outputCols, limit, q.distinct());
        
        return new SQLResult(outputCols, cursor);
    }

    /**
     * Simple SELECT for generic files
     */
    static SQLResult selectGFSimple(SQL q, GenericFile gf, Cursor<Row> rowCursor) throws Exception {
        Meta meta = gf.meta();
        String[] cols = q.columns();
        
        // Expand * wildcard
        String[] outputCols;
        if (cols.length == 1 && "*".equals(cols[0])) {
            Column[] metaCols = meta.columns();
            outputCols = new String[metaCols.length];
            for (int i = 0; i < metaCols.length; i++) {
                outputCols[i] = metaCols[i].name();
            }
        } else {
            outputCols = cols;
        }
        
        Filter.Limit limit = Filter.MaxLimit.parse(q.limit());
        Cursor<Row> cursor = new ProjectionCursor(rowCursor, gf, outputCols, limit, q.distinct());
        
        return new SQLResult(outputCols, cursor);
    }

    /**
     * SELECT with GROUP BY (unified for both binary tables and generic files)
     */
    static SQLResult selectGroupBy(SQL q, Meta meta, Cursor<Row> cursor, String tablePath) throws DatabaseException {
        try {
            if (q.columns().length == 1 && "*".equals(q.columns()[0])) {
                throw new DatabaseException(ErrorCode.COLUMN_MISMATCH,"SELECT * not supported with GROUP BY or aggregate functions");
            }

            // Parse GROUP BY columns
            String[] groupCols = parseGroupByColumns(q.groupby());
            
            // Create Aggregate.Groupby array
            Aggregate.Groupby[] groupbys = new Aggregate.Groupby[groupCols.length];
            for (int i = 0; i < groupCols.length; i++) {
                int colIdx = meta.column(groupCols[i]);
                short colType = Column.TYPE_STRING;
                if (colIdx >= 0) {
                    colType = meta.columns()[colIdx].type();
                } else {
                    throw new DatabaseException(ErrorCode.INVALID_OPERATION, "GROUP BY column not found: '" + groupCols[i] + "'");
                }
                groupbys[i] = new Aggregate.Groupby(groupCols[i], groupCols[i], colType);
            }
            
            // Parse aggregate functions
            Aggregate.Function[] funcs = parseAggregateFunctions(q, groupCols, meta);
            
            // Create aggregator
            Aggregate agg = new Aggregate("sql_groupby", groupbys, funcs);
            
            // Process rows
            Row row;
            while ((row = cursor.next()) != null) {
                agg.row(row);
            }
            
            // Compute results
            Row[] results = agg.compute();
            agg.close();
            
            // Apply HAVING clause filter if present
            if (q.having() != null && !q.having().trim().isEmpty()) {
                results = applyHavingFilter(results, q.having());
            }
            
            // Apply ORDER BY and LIMIT if needed
            return buildGroupByResult(q, results, tablePath);
            
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "GROUP BY failed: " + e.getMessage(), e);
        }
    }

    /**
     * SELECT with ORDER BY (unified for both binary tables and generic files)
     */
    static SQLResult selectOrderBy(SQL q, Meta meta, Cursor<Row> cursor, String[] outputCols) throws DatabaseException {
        try {
            // Apply sorting
            return sortCursor(cursor, q.orderby(), q.limit(), meta, outputCols);
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "ORDER BY failed: " + e.getMessage(), e);
        }
    }

    /**
     * Parse GROUP BY column names (delegated to SQL class)
     */
    static String[] parseGroupByColumns(String groupby) {
        return SQL.parseGroupByColumns(groupby);
    }

    /**
     * Parse aggregate functions from SELECT clause (delegated to SQL class)
     */
    static Aggregate.Function[] parseAggregateFunctions(SQL q, String[] groupCols, Meta meta) throws Exception {
        return SQL.parseAggregateFunctions(q, groupCols, meta);
    }

    /**
     * Build result from GROUP BY computation
     */
    static SQLResult buildGroupByResult(SQL q, Row[] results, String tablePath) throws Exception {
        if (results.length == 0) {
            // Empty result
            String[] columnNames = new String[q.columns().length];
            System.arraycopy(q.columns(), 0, columnNames, 0, columnNames.length);
            return new SQLResult(columnNames, new ListCursor(java.util.Collections.emptyList()));
        }
        
        Meta resultMeta = results[0].meta();
        
        // Fast path: no ORDER BY and no LIMIT
        if ((q.orderby() == null || q.orderby().trim().isEmpty()) && 
            (q.limit() == null || q.limit().trim().isEmpty())) {
            
            java.util.List<Row> rowList = java.util.Arrays.asList(results);
            String[] columnNames = new String[resultMeta.columns().length];
            for (int i = 0; i < columnNames.length; i++) {
                columnNames[i] = resultMeta.columns()[i].name();
            }
            
            if (tablePath != null) {
                returnTable(tablePath);
            }
            
            return new SQLResult(columnNames, new ListCursor(rowList));
        }
        
        // Sort if ORDER BY specified
        if (q.orderby() != null && !q.orderby().trim().isEmpty()) {
            sortRows(results, q.orderby(), resultMeta);
        }
        
        // Apply LIMIT
        Filter.Limit limit = Filter.MaxLimit.parse(q.limit());
        java.util.List<Row> filtered = new java.util.ArrayList<>();
        
        for (Row row : results) {
            if (limit.skip()) {
                continue;
            }
            if (!limit.remains()) {
                break;
            }
            filtered.add(row);
        }
        
        String[] columnNames = new String[resultMeta.columns().length];
        for (int i = 0; i < columnNames.length; i++) {
            columnNames[i] = resultMeta.columns()[i].name();
        }
        
        if (tablePath != null) {
            returnTable(tablePath);
        }
        
        return new SQLResult(columnNames, new ListCursor(filtered));
    }

    /**
     * Sort cursor results using file-based sorting
     */
    static SQLResult sortCursor(Cursor<Row> cursor, String orderby, String limitStr, Meta meta, String[] outputCols) throws Exception {
        Row row;
        
        // Check if empty
        row = cursor.next();
        if (row == null) {
            cursor.close();
            return new SQLResult(outputCols, new ListCursor(java.util.Collections.emptyList()));
        }
        
        Meta rowMeta = row.meta();
        SQL.OrderSpec[] specs = SQL.parseOrderBy(orderby, rowMeta);
        
        // Create file sorter
        Sortable.FileSorter sorter = new Sortable.FileSorter(rowMeta);
        
        try (cursor){
            // Add first row
            sorter.add(row);
            
            // Add remaining rows
            while ((row = cursor.next()) != null) {
                sorter.add(row);
            }
            
            // Sort using file-based sorting
            java.util.Comparator<Row> comparator = (a, b) -> {
                for (SQL.OrderSpec spec : specs) {
                    Object va = a.get(spec.colIndex);
                    Object vb = b.get(spec.colIndex);
                    
                    if (va == null && vb == null) continue;
                    if (va == null) return spec.descending ? 1 : -1;
                    if (vb == null) return spec.descending ? -1 : 1;
                    
                    int cmp = compareValues(va, vb);
                    if (cmp != 0) {
                        return spec.descending ? -cmp : cmp;
                    }
                }
                return 0;
            };
            
            sorter.sort(comparator);
            
            // Create cursor with sorted results
            return new SQLResult(outputCols, new FileSortedCursor(sorter, Filter.MaxLimit.parse(limitStr)));
            
        } catch (Exception e) {
            sorter.close();
            throw e;
        }
    }

    /**
     * Sort rows array based on ORDER BY clause
     */
    static void sortRows(Row[] rows, String orderby, Meta meta) {
        SQL.OrderSpec[] specs = SQL.parseOrderBy(orderby, meta);
        
        java.util.Arrays.sort(rows, (a, b) -> {
            for (SQL.OrderSpec spec : specs) {
                Object va = a.get(spec.colIndex);
                Object vb = b.get(spec.colIndex);
                
                if (va == null && vb == null) continue;
                if (va == null) return spec.descending ? 1 : -1;
                if (vb == null) return spec.descending ? -1 : 1;
                
                int cmp = compareValues(va, vb);
                if (cmp != 0) {
                    return spec.descending ? -cmp : cmp;
                }
            }
            return 0;
        });
    }

    /**
     * Compare two values
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    static int compareValues(Object a, Object b) {
        if (a instanceof Comparable && b instanceof Comparable) {
            try {
                return ((Comparable) a).compareTo(b);
            } catch (Exception e) {
                // Type mismatch, compare as strings
                return a.toString().compareTo(b.toString());
            }
        }
        return a.toString().compareTo(b.toString());
    }

    /**
     * Expand wildcard column selection
     */
    static String[] expandWildcard(String[] columns, Meta meta) {
        if (columns.length == 1 && "*".equals(columns[0])) {
            Column[] metaCols = meta.columns();
            String[] expanded = new String[metaCols.length];
            for (int i = 0; i < metaCols.length; i++) {
                expanded[i] = metaCols[i].name();
            }
            return expanded;
        }
        return columns;
    }

    /**
     * DESCRIBE table
     */
    static SQLResult describeTable(SQL q) throws DatabaseException {
        try {
            File file = new File(q.table());
            String fmt = format(file);
            
            if (!file.exists())
                throw new DatabaseException(ErrorCode.INVALID_OPERATION, "Table file not found: " + q.table());

            Meta meta;
            if (Meta.PRODUCT_NAME_LC.equals(fmt)) {
                try (Table table = Table.open(file, Table.OPEN_READ)) {
                    meta = table.meta();
                }
            } else {
                try (GenericFile gf = GenericFile.open(file)) {
                    meta = gf.meta();
                }
            }
            
            // Build primary key column map
            boolean[] pkColumns = new boolean[meta.columns().length];
            Index[] indexes = meta.indexes();
            if (indexes != null && indexes.length > 0) {
                String[] pkKeys = indexes[0].keys();
                for (String key : pkKeys) {
                    for (int i = 0; i < meta.columns().length; i++) {
                        if (meta.columns()[i].name().equals(key)) {
                            pkColumns[i] = true;
                            break;
                        }
                    }
                }
            }
            
            // Build result rows
            Meta resultMeta = new Meta("describe");
            resultMeta.columns(new Column[]{
                new Column("Column", Column.TYPE_STRING, (short) 256, (short) 0, false, null, null),
                new Column("Type", Column.TYPE_STRING, (short) 64, (short) 0, false, null, null),
                new Column("Key", Column.TYPE_STRING, (short) 8, (short) 0, false, null, null),
                new Column("Default", Column.TYPE_STRING, (short) 256, (short) 0, false, null, null)
            });
            
            List<Row> rows = new ArrayList<>();
            for (int i = 0; i < meta.columns().length; i++) {
                Column col = meta.columns()[i];
                Row row = Row.create(resultMeta);
                
                row.set(0, col.name());
                row.set(1, formatColumnType(col));
                row.set(2, pkColumns[i] ? "PRI" : "");
                row.set(3, col.value() != null ? col.value().toString() : "");
                
                rows.add(row);
            }
            
            String[] columnNames = new String[]{"Column", "Type", "Key", "Default"};
            return new SQLResult(columnNames, new ListCursor(rows));
            
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "DESCRIBE failed: " + e.getMessage(), e);
        }
    }

    /**
     * META table (show CREATE TABLE statement)
     */
    static SQLResult describeTableMeta(SQL q) throws DatabaseException {
        try {
            File file = new File(q.table());
            String fmt = format(file);
            
            if (!file.exists())
                throw new DatabaseException(ErrorCode.INVALID_OPERATION, "Table file not found: " + q.table());

            Meta meta;
            if (Meta.PRODUCT_NAME_LC.equals(fmt)) {
                try (Table table = Table.open(file, Table.OPEN_READ)) {
                    meta = table.meta();
                }
            } else {
                try (GenericFile gf = GenericFile.open(file)) {
                    meta = gf.meta();
                }
            }
            
            String sqlDef = SQL.stringify(meta);
            
            Meta resultMeta = new Meta("meta");
            resultMeta.columns(new Column[] {
                new Column("SQL", Column.TYPE_STRING, (short) 4096, (short) 0, false, null, null)
            });
            
            Row row = Row.create(resultMeta);
            row.set(0, sqlDef);
            
            List<Row> rows = new ArrayList<>();
            rows.add(row);
            
            String[] columnNames = new String[]{"SQL"};
            return new SQLResult(columnNames, new ListCursor(rows));
            
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "META failed: " + e.getMessage(), e);
        }
    }

    /**
     * SHOW TABLES
     */
    static SQLResult showTables(SQL q) throws DatabaseException {
        try {
            String baseDir = q.where();
            if (baseDir == null || baseDir.trim().isEmpty()) {
                baseDir = ".";
            }
            
            File dir = new File(baseDir);
            if (!dir.exists() || !dir.isDirectory()) {
                throw new DatabaseException(ErrorCode.INVALID_OPERATION, "SHOW TABLES directory not found: " + baseDir);
            }
            
            boolean recursive = q.option() != null && q.option().toUpperCase().contains("-R");
            
            Meta resultMeta = new Meta("show_tables");
            resultMeta.columns(new Column[]{
                new Column("Table", Column.TYPE_STRING, (short) 256, (short) 0, false, null, null),
                new Column("Format", Column.TYPE_STRING, (short) 32, (short) 0, false, null, null),
                new Column("Rows", Column.TYPE_STRING, (short) 32, (short) 0, false, null, null),
                new Column("Bytes", Column.TYPE_STRING, (short) 32, (short) 0, false, null, null),
                new Column("Modified", Column.TYPE_STRING, (short) 64, (short) 0, false, null, null),
                new Column("Path", Column.TYPE_STRING, (short) 512, (short) 0, false, null, null)
            });
            
            List<Row> rows = new ArrayList<>();
            collectTables(dir, "", recursive, rows, resultMeta);
            
            String[] columnNames = new String[]{"Table", "Format", "Rows", "Bytes", "Modified", "Path"};
            return new SQLResult(columnNames, new ListCursor(rows));
            
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "SHOW TABLES failed: " + e.getMessage(), e);
        }
    }

    /**
     * Collect table files recursively
     */
    static void collectTables(File dir, String relPath, boolean recursive, List<Row> rows, Meta meta) {
        File[] files = dir.listFiles();
        if (files == null) return;
        
        for (File file : files) {
            if (file.getName().startsWith(".")) continue;
            
            if (file.isDirectory()) {
                if (recursive) {
                    String newRelPath = relPath.isEmpty() ? file.getName() : relPath + "/" + file.getName();
                    collectTables(file, newRelPath, recursive, rows, meta);
                }
                continue;
            }
            
            String name = file.getName();
            if (name.endsWith(Meta.META_NAME_SUFFIX)) continue;
            
            String fmt = format(file);
            if (fmt == null) continue;
            
            // Skip if not a valid table
            if (fmt.equals(Meta.PRODUCT_NAME_LC)) {
                File descFile = new File(file.getParentFile(), name + Meta.META_NAME_SUFFIX);
                if (!descFile.exists()) continue;
            }
            
            Row row = Row.create(meta);
            row.set(0, name);
            row.set(1, fmt);
            
            // Get row count if possible
            try {
                if (fmt.equals(Meta.PRODUCT_NAME_LC)) {
                    long rowCount = Table.rows(file);
                    row.set(2, String.valueOf(rowCount));
                } else {
                    row.set(2, "");
                }
            } catch (Exception e) {
                row.set(2, "");
            }
            
            row.set(3, formatBytes(file.length()));
            row.set(4, formatDate(new Date(file.lastModified())));
            row.set(5, relPath.isEmpty() ? name : relPath + "/" + name);
            
            rows.add(row);
        }
    }


    static String formatColumnType(Column col) {
        String typeName = Column.typename(col.type()).substring(5); // Remove "TYPE_" prefix
        
        if (col.type() == Column.TYPE_DECIMAL) {
            return typeName + "(" + col.bytes() + "," + col.precision() + ")";
        } else if (col.type() == Column.TYPE_STRING || col.type() == Column.TYPE_BYTES) {
            return typeName + "(" + col.bytes() + ")";
        } else {
            return typeName;
        }
    }

    static String formatBytes(long bytes) {
        if (bytes < 0) return "";
        
        String[] units = {"B", "KB", "MB", "GB", "TB"};
        int unit = 0;
        double value = bytes;
        
        while (value >= 1024.0 && unit < units.length - 1) {
            value /= 1024.0;
            unit++;
        }
        
        if (value < 10.0) {
            return String.format("%.2f%s", value, units[unit]);
        } else if (value < 100.0) {
            return String.format("%.1f%s", value, units[unit]);
        } else {
            return String.format("%.0f%s", value, units[unit]);
        }
    }

    static String formatDate(Date date) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());
        return formatter.format(date.toInstant());
    }

    /**
     * Helper cursor classes
     */
    
    static class FileSortedCursor implements Cursor<Row> {
        private final Sortable.FileSorter sorter;
        private final Filter.Limit limit;
        private long index = 0;
        
        FileSortedCursor(Sortable.FileSorter sorter, Filter.Limit limit) {
            this.sorter = sorter;
            this.limit = limit;
        }
        
        @Override
        public Row next() {
            try {
                if (!limit.remains()) return null;
                
                // Skip offset rows
                while (limit.skip()) {
                    if (index >= sorter.rows()) return null;
                    index++;
                }
                
                if (index >= sorter.rows()) return null;
                
                Row row = sorter.read(index++);
                return row;
            } catch (Exception e) {
                throw new RuntimeException("Error reading sorted row: " + e.getMessage(), e);
            }
        }
        
        @Override
        public void close() throws Exception {
            sorter.close();
        }
    }
    
    static class TableIdToCursor implements Cursor<Row> {
        private final Table table;
        private final Cursor<Long> idCursor;
        
        TableIdToCursor(Table table, Cursor<Long> idCursor) {
            this.table = table;
            this.idCursor = idCursor;
        }
        
        @Override
        public Row next() {
            try {
                long id = idCursor.next();
                if (id < 0) return null;
                return table.read(id);
            } catch (Exception e) {
                throw new RuntimeException("Error reading row: " + e.getMessage(), e);
            }
        }
        
        @Override
        public void close() throws Exception {
            idCursor.close();
        }
    }
    
    static class ListCursor implements Cursor<Row> {
        private final List<Row> rows;
        private int index = 0;
        
        ListCursor(List<Row> rows) {
            this.rows = rows;
        }
        
        @Override
        public Row next() {
            if (index >= rows.size()) return null;
            return rows.get(index++);
        }
        
        @Override
        public void close() throws Exception {
            // Nothing to close
        }
    }

    static class TableRowCursor implements Cursor<Row> {
        private final Table table;
        private final String tablePath;
        private final Cursor<Long> idCursor;
        private final String[] outputCols;
        private final Filter.Limit limit;
        private final boolean distinct;
        private final java.util.Set<Integer> seenHashes;
        
        TableRowCursor(Table table, String tablePath, Cursor<Long> idCursor, String[] outputCols, 
                       Filter.Limit limit, boolean distinct) {
            this.table = table;
            this.tablePath = tablePath;
            this.idCursor = idCursor;
            this.outputCols = outputCols;
            this.limit = limit;
            this.distinct = distinct;
            this.seenHashes = distinct ? new java.util.HashSet<>() : null;
        }
        
        @Override
        public Row next() {
            try {
                if (!limit.remains()) return null;
                
                while (true) {
                    // Skip offset rows
                    while (limit.skip()) {
                        long id = idCursor.next();
                        if (id < 0) return null;
                    }
                    
                    long id = idCursor.next();
                    if (id < 0) return null;
                    
                    Row row = table.read(id);
                    if (row == null) continue;
                    
                    // Project columns
                    Row result = projectRow(row, outputCols);
                    
                    // Handle DISTINCT
                    if (distinct) {
                        int hash = rowHash(result);
                        if (seenHashes.contains(hash)) {
                            continue; // Duplicate, skip
                        }
                        seenHashes.add(hash);
                    }
                    
                    return result;
                }
            } catch (Exception e) {
                throw new RuntimeException("Error reading row: " + e.getMessage(), e);
            }
        }
        
        @Override
        public void close() throws Exception {
            idCursor.close();
            returnTable(tablePath);
        }
    }

    static class ProjectionCursor implements Cursor<Row> {
        private final Cursor<Row> innerCursor;
        private final GenericFile gf;
        private final String[] outputCols;
        private final Filter.Limit limit;
        private final boolean distinct;
        private final java.util.Set<Integer> seenHashes;
        
        ProjectionCursor(Cursor<Row> innerCursor, GenericFile gf, String[] outputCols, 
                         Filter.Limit limit, boolean distinct) {
            this.innerCursor = innerCursor;
            this.gf = gf;
            this.outputCols = outputCols;
            this.limit = limit;
            this.distinct = distinct;
            this.seenHashes = distinct ? new java.util.HashSet<>() : null;
        }
        
        @Override
        public Row next() {
            try {
                if (!limit.remains()) return null;
                
                while (true) {
                    // Skip offset rows
                    while (limit.skip()) {
                        Row r = innerCursor.next();
                        if (r == null) return null;
                    }
                    
                    Row row = innerCursor.next();
                    if (row == null) return null;
                    
                    // Project columns
                    Row result = projectRow(row, outputCols);
                    
                    // Handle DISTINCT
                    if (distinct) {
                        int hash = rowHash(result);
                        if (seenHashes.contains(hash)) {
                            continue; // Duplicate, skip
                        }
                        seenHashes.add(hash);
                    }
                    
                    return result;
                }
            } catch (Exception e) {
                throw new RuntimeException("Error reading row: " + e.getMessage(), e);
            }
        }
        
        @Override
        public void close() throws Exception {
            innerCursor.close();
            if (gf != null) {
                gf.close();
            }
        }
    }

    static Row projectRow(Row source, String[] cols) {
        if (cols.length == 1 && "*".equals(cols[0])) {
            return source;
        }
        
        Object[] values = new Object[cols.length];
        for (int i = 0; i < cols.length; i++) {
            values[i] = source.get(cols[i]);
        }
        
        // Create a simple meta for projected row
        Meta projMeta = new Meta("projection");
        Column[] projCols = new Column[cols.length];
        for (int i = 0; i < cols.length; i++) {
            projCols[i] = new Column(cols[i], Column.TYPE_STRING, (short) 256, (short) 0, false, null, null);
        }
        projMeta.columns(projCols);
        
        return Row.create(projMeta, values);
    }

    static int rowHash(Row row) {
        int hash = 0;
        for (int i = 0; i < row.size(); i++) {
            Object val = row.get(i);
            if (val != null) {
                hash = 31 * hash + val.toString().hashCode();
            }
        }
        return hash;
    }

    /**
     * Apply HAVING clause filter to aggregated results
     */
    static Row[] applyHavingFilter(Row[] results, String havingClause) throws DatabaseException {
        if (havingClause == null || havingClause.trim().isEmpty()) {
            return results;
        }
        
        java.util.List<Row> filtered = new java.util.ArrayList<>();
        
        for (Row row : results) {
            if (evaluateHavingCondition(row, havingClause)) {
                filtered.add(row);
            }
        }
        
        return filtered.toArray(new Row[0]);
    }
    
    /**
     * Evaluate HAVING condition for a single row
     */
    static boolean evaluateHavingCondition(Row row, String havingClause) {
        try {
            String condition = havingClause.trim();
            
            // Handle AND/OR operations (simple left-to-right evaluation)
            if (condition.toUpperCase().contains(" AND ")) {
                String[] parts = condition.split("(?i)\\s+AND\\s+");
                for (String part : parts) {
                    if (!evaluateHavingCondition(row, part.trim())) {
                        return false;
                    }
                }
                return true;
            }
            
            if (condition.toUpperCase().contains(" OR ")) {
                String[] parts = condition.split("(?i)\\s+OR\\s+");
                for (String part : parts) {
                    if (evaluateHavingCondition(row, part.trim())) {
                        return true;
                    }
                }
                return false;
            }
            
            // Parse simple comparison: column operator value
            String[] operators = {">=", "<=", "!=", "<>", ">", "<", "="};
            
            for (String op : operators) {
                if (condition.contains(op)) {
                    String[] parts = condition.split(java.util.regex.Pattern.quote(op), 2);
                    if (parts.length == 2) {
                        String leftSide = parts[0].trim();
                        String rightSide = parts[1].trim();
                        
                        // Get the value from the row
                        Object leftValue = getHavingValue(row, leftSide);
                        Object rightValue = parseHavingValue(rightSide);
                        
                        if (leftValue == null && rightValue == null) {
                            return "=".equals(op) || "<=".equals(op) || ">=".equals(op);
                        }
                        if (leftValue == null || rightValue == null) {
                            return false;
                        }
                        
                        // Convert to comparable values
                        double leftNum = convertToNumber(leftValue);
                        double rightNum = convertToNumber(rightValue);
                        
                        switch (op) {
                            case ">=": return leftNum >= rightNum;
                            case "<=": return leftNum <= rightNum;
                            case ">": return leftNum > rightNum;
                            case "<": return leftNum < rightNum;
                            case "=": return leftNum == rightNum;
                            case "!=": return leftNum != rightNum;
                            case "<>": return leftNum != rightNum;
                            default: return false;
                        }
                    }
                }
            }
            
            return true; // If we can't parse the condition, default to true
        } catch (Exception e) {
            // If there's any error in evaluation, log and default to true
            System.err.println("Error evaluating HAVING condition: " + e.getMessage());
            return true;
        }
    }
    
    /**
     * Get value from row by column name or expression
     */
    static Object getHavingValue(Row row, String expression) {
        Meta meta = row.meta();
        Column[] columns = meta.columns();
        
        // Try to get by column name directly
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].name().equalsIgnoreCase(expression)) {
                return row.get(i);
            }
        }
        
        // Try to match by normalized expression
        String normalizedExpr = expression.toUpperCase().replaceAll("\\s+", "");
        for (int i = 0; i < columns.length; i++) {
            String normalizedCol = columns[i].name().toUpperCase().replaceAll("\\s+", "");
            if (normalizedCol.equals(normalizedExpr)) {
                return row.get(i);
            }
        }
        
        return null;
    }
    
    /**
     * Parse string value to appropriate type
     */
    static Object parseHavingValue(String value) {
        if (value == null) return null;
        
        value = value.trim();
        
        // Try to parse as number
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            } else {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            // Return as string if not a number
            // Remove quotes if present
            if ((value.startsWith("'") && value.endsWith("'")) ||
                (value.startsWith("\"") && value.endsWith("\""))) {
                return value.substring(1, value.length() - 1);
            }
            return value;
        }
    }
    
    /**
     * Convert object to number for comparison
     */
    static double convertToNumber(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        
        // Try to parse string as number
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            // For non-numeric strings, use hashCode for comparison
            return value.toString().hashCode();
        }
    }

    // Transaction management - thread-local storage for active transactions
    private static final ThreadLocal<Map<String, Transaction>> activeTransactions = 
        ThreadLocal.withInitial(() -> new java.util.HashMap<>());
    
    /**
     * BEGIN TRANSACTION <table>
     * Start a new transaction for the specified table
     */
    static SQLResult beginTransaction(SQL q) throws DatabaseException {
        String tablePath = q.table();
        if (tablePath == null || tablePath.isEmpty()) {
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "Table name required for BEGIN TRANSACTION");
        }
        
        tablePath = JdbcConfig.resolve(tablePath);
        System.err.println("[beginTransaction] tablePath: " + tablePath);
        
        try {
            Table table = borrowTable(tablePath);
            System.err.println("[beginTransaction] Borrowed table instance: " + System.identityHashCode(table));
            Transaction tx = table.begin();
            System.err.println("[beginTransaction] Created transaction ID: " + tx.id());
            
            // Store transaction in thread-local map
            // Don't return the table to pool yet - keep it borrowed until commit/rollback
            activeTransactions.get().put(tablePath, tx);
            System.err.println("[beginTransaction] Stored in activeTransactions, size: " + activeTransactions.get().size());
            
            // Return transaction in result so CLI can keep it
            return new SQLResult(1, tx);
        } catch (IOException e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to begin transaction: " + e.getMessage(), e);
        }
    }
    
    /**
     * COMMIT [<table>]
     * Commit the active transaction for the specified table (or all if no table specified)
     */
    static SQLResult commitTransaction(SQL q) throws DatabaseException {
        String tablePath = q.table();
        
        try {
            if (tablePath != null && !tablePath.isEmpty()) {
                // Commit specific table transaction
                tablePath = JdbcConfig.resolve(tablePath);
                Transaction tx = activeTransactions.get().remove(tablePath);
                if (tx != null) {
                    tx.commit();
                    // Return the borrowed table to pool after commit
                    try {
                        returnTable(tablePath);
                    } catch (IOException e) {
                        System.err.println("Failed to return table after commit: " + e.getMessage());
                    }
                }
            } else {
                // Commit all active transactions
                for (Map.Entry<String, Transaction> entry : activeTransactions.get().entrySet()) {
                    entry.getValue().commit();
                    // Return the borrowed table to pool after commit
                    try {
                        returnTable(entry.getKey());
                    } catch (IOException e) {
                        System.err.println("Failed to return table after commit: " + e.getMessage());
                    }
                }
                activeTransactions.get().clear();
            }
            
            return new SQLResult(1);
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to commit transaction: " + e.getMessage(), e);
        }
    }
    
    /**
     * ROLLBACK [<table>]
     * Rollback the active transaction for the specified table (or all if no table specified)
     */
    static SQLResult rollbackTransaction(SQL q) throws DatabaseException {
        String tablePath = q.table();
        System.err.println("[rollbackTransaction] tablePath: " + tablePath + ", activeTransactions size: " + activeTransactions.get().size());
        
        try {
            if (tablePath != null && !tablePath.isEmpty()) {
                // Rollback specific table transaction
                tablePath = JdbcConfig.resolve(tablePath);
                System.err.println("[rollbackTransaction] Resolved tablePath: " + tablePath);
                Transaction tx = activeTransactions.get().remove(tablePath);
                System.err.println("[rollbackTransaction] Found transaction: " + (tx != null ? tx.id() : "null"));
                if (tx != null) {
                    System.err.println("[rollbackTransaction] Calling tx.rollback() for transaction ID: " + tx.id());
                    tx.rollback();
                    System.err.println("[rollbackTransaction] tx.rollback() completed");
                    // Return the borrowed table to pool after rollback
                    try {
                        returnTable(tablePath);
                    } catch (IOException e) {
                        System.err.println("Failed to return table after rollback: " + e.getMessage());
                    }
                }
            } else {
                // Rollback all active transactions
                System.err.println("[rollbackTransaction] Rolling back all transactions, count: " + activeTransactions.get().size());
                for (Map.Entry<String, Transaction> entry : activeTransactions.get().entrySet()) {
                    entry.getValue().rollback();
                    // Return the borrowed table to pool after rollback
                    try {
                        returnTable(entry.getKey());
                    } catch (IOException e) {
                        System.err.println("Failed to return table after rollback: " + e.getMessage());
                    }
                }
                activeTransactions.get().clear();
            }
            
            return new SQLResult(1);
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to rollback transaction: " + e.getMessage(), e);
        }
    }

    /**
     * Cleanup all resources including table pool
     */
    public static void cleanup() {
        synchronized (tablePool) {
            for (PooledTable pt : tablePool.values()) {
                try {
                    if (pt.table != null) {
                        pt.table.close();
                    }
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
            tablePool.clear();
        }
    }

    static long createTable(SQL q) throws DatabaseException {
        File path = new File(q.table());
        if (path.exists())
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "Table already exists: " + q.table());

        String fmt = format(path); // Validate format
        if (!Meta.PRODUCT_NAME_LC.equals(fmt))
            throw new DatabaseException(ErrorCode.INVALID_OPERATION, "CREATE TABLE only supported for flintdb format");

        try (Table table = Table.open(path, q.meta())) {
            // Table created
        } catch (Exception e) {
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "CREATE TABLE failed: " + e.getMessage(), e);
        }
        return 1;
    }
}