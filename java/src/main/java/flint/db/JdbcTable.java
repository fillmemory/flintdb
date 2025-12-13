package flint.db;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A read-only table backed by a JDBC connection.
 * <p>
 * This class provides a minimal subset of the {@link Table} interface for read-only access.
 * It supports retrieving metadata, counting rows, and fetching a single row by primary key.
 * <p>
 * Usage example:
 * <pre>
 * try (var db = JdbcTable.open(new URI("jdbc:h2:mem:testdb"), null, null, "T")) {
 *     Meta meta = db.meta();
 *     Map<String,Object> key = new HashMap<>();
 *     key.put("id", 1);
 *     Row row = db.one(Index.PRIMARY, key);
 * }
 * </pre>
 */
public final class JdbcTable implements GenericFile {
    private final Connection connection;
    private final String table;
    private Meta cachedMeta;

    private JdbcTable(final Connection connection, final String table) {
        this.connection = connection;
        this.table = table;
    }

    public static JdbcTable open(final URI uri) {
        // For JDBC URIs, the query parameters are in the schemeSpecificPart, not getQuery()
        // e.g., "jdbc:sqlite:test.db?table=users" -> schemeSpecificPart = "sqlite:test.db?table=users"
        String ssp = uri.getSchemeSpecificPart();
        String queryString = null;
        if (ssp != null) {
            int qmark = ssp.indexOf('?');
            if (qmark >= 0) {
                queryString = ssp.substring(qmark + 1);
            }
        }
        
        Map<String, String> queryParams = split(queryString);
        String tableName = queryParams.get("table");
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("JDBC URI must include 'table' query parameter");
        }
        return open(uri, null, null, tableName);
    }

    static Map<String, String> split(final String q) {
        Map<String, String> map = new LinkedHashMap<>();
        if (q != null && !q.isEmpty()) {
            String[] parts = q.split("&");
            for (String part : parts) {
                int eq = part.indexOf('=');
                if (eq > 0) {
                    String key = part.substring(0, eq).trim();
                    String value = decode(part.substring(eq + 1).trim());
                    map.put(key, value);
                } else {
                    String key = part.trim();
                    map.put(key, "");
                }
            }
        }
        return map;
    }

    static String decode(String s) {
        try {
            return java.net.URLDecoder.decode(s, java.nio.charset.StandardCharsets.UTF_8.name());
        } catch (Exception ex) {
            return s;
        }
    }

    public static JdbcTable open(final URI uri, final String user, final String password, final String tablename) {
        if (uri == null) throw new IllegalArgumentException("uri");
        if (tablename == null || tablename.isEmpty()) throw new IllegalArgumentException("tablename");
        
        // Remove query parameters from the URI for JDBC connection
        // e.g., "jdbc:sqlite:test.db?table=users" -> "jdbc:sqlite:test.db"
        String url = uri.toString();
        String ssp = uri.getSchemeSpecificPart();
        if (ssp != null) {
            int qmark = ssp.indexOf('?');
            if (qmark >= 0) {
                url = "jdbc:" + ssp.substring(0, qmark);
            }
        }
        
        final String driver = detectDriver(url);
        try {
            Class.forName(driver);
            final Connection conn = (user != null || password != null)
                    ? DriverManager.getConnection(url, user, password)
                    : DriverManager.getConnection(url);
            return new JdbcTable(conn, tablename);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to open JDBC connection: " + url + ", driver=" + driver, ex);
        }
    }

    private static String detectDriver(final String url) {
        if (url.startsWith("jdbc:sqlite:")) return "org.sqlite.JDBC";
        if (url.startsWith("jdbc:h2:")) return "org.h2.Driver";
        if (url.startsWith("jdbc:mysql:")) return "com.mysql.jdbc.Driver"; // or com.mysql.cj.jdbc.Driver
        if (url.startsWith("jdbc:mariadb:")) return "org.mariadb.jdbc.Driver";
        if (url.startsWith("jdbc:oracle:")) return "oracle.jdbc.driver.OracleDriver";
        if (url.startsWith("jdbc:sqlserver:")) return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        if (url.startsWith("jdbc:postgresql:")) return "org.postgresql.Driver";
        throw new IllegalArgumentException("Unknown JDBC URL (cannot detect driver): " + url);
    }

    @Override
    public void close() {
        try { if (connection != null && !connection.isClosed()) connection.close(); } catch (Exception ignore) {}
    }

    // --- GenericFile interface implementation ---

    @Override
    public long write(final Row row) throws IOException {
        throw new UnsupportedOperationException("Write operation not supported for JDBC tables yet");
    }

    @Override
    public Cursor<Row> find(final Filter.Limit limit, final Comparable<Row> filter) throws Exception {
        if (table == null || table.isEmpty()) {
            throw new IllegalArgumentException("table name is null or empty");
        }
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(table);
        
        if (filter != null) {
            // TODO: Convert filter to SQL WHERE clause
            throw new UnsupportedOperationException("Filter conversion not yet implemented");
        }
        
        // Extract limit/offset from Filter.Limit if it's a MaxLimit instance
        if (limit != null && limit instanceof Filter.MaxLimit) {
            Filter.MaxLimit maxLimit = (Filter.MaxLimit) limit;
            // Use toString() which generates "LIMIT x" or "LIMIT offset,limit"
            String limitClause = maxLimit.toString();
            if (!limitClause.isEmpty()) {
                sql.append(" ").append(limitClause);
            }
        }
        
        return find(sql.toString());
    }

    @Override
    public Cursor<Row> find() throws Exception {
        return find("SELECT * FROM " + table);
    }


    public Cursor<Row> executeQuery(final String sql, Object ...va) throws Exception {
        if (sql == null || sql.isEmpty()) throw new IllegalArgumentException("sql");
        final PreparedStatement stmt = connection.prepareStatement(sql);
        for (int i = 0; i < va.length; i++) {
            stmt.setObject(i + 1, va[i]);
        }
        final ResultSet rs = stmt.executeQuery();
        return new JdbcCursor(rs, stmt);
    }
    
    @Override
    public long fileSize() {
        // JDBC connections don't have a file size
        return -1L;
    }

    @Override
    public long rows(boolean force) throws IOException {
        return -1;
    }

    // --- Table (read-only subset) ---

    public Meta meta() throws java.io.IOException {
        if (cachedMeta != null) return cachedMeta;
        try {
            final DatabaseMetaData md = connection.getMetaData();
            final Meta meta = new Meta(table);
            final List<Column> cols = new ArrayList<>();

            try (final ResultSet rs = md.getColumns(null, null, table, null)) {
                while (rs.next()) {
                    final String COLUMN_NAME = rs.getString("COLUMN_NAME");
                    final int COLUMN_SIZE = rs.getInt("COLUMN_SIZE");
                    final int COLUMN_SCALE = rs.getInt("DECIMAL_DIGITS");
                    final String TYPE_NAME = rs.getString("TYPE_NAME");
                    cols.add(jdbcColumn(TYPE_NAME.toUpperCase(), COLUMN_NAME, COLUMN_SIZE, COLUMN_SCALE));
                }
            }
            if (cols.isEmpty()) throw new java.io.IOException("No columns found for table: " + table);
            meta.columns(cols.toArray(Column[]::new));

            final Map<Short, String> pk = new java.util.TreeMap<>();
            try (final ResultSet rs = md.getPrimaryKeys(null, null, table)) {
                while (rs.next()) {
                    final String COLUMN_NAME = rs.getString("COLUMN_NAME");
                    final short KEY_SEQ = rs.getShort("KEY_SEQ");
                    pk.put(KEY_SEQ, COLUMN_NAME);
                }
            }
            if (!pk.isEmpty()) {
                meta.indexes(new Index[] { new Table.PrimaryKey(pk.values().toArray(String[]::new)) });
            }

            cachedMeta = meta;
            return meta;
        } catch (Exception ex) {
            throw new java.io.IOException("Failed to read JDBC metadata: " + ex.getMessage(), ex);
        }
    }

    @Override
    public Cursor<Row> find(final String where) {
        if (table == null || table.isEmpty()) {
            throw new IllegalArgumentException("table name is null or empty");
        }
        
        try {
            StringBuilder sql = new StringBuilder("SELECT * FROM ").append(table);
            if (where != null && !where.isEmpty()) {
                sql.append(" WHERE ").append(where);
            }
            
            final PreparedStatement stmt = connection.prepareStatement(sql.toString());
            final ResultSet rs = stmt.executeQuery();
            return new JdbcCursor(rs, stmt);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to execute query on table " + table + ": " + ex.getMessage(), ex);
        }
    }

    public int apply(final String sql, final Object... params) throws Exception {
        if (sql == null || sql.isEmpty()) throw new IllegalArgumentException("sql");
        try (final PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            return stmt.executeUpdate();
        }
    }

    static final class JdbcCursor implements Cursor<Row> {
        private final ResultSet rs;
        private final PreparedStatement stmt;
        private boolean closed = false;
        private Meta dynamicMeta = null;

        JdbcCursor(final ResultSet rs, final PreparedStatement stmt) {
            this.rs = rs;
            this.stmt = stmt;
        }

        @Override
        public Row next() {
            if (closed) return null;
            try {
                if (!rs.next()) {
                    close();
                    return null;
                }

                final ResultSetMetaData rsmd = rs.getMetaData();
                final int cc = rsmd.getColumnCount();
                
                // Build dynamic meta from ResultSet metadata on first row
                if (dynamicMeta == null) {
                    List<Column> cols = new ArrayList<>();
                    for (int i = 1; i <= cc; i++) {
                        String label = rsmd.getColumnLabel(i);
                        String type = rsmd.getColumnTypeName(i);
                        int size = rsmd.getColumnDisplaySize(i);
                        int scale = rsmd.getScale(i);
                        cols.add(jdbcColumn(type, label, size, scale));
                    }
                    dynamicMeta = new Meta("result");
                    dynamicMeta.columns(cols.toArray(new Column[0]));
                }
                
                final Map<String, Object> map = new LinkedHashMap<>();
                for (int i = 1; i <= cc; i++) {
                    final String label = rsmd.getColumnLabel(i);
                    final Object v = rs.getObject(i);
                    map.put(Column.normalize(label), v);
                }
                return Row.create(dynamicMeta, map);
            } catch (Exception ex) {
                close();
                throw new RuntimeException("Failed to read next row from JDBC ResultSet: " + ex.getMessage(), ex);
            }
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            try { if (rs != null && !rs.isClosed()) rs.close(); } catch (Exception ignore) {}
            try { if (stmt != null && !stmt.isClosed()) stmt.close(); } catch (Exception ignore) {}
        }
    }

    // Helper method to convert JDBC type names to FlintDB Column types
    private static Column jdbcColumn(final String type, final String label, final int size, final int scale) {
        // SQLite and some other databases return very large sizes for TEXT/VARCHAR
        // Use a reasonable default for string columns
        int actualSize = (size > 32767 || size <= 0) ? 255 : size;
        
        if ("DATETIME".equals(type))
            return new Column.Builder(label, Column.TYPE_TIME).create();
        else if ("TIMESTAMP".equals(type))
            return new Column.Builder(label, Column.TYPE_TIME).create();
        else if ("DATE".equals(type))
            return new Column.Builder(label, Column.TYPE_DATE).create();
        else if ("INT".equals(type) || "INTEGER".equals(type))
            return new Column.Builder(label, Column.TYPE_INT).create();
        else if ("INT UNSIGNED".equals(type))
            return new Column.Builder(label, Column.TYPE_UINT).create();
        else if ("SMALLINT".equals(type))
            return new Column.Builder(label, Column.TYPE_INT16).create();
        else if ("SMALLINT UNSIGNED".equals(type))
            return new Column.Builder(label, Column.TYPE_UINT16).create();
        else if ("TINYINT".equals(type))
            return new Column.Builder(label, Column.TYPE_INT8).create();
        else if ("TINYINT UNSIGNED".equals(type))
            return new Column.Builder(label, Column.TYPE_UINT8).create();
        else if ("DOUBLE".equals(type) || "REAL".equals(type))
            return new Column.Builder(label, Column.TYPE_DOUBLE).create();
        else if ("DECIMAL".equals(type))
            return new Column.Builder(label, Column.TYPE_DECIMAL).bytes(actualSize, scale).create();
        else if ("DECIMAL UNSIGNED".equals(type))
            return new Column.Builder(label, Column.TYPE_DECIMAL).bytes(actualSize, scale).create();
        else if ("BIGINT".equals(type))
            return new Column.Builder(label, Column.TYPE_DECIMAL).bytes(actualSize).create();
        else if ("BIGINT UNSIGNED".equals(type))
            return new Column.Builder(label, Column.TYPE_DECIMAL).bytes(actualSize).create();
        else
            return new Column.Builder(label, Column.TYPE_STRING).bytes(actualSize).create();
    }
}
