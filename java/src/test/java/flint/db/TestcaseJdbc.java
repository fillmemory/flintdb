package flint.db;

public class TestcaseJdbc {
    
    public static void main(String[] args) throws Exception {
        // Use H2 in-memory database for a hermetic test
        final String jdbcUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
        final String driver = "org.h2.Driver";

        Class.forName(driver);
        try (final java.sql.Connection cn = java.sql.DriverManager.getConnection(jdbcUrl)) {
            try (final java.sql.Statement st = cn.createStatement()) {
                st.execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100), amount DECIMAL(10,2), created TIMESTAMP)");
                st.execute("INSERT INTO t (id, name, amount, created) VALUES (1, 'Alice', 12.34, CURRENT_TIMESTAMP())");
                st.execute("INSERT INTO t (id, name, amount, created) VALUES (2, 'Bob', 56.78, CURRENT_TIMESTAMP())");
            }
        }

        try (var db = JdbcTable.open(new java.net.URI(jdbcUrl), null, null, "T")) {
            // meta()
            final Meta meta = db.meta();
            System.out.println("META: " + SQL.stringify(meta));
            try(var cursor = db.find("SELECT * FROM T")) {
                for(Row r; (r = cursor.next()) != null; ) {
                    System.out.println("ROW: " + r);
                }
            }
        }
    }
}
