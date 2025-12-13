package flint.db;

import java.io.File;

/**
 * Test cases for HAVING clause support in SQL queries
 */
public class TestcaseHaving {
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== Testing HAVING Clause Support ===\n");
        
        // Test 1: Simple HAVING with COUNT
        test1_simpleHavingWithCount();
        
        // Test 2: HAVING with SUM
        test2_havingWithSum();
        
        // Test 3: HAVING with multiple conditions (AND)
        test3_havingWithAnd();
        
        // Test 4: HAVING with OR
        test4_havingWithOr();
        
        // Test 5: HAVING with various comparison operators
        test5_havingWithOperators();
        
        System.out.println("\n=== All HAVING tests completed successfully ===");
    }
    
    static void test1_simpleHavingWithCount() throws Exception {
        System.out.println("Test 1: Simple HAVING with COUNT");
        
        File testFile = new File("temp/test_having_count.flintdb");
        cleanup(testFile);
        
        try {
            // Create table
            String createSQL = """
                CREATE TABLE temp/test_having_count (
                    category STRING(50),
                    product STRING(50),
                    quantity INT,
                    PRIMARY KEY (category, product)
                )
                """;
            SQLExec.execute(createSQL);
            
            // Insert test data
            String[] inserts = {
                "INSERT INTO temp/test_having_count.flintdb VALUES ('Electronics', 'Laptop', 10)",
                "INSERT INTO temp/test_having_count.flintdb VALUES ('Electronics', 'Mouse', 100)",
                "INSERT INTO temp/test_having_count.flintdb VALUES ('Electronics', 'Keyboard', 50)",
                "INSERT INTO temp/test_having_count.flintdb VALUES ('Books', 'Novel', 20)",
                "INSERT INTO temp/test_having_count.flintdb VALUES ('Books', 'Textbook', 15)",
                "INSERT INTO temp/test_having_count.flintdb VALUES ('Clothing', 'Shirt', 30)"
            };
            
            for (String insert : inserts) {
                SQLExec.execute(insert);
            }
            
            // Query with HAVING - get categories with more than 2 products
            String querySQL = """
                SELECT category, COUNT(*) as product_count 
                FROM temp/test_having_count.flintdb.flintdb 
                GROUP BY category 
                HAVING product_count > 2
                """;
            
            SQLResult result = SQLExec.execute(querySQL);
            System.out.println("  Query: " + querySQL.replaceAll("\\s+", " ").trim());
            System.out.println("  Results:");
            
            Cursor<Row> cursor = result.getCursor();
            Row row;
            int count = 0;
            while ((row = cursor.next()) != null) {
                System.out.println("    Category: " + row.get(0) + ", Count: " + row.get(1));
                count++;
            }
            cursor.close();
            
            if (count == 1) {
                System.out.println("  ✓ PASSED: Found 1 category with more than 2 products\n");
            } else {
                System.out.println("  ✗ FAILED: Expected 1 result, got " + count + "\n");
            }
            
        } finally {
            cleanup(testFile);
        }
    }
    
    static void test2_havingWithSum() throws Exception {
        System.out.println("Test 2: HAVING with SUM");
        
        File testFile = new File("temp/test_having_sum.flintdb");
        cleanup(testFile);
        
        try {
            // Create table
            String createSQL = """
                CREATE TABLE temp/test_having_sum (
                    region STRING(50),
                    sales INT,
                    PRIMARY KEY (region, sales)
                )
                """;
            SQLExec.execute(createSQL);
            
            // Insert test data
            String[] inserts = {
                "INSERT INTO temp/test_having_sum.flintdb VALUES ('North', 1000)",
                "INSERT INTO temp/test_having_sum.flintdb VALUES ('North', 2000)",
                "INSERT INTO temp/test_having_sum.flintdb VALUES ('North', 3000)",
                "INSERT INTO temp/test_having_sum.flintdb VALUES ('South', 500)",
                "INSERT INTO temp/test_having_sum.flintdb VALUES ('South', 600)",
                "INSERT INTO temp/test_having_sum.flintdb VALUES ('East', 4000)",
                "INSERT INTO temp/test_having_sum.flintdb VALUES ('East', 5000)"
            };
            
            for (String insert : inserts) {
                SQLExec.execute(insert);
            }
            
            // Query with HAVING - get regions with total sales > 5000
            String querySQL = """
                SELECT region, SUM(sales) as total_sales 
                FROM temp/test_having_sum.flintdb 
                GROUP BY region 
                HAVING total_sales > 5000
                """;
            
            SQLResult result = SQLExec.execute(querySQL);
            System.out.println("  Query: " + querySQL.replaceAll("\\s+", " ").trim());
            System.out.println("  Results:");
            
            Cursor<Row> cursor = result.getCursor();
            Row row;
            int count = 0;
            while ((row = cursor.next()) != null) {
                System.out.println("    Region: " + row.get(0) + ", Total Sales: " + row.get(1));
                count++;
            }
            cursor.close();
            
            if (count == 2) {
                System.out.println("  ✓ PASSED: Found 2 regions with total sales > 5000\n");
            } else {
                System.out.println("  ✗ FAILED: Expected 2 results, got " + count + "\n");
            }
            
        } finally {
            cleanup(testFile);
        }
    }
    
    static void test3_havingWithAnd() throws Exception {
        System.out.println("Test 3: HAVING with AND condition");
        
        File testFile = new File("temp/test_having_and.flintdb");
        cleanup(testFile);
        
        try {
            // Create table
            String createSQL = """
                CREATE TABLE temp/test_having_and (
                    dept STRING(50),
                    amount INT,
                    PRIMARY KEY (dept, amount)
                )
                """;
            SQLExec.execute(createSQL);
            
            // Insert test data
            String[] inserts = {
                "INSERT INTO temp/test_having_and.flintdb VALUES ('IT', 100)",
                "INSERT INTO temp/test_having_and.flintdb VALUES ('IT', 200)",
                "INSERT INTO temp/test_having_and.flintdb VALUES ('IT', 150)",
                "INSERT INTO temp/test_having_and.flintdb VALUES ('HR', 50)",
                "INSERT INTO temp/test_having_and.flintdb VALUES ('HR', 60)",
                "INSERT INTO temp/test_having_and.flintdb VALUES ('Sales', 300)",
                "INSERT INTO temp/test_having_and.flintdb VALUES ('Sales', 400)"
            };
            
            for (String insert : inserts) {
                SQLExec.execute(insert);
            }
            
            // Query with HAVING AND - departments with count > 2 AND total > 400
            String querySQL = """
                SELECT dept, COUNT(*) as cnt, SUM(amount) as total 
                FROM temp/test_having_and.flintdb 
                GROUP BY dept 
                HAVING cnt > 2 AND total > 400
                """;
            
            SQLResult result = SQLExec.execute(querySQL);
            System.out.println("  Query: " + querySQL.replaceAll("\\s+", " ").trim());
            System.out.println("  Results:");
            
            Cursor<Row> cursor = result.getCursor();
            Row row;
            int count = 0;
            while ((row = cursor.next()) != null) {
                System.out.println("    Dept: " + row.get(0) + ", Count: " + row.get(1) + ", Total: " + row.get(2));
                count++;
            }
            cursor.close();
            
            if (count == 1) {
                System.out.println("  ✓ PASSED: Found 1 department matching both conditions\n");
            } else {
                System.out.println("  ✗ FAILED: Expected 1 result, got " + count + "\n");
            }
            
        } finally {
            cleanup(testFile);
        }
    }
    
    static void test4_havingWithOr() throws Exception {
        System.out.println("Test 4: HAVING with OR condition");
        
        File testFile = new File("temp/test_having_or.flintdb");
        cleanup(testFile);
        
        try {
            // Create table
            String createSQL = """
                CREATE TABLE temp/test_having_or (
                    team STRING(50),
                    score INT,
                    PRIMARY KEY (team, score)
                )
                """;
            SQLExec.execute(createSQL);
            
            // Insert test data
            String[] inserts = {
                "INSERT INTO temp/test_having_or.flintdb VALUES ('TeamA', 10)",
                "INSERT INTO temp/test_having_or.flintdb VALUES ('TeamA', 20)",
                "INSERT INTO temp/test_having_or.flintdb VALUES ('TeamB', 100)",
                "INSERT INTO temp/test_having_or.flintdb VALUES ('TeamB', 200)",
                "INSERT INTO temp/test_having_or.flintdb VALUES ('TeamC', 5)"
            };
            
            for (String insert : inserts) {
                SQLExec.execute(insert);
            }
            
            // Query with HAVING OR - teams with count > 2 OR total > 250
            String querySQL = """
                SELECT team, COUNT(*) as cnt, SUM(score) as total 
                FROM temp/test_having_or.flintdb 
                GROUP BY team 
                HAVING cnt > 2 OR total > 250
                """;
            
            SQLResult result = SQLExec.execute(querySQL);
            System.out.println("  Query: " + querySQL.replaceAll("\\s+", " ").trim());
            System.out.println("  Results:");
            
            Cursor<Row> cursor = result.getCursor();
            Row row;
            int count = 0;
            while ((row = cursor.next()) != null) {
                System.out.println("    Team: " + row.get(0) + ", Count: " + row.get(1) + ", Total: " + row.get(2));
                count++;
            }
            cursor.close();
            
            if (count == 1) {
                System.out.println("  ✓ PASSED: Found 1 team matching OR condition\n");
            } else {
                System.out.println("  ✗ FAILED: Expected 1 result, got " + count + "\n");
            }
            
        } finally {
            cleanup(testFile);
        }
    }
    
    static void test5_havingWithOperators() throws Exception {
        System.out.println("Test 5: HAVING with various operators");
        
        File testFile = new File("temp/test_having_ops.flintdb");
        cleanup(testFile);
        
        try {
            // Create table
            String createSQL = """
                CREATE TABLE temp/test_having_ops (
                    status STRING(50),
                    value INT,
                    PRIMARY KEY (status, value)
                )
                """;
            SQLExec.execute(createSQL);
            
            // Insert test data
            String[] inserts = {
                "INSERT INTO temp/test_having_ops.flintdb VALUES ('Active', 10)",
                "INSERT INTO temp/test_having_ops.flintdb VALUES ('Active', 20)",
                "INSERT INTO temp/test_having_ops.flintdb VALUES ('Active', 30)",
                "INSERT INTO temp/test_having_ops.flintdb VALUES ('Inactive', 5)",
                "INSERT INTO temp/test_having_ops.flintdb VALUES ('Pending', 15)",
                "INSERT INTO temp/test_having_ops.flintdb VALUES ('Pending', 25)"
            };
            
            for (String insert : inserts) {
                SQLExec.execute(insert);
            }
            
            // Test >= operator
            String querySQL = """
                SELECT status, AVG(value) as avg_value 
                FROM temp/test_having_ops.flintdb 
                GROUP BY status 
                HAVING avg_value >= 20
                """;
            
            SQLResult result = SQLExec.execute(querySQL);
            System.out.println("  Query (>= 20): " + querySQL.replaceAll("\\s+", " ").trim());
            System.out.println("  Results:");
            
            Cursor<Row> cursor = result.getCursor();
            Row row;
            int count = 0;
            while ((row = cursor.next()) != null) {
                System.out.println("    Status: " + row.get(0) + ", Avg: " + row.get(1));
                count++;
            }
            cursor.close();
            
            if (count == 2) {
                System.out.println("  ✓ PASSED: Found 2 statuses with avg >= 20\n");
            } else {
                System.out.println("  ✗ FAILED: Expected 2 results, got " + count + "\n");
            }
            
        } finally {
            cleanup(testFile);
        }
    }
    
    static void cleanup(File f) {
        try {
            if (f.exists()) {
                Table.drop(f);
            }
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }
}
