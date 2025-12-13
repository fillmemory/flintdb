package flint.db;

import java.io.File;

/**
 * Simple test for HAVING clause
 */
public class TestcaseHavingSimple {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Testing HAVING Clause ===\n");
        
        File testFile = new File("temp/test_having.flintdb");
        cleanup(testFile);
        
        try {
            // Create table
            SQLExec.execute("""
                CREATE TABLE temp/test_having.flintdb (
                    dept STRING(50),
                    amount INT,
                    PRIMARY KEY (dept, amount)
                )
                """);
            
            // Insert test data
            SQLExec.execute("INSERT INTO temp/test_having.flintdb VALUES ('IT', 100)");
            SQLExec.execute("INSERT INTO temp/test_having.flintdb VALUES ('IT', 200)");
            SQLExec.execute("INSERT INTO temp/test_having.flintdb VALUES ('IT', 150)");
            SQLExec.execute("INSERT INTO temp/test_having.flintdb VALUES ('HR', 50)");
            SQLExec.execute("INSERT INTO temp/test_having.flintdb VALUES ('HR', 60)");
            SQLExec.execute("INSERT INTO temp/test_having.flintdb VALUES ('Sales', 300)");
            SQLExec.execute("INSERT INTO temp/test_having.flintdb VALUES ('Sales', 400)");
            
            System.out.println("Inserted 7 rows\n");
            
            // Test 1: HAVING with COUNT
            System.out.println("Test 1: HAVING cnt > 2");
            SQLResult result1 = SQLExec.execute("""
                SELECT dept, COUNT(*) as cnt, SUM(amount) as total
                FROM temp/test_having.flintdb
                GROUP BY dept
                HAVING cnt > 2
                """);
            
            printResults(result1);
            
            // Test 2: HAVING with SUM
            System.out.println("\nTest 2: HAVING total > 400");
            SQLResult result2 = SQLExec.execute("""
                SELECT dept, COUNT(*) as cnt, SUM(amount) as total
                FROM temp/test_having.flintdb
                GROUP BY dept
                HAVING total > 400
                """);
            
            printResults(result2);
            
            // Test 3: HAVING with AND
            System.out.println("\nTest 3: HAVING cnt > 2 AND total > 400");
            SQLResult result3 = SQLExec.execute("""
                SELECT dept, COUNT(*) as cnt, SUM(amount) as total
                FROM temp/test_having.flintdb
                GROUP BY dept
                HAVING cnt > 2 AND total > 400
                """);
            
            printResults(result3);
            
            // Test 4: HAVING with OR  
            System.out.println("\nTest 4: HAVING cnt > 2 OR total > 500");
            SQLResult result4 = SQLExec.execute("""
                SELECT dept, COUNT(*) as cnt, SUM(amount) as total
                FROM temp/test_having.flintdb
                GROUP BY dept
                HAVING cnt > 2 OR total > 500
                """);
            
            printResults(result4);
            
            System.out.println("\n=== All tests completed successfully ===");
            
        } finally {
            cleanup(testFile);
        }
    }
    
    static void printResults(SQLResult result) throws Exception {
        Cursor<Row> cursor = result.getCursor();
        Row row;
        int count = 0;
        while ((row = cursor.next()) != null) {
            System.out.println("  " + row.get(0) + " | cnt=" + row.get(1) + " | total=" + row.get(2));
            count++;
        }
        cursor.close();
        System.out.println("  -> " + count + " row(s) returned");
    }
    
    static void cleanup(File f) {
        try {
            if (f.exists()) {
                Table.drop(f);
            }
        } catch (Exception e) {
            // Ignore
        }
    }
}
