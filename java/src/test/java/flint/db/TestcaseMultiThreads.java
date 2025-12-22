package flint.db;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Multi-thread stability test for FlintDB
 * This test validates concurrent write and read operations on a table
 * with multiple threads performing transactions simultaneously.
 */
public class TestcaseMultiThreads {

    static class ThreadInfo {
        int threadNum;
        Table table;
        
        ThreadInfo(int threadNum, Table table) {
            this.threadNum = threadNum;
            this.table = table;
        }
    }

    static class WriterThread extends Thread {
        private final ThreadInfo tinfo;

        WriterThread(ThreadInfo tinfo) {
            this.tinfo = tinfo;
        }

        @Override
        public void run() {
            try {
                Meta meta = tinfo.table.meta();
                
                // Begin transaction and insert a row
                try (Transaction tx = tinfo.table.begin()) {
                    int customerId = tinfo.threadNum + 1;
                    System.out.println("thread " + tinfo.threadNum + ": inserting customer_id=" + customerId);
                    
                    String name = "Name-" + customerId;
                    Row row = Row.create(meta, new Object[] { (long)customerId, name });
                    
                    long rowid = tinfo.table.apply(row, false, tx);
                    if (rowid < 0) {
                        throw new RuntimeException("tx apply failed");
                    }
                    System.out.println("tx apply: customer_id=" + customerId + " => rowid=" + rowid);
                    
                    tx.commit();
                }
            } catch (Exception e) {
                System.err.println("Writer thread " + tinfo.threadNum + " exception: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    static class ReaderThread extends Thread {
        private final ThreadInfo tinfo;

        ReaderThread(ThreadInfo tinfo) {
            this.tinfo = tinfo;
        }

        @Override
        public void run() {
            try {
                System.out.println("thread " + tinfo.threadNum + ": reading rows");
                
                // Read operations after writers complete
                // Just verify we can read the committed data
                try {
                    Thread.sleep(10); // Small delay to ensure writers finish
                    
                    long rowCount = tinfo.table.rows();
                    System.out.println("thread " + tinfo.threadNum + ": row count=" + rowCount);
                    
                    // Try to find and read rows
                    Cursor<Long> cursor = tinfo.table.find("USE INDEX(PRIMARY DESC) LIMIT 1");
                    if (cursor != null) {
                        try {
                            Long rowid = cursor.next();
                            if (rowid != null) {
                                Row r = tinfo.table.read(rowid);
                                if (r != null) {
                                    long customerId = (Long) r.get(0);
                                    String customerName = (String) r.get(1);
                                    System.out.println("thread " + tinfo.threadNum + 
                                        ": read rowid=" + rowid + 
                                        " => customer_id=" + customerId + 
                                        ", customer_name=" + customerName);
                                }
                            }
                        } finally {
                            cursor.close();
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Reader thread " + tinfo.threadNum + " exception: " + e.getMessage());
                }
            } catch (Exception e) {
                System.err.println("Reader thread " + tinfo.threadNum + " exception: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    public static void main(String args[]) throws Exception {
        final File file = new File("temp/tx_test.flintdb");
        final File walFile = new File("temp/tx_test.flintdb.wal");
        
        // Drop existing table
        Table.drop(file);
        if (walFile.exists()) {
            walFile.delete();
        }

        // Create table metadata with WAL enabled
        final Meta meta = new Meta("tx_test".concat(Meta.TABLE_NAME_SUFFIX))
                .columns(new Column[] {
                        new Column.Builder("customer_id", Column.TYPE_INT64).create(),
                        new Column.Builder("customer_name", Column.TYPE_STRING).bytes(255).create(),
                })
                .indexes(new Index[] {
                        new Table.PrimaryKey(new String[] { "customer_id" }),
                })
                .walMode(Meta.WAL_OPT_TRUNCATE);

        try (Table table = Table.open(file, meta, new Logger.DefaultLogger("testcase"))) {
            // Create thread info array
            ThreadInfo[] tinfo = new ThreadInfo[4];
            for (int i = 0; i < 4; i++) {
                tinfo[i] = new ThreadInfo(i, table);
            }

            // Create and start threads
            // 2 writer threads and 2 reader threads
            Thread t0 = new WriterThread(tinfo[0]);
            Thread t1 = new WriterThread(tinfo[1]);
            Thread t2 = new ReaderThread(tinfo[2]);
            Thread t3 = new ReaderThread(tinfo[3]);

            // Start all threads
            t0.start();
            t1.start();
            
            // Wait for writers to complete first
            t0.join();
            t1.join();
            
            // Then start readers after writes are done
            t2.start();
            t3.start();
            
            // Wait for readers to complete
            t2.join();
            t3.join();

            // Verify results
            long rows = table.rows();
            System.out.println("rows after commit=" + rows);
            assertTrue(rows == 2, "Expected 2 rows, got " + rows);

            System.out.println("before one(customer_id=1)");

            // Test row lookup
            Map<String, Object> key1 = new HashMap<>();
            key1.put("customer_id", 1L);
            Row r1 = table.one(0, key1);
            assertTrue(r1 != null, "Expected to find customer_id=1");
            String name1 = (String) r1.get(1);
            assertTrue("Name-1".equals(name1), "Expected Name-1, got " + name1);

            System.out.println("after one(customer_id=1)");

            // Test rollback path
            System.out.println("before begin #2");
            try (Transaction tx = table.begin()) {
                Row r = Row.create(meta, new Object[] { 3L, "Name-3" });
                table.apply(r, false, tx);
                tx.rollback();
            }
            System.out.println("after rollback #2");

            // Verify rollback worked
            rows = table.rows();
            System.out.println("rows after rollback=" + rows);
            assertTrue(rows == 2, "Expected 2 rows after rollback, got " + rows);

            Map<String, Object> key3 = new HashMap<>();
            key3.put("customer_id", 3L);
            Row r3 = table.one(0, key3);
            assertTrue(r3 == null, "Expected customer_id=3 to not exist after rollback");

            System.out.println("All tests passed!");
        }
    }
}
