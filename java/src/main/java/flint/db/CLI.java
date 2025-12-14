/**
 * FlintDB Command Line Interface (Java Implementation)
 * Based on cli.c - provides command-line access to FlintDB using SQLExec
 * 
 */
package flint.db;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.Iterator;

public final class CLI {
    private static final String VERSION = "0.0.1";
    private static final int MAX_PRETTY_ROWS = 10000;
    
    static boolean LOG = false;

    // // Shutdown hook for graceful cleanup
    // static {
    //     Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    //         // System.err.println("\nReceived shutdown signal, cleaning up...");
    //         try {
    //             SQLExec.cleanup();
    //         } catch (Exception e) {
    //             System.err.println("Error during cleanup: " + e.getMessage());
    //         }
    //     }));
    // }

    public static void main(String[] args) {
        try {
            long result = executeCLI(System.out, args);
            if (result < 0) {
                System.exit(1);
            }
        } catch (DatabaseException e) {
            System.err.println("Database Error (" + e.getErrorCode() + "): " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            if (LOG) {
                e.printStackTrace();
            }
            System.exit(1);
        }
    }

    private static long executeCLI(PrintStream out, String[] args) throws Exception {
        String sql = null;
        String sqlFile = null;
        boolean pretty = false;
        boolean status = false;
        boolean head = true;
        boolean rownum = false;

        for (int i = 0; i < args.length; i++) {
            String s = args[i];
            if ("-help".equals(s)) {
                usage(args.length > 0 ? args[0] : null);
                return 0;
            } else if ("-version".equals(s)) {
                out.println("FlintDB version " + VERSION);
                return 0;
            } else if ("-pretty".equals(s)) {
                pretty = true;
            } else if ("-status".equals(s)) {
                status = true;
            } else if ("-log".equals(s)) {
                LOG = true;
            } else if ("-nohead".equals(s)) {
                head = false;
            } else if ("-rownum".equals(s)) {
                rownum = true;
            } else if ("-sql".equals(s)) {
                if (i + 1 < args.length) {
                    sql = args[++i];
                } else {
                    throw new IllegalArgumentException("-sql requires an argument");
                }
            } else if ("-f".equals(s)) {
                if (i + 1 < args.length) {
                    sqlFile = args[++i];
                } else {
                    throw new IllegalArgumentException("-f requires a file path");
                }
            } else {
                if (sql == null && sqlFile == null) {
                    sql = s;
                }
            }
        }

        if (args.length <= 0) {
            usage(null);
            return 0;
        }

        if (sql == null && sqlFile == null) {
            throw new IllegalArgumentException("SQL statement or file must be specified");
        }

        if (sql != null && sqlFile != null) {
            throw new IllegalArgumentException("Cannot specify both -sql and -f options");
        }

        // Create SQL iterator - from string or file
        SQLIterator sqlIterator = sqlFile != null ? 
            SQLIterator.fromFile(sqlFile) : 
            SQLIterator.fromString(sql);
        
        Iterator<String> statements = sqlIterator.iterator();
        long totalAffected = 0;
        int statementCount = 0;
        boolean hasError = false;

        while (statements.hasNext()) {
            String stmt = statements.next();
            statementCount++;
            
            if (statementCount > 1 && status) {
                out.println(); // blank line between statements
            }

            IO.StopWatch watch = new IO.StopWatch();
            try (SQLResult result = SQLExec.execute(stmt)) {
                if (result == null) {
                    throw new IllegalStateException("Failed to execute SQL");
                }

                long affected;
                
                if (result.getCursor() != null) {
                    String[] columns = result.getColumns();
                    if (columns == null || columns.length == 0) {
                        out.println("Warning: No column information in result");
                        affected = 0;
                    } else {
                        affected = printResults(out, result, pretty, status, head, rownum, watch);
                    }
                } else {
                    long elapsed = watch.elapsed();
                    affected = result.getAffected();
                    
                    if (status) {
                        out.println(formatNumber(affected) + " rows affected, " + IO.StopWatch.humanReadableTime(elapsed));
                    }
                }

                totalAffected += affected;

            } catch (Exception e) {
                System.err.println("Error in statement " + statementCount + ": " + e.getMessage());
                if (LOG) {
                    e.printStackTrace();
                }
                hasError = true;
                // Continue executing remaining statements
            }
        }

        if (hasError) {
            return -1;
        }

        return totalAffected;
    }

    private static long printResults(PrintStream out, SQLResult result, boolean pretty, boolean status,
                                      boolean head, boolean rownum, IO.StopWatch watch) throws Exception {
        String[] columns = result.getColumns();
        Cursor<Row> cursor = result.getCursor();
        PrettyTable table = null;

        if (pretty) {
            table = new PrettyTable(columns.length);
            table.addRow(columns, columns.length);
        } else {
            // Adaptive buffering: start small for quick feedback, grow for performance
            int[] bufferSizes = {256, 512, 1024, 2048, 8192};
            int bufferIndex = 0;
            int currentBufferSize = bufferSizes[bufferIndex];
            
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out), currentBufferSize);
            
            try {
                if (head) {
                    for (int i = 0; i < columns.length; i++) {
                        if (i > 0) writer.write("\t");
                        writer.write(columns[i] != null ? columns[i] : "");
                    }
                    writer.write("\n");
                    writer.flush(); // Flush header immediately
                }

                long rowCount = 0;
                Row r;

                while ((r = cursor.next()) != null) {
                    rowCount++;

                    if (rownum) {
                        writer.write(String.valueOf(rowCount));
                        writer.write("\t");
                    }

                    for (int i = 0; i < columns.length; i++) {
                        if (i > 0) writer.write("\t");
                        Object v = r.get(i);
                        if (v != null) {
                            writer.write(v.toString());
                        } else {
                            writer.write("\\N");
                        }
                    }
                    writer.write("\n");
                    
                    // Adaptive buffer growth and flushing strategy
                    if (rowCount <= 10) {
                        // First 10 rows: flush immediately for quick feedback
                        writer.flush();
                    } else if (rowCount == 50 && bufferIndex < bufferSizes.length - 1) {
                        // After 50 rows, increase buffer size for better performance
                        writer.flush();
                        bufferIndex++;
                        currentBufferSize = bufferSizes[bufferIndex];
                        writer = new BufferedWriter(new OutputStreamWriter(out), currentBufferSize);
                    } else if (rowCount == 200 && bufferIndex < bufferSizes.length - 1) {
                        // After 200 rows, increase again
                        writer.flush();
                        bufferIndex++;
                        currentBufferSize = bufferSizes[bufferIndex];
                        writer = new BufferedWriter(new OutputStreamWriter(out), currentBufferSize);
                    } else if (rowCount == 1000 && bufferIndex < bufferSizes.length - 1) {
                        // After 1000 rows, increase again
                        writer.flush();
                        bufferIndex++;
                        currentBufferSize = bufferSizes[bufferIndex];
                        writer = new BufferedWriter(new OutputStreamWriter(out), currentBufferSize);
                    } else if (rowCount == 5000 && bufferIndex < bufferSizes.length - 1) {
                        // After 5000 rows, max buffer size
                        writer.flush();
                        bufferIndex++;
                        currentBufferSize = bufferSizes[bufferIndex];
                        writer = new BufferedWriter(new OutputStreamWriter(out), currentBufferSize);
                    }
                }

                writer.flush();

                if (status) {
                    long elapsed = watch.elapsed();
                    out.println(formatNumber(rowCount) + " rows, " + IO.StopWatch.humanReadableTime(elapsed));
                }

                return rowCount;
                
            } finally {
                writer.flush();
            }
        }

        // Pretty table mode
        long rowCount = 0;
        Row r;

        while ((r = cursor.next()) != null) {
            rowCount++;
            String[] rowData = new String[columns.length];
            for (int i = 0; i < columns.length; i++) {
                Object v = r.get(i);
                rowData[i] = (v != null) ? v.toString() : "\\N";
            }
            table.addRow(rowData, columns.length);
        }

        table.print(out);

        if (status || pretty) {
            long elapsed = watch.elapsed();
            out.println(formatNumber(rowCount) + " rows, " + IO.StopWatch.humanReadableTime(elapsed));
        }

        return rowCount;
    }

    private static void usage(String progname) {
        String CMD = progname != null ? progname : "./bin/flintdb";
        PrintStream out = System.out;

        out.println("Usage: \"" + CMD + "\" [options]\n");
        out.println(" options:");
        out.println(" \t<SQL>     \tSELECT|INSERT|DELETE|UPDATE|DESC|META|SHOW");
        out.println(" \t-pretty   \tpretty print when sql is SELECT");
        out.println(" \t-status   \tprint the executed status");
        out.println(" \t-log      \tenable detailed logging");
        out.println(" \t-nohead   \tignore header when printing rows");
        out.println(" \t-rownum   \tshow row number when printing rows");
        out.println(" \t-sql <SQL>\tspecify SQL statement");
        out.println(" \t-f <file> \texecute SQL from file");
        out.println(" \t-version  \tshow version information");
        out.println(" \t-help     \tshow this help\n");
        out.println(" examples:");
        out.println("\t# File operations");
        out.println("\t" + CMD + " \"SELECT * FROM temp/tpch_lineitem.flintdb USE INDEX(PRIMARY DESC) WHERE l_orderkey > 1 LIMIT 0, 10\" -rownum -pretty");
        out.println("\t" + CMD + " \"SELECT * FROM temp/tpch_lineitem.tsv.gz WHERE l_orderkey > 1 LIMIT 0, 10\"");
        out.println("\t" + CMD + " \"SELECT * FROM temp/file.flintdb INTO temp/output.tsv.gz\"");
        out.println("\t" + CMD + " \"SELECT col1, col2 FROM temp/input.tsv WHERE col1 > 10 INTO temp/filtered.flintdb\"");
        out.println("\t" + CMD + " \"INSERT INTO temp/file.flintdb FROM temp/input.tsv.gz\"");
        out.println("\t" + CMD + " \"REPLACE INTO temp/file.flintdb FROM temp/input.tsv.gz\"");
        out.println("\t" + CMD + " \"UPDATE temp/file.flintdb SET B = 'abc', C = 2 WHERE A = 1\"");
        out.println("\t" + CMD + " \"DELETE FROM temp/file.flintdb WHERE A = 1\"");
        out.println();
        out.println("\t# JDBC operations (direct URI)");
        out.println("\t" + CMD + " \"SELECT * FROM jdbc:mysql://localhost:3306/mydb?table=users LIMIT 10\" -pretty");
        out.println("\t" + CMD + " \"SELECT * FROM jdbc:h2:mem:test?table=customers WHERE age > 18\"");
        out.println("\t" + CMD + " \"SELECT * FROM jdbc:mysql://localhost:3306/mydb?table=orders INTO output.tsv\"");
        out.println("\t" + CMD + " \"INSERT INTO output.tsv FROM jdbc:postgresql://localhost/db?table=logs\"");
        out.println();
        out.println("\t# JDBC operations (using aliases from jdbc.properties)");
        out.println("\t" + CMD + " \"SELECT * FROM @mydb:users LIMIT 10\" -pretty");
        out.println("\t" + CMD + " \"SELECT * FROM mydb.customers WHERE age > 18\"");
        out.println("\t" + CMD + " \"SELECT name, email FROM @prod:users WHERE status='active' INTO active_users.csv\"");
        out.println("\t" + CMD + " \"INSERT INTO output.tsv FROM @prod:orders\"");
        out.println("\t" + CMD + " \"INSERT INTO data.flintdb FROM testdb.products\"");
        out.println();
        out.println("\t# Metadata operations");
        out.println("\t" + CMD + " \"SHOW TABLES WHERE temp\"");
        out.println("\t" + CMD + " \"DESC temp/file.flintdb\"");
        out.println("\t" + CMD + " \"META temp/file.flintdb\"");
        out.println();
    }

    private static int utf8CharWidth(String s, int pos) {
        if (pos >= s.length()) return 0;
        
        char c = s.charAt(pos);
        if (c >= 0x80) {
            if (c >= 0x4E00 && c <= 0x9FFF) return 2;
            if (c >= 0x3000 && c <= 0x30FF) return 2;
            if (c >= 0xAC00 && c <= 0xD7AF) return 2;
            return 2;
        }
        return 1;
    }

    private static int stringDisplayWidth(String s) {
        int width = 0;
        for (int i = 0; i < s.length(); i++) {
            width += utf8CharWidth(s, i);
        }
        return width;
    }

    static class PrettyTable {
        String[][] rows;
        int rowCount;
        int colCount;
        int[] colWidths;
        int capacity;

        PrettyTable(int colCount) {
            this.colCount = colCount;
            this.capacity = 100;
            this.rowCount = 0;
            this.rows = new String[capacity][];
            this.colWidths = new int[colCount];
        }

        void addRow(String[] rowData, int colCount) {
            if (rowCount >= MAX_PRETTY_ROWS) {
                return;
            }

            if (rowCount >= capacity) {
                int newCapacity = capacity * 2;
                if (newCapacity > MAX_PRETTY_ROWS) newCapacity = MAX_PRETTY_ROWS;
                String[][] newRows = new String[newCapacity][];
                System.arraycopy(rows, 0, newRows, 0, rowCount);
                rows = newRows;
                capacity = newCapacity;
            }

            String[] row = new String[this.colCount];
            for (int i = 0; i < colCount && i < this.colCount; i++) {
                if (rowData[i] != null) {
                    row[i] = rowData[i];
                    int width = stringDisplayWidth(row[i]);
                    if (width > colWidths[i]) {
                        colWidths[i] = width;
                    }
                } else {
                    row[i] = "\\N";
                    if (2 > colWidths[i]) {
                        colWidths[i] = 2;
                    }
                }
            }

            rows[rowCount++] = row;
        }

        void print(PrintStream out) {
            if (rowCount == 0) return;

            printBorder(out);

            if (rowCount > 0) {
                printRow(out, 0);
                printBorder(out);
            }

            for (int i = 1; i < rowCount; i++) {
                printRow(out, i);
            }

            if (rowCount > 1) {
                printBorder(out);
            }
        }

        private void printBorder(PrintStream out) {
            for (int i = 0; i < colCount; i++) {
                if (i > 0) out.print("+");
                for (int j = 0; j < colWidths[i]; j++) {
                    out.print("-");
                }
            }
            out.println();
        }

        private void printRow(PrintStream out, int rowIdx) {
            String[] row = rows[rowIdx];

            for (int i = 0; i < colCount; i++) {
                if (i > 0) out.print("|");

                String cell = row[i] != null ? row[i] : "\\N";
                int displayWidth = stringDisplayWidth(cell);
                int padding = colWidths[i] - displayWidth;

                out.print(cell);
                for (int p = 0; p < padding; p++) {
                    out.print(" ");
                }
            }
            out.println();
        }
    }

    private static String formatNumber(long num) {
        if (num < 1000) {
            return String.valueOf(num);
        } else if (num < 1000000) {
            return String.format("%d,%03d", num / 1000, num % 1000);
        } else if (num < 1000000000) {
            return String.format("%d,%03d,%03d", num / 1000000, (num / 1000) % 1000, num % 1000);
        } else {
            return String.format("%d,%03d,%03d,%03d", num / 1000000000, (num / 1000000) % 1000, (num / 1000) % 1000, num % 1000);
        }
    }


    /**
     * SQL statement iterator that supports both string and file input with streaming
     */
    static class SQLIterator implements Iterable<String> {
        private final java.io.Reader reader;
        private final boolean fromFile;
        private final String sqlString;
        private int pos = 0;
        
        private SQLIterator(String sql) {
            this.sqlString = sql;
            this.reader = null;
            this.fromFile = false;
        }
        
        private SQLIterator(java.io.Reader reader) {
            this.reader = reader;
            this.sqlString = null;
            this.fromFile = true;
        }
        
        public static SQLIterator fromString(String sql) {
            return new SQLIterator(sql);
        }
        
        public static SQLIterator fromFile(String filepath) throws Exception {
            java.io.FileReader fr = new java.io.FileReader(filepath);
            java.io.BufferedReader br = new java.io.BufferedReader(fr, 65536); // 64KB buffer
            return new SQLIterator(br);
        }
        
        @Override
        public Iterator<String> iterator() {
            return new Iterator<String>() {
                private String nextStmt = null;
                private boolean finished = false;
                
                @Override
                public boolean hasNext() {
                    if (nextStmt != null) return true;
                    if (finished) return false;
                    
                    try {
                        nextStmt = readNextStatement();
                        if (nextStmt == null) {
                            finished = true;
                            if (fromFile && reader != null) {
                                reader.close();
                            }
                        }
                        return nextStmt != null;
                    } catch (Exception e) {
                        finished = true;
                        throw new RuntimeException("Error reading SQL statement", e);
                    }
                }
                
                @Override
                public String next() {
                    if (!hasNext()) {
                        throw new java.util.NoSuchElementException();
                    }
                    String stmt = nextStmt;
                    nextStmt = null;
                    return stmt;
                }
                
                private String readNextStatement() throws Exception {
                    final StringBuilder current = new StringBuilder();
                    
                    char quote = 0; // 0=none, '\'', '"', '`'
                    char commentEnd = 0; // 0=none, '\n', '*'
                    char prev = 0;
                    
                    while (true) {
                        int chInt;
                        char ch;
                        
                        if (fromFile) {
                            chInt = reader.read();
                            if (chInt == -1) break; // EOF
                            ch = (char) chInt;
                        } else {
                            if (pos >= sqlString.length()) break;
                            ch = sqlString.charAt(pos++);
                        }
                        
                        // Check for comment start (only when not in quote)
                        if (quote == 0 && commentEnd == 0) {
                            // Single-line comment: --
                            if (ch == '-') {
                                char next = peekNext();
                                if (next == '-') {
                                    consumeNext(); // skip second dash
                                    commentEnd = '\n';
                                    continue;
                                }
                            }
                            // Multi-line comment: /*
                            else if (ch == '/') {
                                char next = peekNext();
                                if (next == '*') {
                                    consumeNext(); // skip asterisk
                                    commentEnd = '*';
                                    continue;
                                }
                            }
                        }
                        
                        // Inside comment - check for end
                        if (commentEnd != 0) {
                            if (commentEnd == '\n') {
                                if (ch == '\n') {
                                    commentEnd = 0;
                                    current.append(' ');
                                }
                            } else if (commentEnd == '*') {
                                // Multi-line comment ends with */
                                if (ch == '*') {
                                    char next = peekNext();
                                    if (next == '/') {
                                        consumeNext(); // skip slash
                                        commentEnd = 0;
                                        current.append(' ');
                                        prev = ch;
                                        continue;
                                    }
                                }
                            }
                            prev = ch;
                            continue;
                        }
                        
                        // Track quotes
                        if (quote != 0) {
                            if (prev != '\\' && ch == quote) {
                                quote = 0;
                            }
                            current.append(ch);
                        } else if (ch == '\'' || ch == '"' || ch == '`') {
                            quote = ch;
                            current.append(ch);
                        } else if (ch == ';') {
                            // Statement separator
                            break;
                        } else {
                            current.append(ch);
                        }
                        
                        prev = ch;
                    }
                    
                    String stmt = current.toString().trim();
                    return stmt.isEmpty() ? null : stmt;
                }
                
                private char peekNext() throws Exception {
                    if (fromFile) {
                        reader.mark(1);
                        int next = reader.read();
                        reader.reset();
                        return next == -1 ? 0 : (char) next;
                    } else {
                        return pos < sqlString.length() ? sqlString.charAt(pos) : 0;
                    }
                }
                
                private void consumeNext() throws Exception {
                    if (fromFile) {
                        reader.read();
                    } else {
                        if (pos < sqlString.length()) pos++;
                    }
                }
            };
        }
    }
}
