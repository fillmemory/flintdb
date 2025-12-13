package flint.db;


public class TestcaseJsonl {
    public static void main(String[] args) throws Exception {
        System.out.println("[Jsonl] demo start");
        final java.io.File out = new java.io.File("temp/jsonl_demo.jsonl");
        out.getParentFile().mkdirs();

        // Define a tiny schema
        final Column[] columns = new Column[] {
            new Column.Builder("id", Column.TYPE_INT64).create(),
            new Column.Builder("name", Column.TYPE_STRING).bytes(128).create(),
            new Column.Builder("score", Column.TYPE_DECIMAL).bytes(20, 5).create(), // DECIMAL(20,5)
            new Column.Builder("dt", Column.TYPE_DATE).create(),
            new Column.Builder("ts", Column.TYPE_TIME).create()
        };

        // Write a few rows
        try (var pfw = JsonlFile.create(out, columns, new Logger.NullLogger())) {
            var meta = pfw.meta();
            {
                Row r = Row.create(meta, Row.mapify(
                    "id", 1L,
                    "name", "Alice",
                    "score", "12.345",
                    "dt", "2025-08-09",
                    "ts", "2025-08-09 10:20:30"
                ));
                pfw.write(r);
            }
            {
                Row r = Row.create(meta, Row.mapify(
                    "id", 2L,
                    "name", "Bob",
                    "score", "98.765",
                    "dt", "2025-08-08",
                    "ts", "2025-08-08 23:59:59"
                ));
                pfw.write(r);
            }
            {
                Row r = Row.create(meta, Row.mapify(
                    "id", 3L,
                    "name", "Carol",
                    "score", "77.000",
                    "dt", "2025-08-07",
                    "ts", "2025-08-07 00:00:00"
                ));
                pfw.write(r);
            }
            System.out.println("[Jsonl] wrote: " + out.getAbsolutePath());
        }

        // Read all rows
        try (var pfr = JsonlFile.open(out, new Logger.NullLogger()); Cursor<Row> c = pfr.find()) {
            System.out.println("[Jsonl] meta: " + pfr.meta());
            Row row;
            while ((row = c.next()) != null) {
                System.out.println(row.toString("\t"));
            }
        }

        // Read with a simple filter
        try (var pfr = JsonlFile.open(out, new Logger.NullLogger()); Cursor<Row> c = pfr.find("WHERE id >= 2")) {
            System.out.println("[Jsonl] filter: WHERE id >= 2");
            Row row;
            while ((row = c.next()) != null) {
                System.out.println(row.toString("\t"));
            }
        }

        System.out.println("[Jsonl] demo done");
    }
}
