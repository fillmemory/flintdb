
import java.io.File;
import flint.db.Column;
import flint.db.IO;
import flint.db.Meta;
import flint.db.GenericFile;
import flint.db.Row;
import flint.db.Table;

public class TSVExample {
    public static void main(String[] args) throws Exception {
        // This example demonstrates how to use TSV (Tab-Separated Values) / CSV files with the FlintDB library.
        // It covers inserting new records and querying existing records using a cursor.
        var file = new File("tutorial/tutorial.tsv.gz"); // tsv.gz, csv.gz, tsv, csv, parquet, jsonl.gz, jsonl
        Table.drop(file);

        var meta = new Meta(file.getName())
                .columns(new Column[] {
                        new Column.Builder("customer_id", Column.TYPE_UINT).create(),
                        new Column.Builder("name", Column.TYPE_STRING).bytes(100).create(),
                        new Column.Builder("email", Column.TYPE_STRING).bytes(120).create(),
                        new Column.Builder("created_at", Column.TYPE_TIME).create(),
                })
                .absentHeader(false) // TSV/CSV has header row
                ; // Define metadata for the file

        // 1. Insert a new customer
        try (var closer = new IO.Closer()) {
            System.out.println("---- Inserting customers into TSV:");
            var f = closer.register(GenericFile.create(file, meta.columns())); // OPEN_RDWR
            for (int i = 0; i < 2; i++) {
                var r = Row.create(meta);
                r.set("customer_id", i + 1);
                r.set("name", "John " + (i + 1));
                r.set("email", "john" + (i + 1) + "@tutorial.temp");
                r.set("created_at", new java.util.Date());
                long ok = f.write(r); // success = 0, failure = -1
                if (ok < 0) {
                    System.err.printf("Error %d\n", ok);
                } else {
                    System.out.println("Successfully written: " + r);
                }
            }
            Meta.make(file, meta); // Optional: Create metadata for the file for reading
        }

        // 2. Find customer by ID using cursor
        try (var closer = new IO.Closer()) {
            System.out.println("\n---- Cursor lookup:");
            var f = closer.register(GenericFile.open(file)); // OPEN_RDONLY
            var cursor = f.find("WHERE customer_id = 2 LIMIT 1"); // "ORDER BY" not supported
            for (Row r; (r = cursor.next()) != null;) {
                System.out.println(r);
            }
        }
    }
}