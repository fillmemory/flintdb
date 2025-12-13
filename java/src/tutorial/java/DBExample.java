
import java.io.File;

import flint.db.Column;
import flint.db.Filter;
import flint.db.IO;
import flint.db.Index;
import flint.db.Meta;
import flint.db.Row;
import flint.db.Table;

public class DBExample {
    
    public static void main(String[] args) throws Exception {
        var file = new File("tutorial/example.flintdb");
        var meta = new Meta(file.getName())
                .columns(new Column[] {
                        new Column.Builder("customer_id", Column.TYPE_UINT).create(),
                        new Column.Builder("name", Column.TYPE_STRING).bytes(100).create(),
                        new Column.Builder("email", Column.TYPE_STRING).bytes(120).create(),
                        new Column.Builder("created_at", Column.TYPE_TIME).create(),
                })
                .indexes(new Index[] {
                        new Table.PrimaryKey(new String[] { "customer_id" }),
                        new Table.SortKey("ix_email", new String[] { "email" }),
                })
        // .cacheSize(1024*10) // Optional: Memory cache size
        // .storage(Storage.TYPE_DEFAULT) // Optional: Memory-mapped file
        // .compact(512) // Optional: Block by 512 bytes
        ; // Define metadata for the file

        Table.drop(file);

        // 1. Insert a new customer
        try (var closer = new IO.Closer()) {
            System.out.println("---- Inserting customers into FlintDB:");
            var table = closer.register(Table.open(file, meta)); // OPEN_WRITE
            for (int i = 0; i < 2; i++) {
                var r = Row.create(meta);
                r.set("customer_id", i + 1);
                r.set("name", "John " + (i + 1));
                r.set("email", "john" + (i + 1) + "@tutorial.temp");
                r.set("created_at", new java.util.Date());
                long id = table.apply(r); // UPSERT
                if (id > -1) {
                    System.out.printf("applied at %d\n", id);
                } else {
                    System.err.printf("Error %d\n", id);
                }
            }
        }

        // 2. Find customer by ID using cursor with filter
        try (var closer = new IO.Closer()) {
            System.out.println("\n---- Cursor lookup - Type 1:");
            var table = closer.register(Table.open(file)); // OPEN_READ
            var index = table.meta().index(Index.PRIMARY);
            var cursor = table.find(
                    Index.PRIMARY,
                    Filter.ASCENDING, // Cursor direction
                    new Filter.MaxLimit(1),
                    Filter.compile(table.meta(), index, "customer_id = 2")); // = SELECT * FROM <file> WHERE customer_id = 2
            for (long i; (i = cursor.next()) != -1L;) {
                System.out.println(table.read(i));
            }
        }

        // 3. Find customer by ID using cursor with raw query
        try (var closer = new IO.Closer()) {
            System.out.println("\n---- Cursor lookup - Type 2:");
            var table = closer.register(Table.open(file)); // OPEN_READ
            var cursor = table.find("USING INDEX(PRIMARY ASC) WHERE customer_id = 2 LIMIT 1"); // "ORDER BY" not supported
            // if 'USING INDEX(PRIMARY)' is unset, Primary Key will be used
            for (long i; (i = cursor.next()) != -1L;) {
                System.out.println(table.read(i));
            }
        }

        // 4. Find customer by ID using direct lookup
        try (var closer = new IO.Closer()) {
            System.out.println("\n---- Direct lookup:");
            var table = closer.register(Table.open(file)); // OPEN_READ
            Row row = table.one(Index.PRIMARY, Row.mapify("customer_id", 2)); // = SELECT * FROM <file> WHERE customer_id = 2 LIMIT 1
            if (row != null) {
                System.out.println(row);
            } else {
                System.out.println("Not found");
            }
        }

        // 5. Find customers by secondary index
        try (var closer = new IO.Closer()) {
            System.out.println("\n---- Email domain lookup:");
            var table = closer.register(Table.open(file)); // OPEN_READ
            var cursor = table.find(
                    "USING INDEX(ix_email ASC) WHERE email LIKE 'john2@tutorial.temp'"); // = SELECT * FROM <file> WHERE email LIKE '%@tutorial.temp'
            for (long i; (i = cursor.next()) != -1L;) {
                System.out.println(table.read(i));
            }
        }   
    }
}
