
import java.io.File;
import flint.db.Aggregate;
import flint.db.Column;
import flint.db.IO;
import flint.db.Meta;
import flint.db.GenericFile;
import flint.db.Row;
import flint.db.Sortable;
import flint.db.Table;

public class GroupbyExample {
    
    public static void main(String[] args) throws Exception {
        var file = new File("tutorial/example_groupby.tsv");
        var meta = new Meta(file.getName())
                .columns(new Column[] {
                        new Column.Builder("category", Column.TYPE_STRING).bytes(50).create(),
                        new Column.Builder("item", Column.TYPE_STRING).bytes(100).create(),
                        new Column.Builder("price", Column.TYPE_UINT).create(),
                });

        Table.drop(file); // DROP FILE

        // 1. Insert sample items
        try (var closer = new IO.Closer()) {
            System.out.println("---- Inserting items into TSV:");
            var f = closer.register(GenericFile.create(file, meta.columns())); // OPEN_RDWR
            String[][] items = {
                    { "Fruit", "Apple", "100" },
                    { "Fruit", "Banana", "80" },
                    { "Fruit", "Orange", "90" },
                    { "Vegetable", "Carrot", "50" },
                    { "Vegetable", "Broccoli", "70" },
            };
            for (var it : items) {
                var r = Row.create(meta);
                r.set("category", it[0]);
                r.set("item", it[1]);
                r.set("price", Integer.valueOf(it[2]));
                long ok = f.write(r); // success = 0, failure = -1
                if (ok < 0) {
                    System.err.printf("Error %d\n", ok);
                } else {
                    System.out.println("Successfully written: " + r);
                }
            }
            Meta.make(file, meta); // Optional: Create metadata for the file for reading
        }

        // 2. Group by category and calculate total price (= SQL GROUP BY)
        try (var closer = new IO.Closer()) {
            System.out.println("\n---- Group By category:");
            var f = closer.register(GenericFile.open(file)); // OPEN_RDONLY
            var cursor = closer.register(f.find()); // SELECT * FROM

            var groupby = new Aggregate.Groupby[] { // GROUP BY
                    new Aggregate.Groupby("category", Column.TYPE_STRING)
            };
            var funs = new Aggregate.Function[] { // AGGREGATE FUNCTIONS
                    new Aggregate.SUM("total_price", "price", Aggregate.Condition.True),
                    new Aggregate.COUNT("item_count", "item", Aggregate.Condition.True),
                    new Aggregate.AVG("average_price", "price", Aggregate.Condition.True),
                    new Aggregate.DISTINCT_COUNT("item_count_distinct", new String[] {"item"}, Aggregate.Condition.True),
                    new Aggregate.DISTINCT_APPROX_COUNT("item_count_distinct_hll", new String[] {"item"}, Aggregate.Condition.True),
                    new Aggregate.DISTINCT_ROARING_BITMAP_COUNT("item_count_distinct_rb", new String[] {"item"}, Aggregate.Condition.True)
            };

            // Create aggregation object
            var aggr = closer.register(new Aggregate(groupby, funs));

            // Process each row 
            for (Row r; (r = cursor.next()) != null;) {
                // System.out.println(r);
                aggr.row(r);
            }

            // Compute the results
            var result = aggr.compute();

            // ORDER BY category, if needed
            var tempFile = new File("tutorial/groupby.bin");
            tempFile.getParentFile().mkdirs();
            var sorter = closer.register(new Sortable.FileSorter(tempFile, aggr.meta()));
            for(Row r: result) {
                sorter.add(r);
            }
            sorter.sort((r1, r2) -> {
                return r1.getString("category").compareTo(r2.getString("category"));
            });

            // Print the results
            for(int i=0; i<sorter.rows(); i++) {
                System.out.println(sorter.read(i));
            }
        }
    }
}
