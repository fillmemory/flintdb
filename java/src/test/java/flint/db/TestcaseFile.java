package flint.db;


public class TestcaseFile {
    public static void main(String[] args) throws Exception {
        var meta = new Meta("test.csv")
            .columns(new Column[] {
                new Column.Builder("id", Column.TYPE_STRING).bytes(36).create(),
                new Column.Builder("name", Column.TYPE_STRING).bytes(64).create(),
                new Column.Builder("age", Column.TYPE_INT).create(),
                new Column.Builder("created_at", Column.TYPE_TIME).create()
            });
        try (var fmt = new TSVFile.TEXTROWFORMATTER("test.csv.gz", 
            meta.columns(),
            TSVFile.Format.CSV
        )) {
            var row = Row.create(meta, new String[] {
                "123e4567-e89b-12d3-a456-426614174000",
                "John,\tDoe",
                "30",
                "2023-10-01 12:34:56"
            });
            var formatted = new String(fmt.format(row));
            System.out.println("Original Row: " + row);
            System.out.printf("Formatted: %s\n", formatted);

            var parsed = fmt.parse(formatted);
            System.out.println("Parsed Row: " + parsed.map());
        }
    }
}
