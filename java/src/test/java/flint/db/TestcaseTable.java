package flint.db;

import java.io.File;

public final class TestcaseTable {
    
    public static void main(String[] args) throws Exception {
        var file = new File(System.getProperty("user.home"), "works/aml/data/infer/merged_results.flintdb");

        try(var CLOSER = new IO.Closer()) {
            var table = CLOSER.register(Table.open(file));
            var cursor = CLOSER.register(table.find(
                "WHERE customer_id = 2229645237098762688 LIMIT 0, 1000"
            ));
            for(long i; (i = cursor.next()) != -1;) {
                System.out.println(table.read(i));
            }
        }
    }
}
