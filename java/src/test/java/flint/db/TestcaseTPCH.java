package flint.db;

import java.io.File;

public class TestcaseTPCH {
    // Test case for TPC-H benchmark    

    static String metaSQL = """
CREATE TABLE tpch_lineitem (
	l_orderkey    UINT,
	l_partkey     UINT,
	l_suppkey     UINT16,
	l_linenumber  UINT8,
	l_quantity    DECIMAL(4,2),
	l_extendedprice  DECIMAL(4,2),
	l_discount    DECIMAL(4,2),
	l_tax         DECIMAL(4,2),
	l_returnflag  STRING(1),
	l_linestatus  STRING(1),
	l_shipDATE    DATE,
	l_commitDATE  DATE,
	l_receiptDATE DATE,
	l_shipinstruct STRING(25),
	l_shipmode     STRING(10),
	l_comment      STRING(44),
	
	PRIMARY KEY (l_orderkey, l_linenumber)
) CACHE=50K
"""; // , STORAGE=V2


    public static void main(String[] args) throws Exception {
        var gzFilePath = new File("temp/tpch/lineitem.tbl.gz");
        var outputFile = new File("temp/tpch_lineitem");
        Table.drop(outputFile);

        long MAX_ROWS = Long.MAX_VALUE; // 0 means no limit
        var watch = new IO.StopWatch();
        long rows = 0;
        try(var CLOSER = new IO.Closer()) {
            var tsv = CLOSER.register(TSVFile.open(gzFilePath));
            var target = CLOSER.register(Table.open(outputFile, SQL.parse(metaSQL).meta()));
            var cursor = tsv.find();
            Row r;
            watch.start();
            for(; (r = cursor.next()) != null; rows++) {
                r.id(-1); // reset id
                target.apply(r);
                if (rows > 0 && rows % 1_000_000 == 0) {
                    System.out.printf("Progress: %,d rows%n", rows);
                }
                if (MAX_ROWS > 0 && rows >= MAX_ROWS) {
                    System.out.printf("Max rows reached: %,d%n", rows);
                    break;
                }
            }
        }
        System.out.printf("%,d rows, %s, %sops%n", rows, IO.StopWatch.humanReadableTime(watch.elapsed()), watch.ops(rows)) ;
    }
}
