package flint.db;

public class TSVBenchmark {

    public static void main(String[] args) throws Exception {
        try(var CLOSER = new IO.Closer()) {
            var watch = new IO.StopWatch();
            var tsv = CLOSER.register(TSVFile.open(new java.io.File("java/temp/tpch_lineitem.tsv.gz")));
            var cursor = tsv.find();
            var rows = 0;
            for(Row r = null; (r = cursor.next()) != null; ) {
                assert r != null;
                rows++;
            }
            System.out.println("Elapsed: " + watch.elapsed() + ", rows: " + rows + ", ops:" + watch.ops(rows));
        }
    }
}
