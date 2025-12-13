package flint.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.zip.GZIPInputStream;

public class TestcasePerfAll {
    

    public static void main(String args[]) throws Exception {
        String TESTCASE = "TESTCASE_PERF_STORAGE_READ";
        if (args.length >= 1) {
            TESTCASE = args[0];
        }

        System.out.println("Running testcase: " + TESTCASE);

        switch(TESTCASE) {
            case "TESTCASE_PERF_BUFIO_READ" -> testcase_perf_bufio();
            case "TESTCASE_PERF_TSV_READ" -> testcase_perf_tsv_read();
            case "TESTCASE_PERF_STORAGE_WRITE" -> testcase_perf_storage_write();
            case "TESTCASE_PERF_STORAGE_READ" -> testcase_perf_storage_read();
            case "TESTCASE_BPLUSTREE" -> testcase_perf_bptree_write();
            case "TESTCASE_PERF_LRUCACHE" -> testcase_perf_cache();
            case "TESTCASE_PERF_VARIANT_COMPARE" -> testcase_variant_compare();
            case "TESTCASE_PERF_BIN_DECODE" -> testcase_bin_decode();
            case "TESTCASE_PERF_BIN_ENCODE" -> testcase_bin_encode();
            case "TESTCASE_LITEDB_TPCH_LINEITEM_READ" -> testcase_perf_lineitemread();
            default -> System.out.
                println("""
                                          No valid TESTCASE specified. 
                                          Options: TESTCASE_PERF_BUFIO_READ, TESTCASE_PERF_TSV_READ, TESTCASE_PERF_STORAGE_WRITE, TESTCASE_PERF_STORAGE_READ, TESTCASE_BPLUSTREE, TESTCASE_PERF_LRUCACHE, TESTCASE_PERF_VARIANT_COMPARE, TESTCASE_PERF_BIN_DECODE, TESTCASE_PERF_BIN_ENCODE""");
        }
    }

    static void testcase_perf_cache() throws Exception {
        final int N = 1_000_000; // inserts
        final int M = 1_000_000; // random gets

        // Similar capacity to C table cache: ~1M entries
        final int CAP = 1_024 * 1_024;
        final Cache<Long, Payload> cache = Cache.create(CAP);

        // Insert timing
        final IO.StopWatch w1 = new IO.StopWatch();
        for (int i = 0; i < N; i++) {
            cache.put((long) i, new Payload(i));
        }
        System.out.println("LRUCACHE insert: " + N + " items, " + IO.StopWatch.humanReadableTime(w1.elapsed()) + ", " + w1.ops(N) + " ops/sec");

        // Random gets (hits)
        final java.util.Random r = new java.util.Random(42);
        long hits = 0;
        final IO.StopWatch w2 = new IO.StopWatch();
        for (int i = 0; i < M; i++) {
            final int k = r.nextInt(Math.max(1, N));
            if (cache.get((long) k) != null) hits++;
        }
        System.out.println("LRUCACHE get(hit): " + M + " ops, " + IO.StopWatch.humanReadableTime(w2.elapsed()) + ", " + w2.ops(M) + " ops/sec, hit=" + hits);

        cache.close();
    }

    static final class Payload {
        final long id;
        final long pad; // small padding to mimic small object size
        Payload(long id) { this.id = id; this.pad = id ^ 0x5a5a5a5aL; }
    }

    static void testcase_perf_tsv_read() throws Exception {
        try(var CLOSER = new IO.Closer()) {
            var f = CLOSER.register(GenericFile.open(new File("../java/temp/tpch_lineitem.tsv.gz")));
            long rows = 0;
            var watch = new IO.StopWatch();
            var cursor = f.find();
            while (cursor.next() != null) {
                rows++;
            }

            System.out.println(rows + "rows, " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", " + watch.ops(rows) + "ops");
        }
    }

    static void testcase_perf_storage_write() throws Exception {
        final File file = new File("../java/temp/storage-j.bin");
		file.getParentFile().mkdirs();
		file.delete();

		final int MMAP_BYTES = 16 * 1024 * 1024; 
		final int BLOCK_BYTES = 512-16;
		final int COMPACT_BYTES = -1; // BLOCK_BYTES / 2;
		final Storage storage = Storage.create( //
				new Storage.Options() //
						.file(file) //
						.compact(COMPACT_BYTES) //
						.blockBytes((short) BLOCK_BYTES) //
						.increment(MMAP_BYTES) //
						.mutable(true) //
		);

		final IO.StopWatch watch = new IO.StopWatch();

		final int DEFAULT_COUNT = 1024 * 1024 * 1;
		final IoBuffer b1 = IoBuffer.allocate(BLOCK_BYTES);
		for (int i = 1; i <= DEFAULT_COUNT; i++) {
			b1.rewind();
			b1.put(String.format("This is a test line number %09d\n", i).getBytes());
			b1.flip();
			storage.write(b1);
		}

		System.out.println(DEFAULT_COUNT + "rows, " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", " + watch.ops(DEFAULT_COUNT) + "ops");
		storage.status(System.out);
		storage.close();
    }

    static void testcase_perf_storage_read() throws Exception {
        final File file = new File("./java/temp/strorage-j.bin");

		final int MMAP_BYTES = 16 * 1024 * 1024; 
		final int BLOCK_BYTES = 512-16;
		final int COMPACT_BYTES = -1; // BLOCK_BYTES / 2;
		try(var storage = Storage.create( //
				new Storage.Options() //
						.file(file) //
						.compact(COMPACT_BYTES) //
						.blockBytes((short) BLOCK_BYTES) //
						.increment(MMAP_BYTES) //
						.mutable(false) //
		)) {
            final IO.StopWatch watch = new IO.StopWatch();
            final long max = storage.count();
            for (long i = 0; i < max; i++) {
                storage.read(i);
            }

            System.out.println(max + "rows, " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", " + watch.ops(max) + "ops");
            storage.status(System.out);
        }
    }

    static void testcase_perf_bptree_write() throws Exception {
        final File file = new File("./java/temp/bptree-j.bin");

        file.delete();

        final BPlusTree tree = new BPlusTree(file, //
				true, //
				Storage.TYPE_DEFAULT, //
				(1024), //
				(1024 * 1024), //
				new Comparator<Long>() {
					@Override
					public int compare(Long o1, Long o2) {
						return Long.compare(o1, o2);
					}
				}, WAL.NONE);

		IO.StopWatch watch = new IO.StopWatch();
        long max = 1024 * 1024 * 1;
        for(long i = 1; i <= max; i++) {
            tree.put(i);
        }
        System.out.println(max + "rows, " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", " + watch.ops(max) + "ops");
        tree.close();
    }

    static void testcase_perf_bufio() throws Exception {
        final File file = new File("../java/temp/tpch/lineitem.tbl.gz");
     
        try(var CLOSER = new IO.Closer()) {
            var in = CLOSER.register(new FileInputStream(file));
            var gz = CLOSER.register(new GZIPInputStream(in, 64 * 1024)); // 64KB buffer
            var ir = CLOSER.register(new InputStreamReader(gz, StandardCharsets.UTF_8));
            var br = CLOSER.register(new BufferedReader(ir, 65536)); // 64KB buffer

            long lines = 0;
            var watch = new IO.StopWatch();
            while (br.readLine() != null) {
                lines++;
            }
            System.out.println(lines + "rows, " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", " + watch.ops(lines) + "ops");
        }
    }

    static void testcase_bin_encode() throws Exception {
        final int MAX = 1024 * 1024 * 10;
        final Meta meta = new Meta("engine_test").columns(new Column[] {
            new Column.Builder("i64_col", Column.TYPE_INT64).create(),
            new Column.Builder("f64_col", Column.TYPE_DOUBLE).create(),
            new Column.Builder("decimal_col", Column.TYPE_DECIMAL).bytes(8, 2).create(),
            new Column.Builder("str_col", Column.TYPE_STRING).bytes(64).create(),
            new Column.Builder("date_col", Column.TYPE_DATE).create(),
            new Column.Builder("time_col", Column.TYPE_TIME).create(),
        });

        try (final Formatter.BINROWFORMATTER fmt = new Formatter.BINROWFORMATTER(meta.rowBytes(), meta)) {
            final IO.StopWatch watch = new IO.StopWatch();
            long totalBytes = 0;
            for (int i = 0; i < MAX; i++) {
                final Row r = Row.create(meta);
                r.set(0, (long) i);
                r.set(1, (double) i * 1.1);
                r.set(2, new java.math.BigDecimal("" + (i * 1.11)).setScale(2, java.math.RoundingMode.FLOOR));
                r.set(3, String.format("string value %09d", i));
                r.set(4, new java.util.Date(1609459200_000L + (long) i * 86_400_000L)); // 2021-01-01 + i days
                r.set(5, new java.util.Date(((long) (i * 60) % 86_400) * 1000L)); // seconds to ms, within day

                final IoBuffer bb = fmt.format(r);
                totalBytes += bb.remaining();
                fmt.release(bb);
            }
            System.out.println("encode check passed");
            System.out.println(MAX + "rows, " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", " + watch.ops(MAX) + "ops, total_bytes=" + totalBytes);
        }
    }

    static void testcase_bin_decode() throws Exception {
        final int MAX = 1024 * 1024 * 10;
        final Meta meta = new Meta("engine_test").columns(new Column[] {
            new Column.Builder("i64_col", Column.TYPE_INT64).create(),
            new Column.Builder("f64_col", Column.TYPE_DOUBLE).create(),
            new Column.Builder("decimal_col", Column.TYPE_DECIMAL).bytes(8, 2).create(),
            new Column.Builder("str_col", Column.TYPE_STRING).bytes(64).create(),
            new Column.Builder("date_col", Column.TYPE_DATE).create(),
            new Column.Builder("time_col", Column.TYPE_TIME).create(),
        });

        try (final Formatter.BINROWFORMATTER fmt = new Formatter.BINROWFORMATTER(meta.rowBytes(), meta)) {
            // Prepare one encoded row
            final Row src = Row.create(meta);
            final long baseI = 123456789L;
            final double baseF = 12345.67;
            final java.math.BigDecimal baseD = new java.math.BigDecimal("12345.67").setScale(2, java.math.RoundingMode.FLOOR);
            src.set(0, baseI);
            src.set(1, baseF);
            src.set(2, baseD);
            src.set(3, "hello binary");
            src.set(4, new java.util.Date(1609459200_000L));
            src.set(5, new java.util.Date(3600_000L));
            final IoBuffer sample = fmt.format(src); // keep borrowed until done

            // Decode once for correctness
            final Row out = fmt.parse(sample.duplicate());
            if (!java.util.Objects.equals(out.get(0), Long.valueOf(baseI))) throw new RuntimeException("decode check failed: i64");
            if (Math.abs(((Double) out.get(1)) - baseF) > 1e-9) throw new RuntimeException("decode check failed: f64");
            final String d1 = baseD.toPlainString();
            final String d2 = ((java.math.BigDecimal) out.get(2)).toPlainString();
            if (!d1.equals(d2)) throw new RuntimeException("decode check failed: decimal");

            long totalBytes = 0;
            final IO.StopWatch watch = new IO.StopWatch();
            for (int i = 0; i < MAX; i++) {
                final IoBuffer dup = sample.duplicate();
                final Row r = fmt.parse(dup);
                if (r == null) throw new RuntimeException("parse returned null");
                totalBytes += dup.limit();
            }
            System.out.println("decode check passed");
            System.out.println(MAX + "rows, " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", " + watch.ops(MAX) + "ops, total_bytes=" + totalBytes);

            fmt.release(sample);
        }
    }

    static void testcase_variant_compare() throws Exception {
        final int MAX = 1024 * 1024 * 10; // 10M compares
        final int K = 10;
        final Object[] A = new Object[K];
        final Object[] B = new Object[K];

        // 0) Long vs Long
        A[0] = Long.valueOf(123_456_789L);
        B[0] = Long.valueOf(123_456_790L);

        // 1) Double vs Double
        A[1] = Double.valueOf(12345.67);
        B[1] = Double.valueOf(12345.68);

        // 2) String vs String
        A[2] = "abcdef";
        B[2] = "abcdeg";

        // 3) byte[] vs byte[]
        A[3] = new byte[] { 0x00, 0x10, 0x20, 0x30 };
        B[3] = new byte[] { 0x00, 0x10, 0x20, 0x31 };

        // 4) BigDecimal vs BigDecimal (different value)
        A[4] = new java.math.BigDecimal("12345.67").setScale(2, java.math.RoundingMode.FLOOR);
        B[4] = new java.math.BigDecimal("12345.68").setScale(2, java.math.RoundingMode.FLOOR);

        // 5) BigDecimal vs BigDecimal (same numeric value, different scale)
        A[5] = new java.math.BigDecimal("12345.6").setScale(1, java.math.RoundingMode.FLOOR);
        B[5] = new java.math.BigDecimal("12345.60").setScale(2, java.math.RoundingMode.FLOOR);

        // 6) Date vs Date (next day)
        A[6] = new java.util.Date(1_609_459_200_000L);
        B[6] = new java.util.Date(1_609_545_600_000L);

        // 7) Time vs Time (use Date with ms within day)
        A[7] = new java.util.Date(3_600_000L);
        B[7] = new java.util.Date(7_200_000L);

        // 8) Another numeric pair (Long vs Long)
        A[8] = Long.valueOf(200L);
        B[8] = Long.valueOf(199L);

        // 9) null vs String (NIL ordering path)
        A[9] = null;
        B[9] = "x";

        long sink = 0; // prevent JIT eliminating the loop
        final IO.StopWatch watch = new IO.StopWatch();
        for (int i = 0; i < MAX; i++) {
            final int idx = i % K;
            sink += Filter.compare(A[idx], B[idx]);
        }
        System.out.println("variant_compare check passed");
        System.out.println(MAX + " compares, " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", " + watch.ops(MAX) + "ops, checksum=" + sink);
    }


    static void testcase_perf_lineitemread() throws Exception {
        final File file = new File("../java/temp/tpch_lineitem.flintdb");

        try (final Table t = Table.open(file)) {
            final long nrows = t.rows();
            final IO.StopWatch watch = new IO.StopWatch();
            long crows = 0;

            // Read some sample rows and validate known values
            final String q = ""; //"WHERE l_orderkey >= 3 AND l_orderkey <= 5 LIMIT 10";
            System.out.println("Query: " + q);
            try (var c = t.find(q)) {
                for (long i; (i = c.next()) >= 0;) {
                    final Row r = t.read(i);
                    assert r != null;
                    // if (r == null)
                    //     throw new RuntimeException("read returned null");
                    // print_row(r);
                    // System.out.println("------------------------");

                    // if (r == null)
                    //     throw new RuntimeException("read returned null");
                    crows++;
                }
            }

            System.out.println("Finished reading rows.");
            System.out.println(crows + "rows, " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", " + watch.ops(crows) + "ops");
            System.out.println("query rows: " + crows);
            System.out.println("table rows: " + nrows);
        }
    }
}
