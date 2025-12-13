package flint.db;

import java.io.File;
import java.time.LocalDate;
import java.util.List;
import java.util.regex.Pattern;

public class TestcaseUnionFiles {

    public static void main(String[] args) throws Exception {
        testDateFileFilter();
        // testDateDirectoryFilterSynthetic();
    }


    @SuppressWarnings("unchecked")
    public static void testDateFileFilter() throws Exception {
        File dir = new File(System.getProperty("user.home"), "works/aml/data/train");
        String pattern = "app_logs_.*\\.parquet"; //
        var maxdays = 365;
        var to = LocalDate.now();
        var from = to.minusDays(maxdays);
        // var formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        System.out.println("Scanning directory: " + dir);

        var watch = new IO.StopWatch();
        try (var t = Union.open(dir, 2, pattern, "ASC", new Union.DateFileFilter(from, to))) {
            Comparable<Row>[] filter = new Comparable[] {
                Filter.ALL, new Comparable<Row>() {
                    @Override
                    public int compareTo(Row o) {
                        return 0;
                    }
                }
            };

            filter = Filter.compile(t.meta(), null, "customer_id = 2229645237103779204 ");

            long rows = 0;
            try(Cursor<Row> cursor = t.find(5, new Filter.MaxLimit(0, 100), filter)) {
                for(Row row; (row = cursor.next()) != null;) {
                    System.out.println("" + row);
                    rows++;
                }
            }
            System.out.println("Total rows: " + rows);
            System.out.println("Elapsed: " + IO.StopWatch.humanReadableTime(watch.elapsed()));
            // System.out.println(SQL.stringify(t.meta()));
        }
    }

    /**
     * Synthetic verification of DateDirectoryFilter without opening files.
     * It creates a temporary directory structure with various date encodings in folder names
     * and asserts that Union.findFiles picks only files within the date range from ancestor directories.
     */
    public static void testDateDirectoryFilterSynthetic() throws Exception {
        File root = createTempDir("ddf_test_");
        try {
            // Build directory tree
            // .../2024/07/27/app_logs_1.csv.gz (OUT)
            // .../2024/07/28/app_logs_2.csv.gz (IN)
            // .../2024/07/29/app_logs_3.csv.gz (IN)
            // .../logs_2024-07-30/app_logs_4.csv.gz (IN)
            // .../20240731/app_logs_5.csv.gz (IN)
            // .../2024-08-01/app_logs_6.csv.gz (OUT)
            // .../other/no_date/app_logs_7.csv.gz (OUT - no date)

            touch(file(root, "2024/07/27/app_logs_1.csv.gz"));
            touch(file(root, "2024/07/28/app_logs_2.csv.gz"));
            touch(file(root, "2024/07/29/app_logs_3.csv.gz"));
            touch(file(root, "logs_2024-07-30/app_logs_4.csv.gz"));
            touch(file(root, "20240731/app_logs_5.csv.gz"));
            touch(file(root, "2024-08-01/app_logs_6.csv.gz"));
            touch(file(root, "other/no_date/app_logs_7.csv.gz"));

            LocalDate from = LocalDate.of(2024, 7, 28);
            LocalDate to = LocalDate.of(2024, 7, 31);
            Pattern regex = Pattern.compile("app_logs_.*\\.csv\\.gz");

            List<File> matched = Union.findFiles(root, regex, new Union.DateDirectoryFilter(from, to), 0, 4);

            // Expect 4 files within range
            System.out.println("[DateDirectoryFilter] matched count = " + matched.size());
            for (File f : matched) {
                System.out.println("  -> " + f.getAbsolutePath());
            }

            if (matched.size() != 4)
                throw new AssertionError("Expected 4 files, got " + matched.size());
        } finally {
            deleteRecursively(root);
        }
    }

    private static File createTempDir(String prefix) {
        File base = new File("temp/test1");
        File dir = new File(base, prefix + Long.toHexString(System.nanoTime()));
        dir.mkdirs();
        return dir;
    }

    private static File file(File root, String rel) {
        File f = new File(root, rel);
        f.getParentFile().mkdirs();
        return f;
    }

    private static void touch(File f) throws Exception {
        if (!f.exists()) {
            f.getParentFile().mkdirs();
            if (!f.createNewFile()) {
                throw new RuntimeException("Failed to create file: " + f);
            }
        }
    }

    private static void deleteRecursively(File f) {
        if (f == null || !f.exists()) return;
        if (f.isDirectory()) {
            File[] kids = f.listFiles();
            if (kids != null) {
                for (File k : kids) deleteRecursively(k);
            }
        }
        try { f.delete(); } catch (Throwable ignore) {}
    }
}
