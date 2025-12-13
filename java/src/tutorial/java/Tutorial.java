
import java.io.File;
import java.sql.Timestamp;
import java.util.Locale;
import java.util.Random;

import flint.db.Column;
import flint.db.GenericFile;
import flint.db.Meta;
import flint.db.Row;

/**
 * Tutorial data generator for FlintDB
 * - Deterministic with seed
 * - Configurable (rows, outdir, format, gzip)
 * - Referential integrity across tables
 */
final class Tutorial {

    static final class Config {
        int customers = 1_000;          // number of customers
        int avgOrdersPerCustomer = 5;   // average orders per customer
        int avgItemsPerOrder = 4;       // average items per order
        long seed = 42L;                // RNG seed for determinism
        String format = "tsv";          // tsv | csv
        boolean gzip = true;            // compress outputs
        File outDir = new File("tutorial/data");
        long now = System.currentTimeMillis();
    }

    public static void main(String[] args) throws Exception {
        // Print usage and exit if help is requested
        if (args != null) {
            for (String a : args) {
                if (a == null) continue;
                final String s = a.trim();
                if ("--help".equalsIgnoreCase(s) || "-h".equalsIgnoreCase(s) || "help".equalsIgnoreCase(s)) {
                    printUsage(new Config());
                    return;
                }
            }
        }

        final Config c = parseArgs(args);
        c.outDir.mkdirs();

        final Random rnd = new Random(c.seed);
        final String ext = c.format.equalsIgnoreCase("csv") ? ".csv" : ".tsv";
        final String gz = c.gzip ? ".gz" : "";

        final File customersFile = new File(c.outDir, "customers" + ext + gz);
        final File ordersFile = new File(c.outDir, "orders" + ext + gz);
        final File itemsFile = new File(c.outDir, "order_items" + ext + gz);

        System.out.printf(Locale.ROOT,
                "Generating tutorial data -> outDir=%s, customers=%d, avgOrders=%d, avgItems=%d, format=%s, gzip=%s, seed=%d%n",
                c.outDir.getPath(), c.customers, c.avgOrdersPerCustomer, c.avgItemsPerOrder, c.format.toUpperCase(Locale.ROOT), c.gzip, c.seed);

        int totalOrders = 0;
        int totalItems = 0;

        try (var tsv = GenericFile.create(customersFile, customersSchema())) {
            final var meta = tsv.meta();
            for (int cid = 1; cid <= c.customers; cid++) {
                final var row = Row.create(meta);
                row.set("customer_id", cid + 1_000_000);
                final String name = fakeName(rnd, cid);
                row.set("name", name);
                row.set("email", emailFor(name, cid));
                row.set("created_at", randomTime(rnd, c.now, 365));
                tsv.write(row);
            }
            Meta.make(new File(customersFile.getParentFile(), customersFile.getName() + Meta.META_NAME_SUFFIX), meta);
        }

        try (var tsv = GenericFile.create(ordersFile, ordersSchema())) {
            final var meta = tsv.meta();
            // Poisson-ish using geometric around avg
            int orderkey = 1_000_000;
            for (int cid = 1; cid <= c.customers; cid++) {
                final int ordersForCustomer = clamp((int) Math.round(geometric(rnd, 1.0 / Math.max(1, c.avgOrdersPerCustomer))), 0, c.avgOrdersPerCustomer * 3);
                for (int i = 0; i < ordersForCustomer; i++) {
                    final var row = Row.create(meta);
                    row.set("orderkey", orderkey);
                    row.set("customer_id", cid);
                    row.set("amount", 10 + rnd.nextDouble() * 990);         // 10.00 ~ 1000.00
                    row.set("order_at", randomTime(rnd, c.now, 365));
                    row.set("status", (i % 4 == 0) ? 2 : (i % 2));            // 0/1/2 spread
                    tsv.write(row);
                    orderkey++;
                }
                totalOrders += ordersForCustomer;
            }
            Meta.make(new File(ordersFile.getParentFile(), ordersFile.getName() + Meta.META_NAME_SUFFIX), meta);
        }

        try (var tsv = GenericFile.create(itemsFile, orderItemsSchema())) {
            final var meta = tsv.meta();
            int linenoMax = Math.max(1, c.avgItemsPerOrder * 2 + 2);
            int orderkey = 1_000_000;
            for (int cid = 1; cid <= c.customers; cid++) {
                final int ordersForCustomer = clamp((int) Math.round(geometric(new Random(c.seed ^ cid), 1.0 / Math.max(1, c.avgOrdersPerCustomer))), 0, c.avgOrdersPerCustomer * 3);
                for (int o = 0; o < ordersForCustomer; o++, orderkey++) {
                    final int items = clamp((int) Math.round(geometric(rnd, 1.0 / Math.max(1, c.avgItemsPerOrder))), 1, linenoMax);
                    for (int ln = 1; ln <= items; ln++) {
                        final long now = c.now - (long) (rnd.nextDouble() * 1000L * 60 * 60 * 24 * 365);
                        final var row = Row.create(meta);
                        row.set("orderkey", orderkey);
                        row.set("partkey", rnd.nextInt(10_000));
                        row.set("suppkey", rnd.nextInt(10_000));
                        row.set("linenumber", ln);
                        final double qty = 1 + rnd.nextInt(10);
                        final double price = 5 + rnd.nextDouble() * 495; // 5.00 ~ 500.00
                        final double disc = round2(rnd.nextDouble() * 0.3); // 0.00 ~ 0.30
                        final double tax = round2(rnd.nextDouble() * 0.25); // 0.00 ~ 0.25
                        row.set("quantity", qty);
                        row.set("extendedprice", price * qty);
                        row.set("discount", disc);
                        row.set("tax", tax);
                        row.set("returnflag", (ln % 7 == 0) ? "R" : (ln % 2 == 0 ? "A" : "N"));
                        row.set("linestatus", (ln % 3 == 0) ? "F" : "O");
                        row.set("ship_at", new Timestamp(now));
                        row.set("commit_at", new Timestamp(now + 1000L * 60 * 60 * 24)); // +1d
                        row.set("receipt_at", new Timestamp(now + 1000L * 60 * 60 * 48)); // +2d
                        row.set("shipinstruct", (ln % 2 == 0) ? "DELIVER IN PERSON" : "NONE");
                        row.set("shipmode", (ln % 2 == 0) ? "AIR" : "TRUCK");
                        row.set("comment", "sample item");
                        tsv.write(row);
                        totalItems++;
                    }
                }
            }
            Meta.make(new File(itemsFile.getParentFile(), itemsFile.getName() + Meta.META_NAME_SUFFIX), meta);
        }

        System.out.printf(Locale.ROOT, "Done. customers=%d, orders≈%d, items≈%d%n", c.customers, totalOrders, totalItems);
        System.out.printf(Locale.ROOT, "Files:%n  - %s%n  - %s%n  - %s%n", customersFile, ordersFile, itemsFile);
    }

    // Schemas
    private static Column[] customersSchema() {
        return new Column[] {
                new Column.Builder("customer_id", Column.TYPE_UINT).create(),
                new Column.Builder("name", Column.TYPE_STRING).bytes(100).create(),
                new Column.Builder("email", Column.TYPE_STRING).bytes(120).create(),
                new Column.Builder("created_at", Column.TYPE_TIME).create(),
        };
    }

    private static Column[] ordersSchema() {
        return new Column[] {
                new Column.Builder("orderkey", Column.TYPE_UINT).create(),
                new Column.Builder("customer_id", Column.TYPE_UINT).create(),
                new Column.Builder("amount", Column.TYPE_DECIMAL).bytes(12, 2).create(),
                new Column.Builder("order_at", Column.TYPE_TIME).create(),
                new Column.Builder("status", Column.TYPE_INT8).create(),
        };
    }

    private static Column[] orderItemsSchema() {
        return new Column[] {
                new Column.Builder("orderkey", Column.TYPE_UINT).create(),
                new Column.Builder("partkey", Column.TYPE_UINT).create(),
                new Column.Builder("suppkey", Column.TYPE_UINT16).create(),
                new Column.Builder("linenumber", Column.TYPE_UINT8).create(),
                new Column.Builder("quantity", Column.TYPE_DECIMAL).bytes(8, 2).create(),
                new Column.Builder("extendedprice", Column.TYPE_DECIMAL).bytes(14, 2).create(),
                new Column.Builder("discount", Column.TYPE_DECIMAL).bytes(4, 2).create(),
                new Column.Builder("tax", Column.TYPE_DECIMAL).bytes(4, 2).create(),
                new Column.Builder("returnflag", Column.TYPE_STRING).bytes(1).create(),
                new Column.Builder("linestatus", Column.TYPE_STRING).bytes(1).create(),
                new Column.Builder("ship_at", Column.TYPE_TIME).create(),
                new Column.Builder("commit_at", Column.TYPE_TIME).create(),
                new Column.Builder("receipt_at", Column.TYPE_TIME).create(),
                new Column.Builder("shipinstruct", Column.TYPE_STRING).bytes(25).create(),
                new Column.Builder("shipmode", Column.TYPE_STRING).bytes(10).create(),
                new Column.Builder("comment", Column.TYPE_STRING).bytes(64).create(),
        };
    }

    // Helpers
    private static void printUsage(Config d) {
        var clazzName = Tutorial.class.getName();
        System.out.println("Usage: java " + clazzName + " [options]\n");
        System.out.println("Options:");
        System.out.println("  --customers=N         Number of customers (alias: --rows) [default: " + d.customers + "]");
        System.out.println("  --avg-orders=N        Average orders per customer [default: " + d.avgOrdersPerCustomer + "]");
        System.out.println("  --avg-items=N         Average items per order [default: " + d.avgItemsPerOrder + "]");
        System.out.println("  --outdir=PATH         Output directory [default: " + d.outDir.getPath() + "]");
        System.out.println("  --format=tsv|csv      Output format [default: " + d.format + "]");
        System.out.println("  --gzip=true|false     Compress output with gzip [default: " + d.gzip + "]");
        System.out.println("  --seed=N              RNG seed for deterministic data [default: " + d.seed + "]");
        System.out.println("  --help|-h             Show this help and exit\n");

        System.out.println("Examples:");
        System.out.println("  java " + clazzName + " --customers=1000 --avg-orders=5 --avg-items=3");
        System.out.println("  java " + clazzName + " --outdir=temp/demo --format=csv --gzip=false");
        System.out.println("  java " + clazzName + " --seed=123 --customers=100 --avg-orders=2 --avg-items=2");
    }

    private static Config parseArgs(String[] args) {
        final Config c = new Config();
        if (args == null) return c;
        for (String a : args) {
            if (a == null) continue;
            if (a.startsWith("--rows=") || a.startsWith("--customers=")) {
                c.customers = Integer.parseInt(a.substring(a.indexOf('=') + 1));
            } else if (a.startsWith("--avg-orders=")) {
                c.avgOrdersPerCustomer = Integer.parseInt(a.substring(a.indexOf('=') + 1));
            } else if (a.startsWith("--avg-items=")) {
                c.avgItemsPerOrder = Integer.parseInt(a.substring(a.indexOf('=') + 1));
            } else if (a.startsWith("--outdir=")) {
                c.outDir = new File(a.substring(a.indexOf('=') + 1));
            } else if (a.startsWith("--format=")) {
                final String f = a.substring(a.indexOf('=') + 1).toLowerCase(Locale.ROOT);
                c.format = ("csv".equals(f)) ? "csv" : "tsv";
            } else if (a.startsWith("--gzip=")) {
                c.gzip = Boolean.parseBoolean(a.substring(a.indexOf('=') + 1));
            } else if (a.startsWith("--seed=")) {
                c.seed = Long.parseLong(a.substring(a.indexOf('=') + 1));
            }
        }
        return c;
    }

    private static String fakeName(Random rnd, int id) {
        String[] first = { "Alex", "Chris", "Dana", "Evan", "Grace", "Helen", "Ira", "Jules", "Kai", "Lee", "Mina", "Noah", "Owen", "Pia", "Quinn", "Rae", "Sam", "Tess", "Uma", "Vik", "Wes", "Xena", "Yuri", "Zoe" };
        String[] last = { "Kim", "Park", "Choi", "Jung", "Han", "Yoon", "Kang", "Cho", "Shin", "Lim", "Song", "Lee" };
        return first[(id + rnd.nextInt(first.length)) % first.length] + " " + last[(id + rnd.nextInt(last.length)) % last.length];
    }

    private static String emailFor(String name, int id) {
        final String local = name.toLowerCase(Locale.ROOT).replace(' ', '.');
        return local + id + "@example.com";
    }

    private static Timestamp randomTime(Random rnd, long base, int pastDays) {
        long delta = (long) (rnd.nextDouble() * pastDays * 24L * 60 * 60 * 1000);
        return new Timestamp(base - delta);
    }

    private static int clamp(int v, int lo, int hi) { return Math.max(lo, Math.min(hi, v)); }

    private static double geometric(Random rnd, double p) {
        // mean = 1/p, support >= 1
        double u = rnd.nextDouble();
        return Math.ceil(Math.log(1 - u) / Math.log(1 - p));
    }

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }
}
