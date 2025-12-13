package flint.db.pipeline;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import flint.db.Column;
import flint.db.Cursor;
import flint.db.GenericFile;
import flint.db.IO;
import flint.db.Index;
import flint.db.Logger;
import flint.db.Meta;
import flint.db.Row;
import flint.db.Sortable;
import flint.db.Table;

/**
 * File Format Data Pipeline
 * 
 * Transform record files from input to output with mapping, sorting, and
 * optional filtering,
 * 
 * @see tutorial/java/DATA_PIPELINE.md
 * @see tutorial/java/data_pipeline.xml
 * 
 *      <pre>
 * Usage: ./bin/pipeline.sh rule1.xml rule2.xml ...
 * 
 * // Programmatic API usage:
 * DataPipeline.createBuilder("my-transform")
 *     .maxThreads(4)
 *     .maxErrors(10)
 *     .dateRangeVariable("date", "yyyyMMdd", -29, 0)
 *     .envVariable("home", "HOME")
 *     .input()
 *         .directory("{home}/data/input/{date}")
 *         .pattern(".*\\.csv")
 *         .formatType("csv")
 *         .done()
 *     .output()
 *         .directory("/data/output")
 *         .filename("{date}-{basename}.parquet")
 *         .column("id", "long").from("user_id").done()
 *         .column("name", "string").bytes(100).from("user_name").done()
 *         .done()
 *     .build()
 *     .run();
 *      </pre>
 * <pre>
 * TODO: 
 * 1. Add more data pipeline functions (weekday, month start/end, hour, etc.)
 * 2. MULTI OUTPUT support
 * </pre>
 */
public final class DataPipeline implements Runnable {

    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(DataPipeline.class.getName());
    
    static {
        // Configure console logging with custom format
        LOGGER.setUseParentHandlers(false);
        final ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.ALL);
        handler.setFormatter(new Formatter() {
            private final DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            @Override
            public String format(LogRecord record) {
                String timestamp = java.time.LocalDateTime.now().format(df);
                String threadName = Thread.currentThread().getName();
                return String.format("%s [%s] %s", timestamp, threadName, record.getMessage());
            }
        });
        LOGGER.addHandler(handler);
        LOGGER.setLevel(Level.INFO);
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            System.exit(1);
        }
        for (String arg : args) {
            var f = new File(arg);
            if (!f.exists() || !f.isFile()) {
                error("Data pipeline config file not found: %s%n", arg);
                continue;
            }
            new DataPipeline(f).run();
        }
    }

    static void usage() {
        LOGGER.info("Usage: java flint.db.pipeline.DataPipeline <data_pipeline-config.xml> ...");
        LOGGER.info("  Data pipeline record files according to the specified XML config files.");
    }

    private final File file;
    private final DataPipelineConfig config;

    public DataPipeline(File file) {
        this.file = file;
        this.config = null;
    }

    private DataPipeline(DataPipelineConfig config) {
        this.file = null;
        this.config = config;
    }

    /**
     * Create a builder for programmatic data pipeline configuration
     * @param name Data pipeline name
     * @return Builder instance
     */
    public static Builder createBuilder(String name) {
        return new Builder(name);
    }

    static void log(String fmt, Object... args) {
        LOGGER.info(String.format(fmt, args));
    }
    
    static void error(String fmt, Object... args) {
        LOGGER.severe(String.format(fmt, args));
    }

    @Override
    public void run() {
        try {
            final var t0 = new IO.StopWatch();
            final DataPipelineConfig cfg;
            if (config != null) {
                log("[DATA_PIPELINE] using programmatic config: %s\n", config.name);
                cfg = config;
            } else {
                log("[DATA_PIPELINE] loading xml: %s\n", file.getCanonicalPath());
                cfg = DataPipelineConfig.parse(file);
            }
            log("[DATA_PIPELINE] name=%s, maxThreads=%d, maxErrors=%d\n", cfg.name, cfg.maxThreads, cfg.maxErrors);
            final java.util.List<Map<String, String>> varSets = cfg.computeVariableSets();
            if (varSets.size() > 1)
                log("[DATA_PIPELINE] variable sets=%d (range processing)\n", varSets.size());
            // Single shared pool for all work (dates and groups)
            final int threads = Math.max(1, cfg.maxThreads);
            final ExecutorService sharedPool = Executors.newFixedThreadPool(threads);

            // Schedule all groups across all variable sets into the same pool
            final java.util.List<Future<long[]>> allFutures = new java.util.ArrayList<>();
            int idx = 0;
            for (final Map<String, String> vars : varSets) {
                final int cur = ++idx;
                if (!vars.isEmpty())
                    log("[VARS %d/%d] %s\n", cur, varSets.size(), vars.toString());
                // Execute optional <onstart> for each variable set (after substitution)
                if (cfg.onStart != null && cfg.onStart.script != null && !cfg.onStart.script.isBlank()) {
                    final String shType = cfg.onStart.type == null ? defaultShellType() : cfg.onStart.type;
                    final String script = substitute(cfg.onStart.script, vars);
                    log("[ONSTART] type=%s, policy=%s\n", shType, cfg.onStart.errorPolicy);
                    final int ec = runShell(shType, script);
                    if (ec != 0) {
                        if ("exit".equalsIgnoreCase(cfg.onStart.errorPolicy)) {
                            throw new RuntimeException("onstart script failed with exit code " + ec);
                        } else {
                            error("[ONSTART] non-zero exit (%d), continuing due to policy=%s%n", ec,
                                    cfg.onStart.errorPolicy);
                        }
                    }
                }
                allFutures.addAll(schedule(cfg, vars, sharedPool));
            }

            long totalIn = 0, totalOut = 0;
            for (final Future<long[]> f : allFutures) {
                final long[] r = (long[]) f.get();
                if (r != null && r.length == 2) {
                    totalIn += r[0];
                    totalOut += r[1];
                }
            }
            sharedPool.shutdown();
            if (!sharedPool.awaitTermination(1, TimeUnit.SECONDS))
                sharedPool.shutdownNow();
            log("[TOTAL] rows: in=%d out=%d (groups=%d)\n", totalIn, totalOut, allFutures.size());
            log("[DATA_PIPELINE] done in %,d ms\n", t0.elapsed());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    // Schedule tasks for one variable set and return futures without waiting
    private java.util.List<Future<long[]>> schedule(final DataPipelineConfig cfg, final Map<String, String> vars,
            final ExecutorService pool)
            throws Exception {

        // Resolve input candidates (support {var} substitution in directory)
        final String inDirStr = substitute(cfg.input.directory, vars);
        final Path inDir = resolvePath(inDirStr);
        final Pattern pattern = Pattern.compile(cfg.input.pattern);
        final List<File> candidates = new ArrayList<>();
        final int maxDepth = cfg.input.maxDepth;

        log("[INPUT] dir=%s, maxDepth=%d, pattern=%s, format=%s\n", inDir, maxDepth, cfg.input.pattern,
                cfg.input.formatType);
        if (Files.isDirectory(inDir)) {
            Files.walk(inDir, maxDepth).forEach(path -> {
                final String name = path.getFileName().toString();
                final Matcher m = pattern.matcher(name);
                if (m.matches()) {
                    if (cfg.input.matches(path.toFile(), vars)) {
                        candidates.add(path.toFile());
                    }
                }
            });
        } else {
            log("[INPUT] directory not found: %s\n", inDir);
        }

        candidates.sort(Comparator.comparing(File::getName));
        log("[INPUT] matched files: %,d\n", candidates.size());
        if (candidates.isEmpty()) {
            log("[WARN ] No input files found matching pattern '%s' in directory: %s\n", cfg.input.pattern, inDir);
        }
        for (int i = 0; i < Math.min(5, candidates.size()); i++) {
            log("  - %s\n", candidates.get(i).getAbsolutePath());
        }

        // Prepare output (support {var} substitution in directory as well)
        final String outDirStr = substitute(cfg.output.directory, vars);
        final Path outDir = resolvePath(outDirStr);
        Files.createDirectories(outDir);
        log("[OUTPUT] dir=%s, filename=%s\n", outDir, cfg.output.filename);

        // Build output columns
        final Column[] outCols = cfg.output.toColumns();
        final StringBuilder colNames = new StringBuilder();
        for (int i = 0; i < outCols.length; i++) {
            if (i > 0)
                colNames.append(", ");
            colNames.append(outCols[i].name());
        }
        log("[OUTPUT] columns=%d : %s\n", outCols.length, colNames.toString());
        final Meta outMeta = new Meta("pipeline").columns(outCols);
        // Apply primary index if defined
        if (cfg.output.meta.primaryKeys != null && cfg.output.meta.primaryKeys.length > 0) {
            final Index[] indexes = new Index[] { new Table.PrimaryKey(cfg.output.meta.primaryKeys) };
            outMeta.indexes(indexes);
        }

        // Prepare filter spec if defined (each thread will open its own lookup table)
        final FilterSpec filterSpec = cfg.output.filter;
        if (filterSpec != null) {
            final String lookupPath = resolvePath(substitute(filterSpec.sourceFile, vars)).toString();
            final File lookupFile = new File(lookupPath);
            if (lookupFile.exists()) {
                log("[FILTER] lookup from %s column=%s\n", lookupFile.getName(), filterSpec.fromColumn);
            } else {
                log("[WARN ] lookup file not found: %s\n", lookupPath);
                throw new IllegalArgumentException("Lookup file not found: " + lookupPath);
            }
        }

        // Group inputs by target output file (allows merging multiple inputs)
        final Map<File, List<File>> groups = new java.util.LinkedHashMap<>();
        for (final File inFile : candidates) {
            final String basename = stripMultiExtension(inFile.getName());
            final String templ = cfg.output.filename;
            final String substituted = substitute(templ, vars);
            final String outName = substituted.replace("{basename}", basename);
            final File outFile = outDir.resolve(outName).toFile();
            groups.computeIfAbsent(outFile, k -> new ArrayList<>()).add(inFile);
        }
        log("[GROUP] outputs=%d\n", groups.size());
        int gi = 0;
        for (var e : groups.entrySet()) {
            if (gi++ >= 5)
                break;
            log("  - %s (inputs=%d)\n", e.getKey().getName(), e.getValue().size());
        }

        // Precompute sort keys if sorting is requested
        final boolean doSort = cfg.output.sortColumns != null && cfg.output.sortColumns.length > 0;
        final byte[] sortKeys = doSort ? buildSortKeys(outMeta, cfg.output.sortColumns) : null;

        // Execute each output group in parallel using the provided ExecutorService
        final java.util.List<Future<long[]>> futures = new java.util.ArrayList<>();

        for (final Map.Entry<File, List<File>> ge : groups.entrySet()) {
            final File outFile = ge.getKey();
            final List<File> ins = ge.getValue();
            futures.add(pool.submit(() -> {
                if (cfg.output.skipIfExists && outFile.exists()) {
                    log("[SKIP ] exists: %s (%d inputs)\n", outFile, ins.size());
                    return new long[] { 0L, 0L };
                }
                if (cfg.output.overwriteIfExists && outFile.exists()) {
                    try {
                        log("[OVERW] remove existing: %s\n", outFile.getAbsolutePath());
                        GenericFile.drop(outFile);
                    } catch (Exception ex) {
                        error("[WARN] failed to remove existing %s: %s%n", outFile.getName(),
                                ex.getMessage());
                    }
                }

                long inRows = 0, outRows = 0;
                int errors = 0;
                final IO.StopWatch groupWatch = new IO.StopWatch();
                log("[START] -> %s (inputs=%d)\n", outFile, ins.size());

                // Create per-thread filter if needed
                RowFilter threadFilter = null;
                if (filterSpec != null) {
                    try {
                        final String lookupPath = resolvePath(substitute(filterSpec.sourceFile, vars)).toString();
                        final File lookupFile = new File(lookupPath);
                        if (lookupFile.exists()) {
                            final GenericFile lookupTable = GenericFile.open(lookupFile);
                            threadFilter = new LookupFilter(
                                filterSpec.fromColumn,
                                lookupTable,
                                filterSpec.lookupQuery,
                                filterSpec.operator
                            );
                        }
                    } catch (Exception ex) {
                        error("[ERROR] thread failed to open lookup table: %s%n", ex.getMessage());
                    }
                }
                final RowFilter rowFilter = threadFilter;

                // Create handlers per task to avoid cross-thread interference
                final java.util.List<RowHandler> rowHandlers = createHandlers(cfg.handlers);
                try {
                    // Resolve WHERE clause with variable substitution if provided
                    final String whereExpr = (cfg.input.where == null || cfg.input.where.isBlank())
                            ? null
                            : substitute(cfg.input.where, vars);
                    if (doSort) {
                        log("[SORT ] columns=%s\n", java.util.Arrays.toString(cfg.output.sortColumns));
                        try (final Sortable.FileSorter sorter = new Sortable.FileSorter(outMeta)) {
                            for (final File inFile : ins) {
                                log("[READ ] %s\n", inFile.getName());
                                try (final GenericFile in = openInput(inFile, cfg.input.formatType)) {
                                    final Meta inMeta = in.meta();
                                    log("[META ] %s columns=%d\n", inFile.getName(), inMeta.columns().length);
                                    final Cursor<Row> cursor = (whereExpr != null && !whereExpr.isBlank())
                                            ? in.find(whereExpr)
                                            : in.find();
                                    Row r;
                                    final IO.StopWatch prog = new IO.StopWatch();
                                    while ((r = cursor.next()) != null) {
                                        inRows++;
                                        try {
                                            final java.util.Map<String, Object> overlay = rowHandlers.isEmpty()
                                                    ? java.util.Collections.emptyMap()
                                                    : new java.util.HashMap<>();
                                            for (final RowHandler h : rowHandlers)
                                                h.handle(r, vars, overlay);
                                            final Row effective = overlay.isEmpty() ? r : new OverlayRow(r, overlay);
                                            final Row mapped = cfg.output.mapRow(outMeta, inMeta, effective, rowFilter);
                                            if (mapped == null)
                                                continue;
                                            sorter.add(mapped);
                                        } catch (Exception ex) {
                                            errors++;
                                            if (errors <= Math.max(1, cfg.maxErrors)) {
                                                error("[WARN] row %d error: %s%n", inRows, ex.getMessage());
                                            }
                                            if (errors > cfg.maxErrors) {
                                                error("[STOP] too many errors: %d > %d%n", errors,
                                                        cfg.maxErrors);
                                                break;
                                            }
                                        }
                                        if ((inRows % 100_000) == 0) {
                                            final long dt = prog.elapsed(true);
                                            log("[PROG ] %,d rows (+100k in %,d ms)\n", inRows, dt);
                                        }
                                    }
                                }
                                if (errors > cfg.maxErrors)
                                    break;
                            }

                            if (errors <= cfg.maxErrors) {
                                final IO.StopWatch sortWatch = new IO.StopWatch();
                                sorter.sort((a, b) -> Row.compareTo(sortKeys, a, b));
                                final long sdt = sortWatch.elapsed();
                                log("[SORT ] done %,d rows in %,d ms\n", sorter.rows(), sdt);

                                try (final GenericFile out = GenericFile.create(outFile, outMeta, new Logger() {
                                    @Override
                                    public void log(String fmt, Object... args) {
                                        DataPipeline.log(fmt + "\n", args);
                                    }
                                    @Override
                                    public void error(String fmt, Object... args) {
                                        DataPipeline.error(fmt + "\n", args);
                                    }
                                })) {
                                    final long n = sorter.rows();
                                    for (long i = 0; i < n; i++) {
                                        final Row rr = sorter.read(i);
                                        out.write(rr);
                                        outRows++;
                                    }
                                }
                            }
                        }
                    } else {
                        try (final GenericFile out = GenericFile.create(outFile, outMeta, new Logger() {
                            @Override
                            public void log(String fmt, Object... args) {
                                DataPipeline.log(fmt + "\n", args);
                            }
                            @Override
                            public void error(String fmt, Object... args) {
                                DataPipeline.error(fmt + "\n", args);
                            }
                        })) {
                            for (final File inFile : ins) {
                                log("[READ ] %s\n", inFile.getName());
                                try (final GenericFile in = openInput(inFile, cfg.input.formatType)) {
                                    final Meta inMeta = in.meta();
                                    log("[META ] %s columns=%d\n", inFile.getName(), inMeta.columns().length);
                                    final Cursor<Row> cursor = (whereExpr != null && !whereExpr.isBlank())
                                            ? in.find(whereExpr)
                                            : in.find();
                                    Row r;
                                    final IO.StopWatch prog = new IO.StopWatch();
                                    while ((r = cursor.next()) != null) {
                                        inRows++;
                                        try {
                                            final java.util.Map<String, Object> overlay = rowHandlers.isEmpty()
                                                    ? java.util.Collections.emptyMap()
                                                    : new java.util.HashMap<>();
                                            for (final RowHandler h : rowHandlers)
                                                h.handle(r, vars, overlay);
                                            final Row effective = overlay.isEmpty() ? r : new OverlayRow(r, overlay);
                                            final Row mapped = cfg.output.mapRow(outMeta, inMeta, effective, rowFilter);
                                            if (mapped == null)
                                                continue;
                                            out.write(mapped);
                                            outRows++;
                                        } catch (Exception ex) {
                                            errors++;
                                            if (errors <= Math.max(1, cfg.maxErrors)) {
                                                error("[WARN] row %d error: %s%n", inRows, ex.getMessage());
                                            }
                                            if (errors > cfg.maxErrors) {
                                                error("[STOP] too many errors: %d > %d%n", errors,
                                                        cfg.maxErrors);
                                                break;
                                            }
                                        }
                                        if ((inRows % 100_000) == 0) {
                                            final long dt = prog.elapsed(true);
                                            log("[PROG ] %,d rows (+100k in %,d ms)\n", inRows, dt);
                                        }
                                    }
                                }
                                if (errors > cfg.maxErrors)
                                    break;
                            }
                        }
                    }
                } finally {
                    // Close per-task handlers
                    for (final RowHandler h : rowHandlers) {
                        try {
                            h.close();
                        } catch (Exception ignore) {
                        }
                    }
                    // Close per-thread filter
                    if (rowFilter != null) {
                        try {
                            rowFilter.close();
                        } catch (Exception ignore) {
                        }
                    }
                }

                final long gdt = groupWatch.elapsed();
                final long rps = IO.StopWatch.ops(outRows, gdt);
                log("[DONE ] %s rows: in=%d out=%d time=%,d ms (%,d rows/s)\n", outFile.getName(), inRows, outRows,
                        gdt, rps);
                return new long[] { inRows, outRows };
            }));
        }

        // Return futures; caller is responsible for waiting and totals
        return futures;
    }

    private static byte[] buildSortKeys(final Meta meta, final String[] sortColumns) {
        if (sortColumns == null || sortColumns.length == 0)
            return null;
        final Column[] cols = meta.columns();
        final byte[] keys = new byte[sortColumns.length];
        for (int i = 0; i < sortColumns.length; i++) {
            final String name = sortColumns[i];
            int idx = -1;
            for (int c = 0; c < cols.length; c++) {
                if (cols[c].name().equalsIgnoreCase(name)) {
                    idx = c;
                    break;
                }
            }
            if (idx < 0)
                throw new IllegalArgumentException("Sort column not found: " + name);
            keys[i] = (byte) idx;
        }
        return keys;
    }

    // ---------------------- Row Handler SPI ----------------------

    public static interface RowHandler extends AutoCloseable {
        default void init(final Map<String, String> params) throws Exception {
        }

        void handle(final Row in, final Map<String, String> vars, final Map<String, Object> out) throws Exception;

        @Override
        default void close() throws Exception {
        }
    }

    static final class HandlerSpec {
        final String className;
        final Map<String, String> params;

        HandlerSpec(final String className, final Map<String, String> params) {
            this.className = className;
            this.params = params;
        }

        static HandlerSpec parse(final Element e) {
            final String cls = attr(e, "class", null);
            if (cls == null || cls.isEmpty())
                throw new IllegalArgumentException("<handler class=...> required");
            final Map<String, String> p = new java.util.HashMap<>();
            for (Node n = e.getFirstChild(); n != null; n = n.getNextSibling()) {
                if (n.getNodeType() == Node.ELEMENT_NODE && Objects.equals("param", n.getNodeName())) {
                    final Element pe = (Element) n;
                    final String name = attr(pe, "name", null);
                    final String value = attr(pe, "value", null);
                    if (name != null && value != null)
                        p.put(name, value);
                }
            }
            return new HandlerSpec(cls, p);
        }
    }

    static java.util.List<RowHandler> createHandlers(final java.util.List<HandlerSpec> specs) {
        final java.util.List<RowHandler> list = new java.util.ArrayList<>();
        if (specs == null)
            return list;
        for (final HandlerSpec s : specs) {
            try {
                final Class<?> c = Class.forName(s.className);
                final Object o = c.getDeclaredConstructor().newInstance();
                if (!(o instanceof RowHandler)) {
                    throw new IllegalArgumentException("Handler does not implement RowHandler: " + s.className);
                }
                final RowHandler h = (RowHandler) o;
                h.init(s.params);
                list.add(h);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        return list;
    }

    static final class OverlayRow implements Row {
        private final Row base;
        private final Map<String, Object> overlay;

        OverlayRow(final Row base, final Map<String, Object> overlay) {
            this.base = base;
            this.overlay = overlay;
        }

        @Override
        public Object[] array() {
            return base.array();
        }

        @Override
        public int size() {
            return base.size();
        }

        @Override
        public long id() {
            return base.id();
        }

        @Override
        public void id(long id) {
            base.id(id);
        }

        @Override
        public Meta meta() {
            return base.meta();
        }

        @Override
        public Row copy() {
            return base.copy();
        }

        @Override
        public <T extends Map<String, Object>> T map(T dest) {
            return base.map(dest);
        }

        @Override
        public Map<String, Object> map() {
            return base.map();
        }

        @Override
        public boolean contains(String name) {
            return overlay.containsKey(Column.normalize(name)) || base.contains(name);
        }

        @Override
        public Object get(int i) {
            return base.get(i);
        }

        @Override
        public Object get(String name) {
            final String k = Column.normalize(name);
            if (overlay.containsKey(k))
                return overlay.get(k);
            return base.get(name);
        }

        @Override
        public void set(int i, Object value) {
            base.set(i, value);
        }

        @Override
        public void set(String name, Object value) {
            base.set(name, value);
        }

        @Override
        public String getString(int i) {
            return base.getString(i);
        }

        @Override
        public String getString(String name) {
            return base.getString(name);
        }

        @Override
        public Integer getInt(int i) {
            return base.getInt(i);
        }

        @Override
        public Integer getInt(String name) {
            return base.getInt(name);
        }

        @Override
        public Long getLong(int i) {
            return base.getLong(i);
        }

        @Override
        public Long getLong(String name) {
            return base.getLong(name);
        }

        @Override
        public Double getDouble(int i) {
            return base.getDouble(i);
        }

        @Override
        public Double getDouble(String name) {
            return base.getDouble(name);
        }

        @Override
        public Float getFloat(int i) {
            return base.getFloat(i);
        }

        @Override
        public Float getFloat(String name) {
            return base.getFloat(name);
        }

        @Override
        public Short getShort(int i) {
            return base.getShort(i);
        }

        @Override
        public Short getShort(String name) {
            return base.getShort(name);
        }

        @Override
        public Byte getByte(int i) {
            return base.getByte(i);
        }

        @Override
        public Byte getByte(String name) {
            return base.getByte(name);
        }

        @Override
        public java.math.BigDecimal getBigDecimal(int i) {
            return base.getBigDecimal(i);
        }

        @Override
        public java.math.BigDecimal getBigDecimal(String name) {
            return base.getBigDecimal(name);
        }

        @Override
        public java.util.Date getDate(int i) {
            return base.getDate(i);
        }

        @Override
        public java.util.Date getDate(String name) {
            return base.getDate(name);
        }

        @Override
        public byte[] getBytes(int i) {
            return base.getBytes(i);
        }

        @Override
        public byte[] getBytes(String name) {
            return base.getBytes(name);
        }

        @Override
        public String toString(final String delimiter) {
            return base.toString(delimiter);
        }

        @Override
        public boolean validate() {
            return base.validate();
        }
    }

    private static Path resolvePath(final String path) {
        final Path p = Paths.get(path);
        if (p.isAbsolute())
            return p;
        return Paths.get("").toAbsolutePath().resolve(path);
    }

    private static String stripMultiExtension(final String name) {
        // Remove .gz, .zip, then base extension
        String n = name;
        if (n.endsWith(".gz"))
            n = n.substring(0, n.length() - 3);
        if (n.endsWith(".zip"))
            n = n.substring(0, n.length() - 4);
        final int i = n.lastIndexOf('.');
        return (i > 0) ? n.substring(0, i) : n;
    }

    /**
     * Open input file according to format type
     * @param file      Input file
     * @param formatType CSV|TSV|PARQUET|ORC|JSON|AVRO etc. (as needed in future)
     * @return
     * @throws Exception
     */
    private static GenericFile openInput(final File file, final String formatType) throws Exception {
        return GenericFile.open(file);
    }

    // ---------------------- Config model & parsing ----------------------

    static final class DataPipelineConfig {
        String name;
        int maxThreads = 1;
        int maxErrors = 10;
        final Map<String, Var> variables = new HashMap<>();
        OnStart onStart; // optional pre-execution hook
        Input input;
        Output output;
        final java.util.List<HandlerSpec> handlers = new java.util.ArrayList<>();

        java.util.List<Map<String, String>> computeVariableSets() {
            // If any date variable has a [from,to] range, iterate that range and compute
            // all variables per date
            Var rangeVar = null;
            for (var e : variables.values()) {
                if (e.hasRange()) {
                    rangeVar = e;
                    break;
                }
            }
            if (rangeVar == null) {
                return java.util.List.of(computeVariables());
            }
            final java.util.List<Map<String, String>> sets = new java.util.ArrayList<>();
            final LocalDate start = rangeVar.from;
            final LocalDate end = rangeVar.to;
            for (LocalDate d = start; !d.isAfter(end); d = d.plusDays(1)) {
                final Map<String, String> m = new HashMap<>();
                for (var e : variables.entrySet()) {
                    final Var v = e.getValue();
                    m.put(e.getKey(), v.computeFor(d));
                }
                sets.add(m);
            }
            return sets;
        }

        Map<String, String> computeVariables() {
            final Map<String, String> m = new HashMap<>();
            for (var e : variables.entrySet()) {
                m.put(e.getKey(), e.getValue().compute());
            }
            return m;
        }

        static DataPipelineConfig parse(final File xmlFile) throws Exception {
            final Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile);
            doc.getDocumentElement().normalize();

            final Element root = doc.getDocumentElement();
            if (!"pipeline".equals(root.getNodeName()))
                throw new IllegalArgumentException("Root element must be <pipeline>");

            final DataPipelineConfig cfg = new DataPipelineConfig();
            cfg.name = attr(root, "name", "pipeline");
            cfg.maxThreads = parseInt(attr(root, "max-threads", "1"), 1);
            cfg.maxErrors = parseInt(attr(root, "max-errors", "1"), 1);

            // optional onstart
            final Element onstartElem = child(root, "onstart");
            if (onstartElem != null) {
                cfg.onStart = OnStart.parse(onstartElem);
            }

            // Check for new <loop> syntax first
            final Element loopElem = child(root, "loop");
            if (loopElem != null) {
                // New loop syntax: parse loop variables from attributes
                parseLoopVariables(cfg, loopElem);
                
                // Parse input/output from within loop
                final Element loopInput = child(loopElem, "input");
                final Element loopOutput = child(loopElem, "output");
                if (loopInput == null || loopOutput == null) {
                    throw new IllegalArgumentException("<loop> must contain <input> and <output>");
                }
                cfg.input = Input.parse(loopInput);
                cfg.output = Output.parse(loopOutput);
                
                // row-handlers inside loop
                final Element loopHandlers = child(loopElem, "row-handlers");
                if (loopHandlers != null) {
                    final NodeList hNodes = loopHandlers.getElementsByTagName("handler");
                    for (int i = 0; i < hNodes.getLength(); i++) {
                        cfg.handlers.add(HandlerSpec.parse((Element) hNodes.item(i)));
                    }
                }
            } else {
                // Legacy syntax: variables, input, output at root level
                final NodeList varBlocks = root.getElementsByTagName("variables");
                if (varBlocks != null && varBlocks.getLength() > 0) {
                    final Element vars = (Element) varBlocks.item(0);
                    final NodeList varNodes = vars.getElementsByTagName("variable");
                    for (int i = 0; i < varNodes.getLength(); i++) {
                        final Element v = (Element) varNodes.item(i);
                        final String name = attr(v, "name", null);
                        final String type = attr(v, "type", "string");
                        if (name == null)
                            continue;
                        cfg.variables.put(name, Var.parse(type, v));
                    }
                }

                // input
                final NodeList inNodes = root.getElementsByTagName("input");
                if (inNodes == null || inNodes.getLength() == 0)
                    throw new IllegalArgumentException("<input> required");
                cfg.input = Input.parse((Element) inNodes.item(0));

                // row-handlers (optional)
                final Element hs = child(root, "row-handlers");
                if (hs != null) {
                    final NodeList hNodes = hs.getElementsByTagName("handler");
                    for (int i = 0; i < hNodes.getLength(); i++) {
                        cfg.handlers.add(HandlerSpec.parse((Element) hNodes.item(i)));
                    }
                }

                // output
                final NodeList outNodes = root.getElementsByTagName("output");
                if (outNodes == null || outNodes.getLength() == 0)
                    throw new IllegalArgumentException("<output> required");
                cfg.output = Output.parse((Element) outNodes.item(0));
            }

            return cfg;
        }

        /**
         * Parse loop variables from loop element attributes
         * Syntax: <loop date="-29..0" home="env:HOME" threads="4">
         */
        static void parseLoopVariables(final DataPipelineConfig cfg, final Element loopElem) {
            // Get threads attribute if specified
            if (hasAttr(loopElem, "threads")) {
                cfg.maxThreads = parseInt(attr(loopElem, "threads", "1"), 1);
            }
            
            // Parse all attributes as variables
            final org.w3c.dom.NamedNodeMap attrs = loopElem.getAttributes();
            for (int i = 0; i < attrs.getLength(); i++) {
                final org.w3c.dom.Node attrNode = attrs.item(i);
                final String attrName = attrNode.getNodeName();
                final String attrValue = attrNode.getNodeValue();
                
                // Skip special attributes
                if ("threads".equals(attrName) || "max-errors".equals(attrName)) {
                    continue;
                }
                
                // Parse variable based on value pattern
                if (attrValue.startsWith("env:")) {
                    // Environment variable: home="env:HOME"
                    final String envKey = attrValue.substring(4);
                    cfg.variables.put(attrName, new Var("string", 0, null, null, null, null, envKey, ""));
                } else if (attrValue.contains("..")) {
                    // Date range: date="-29..0" or date="2025-01-01..2025-12-31"
                    parseLoopDateRange(cfg, attrName, attrValue, loopElem);
                } else {
                    // Simple string value: status="active"
                    cfg.variables.put(attrName, new Var("string", 0, null, null, null, attrValue, null, ""));
                }
            }
        }

        /**
         * Parse date range variable: date="-29..0" or date="2025-01-01..2025-12-31"
         */
        static void parseLoopDateRange(final DataPipelineConfig cfg, final String varName, final String range, final Element loopElem) {
            final String[] parts = range.split("\\.\\.");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid range format: " + range);
            }
            
            // Get format attribute if specified (e.g., format-date="yyyyMMdd")
            final String formatAttr = "format-" + varName;
            final String format = hasAttr(loopElem, formatAttr) 
                ? attr(loopElem, formatAttr, "yyyyMMdd")
                : "yyyyMMdd";
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format, Locale.ROOT);
            
            LocalDate from, to;
            if (parts[0].startsWith("-") || parts[0].startsWith("+")) {
                // Relative range: -29..0
                final int startOffset = parseSignedDays(parts[0]);
                final int endOffset = parseSignedDays(parts[1]);
                final LocalDate today = LocalDate.now(ZoneId.systemDefault());
                from = today.plusDays(startOffset);
                to = today.plusDays(endOffset);
            } else {
                // Absolute range: 2025-01-01..2025-12-31
                from = LocalDate.parse(parts[0]);
                to = LocalDate.parse(parts[1]);
            }
            
            cfg.variables.put(varName, new Var("date", 0, formatter, from, to, null, null, null));
        }
    }

    static final class OnStart {
        final String type; // bash|zsh|powershell|sh
        final String errorPolicy; // exit|continue|ignore
        final String script; // script content (may be multiline)

        OnStart(final String type, final String errorPolicy, final String script) {
            this.type = type;
            this.errorPolicy = errorPolicy == null ? "continue" : errorPolicy;
            this.script = script;
        }

        static OnStart parse(final Element e) {
            final String type = attr(e, "type", null);
            final String err = attr(e, "error", "continue");
            final String body = text(e);
            return new OnStart(type, err, body);
        }
    }

    static final class Var {
        final String type; // date|string
        final int offsetDays; // for date
        final DateTimeFormatter fmt; // for date
        final LocalDate from; // optional range start (for date)
        final LocalDate to; // optional range end (for date)
        // string variable support
        final String strValue; // explicit value
        final String strEnvKey; // environment variable key
        final String strDefault; // default if env missing and value absent

        Var(final String type, final int offsetDays, final DateTimeFormatter fmt, final LocalDate from,
                final LocalDate to,
                final String strValue, final String strEnvKey, final String strDefault) {
            this.type = type;
            this.offsetDays = offsetDays;
            this.fmt = fmt;
            this.from = from;
            this.to = to;
            this.strValue = strValue;
            this.strEnvKey = strEnvKey;
            this.strDefault = strDefault;
        }

        static Var parse(final String type, final Element e) {
            if ("date".equalsIgnoreCase(type)) {
                // Parse offset: can be "offset" attribute or "last-days" (negative offset)
                int days = 0;
                if (hasAttr(e, "offset")) {
                    days = parseSignedDays(attr(e, "offset", "0"));
                } else if (hasAttr(e, "last-days")) {
                    // last-days="30" means 30 days ago, so offset = -30
                    final int n = parseInt(attr(e, "last-days", "0"), 0);
                    days = -n;
                }
                
                final String f = attr(e, "format", "yyyyMMdd");
                final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(f, Locale.ROOT);
                LocalDate from = null, to = null;
                // absolute range
                if (hasAttr(e, "from")) {
                    from = LocalDate.parse(attr(e, "from", null));
                }
                if (hasAttr(e, "to")) {
                    to = LocalDate.parse(attr(e, "to", null));
                }
                // relative offset range: ONLY offset-range creates a range for iteration
                // last-days just sets the offset, does NOT create a range
                if (from == null && to == null) {
                    if (hasAttr(e, "offset-range")) {
                        final String range = attr(e, "offset-range", null);
                        final String[] parts = range.split("\\.\\.");
                        if (parts.length == 2) {
                            final int start = parseSignedDays(parts[0]);
                            final int end = parseSignedDays(parts[1]);
                            final LocalDate today = LocalDate.now(ZoneId.systemDefault());
                            from = today.plusDays(start);
                            to = today.plusDays(end);
                        }
                    }
                    // last-days is treated as offset, not a range
                    // If you want range iteration, use offset-range="-N..0" instead
                }
                return new Var("date", days, formatter, from, to, null, null, null);
            }
            // string variable: support value, env and default
            final String value = attr(e, "value", null);
            final String env = attr(e, "env", null);
            final String def = attr(e, "default", "");
            return new Var("string", 0, null, null, null, value, env, def);
        }

        boolean hasRange() {
            return from != null && to != null;
        }

        String computeFor(final LocalDate base) {
            if ("date".equalsIgnoreCase(type)) {
                final LocalDate d = base.plusDays(offsetDays);
                return d.format(fmt);
            }
            // string variables ignore base date
            return compute();
        }

        String compute() {
            if ("date".equalsIgnoreCase(type)) {
                final LocalDate d = LocalDate.now(ZoneId.systemDefault()).plusDays(offsetDays);
                return d.format(fmt);
            }
            // string: prefer env, then explicit value, then default
            if (strEnvKey != null && !strEnvKey.isEmpty()) {
                final String v = System.getenv(strEnvKey);
                if (v != null && !v.isEmpty())
                    return v;
            }
            if (strValue != null)
                return strValue;
            return strDefault == null ? "" : strDefault;
        }
    }

    static final class Input {
        String type; // filesystem
        String directory;
        String pattern;
        int maxDepth = 1;
        String formatType; // e.g., tsv.gz, csv.gz
        String where; // optional SQL-like predicate for GenericFile.find
        final List<Condition> conditions = new ArrayList<>();

        static Input parse(final Element e) {
            final Input in = new Input();
            in.type = attr(e, "type", "filesystem");
            in.directory = text(child(e, "directory"));
            in.pattern = text(child(e, "pattern"));
            in.maxDepth = Integer.parseInt(attr(e, "max-depth", "1"));

            final Element fmt = child(e, "format");
            if (fmt != null) {
                in.formatType = attr(fmt, "type", null);
            }

            // optional WHERE clause
            final Element whereElem = child(e, "where");
            if (whereElem != null) {
                final String w = text(whereElem);
                in.where = (w == null || w.isEmpty()) ? null : w;
            }

            final Element matcher = child(e, "matcher");
            if (matcher != null) {
                final NodeList cs = matcher.getElementsByTagName("condition");
                for (int i = 0; i < cs.getLength(); i++) {
                    final Element c = (Element) cs.item(i);
                    in.conditions.add(Condition.parse(c));
                }
            }
            return in;
        }

        boolean matches(final File f, final Map<String, String> vars) {
            final String base = f.getName();
            final long mtime = f.lastModified();
            for (Condition c : conditions) {
                if (!c.test(base, mtime, vars))
                    return false;
            }
            return true;
        }
    }

    static final class Condition {
        String operator; // gte, lte, gt, lt, eq, neq
        String mtime; // ex) -10d
        String basename; // ex) app_logs_{date}.tsv.gz
        String basenamePattern; // regex for basename matching

        static Condition parse(final Element e) {
            final Condition c = new Condition();
            c.operator = attr(e, "operator", "gte");
            c.mtime = e.hasAttribute("mtime") ? e.getAttribute("mtime") : null;
            c.basename = e.hasAttribute("basename") ? e.getAttribute("basename") : null;
            c.basenamePattern = e.hasAttribute("basename-pattern") ? e.getAttribute("basename-pattern") : null;
            return c;
        }

        boolean test(final String base, final long lastModified, final Map<String, String> vars) {
            if (basenamePattern != null) {
                final boolean match = base.matches(basenamePattern);
                if ("neq".equalsIgnoreCase(operator) || "not-match".equalsIgnoreCase(operator))
                    return !match;
                return match; // default match
            }
            if (mtime != null) {
                final long since = System.currentTimeMillis() + parseRelativeMillis(mtime);
                return compare(lastModified, since, operator);
            }
            if (basename != null) {
                final String expected = substitute(basename, vars);
                return compare(base.compareTo(expected), 0, operator);
            }
            return true;
        }

        private static boolean compare(final long a, final long b, final String op) {
            return switch (op) {
                case "gt" -> a > b;
                case "lt" -> a < b;
                case "gte" -> a >= b;
                case "lte" -> a <= b;
                case "eq" -> a == b;
                case "neq" -> a != b;
                default -> true;
            };
        }
    }

    static final class Output {
        String directory;
        String filename;
        boolean skipIfExists;
        boolean overwriteIfExists;
        MetaSpec meta;
        String[] sortColumns; // optional sort keys
        FilterSpec filter; // optional lookup filter

        static Output parse(final Element e) {
            final Output o = new Output();
            o.meta = MetaSpec.parse(child(e, "meta"));
            o.directory = text(child(e, "directory"));
            o.filename = text(child(e, "filename"));
            final Element iff = child(e, "if");
            if (iff != null) {
                final String exists = attr(iff, "exists", "no");
                final String action = (iff.getTextContent() == null ? "" : iff.getTextContent()).trim()
                        .toLowerCase(Locale.ROOT);
                if ("yes".equalsIgnoreCase(exists)) {
                    if ("overwrite".equals(action)) {
                        o.overwriteIfExists = true;
                        o.skipIfExists = false;
                    } else if ("skip".equals(action) || action.isEmpty()) {
                        o.skipIfExists = true;
                        o.overwriteIfExists = false;
                    }
                }
            }
            final Element sort = child(e, "sort");
            if (sort != null) {
                final String cols = attr(sort, "columns", null);
                o.sortColumns = splitCsv(cols);
            }
            final Element filter = child(e, "filter");
            if (filter != null) {
                o.filter = FilterSpec.parse(filter);
            }
            return o;
        }

        Column[] toColumns() {
            return meta.toColumns();
        }

        Row mapRow(final Meta outMeta, final Meta inMeta, final Row inRow, final RowFilter rowFilter) {
            // Apply filter if defined
            if (rowFilter != null && !rowFilter.accept(inRow)) {
                return null; // Skip this row
            }
            
            final Map<String, Object> m = new HashMap<>();
            for (final ColumnSpec cs : meta.columns) {
                final Object v = cs.resolve(inRow);
                if (v == null) {
                    if (cs.notNull)
                        throw new IllegalArgumentException("column " + cs.name + " is null");
                    if (cs.defaultValue != null) {
                        m.put(cs.name, cs.defaultValue);
                        continue;
                    } else {
                        m.put(cs.name, null);
                        continue;
                    }
                }
                m.put(cs.name, v);
            }
            return Row.create(outMeta, m);
        }
    }

    static final class MetaSpec {
        final List<ColumnSpec> columns = new ArrayList<>();
        String formatType; // parquet, tsv.gz, csv.gz, flintdb (unused here)
        String[] primaryKeys; // primary index columns

        static MetaSpec parse(final Element e) {
            final MetaSpec m = new MetaSpec();
            if (e == null)
                return m;
            final Element columns = child(e, "columns");
            if (columns != null) {
                final NodeList cNodes = columns.getElementsByTagName("column");
                for (int i = 0; i < cNodes.getLength(); i++) {
                    m.columns.add(ColumnSpec.parse((Element) cNodes.item(i)));
                }
            }
            final Element format = child(e, "format");
            if (format != null) {
                m.formatType = attr(format, "type", null);
            }
            // Parse indexes
            final Element indexes = child(e, "indexes");
            if (indexes != null) {
                final Element primary = child(indexes, "primary");
                if (primary != null) {
                    final String cols = attr(primary, "columns", null);
                    if (cols != null && !cols.isEmpty()) {
                        m.primaryKeys = splitCsv(cols);
                    }
                }
            }
            return m;
        }

        Column[] toColumns() {
            final Column[] a = new Column[columns.size()];
            for (int i = 0; i < a.length; i++)
                a[i] = columns.get(i).toColumn();
            return a;
        }
    }

    static final class ColumnSpec {
        final String name;
        final short type;
        final Integer bytes;
        final Integer precision;
        final String[] from;
        final String transform; // e.g., hash
        final boolean notNull;
        final Object defaultValue;

        ColumnSpec(String name, String type, Integer bytes, Integer precision, String[] from, String transform,
                boolean notNull,
                Object defaultValue) {
            this.name = name;
            this.type = Column.valueOf(type.trim());
            this.bytes = bytes;
            this.precision = precision;
            this.from = from;
            this.transform = transform;
            this.notNull = notNull;
            this.defaultValue = defaultValue;
        }

        static ColumnSpec parse(final Element e) {
            final String name = attr(e, "name", null);
            final String type = attr(e, "type", "string");
            final Integer bytes = hasAttr(e, "bytes") ? Integer.valueOf(attr(e, "bytes", "0")) : null;
            final Integer precision = hasAttr(e, "precision") ? Integer.valueOf(attr(e, "precision", "0")) : null;
            final String from = attr(e, "from", null);
            final String transform = attr(e, "transform", null);
            final boolean notNull = Boolean.parseBoolean(attr(e, "not-null", "false"));
            final String def = attr(e, "default", null);
            Object defVal = def;
            return new ColumnSpec(name, type, bytes, precision, splitCsv(from), transform, notNull, defVal);
        }

        Column toColumn() {
            // Handle DECIMAL precision/bytes combo explicitly so precision is preserved
            if (type == Column.TYPE_DECIMAL) {
                final short b = (bytes != null) ? (short) (int) bytes : (short) -1;
                final short p = (precision != null) ? (short) (int) precision : (short) -1;
                // Create directly to leverage Column.bytes(type, bytes, precision)
                return new Column(name, type, b, p, notNull, defaultValue, null);
            }

            final Column.Builder bld = new Column.Builder(name, type);
            int effBytes = (bytes != null) ? bytes.intValue() : -1;
            // Provide a safe default for STRING types if bytes not specified
            if (type == Column.TYPE_STRING && effBytes <= 0) {
                effBytes = 64; // default max length if not provided
            }
            if (effBytes > 0)
                bld.bytes(effBytes);
            if (defaultValue != null)
                bld.value(defaultValue);
            return bld.create();
        }

        Object resolve(final Row in) {
            if (from == null || from.length == 0) {
                return defaultValue;
            }
            // Single source: return value if column exists, otherwise default

            if (transform == null) {
                if (from.length == 1) {
                    final String f = from[0];
                    try {
                        if (in.contains(f)) {
                            Object v = in.get(f);
                            return (v != null) ? v : defaultValue;
                        }
                    } catch (Exception ignore) {
                    }
                    return defaultValue;
                }
            }

            for (String f : from) {
                final Object v = in.get(f.trim());
                if (v == null)
                    continue;

                if ("hash".equalsIgnoreCase(String.valueOf(transform))) {
                    final String s = v.toString();
                    if (s != null && !s.trim().isEmpty()) {
                        return UnsignedHash64.hash63(s);
                    }
                } else {
                    throw new IllegalArgumentException("unsupported transform " + transform + " for column " + name);
                }
            }
            return defaultValue;
        }
    }

    // ---------------------- Row Filter SPI ----------------------

    interface RowFilter extends AutoCloseable {
        boolean accept(Row row);
        @Override
        default void close() throws Exception {}
    }

    static final class LookupFilter implements RowFilter {
        private final String fromColumn;
        private final GenericFile lookupTable;
        private final String lookupQuery;
        private final String operator;

        LookupFilter(String fromColumn, GenericFile lookupTable, String lookupQuery, String operator) {
            this.fromColumn = fromColumn;
            this.lookupTable = lookupTable;
            this.lookupQuery = lookupQuery;
            this.operator = operator;
        }

        @Override
        public boolean accept(Row row) {
            try {
                if (!row.contains(fromColumn)) {
                    return false;
                }
                final Object value = row.get(fromColumn);
                if (value == null) {
                    return false;
                }
                
                // Build lookup query - replace ? with actual value
                String query;
                if (lookupQuery != null && lookupQuery.contains("?")) {
                    // Replace ? with quoted value
                    final String quotedValue = "'" + value.toString().replace("'", "''") + "'";
                    query = lookupQuery.replace("?", quotedValue);
                } else if (lookupQuery != null) {
                    query = lookupQuery;
                } else {
                    query = "WHERE " + fromColumn + " = '" + value.toString().replace("'", "''") + "'";
                }
                
                // Execute lookup
                try (final Cursor<Row> cursor = lookupTable.find(query)) {
                    final Row found = cursor.next();
                    final boolean exists = (found != null);
                    
                    return switch (operator) {
                        case "eq", "exists" -> exists;
                        case "neq", "not-exists" -> !exists;
                        default -> exists;
                    };
                }
            } catch (Exception ex) {
                error("[FILTER] lookup error: %s%n", ex.getMessage());
                return false;
            }
        }

        @Override
        public void close() throws Exception {
            if (lookupTable != null) {
                lookupTable.close();
            }
        }
    }

    static final class FilterSpec {
        final String fromColumn;  // Column name to extract value from input row
        final String sourceFile;  // Lookup table file path
        final String lookupQuery; // SQL query for lookup (e.g., "WHERE client_id = ?")
        final String operator;    // Operator: eq, neq, exists, not-exists

        FilterSpec(String fromColumn, String sourceFile, String lookupQuery, String operator) {
            this.fromColumn = fromColumn;
            this.sourceFile = sourceFile;
            this.lookupQuery = lookupQuery;
            this.operator = operator == null ? "eq" : operator;
        }

        static FilterSpec parse(final Element e) {
            final String from = attr(e, "from", null);
            final String source = attr(e, "source", null);
            if (from == null || source == null) {
                throw new IllegalArgumentException("<filter> requires 'from' and 'source' attributes");
            }
            final Element condition = child(e, "condition");
            String lookup = null;
            String operator = "eq";
            if (condition != null) {
                lookup = attr(condition, "lookup", null);
                operator = attr(condition, "operator", "eq");
            }
            return new FilterSpec(from, source, lookup, operator);
        }
    }

    // ---------------------- Helpers ----------------------

    static int parseInt(final String s, final int dfl) {
        try {
            return Integer.parseInt(s);
        } catch (Exception ignore) {
            return dfl;
        }
    }

    static boolean hasAttr(final Element e, final String name) {
        return e != null && e.hasAttribute(name) && !e.getAttribute(name).isEmpty();
    }

    static String attr(final Element e, final String name, final String dfl) {
        return (e != null && e.hasAttribute(name)) ? e.getAttribute(name) : dfl;
    }

    static Element child(final Element e, final String name) {
        if (e == null)
            return null;
        for (Node n = e.getFirstChild(); n != null; n = n.getNextSibling()) {
            if (n.getNodeType() == Node.ELEMENT_NODE && Objects.equals(name, n.getNodeName()))
                return (Element) n;
        }
        return null;
    }

    static String text(final Element e) {
        return e == null ? null : e.getTextContent().trim();
    }

    static String[] splitCsv(final String s) {
        if (s == null || s.isEmpty())
            return null;
        final String[] a = s.split(",");
        for (int i = 0; i < a.length; i++)
            a[i] = a[i].trim().toLowerCase(Locale.ROOT);
        return a;
    }

    static String substitute(final String s, final Map<String, String> vars) {
        String out = s;
        for (var e : vars.entrySet()) {
            out = out.replace("{" + e.getKey() + "}", e.getValue());
        }
        return out;
    }

    static String defaultShellType() {
        return isWindows() ? "powershell" : "bash";
    }

    static boolean isWindows() {
        final String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return os.contains("win");
    }

    static int runShell(final String type, final String script) throws Exception {
        final String shell = (type == null || type.isBlank()) ? defaultShellType() : type.toLowerCase(Locale.ROOT);
        final java.util.List<String> cmd = new java.util.ArrayList<>();
        switch (shell) {
            case "zsh" -> {
                final String exe = new File("/bin/zsh").exists() ? "/bin/zsh" : "zsh";
                cmd.add(exe); cmd.add("-lc"); cmd.add(script);
            }
            case "bash" -> {
                final String exe = new File("/bin/bash").exists() ? "/bin/bash" : "bash";
                cmd.add(exe); cmd.add("-lc"); cmd.add(script);
            }
            case "sh" -> {
                final String exe = new File("/bin/sh").exists() ? "/bin/sh" : "sh";
                cmd.add(exe); cmd.add("-lc"); cmd.add(script);
            }
            case "powershell" -> {
                cmd.add("powershell"); cmd.add("-NoProfile"); cmd.add("-Command"); cmd.add(script);
            }
            default -> {
                if (isWindows()) {
                    cmd.add("cmd.exe"); cmd.add("/C"); cmd.add(script);
                } else {
                    final String exe = new File("/bin/sh").exists() ? "/bin/sh" : "sh";
                    cmd.add(exe); cmd.add("-lc"); cmd.add(script);
                }
            }
        }
        final ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        final Process p = pb.start();
        try (final java.io.InputStream in = p.getInputStream()) {
            in.transferTo(System.out);
        }
        return p.waitFor();
    }

    static int parseSignedDays(final String s) {
        // e.g., "-1", "-1d", "+2d"
        String v = s.trim().toLowerCase(Locale.ROOT);
        v = v.endsWith("d") ? v.substring(0, v.length() - 1) : v;
        return Integer.parseInt(v);
    }

    static long parseRelativeMillis(final String expr) {
        // "-10d" => -10 days in millis
        String v = expr.trim().toLowerCase(Locale.ROOT);
        long sign = v.startsWith("-") ? -1 : 1;
        if (v.startsWith("+") || v.startsWith("-"))
            v = v.substring(1);
        long unitMillis = 1000L;
        if (v.endsWith("ms")) {
            unitMillis = 1L;
            v = v.substring(0, v.length() - 2);
        } else if (v.endsWith("s")) {
            unitMillis = 1000L;
            v = v.substring(0, v.length() - 1);
        } else if (v.endsWith("m")) {
            unitMillis = 60_000L;
            v = v.substring(0, v.length() - 1);
        } else if (v.endsWith("h")) {
            unitMillis = 3_600_000L;
            v = v.substring(0, v.length() - 1);
        } else if (v.endsWith("d")) {
            unitMillis = 86_400_000L;
            v = v.substring(0, v.length() - 1);
        }
        long val = Long.parseLong(v);
        return sign * val * unitMillis;
    }

    static final class UnsignedHash64 {
        // Simple 64-bit FNV-1a
        private static final long FNV64_OFFSET = 0xcbf29ce484222325L;
        private static final long FNV64_PRIME = 0x100000001b3L;

        static long hash63(final String s) {
            long h = FNV64_OFFSET;
            for (int i = 0; i < s.length(); i++) {
                h ^= (s.charAt(i) & 0xff);
                h *= FNV64_PRIME;
            }
            h ^= (h >>> 32);
            return h & 0x7fffffffffffffffL; // ensure non-negative (63-bit)
        }
    }

    // ---------------------- Programmatic Builder API ----------------------

    /**
     * Builder for programmatic Transformer configuration
     */
    public static final class Builder {
        private final DataPipelineConfig config;
        private InputBuilder inputBuilder;
        private OutputBuilder outputBuilder;

        private Builder(String name) {
            this.config = new DataPipelineConfig();
            this.config.name = name;
        }

        public Builder maxThreads(int threads) {
            this.config.maxThreads = threads;
            return this;
        }

        public Builder maxErrors(int errors) {
            this.config.maxErrors = errors;
            return this;
        }

        public Builder variable(String name, String value) {
            final Var v = new Var("string", 0, null, null, null, value, null, "");
            this.config.variables.put(name, v);
            return this;
        }

        public Builder dateVariable(String name, String format, int offsetDays) {
            final DateTimeFormatter fmt = DateTimeFormatter.ofPattern(format, Locale.ROOT);
            final Var v = new Var("date", offsetDays, fmt, null, null, null, null, null);
            this.config.variables.put(name, v);
            return this;
        }

        public Builder dateRangeVariable(String name, String format, int startOffset, int endOffset) {
            final DateTimeFormatter fmt = DateTimeFormatter.ofPattern(format, Locale.ROOT);
            final LocalDate today = LocalDate.now(ZoneId.systemDefault());
            final LocalDate from = today.plusDays(startOffset);
            final LocalDate to = today.plusDays(endOffset);
            final Var v = new Var("date", 0, fmt, from, to, null, null, null);
            this.config.variables.put(name, v);
            return this;
        }

        public Builder envVariable(String name, String envKey) {
            final Var v = new Var("string", 0, null, null, null, null, envKey, "");
            this.config.variables.put(name, v);
            return this;
        }

        public InputBuilder input() {
            if (inputBuilder == null) {
                inputBuilder = new InputBuilder(this);
            }
            return inputBuilder;
        }

        public OutputBuilder output() {
            if (outputBuilder == null) {
                outputBuilder = new OutputBuilder(this);
            }
            return outputBuilder;
        }

        public Builder handler(Class<? extends RowHandler> handlerClass, Map<String, String> params) {
            final HandlerSpec spec = new HandlerSpec(handlerClass.getName(), params != null ? params : new HashMap<>());
            this.config.handlers.add(spec);
            return this;
        }

        public DataPipeline build() {
            if (config.input == null) {
                throw new IllegalStateException("input configuration is required");
            }
            if (config.output == null) {
                throw new IllegalStateException("output configuration is required");
            }
            return new DataPipeline(config);
        }
    }

    public static final class InputBuilder {
        private final Builder parent;
        private final Input input;

        private InputBuilder(Builder parent) {
            this.parent = parent;
            this.input = new Input();
            this.input.type = "filesystem";
        }

        public InputBuilder directory(String dir) {
            this.input.directory = dir;
            return this;
        }

        public InputBuilder pattern(String pattern) {
            this.input.pattern = pattern;
            return this;
        }

        public InputBuilder maxDepth(int depth) {
            this.input.maxDepth = depth;
            return this;
        }

        public InputBuilder formatType(String type) {
            this.input.formatType = type;
            return this;
        }

        public InputBuilder where(String whereClause) {
            this.input.where = whereClause;
            return this;
        }

        public Builder done() {
            parent.config.input = this.input;
            return parent;
        }
    }

    public static final class OutputBuilder {
        private final Builder parent;
        private final Output output;
        private final List<ColumnSpec> columns;

        private OutputBuilder(Builder parent) {
            this.parent = parent;
            this.output = new Output();
            this.output.meta = new MetaSpec();
            this.columns = new ArrayList<>();
        }

        public OutputBuilder directory(String dir) {
            this.output.directory = dir;
            return this;
        }

        public OutputBuilder filename(String filename) {
            this.output.filename = filename;
            return this;
        }

        public OutputBuilder skipIfExists(boolean skip) {
            this.output.skipIfExists = skip;
            return this;
        }

        public OutputBuilder overwriteIfExists(boolean overwrite) {
            this.output.overwriteIfExists = overwrite;
            return this;
        }

        public OutputBuilder sortBy(String... columnNames) {
            this.output.sortColumns = columnNames;
            return this;
        }

        public ColumnBuilder column(String name, String type) {
            return new ColumnBuilder(this, name, type);
        }

        public Builder done() {
            this.output.meta.columns.addAll(this.columns);
            parent.config.output = this.output;
            return parent;
        }
    }

    public static final class ColumnBuilder {
        private final OutputBuilder parent;
        private final String name;
        private final String type;
        private Integer bytes;
        private Integer precision;
        private String[] from;
        private String transform;
        private boolean notNull;
        private Object defaultValue;

        private ColumnBuilder(OutputBuilder parent, String name, String type) {
            this.parent = parent;
            this.name = name;
            this.type = type;
        }

        public ColumnBuilder bytes(int bytes) {
            this.bytes = bytes;
            return this;
        }

        public ColumnBuilder precision(int precision) {
            this.precision = precision;
            return this;
        }

        public ColumnBuilder from(String... sourceColumns) {
            this.from = sourceColumns;
            return this;
        }

        public ColumnBuilder transform(String transformFunc) {
            this.transform = transformFunc;
            return this;
        }

        public ColumnBuilder notNull(boolean notNull) {
            this.notNull = notNull;
            return this;
        }

        public ColumnBuilder defaultValue(Object value) {
            this.defaultValue = value;
            return this;
        }

        public OutputBuilder done() {
            final ColumnSpec spec = new ColumnSpec(name, type, bytes, precision, from, transform, notNull, defaultValue);
            parent.columns.add(spec);
            return parent;
        }
    }
}
