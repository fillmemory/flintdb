/**
 * 
 */
package flint.db;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.List;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Deque;
import java.util.ArrayDeque;

/**
 * Single view class for managing a collection of files
 */
public final class Union implements GenericFile {

    public static final String FILENAME_EXTENSION = ".union";

    private final List<File> tablePaths = new java.util.ArrayList<>();
    private final List<File> recordPaths = new java.util.ArrayList<>();
    private long rows = -1L; // cache total rows if computed

    /**
     * Open a UnionAll instance for the specified directory and file pattern.
     * 
     * @param dir     The directory to search for files.
     * @param pattern The regex pattern to match file names.
     * @return A UnionAll instance.
     * @throws Exception If an error occurs while opening the files.
     */
    public static Union open(File dir, String pattern) throws IOException {
        return open(dir, 2, "none", pattern);
    }

    /**
     * Open a UnionAll instance for the specified directory, file pattern, and
     * maximum depth.
     * 
     * @param dir      The directory to search for files.
     * @param maxDepth The maximum depth to search for files.
     * @param sort     The sort order: "asc", "desc", or "none".
     * @param pattern  The regex pattern to match file names.
     * @return A UnionAll instance.
     * @throws Exception If an error occurs while opening the files.
     */
    public static Union open(File dir, int maxDepth, String sort, String pattern) throws IOException {
        return open(dir, maxDepth, pattern, sort, ((file) -> {
            return true;
        }));
    }

    /**
     * Open with an optional DateFilter. Keeps backward compatibility.
     */
    public static Union open(File dir, int maxDepth, String pattern, String sort, FileFilter fileFilter)
            throws IOException {
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Invalid directory: " + dir);
        }

        var t = new Union();
        // recursively find files with max depth 2
        if (maxDepth < 0)
            maxDepth = 0;

        final Pattern regex = Pattern.compile(pattern);
        var a = findFiles(dir, regex, fileFilter, 0, maxDepth);
        int d = "desc".equalsIgnoreCase(sort) ? -1 : 1;
        a.sort((f1, f2) -> d * f1.getName().compareTo(f2.getName()));

        for (var f : a) {
            if (f.isFile()) {
                t.add(f);
            }
        }
        return t;
    }

    /**
     * File format
     * 
     * <pre>
     * directory = /path/to/dir
     * pattern = .*\.csv
     * max_depth = 2
     * sort = asc|desc|none
     * # Comments start with # or //
     * </pre>
     * 
     * @param descriptor
     * @return
     * @throws Exception
     */
    public static Union open(File descriptor) throws IOException {
        if (descriptor == null) {
            throw new IllegalArgumentException("Descriptor cannot be null");
        }
        if (!descriptor.getName().endsWith(FILENAME_EXTENSION)) {
            throw new IllegalArgumentException("Invalid descriptor file: " + descriptor);
        }

        // If a directory is passed, default to matching all visible files under it
        // (depth <= 2)
        if (descriptor.isDirectory()) {
            return open(descriptor, ".*");
        }

        if (!descriptor.isFile()) {
            throw new IllegalArgumentException("Descriptor must be a file or directory: " + descriptor);
        }

        String dirStr = null;
        String pattern = null;
        String sort = null; // none/asc/desc
        Integer max_depth = null; // optional, defaults to 2

        final List<String> lines = Files.readAllLines(descriptor.toPath(), StandardCharsets.UTF_8);
        for (String raw : lines) {
            if (raw == null)
                continue;
            String line = raw.trim();
            if (line.isEmpty())
                continue;
            if (line.startsWith("#") || line.startsWith("//"))
                continue; // comment lines

            int sep = line.indexOf('=');
            if (sep < 0)
                sep = line.indexOf(':');
            if (sep < 0)
                continue; // ignore unknown lines

            String key = line.substring(0, sep).trim().toLowerCase();
            String val = line.substring(sep + 1).trim();

            // Strip surrounding quotes if any
            if ((val.startsWith("\"") && val.endsWith("\"")) || (val.startsWith("'") && val.endsWith("'"))) {
                if (val.length() >= 2) {
                    val = val.substring(1, val.length() - 1);
                }
            }

            if (null != key)
                switch (key) {
                    case "directory":
                    case "dir":
                    case "path":
                        dirStr = val;
                        break;
                    case "pattern":
                    case "regex":
                        pattern = val;
                        break;
                    case "sort":
                    case "orderby":
                        sort = val;
                        break;
                    case "max_depth":
                        try {
                            max_depth = Integer.valueOf(val);
                        } catch (NumberFormatException ignore) {
                            // ignore invalid value; will fallback to default
                        }
                        break;
                    default:
                        break;
                }
        }

        if (dirStr == null || dirStr.isEmpty()) {
            throw new IllegalArgumentException("Descriptor missing 'directory' (or 'dir'/'path')");
        }
        if (pattern == null || pattern.isEmpty()) {
            throw new IllegalArgumentException("Descriptor missing 'pattern' (or 'regex')");
        }

        // Defensive: allow a couple of common typos for file extensions like \csv ->
        // \.csv
        if (pattern.contains("\\csv"))
            pattern = pattern.replace("\\csv", "\\.csv");
        if (pattern.contains("\\gz"))
            pattern = pattern.replace("\\gz", "\\.gz");

        int depth = (max_depth == null ? 2 : Math.max(0, max_depth));

        return open(IO.path(dirStr), depth, pattern, sort, (file) -> {
            return true; // default filter accepts all files
        });
    }

    static List<File> findFiles(File dir, Pattern regex, FileFilter fileFilter, int depth, int maxDepth) {
        if (depth > maxDepth)
            return List.of();

        File[] files = dir.listFiles();
        if (files == null)
            return List.of();

        List<File> result = new java.util.ArrayList<>();
        for (File f : files) {
            if (f.isDirectory()) {
                result.addAll(findFiles(f, regex, fileFilter, depth + 1, maxDepth));
            } else if (f.isFile() && !f.isHidden()) {
                final String name = f.getName();
                final Matcher m = regex.matcher(name);
                if (m.matches()) {
                    if (fileFilter.accept(f)) {
                        result.add(f);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void close() throws Exception {
    }

    public void add(final File file) throws IOException {
        if (file == null || !file.exists()) {
            throw new IllegalArgumentException("File cannot be null or non-existent");
        }

        // Defer opening until meta()/find(); just classify and store the path
        if (GenericFile.supports(file)) {
            recordPaths.add(file);
        } else {
            tablePaths.add(file);
        }
    }

    public void addAll(final File... files) throws Exception {
        for (File file : files) {
            add(file);
        }
    }

    @Override
    public Meta meta() throws IOException {
        // Open just long enough to read metadata, then close immediately
        try {
            if (!tablePaths.isEmpty()) {
                final File f = tablePaths.get(0);
                try (var t = Table.open(f, Table.OPEN_RDONLY)) {
                    return t.meta();
                }
            }
            if (!recordPaths.isEmpty()) {
                final File f = recordPaths.get(0);
                try (var rf = GenericFile.open(f)) {
                    return rf.meta();
                }
            }
            return null;
        } catch (IOException ioe) {
            throw ioe;
        } catch (Exception e) {
            throw new IOException("Failed to open meta() lazily", e);
        }
    }

    public Cursor<Row> find(final Filter.Limit limit, final Comparable<Row>[] filter) throws Exception {
        // Normalize null or empty filter to {Filter.ALL, Filter.ALL}
        final Comparable<Row>[] effectiveFilter;
        if (filter == null || filter.length == 0) {
            @SuppressWarnings("unchecked")
            Comparable<Row>[] tmp = new Comparable[] { Filter.ALL, Filter.ALL };
            effectiveFilter = tmp;
        } else {
            effectiveFilter = filter;
        }

        if (recordPaths.isEmpty() && tablePaths.isEmpty()) {
            throw new IllegalStateException("No sources to union");
        }

        final Meta base = meta();
        if (base == null)
            throw new IllegalStateException("No metadata available");

        // Schema validation will be done lazily when each file is opened
        final Column[] baseCols = base.columns();

        // Stream UNION ALL using a single global limit/offset across all sources (files
        // first, then tables)
        return new Cursor<Row>() {
            private int fidx = 0; // iterate forward
            private int tidx = 0; // iterate forward
            private Cursor<Row> rowCursor = null; // for NonIndexedFile
            private Cursor<Long> idCursor = null; // for Table
            private Table currentTable = null; // table associated with idCursor
            private GenericFile currentNonIndexedFile = null; // record file associated with rowCursor
            private boolean closed = false;

            private void validateSchema(Meta m) {
                final Column[] cs = m.columns();
                if (cs.length != baseCols.length) {
                    throw new IllegalArgumentException(
                            "Schema mismatch in union source: column count differs: " + m.name());
                }
                for (int k = 0; k < cs.length; k++) {
                    if (!Column.normalize(cs[k].name()).equals(Column.normalize(baseCols[k].name()))) {
                        throw new IllegalArgumentException(
                                "Schema mismatch in union source: column name differs at position " + k + ": "
                                        + cs[k].name());
                    }
                }
            }

            private boolean openNextSource() throws Exception {
                // Close previous cursors if any
                IO.close(rowCursor);
                rowCursor = null;
                IO.close(currentNonIndexedFile);
                currentNonIndexedFile = null;
                IO.close(idCursor);
                idCursor = null;
                IO.close(currentTable);
                currentTable = null;

                // Open next NonIndexedFile cursor (forward)
                if (fidx < recordPaths.size()) {
                    currentNonIndexedFile = GenericFile.open(recordPaths.get(fidx++));
                    validateSchema(currentNonIndexedFile.meta());
                    final Comparable<Row> merged = Filter.ALL.equals(effectiveFilter[0]) ? effectiveFilter[1]
                            : (Row r) -> {
                                int d = 0;
                                for (int i = 0; i < effectiveFilter.length; i++) {
                                    d = effectiveFilter[i].compareTo(r);
                                    if (d != 0)
                                        break;
                                }
                                return d;
                            };
                    rowCursor = currentNonIndexedFile.find(limit, merged); // uses the same global limit
                    return true;
                }

                // Open next Table cursor (primary index, ascending) - forward
                if (tidx < tablePaths.size()) {
                    currentTable = Table.open(tablePaths.get(tidx++), Table.OPEN_RDONLY);
                    validateSchema(currentTable.meta());
                    // @SuppressWarnings("unchecked")
                    // final Comparable<Row>[] compiled = new Comparable[] { Filter.ALL, filter };
                    idCursor = currentTable.find(Index.PRIMARY, Filter.ASCENDING, limit, effectiveFilter);
                    return true;
                }
                return false;
            }

            @Override
            public Row next() {
                if (closed)
                    return null;
                try {
                    for (;;) {
                        // From NonIndexedFile cursor
                        if (rowCursor != null) {
                            final Row r = rowCursor.next();
                            if (r != null)
                                return r;
                            // exhausted -> open next
                            openNextSource();
                            continue;
                        }

                        // From Table cursor (IDs -> read rows)
                        if (idCursor != null && currentTable != null) {
                            final Long id = idCursor.next();
                            if (id != null && id > -1L) {
                                return currentTable.read(id);
                            }
                            // exhausted -> open next
                            openNextSource();
                            continue;
                        }

                        // Initialize first source or move to next when none open
                        if (!openNextSource())
                            return null; // no more sources
                    }
                } catch (Exception ex) {
                    throw new RuntimeException("Union iteration error", ex);
                }
            }

            @Override
            public void close() throws Exception {
                if (closed)
                    return;
                closed = true;
                IO.close(rowCursor);
                rowCursor = null;
                IO.close(currentNonIndexedFile);
                currentNonIndexedFile = null;
                IO.close(idCursor);
                idCursor = null;
                IO.close(currentTable);
                currentTable = null;
            }
        };
    }

    /**
     * Find with multiple threads.
     * Row order is preserved as if sequentially iterating through each source in
     * order.
     * Records are emitted first (in source order), then tables (in source order).
     * 
     * @param nthreads Number of threads to use; if <= 1, falls back to sequential
     *                 implementation
     * @param limit    Global limit/offset
     * @param filter   Filter array
     * @return
     * @throws Exception
     */
    public Cursor<Row> find(final int nthreads, final Filter.Limit limit, final Comparable<Row>[] filter)
            throws Exception {
        if (nthreads <= 1) {
            return find(limit, filter); // fall back to sequential implementation
        }

        var executor = Executors.newFixedThreadPool(nthreads);
        try {
            return find(executor, limit, filter);
        } finally {
            executor.shutdown();
        }
    }

    public Cursor<Row> find(final Executor executor, final Filter.Limit limit, final Comparable<Row>[] filter)
            throws Exception {
        if (executor == null) {
            return find(limit, filter); // fall back to sequential implementation
        }

        // --- Preparation (reuse logic from sequential find) ---
        if (recordPaths.isEmpty() && tablePaths.isEmpty()) {
            throw new IllegalStateException("No sources to union");
        }
        final Meta base = meta();
        if (base == null)
            throw new IllegalStateException("No metadata available");

        final Column[] baseCols = base.columns();
        // Schema validation (record files)
        for (int i = 0; i < recordPaths.size(); i++) {
            final File f = recordPaths.get(i);
            final GenericFile rf = GenericFile.open(f);
            final Meta m;
            try {
                m = rf.meta();
            } finally {
                IO.close(rf);
            }
            final Column[] cs = m.columns();
            if (cs.length != baseCols.length) {
                throw new IllegalArgumentException(
                        "Schema mismatch in union source: column count differs: " + m.name());
            }
            for (int k = 0; k < cs.length; k++) {
                if (!Column.normalize(cs[k].name()).equals(Column.normalize(baseCols[k].name()))) {
                    throw new IllegalArgumentException(
                            "Schema mismatch in union source: column name differs at position " + k + ": "
                                    + cs[k].name());
                }
            }
        }
        // Schema validation (tables)
        for (int i = 0; i < tablePaths.size(); i++) {
            final File f = tablePaths.get(i);
            final Table t = Table.open(f, Table.OPEN_RDONLY);
            final Meta m;
            try {
                m = t.meta();
            } finally {
                IO.close(t);
            }
            final Column[] cs = m.columns();
            if (cs.length != baseCols.length) {
                throw new IllegalArgumentException(
                        "Schema mismatch in union source: column count differs: " + m.name());
            }
            for (int k = 0; k < cs.length; k++) {
                if (!Column.normalize(cs[k].name()).equals(Column.normalize(baseCols[k].name()))) {
                    throw new IllegalArgumentException(
                            "Schema mismatch in union source: column name differs at position " + k + ": "
                                    + cs[k].name());
                }
            }
        }

        // Defensive: if filter array smaller than expected, pad with ALL
        final Comparable<Row>[] effFilter;
        if (filter == null || filter.length == 0) {
            @SuppressWarnings("unchecked")
            Comparable<Row>[] tmp = new Comparable[] { Filter.ALL, Filter.ALL };
            effFilter = tmp;
        } else if (filter.length == 1) {
            @SuppressWarnings("unchecked")
            Comparable<Row>[] tmp = new Comparable[] { filter[0], Filter.ALL };
            effFilter = tmp;
        } else {
            effFilter = filter;
        }

        // Build unified list of sources (record files first to preserve sequential
        // ordering semantics)
        final int totalSources = recordPaths.size() + tablePaths.size();
        if (totalSources == 0)
            throw new IllegalStateException("No sources to union");

        // Worker data structure
        final class Source implements AutoCloseable {
            final int index; // global order index
            final File file;
            final boolean record; // true => NonIndexedFile, false => Table
            GenericFile rif; // when record
            Table table; // when table
            Cursor<Row> rowCursor; // for record
            Cursor<Long> idCursor; // for table
            boolean opened = false;
            boolean exhausted = false;

            Source(int index, File file, boolean record) {
                this.index = index;
                this.file = file;
                this.record = record;
            }

            void open() throws Exception {
                if (opened)
                    return;
                if (record) {
                    rif = GenericFile.open(file);
                    final Comparable<Row> merged = Filter.ALL.equals(effFilter[0]) ? effFilter[1] : (Row r) -> {
                        int d = 0;
                        for (int i = 0; i < effFilter.length; i++) {
                            d = effFilter[i].compareTo(r);
                            if (d != 0)
                                break;
                        }
                        return d;
                    };
                    rowCursor = rif.find(Filter.NOLIMIT, merged); // no per-source limit
                } else {
                    table = Table.open(file, Table.OPEN_RDONLY);
                    idCursor = table.find(Index.PRIMARY, Filter.ASCENDING, Filter.NOLIMIT, effFilter);
                }
                opened = true;
            }

            Row nextRow() throws Exception {
                if (!opened || exhausted)
                    return null;
                if (record) {
                    Row r = rowCursor.next();
                    if (r == null) {
                        exhausted = true;
                        return null;
                    }
                    return r;
                }
                Long id = idCursor.next();
                if (id == null || id < 0) {
                    exhausted = true;
                    return null;
                }
                return table.read(id);
            }

            @Override
            public void close() throws Exception {
                IO.close(rowCursor);
                IO.close(idCursor);
                IO.close(rif);
                IO.close(table);
            }
        }

        final Source[] sources = new Source[totalSources];
        int gi = 0;
        for (File f : recordPaths)
            sources[gi] = new Source(gi++, f, true);
        for (File f : tablePaths)
            sources[gi] = new Source(gi++, f, false);

        // Queue items
        final class Item {
            final int sidx;
            final Row row;
            final boolean done;

            Item(int sidx, Row row, boolean done) {
                this.sidx = sidx;
                this.row = row;
                this.done = done;
            }
        }
        final int queueCapacity = Math.max(1024, 128);
        final BlockingQueue<Item> queue = new ArrayBlockingQueue<>(queueCapacity);
        final AtomicBoolean cancelled = new AtomicBoolean(false);

        // Process sources using provided executor
        for (int si = 0; si < sources.length; si++) {
            final int sourceIndex = si;
            executor.execute(() -> {
                final Source src = sources[sourceIndex];
                try {
                    try {
                        src.open();
                    } catch (Exception ex) { // signal done early
                        queue.offer(new Item(src.index, null, true));
                        return;
                    }
                    for (;;) {
                        if (cancelled.get())
                            break;
                        Row r;
                        try {
                            r = src.nextRow();
                        } catch (Exception ex) {
                            r = null;
                            src.exhausted = true;
                        }
                        if (r == null) { // exhausted
                            queue.put(new Item(src.index, null, true));
                            break;
                        }
                        // Blocking put with cancellation check
                        while (!cancelled.get()) {
                            if (queue.offer(new Item(src.index, r, false), 100, TimeUnit.MILLISECONDS))
                                break;
                        }
                    }
                } catch (InterruptedException ie) {
                    // exit
                } catch (Exception ex) {
                    // signal done with error
                    queue.offer(new Item(src.index, null, true));
                }
            });
        }

        // Cursor that merges in original source order (records first, then tables)
        return new Cursor<Row>() {
            @SuppressWarnings({ "unchecked" }) // Safe: only Row instances stored
            private final Deque<Row>[] buffers = new Deque[totalSources];
            private final boolean[] sourceDone = new boolean[totalSources];
            private int expected = 0; // next source index whose rows should be emitted
            private boolean closed = false;

            @Override
            public Row next() {
                if (closed)
                    return null;
                try {
                    for (;;) {
                        // Emit if we have data for expected source
                        if (expected < totalSources) {
                            Deque<Row> buf = buffers[expected];
                            if (buf != null && !buf.isEmpty()) {
                                Row r = buf.pollFirst();
                                // Apply global limit/offset here
                                if (limit.skip()) {
                                    // skip and fetch next
                                    continue;
                                }
                                if (!limit.remains()) {
                                    cancel();
                                    return null;
                                }
                                return r;
                            }
                            if (sourceDone[expected]) { // move to next source
                                expected++;
                                continue;
                            }
                        } else {
                            return null; // all sources consumed
                        }

                        // Need to fetch more data from queue
                        Item it = queue.poll(200, TimeUnit.MILLISECONDS);
                        if (it == null) {
                            // If queue empty and all workers might be idle, check termination
                            boolean allDone = true;
                            for (int i = 0; i < totalSources; i++)
                                if (!sourceDone[i]) {
                                    allDone = false;
                                    break;
                                }
                            if (allDone)
                                return null; // finished
                            continue; // wait again
                        }
                        if (it.done) {
                            sourceDone[it.sidx] = true;
                        } else if (it.row != null) {
                            Deque<Row> buf = buffers[it.sidx];
                            if (buf == null) {
                                buf = buffers[it.sidx] = new ArrayDeque<>();
                            }
                            buf.addLast(it.row);
                        }
                    }
                } catch (InterruptedException ie) {
                    cancel();
                    return null;
                } catch (Exception ex) {
                    cancel();
                    throw new RuntimeException("Executor-based union iteration error", ex);
                }
            }

            private void cancel() {
                cancelled.set(true);
            }

            @Override
            public void close() throws Exception {
                if (closed)
                    return;
                closed = true;
                cancel();
                // Drain queue quickly
                queue.clear();
                // Close sources
                for (Source s : sources)
                    IO.close(s);
            }
        };
    }

    /**
     * Filter for files based on their date in the filename.
     */
    public static final class DateFileFilter implements FileFilter {
        private final LocalDate from;
        private final LocalDate to;
        // Match dates like:
        // - 20240728
        // - 2024-07-28
        // - 2024_07_28
        // - 2024.7.8 (also allow 1-digit month/day)
        private static final Pattern DATE_YMD_ANY = Pattern.compile(
                "(?<!\\d)(?<y>\\d{4})[-_.]?(?<m>\\d{1,2})[-_.]?(?<d>\\d{1,2})(?!\\d)");

        public DateFileFilter(LocalDate from, LocalDate to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean accept(File file) {
            String fileName = file.getName();
            // System.err.println("File : " + fileName);
            if (file.isDirectory())
                return true;

            // Extract a plausible date anywhere in the filename
            Matcher m = DATE_YMD_ANY.matcher(fileName);
            if (!m.find()) {
                return false;
            }

            try {
                int y = Integer.parseInt(m.group("y"));
                int mo = Integer.parseInt(m.group("m"));
                int da = Integer.parseInt(m.group("d"));
                // Validate and normalize using LocalDate
                LocalDate fileDate = LocalDate.of(y, mo, da);
                return !fileDate.isBefore(from) && !fileDate.isAfter(to);
            } catch (NumberFormatException | java.time.DateTimeException ignore) {
                return false;
            }
        }
    }

    /**
     * Filter for files when dates are represented in directory names.
     *
     * Supports patterns such as:
     * - .../2024/07/28/filename.ext (Y/M/D across three levels)
     * - .../2024-07-28/filename.ext (single segment with separators)
     * - .../20240728/filename.ext (single compact segment)
     * - Also matches when the directory name embeds a date, e.g., logs_2024-07-28
     *
     * Behavior:
     * - If a valid date is found in any ancestor directory name(s), the file is
     * accepted
     * only when from <= date <= to.
     * - If no date is found, the file is rejected (directories are always accepted
     * to allow traversal).
     */
    public static final class DateDirectoryFilter implements FileFilter {
        private final LocalDate from;
        private final LocalDate to;

        // Reuse the same permissive Y-M-D pattern as DateFileFilter for single segment
        // names
        private static final Pattern DATE_YMD_ANY = Pattern.compile(
                "(?<!\\d)(?<y>\\d{4})[-_.]?(?<m>\\d{1,2})[-_.]?(?<d>\\d{1,2})(?!\\d)");

        public DateDirectoryFilter(LocalDate from, LocalDate to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean accept(File file) {
            if (file.isDirectory()) {
                // Always allow traversal; actual filtering happens on files
                return true;
            }

            // Try to resolve a date from ancestor directory names
            LocalDate date = extractDateFromAncestors(file);
            if (date == null) {
                return false;
            }
            return !date.isBefore(from) && !date.isAfter(to);
        }

        private LocalDate extractDateFromAncestors(File file) {
            // Walk up parents, nearest first
            File p = file.getParentFile();
            File p1 = (p != null) ? p : null; // nearest
            File p2 = (p1 != null) ? p1.getParentFile() : null;
            File p3 = (p2 != null) ? p2.getParentFile() : null; // farthest among first three

            // 1) Check any single directory name with embedded Y-M-D or YYYYMMDD
            for (File cur = p1; cur != null; cur = cur.getParentFile()) {
                String name = cur.getName();
                Matcher m = DATE_YMD_ANY.matcher(name);
                if (m.find()) {
                    try {
                        int y = Integer.parseInt(m.group("y"));
                        int mo = Integer.parseInt(m.group("m"));
                        int da = Integer.parseInt(m.group("d"));
                        return LocalDate.of(y, mo, da);
                    } catch (NumberFormatException | java.time.DateTimeException ignore) {
                        // fallthrough
                    }
                }
                // Limit how far we scan upward to avoid walking the entire filesystem
                if (cur == p3)
                    break;
            }

            // 2) Check pattern across three levels: .../YYYY/MM/DD/...
            if (p3 != null && p2 != null && p1 != null) {
                String d1 = p1.getName(); // likely DD
                String d2 = p2.getName(); // likely MM
                String d3 = p3.getName(); // likely YYYY
                if (is1to2Digits(d1) && is1to2Digits(d2) && is4Digits(d3)) {
                    try {
                        int y = Integer.parseInt(d3);
                        int mo = Integer.parseInt(d2);
                        int da = Integer.parseInt(d1);
                        return LocalDate.of(y, mo, da);
                    } catch (NumberFormatException | java.time.DateTimeException ignore) {
                        // invalid path-based date
                    }
                }
            }

            return null;
        }

        private static boolean is4Digits(String s) {
            int n = s.length();
            if (n != 4)
                return false;
            for (int i = 0; i < 4; i++) {
                char c = s.charAt(i);
                if (c < '0' || c > '9')
                    return false;
            }
            return true;
        }

        private static boolean is1to2Digits(String s) {
            int n = s.length();
            if (n < 1 || n > 2)
                return false;
            for (int i = 0; i < n; i++) {
                char c = s.charAt(i);
                if (c < '0' || c > '9')
                    return false;
            }
            return true;
        }
    }

    @Override
    public long write(Row row) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'write'");
    }

    @Override
    public Cursor<Row> find(Filter.Limit limit, Comparable<Row> filter) throws Exception {
        @SuppressWarnings("unchecked")
        final Comparable<Row>[] filterArray = new Comparable[] { Filter.ALL, filter };
        return find(limit, filterArray);
    }

    @Override
    public Cursor<Row> find() throws Exception {
        return find("");
    }

    @Override
    public Cursor<Row> find(String where) throws Exception {
        if (where == null || where.trim().isEmpty()) {
            return find(Filter.NOLIMIT, (Comparable<Row>[]) null);
        }

        final Meta meta = meta();
        if (meta == null) {
            throw new IllegalStateException("No metadata available");
        }

        // Parse as SQL to extract WHERE clause without keyword
        final SQL sql = SQL.parse(String.format("SELECT * FROM union %s", where));
        final Comparable<Row>[] filterArray = Filter.compile(meta, null, sql.where());
        return find(Filter.NOLIMIT, filterArray);
    }

    @Override
    public long fileSize() {
        return -1; // size not applicable for Union
    }

    @Override
    public long rows(boolean force) throws IOException {
        if (!force)
            return this.rows;

        long totalRows = 0;
        // Sum rows from record files
        for (File f : recordPaths) {
            try (var rf = GenericFile.open(f)) {
                long v = rf.rows(force);
                if (v > 0)
                    totalRows += v;
            } catch (Exception e) {
                throw new IOException("Failed to get rows() from record file: " + f.getAbsolutePath(), e);
            }
        }
        this.rows = totalRows;
        return totalRows;
    }

    //

    /**
     * Adapter to wrap a Table as a GenericFile.
     */
    static final class TableAdapter implements GenericFile {
        final Table table;

        TableAdapter(final Table table) throws IOException {
            this.table = table;
        }

        static TableAdapter open(final File file) throws IOException {
            return new TableAdapter(Table.open(file, Table.OPEN_RDONLY));
        }

        static TableAdapter create(final File file, final Meta meta, final Logger logger) throws IOException {
            return new TableAdapter(Table.open(file, meta, logger));
        }

        @Override
        public void close() throws Exception {
            table.close();
        }

        @Override
        public Meta meta() throws IOException {
            return table.meta();
        }

        @Override
        public long write(Row row) throws IOException {
            return table.apply(row);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Cursor<Row> find(Filter.Limit limit, Comparable<Row> filter) throws Exception {
            final Cursor<Long> cursor = table.find(Index.PRIMARY, Filter.ASCENDING, limit, new Comparable[] {
                    Filter.ALL, filter
            });
            return adapt(cursor, table);
        }

        @Override
        public Cursor<Row> find() throws Exception {
            return find("");
        }

        @Override
        public Cursor<Row> find(String where) throws Exception {
            final Cursor<Long> cursor = table.find(where);
            return adapt(cursor, table);
        }

        static Cursor<Row> adapt(final Cursor<Long> cursor, final Table table) {
            return new Cursor<Row>() {
                @Override
                public Row next() {
                    try {
                        long i = cursor.next();
                        if (i == -1)
                            return null;
                        return table.read(i);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }

                @Override
                public void close() throws Exception {
                    cursor.close();
                }
            };
        }

        @Override
        public long fileSize() {
            return table.bytes();
        }

        @Override
        public long rows(boolean force) throws IOException {
            return table.rows();
        }
    }

}