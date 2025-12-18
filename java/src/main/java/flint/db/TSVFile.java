/**
 * FlintDB TSV/CSV File Handler
 * 
 * Provides high-performance reading and writing of Tab-Separated Values (TSV) and 
 * Comma-Separated Values (CSV) files with automatic type inference and streaming support.
 */
package flint.db;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TSV/CSV File Handler for FlintDB
 * 
 * Provides efficient reading and writing of delimited text files with support
 * for:
 * - Automatic type inference from data samples
 * - Compressed file formats (gzip, zip)
 * - Streaming operations for large datasets
 * - CSV and TSV format support with customizable delimiters
 * - Quote handling and escape sequences
 */
final class TSVFile implements GenericFile {

    static final int OPEN_APPEND = (Meta.OPEN_RDWR | (Meta.OPEN_RDWR << 1));
    static final int GZIP_BUFSZ = 8192;
    static final boolean TYPE_PREDICT = System.getProperty("FLINTDB_TYPE_PREDICT", "0").equals("1"); // experimental

    /**
     * Open TSV/CSV file for reading with specified format and logger
     * 
     * @param file   The file to open
     * @param format The format specification (TSV/CSV)
     * @param logger Logger for operation tracking
     * @return TSVFile instance for reading
     * @throws IOException if file cannot be opened
     */
    static TSVFile open(final File file, final Format format, final Logger logger) throws IOException {
        final TSVFile f = new TSVFile(file, format, null, logger);
        f.open(Meta.OPEN_RDONLY);
        return f;
    }

    /**
     * Open TSV/CSV file for reading with default logger
     * 
     * @param file   The file to open
     * @param logger Logger for operation tracking
     * @return TSVFile instance for reading
     * @throws IOException if file cannot be opened
     */
    public static TSVFile open(final File file, final Logger logger) throws IOException {
        return open(file, null, logger);
    }

    /**
     * Open TSV/CSV file for reading with specified format
     * 
     * @param file   The file to open
     * @param format The format specification (TSV/CSV)
     * @return TSVFile instance for reading
     * @throws IOException if file cannot be opened
     */
    public static TSVFile open(final File file, final Format format) throws IOException {
        return open(file, format, new Logger.NullLogger());
    }

    /**
     * Open TSV/CSV file for reading with auto-detected format
     * 
     * @param file The file to open
     * @return TSVFile instance for reading
     * @throws IOException if file cannot be opened
     */
    public static TSVFile open(final File file) throws IOException {
        return open(file, null, new Logger.NullLogger());
    }

    /**
     * Open TSV/CSV file with specified columns and open option
     * 
     * @param file    The file to open
     * @param columns Column definitions for the file
     * @param option  Open option (CREATE_NEW, CREATE, APPEND, READ)
     * @return TSVFile instance
     * @throws IOException if file cannot be opened
     */
    public static TSVFile open(final File file, final Column[] columns, final StandardOpenOption option)
            throws IOException {
        return open(file, columns, option, new Logger.NullLogger());
    }

    /**
     * Open TSV/CSV file with specified columns, open option, and logger
     * 
     * @param file    The file to open
     * @param columns Column definitions for the file
     * @param option  Open option (CREATE_NEW, CREATE, APPEND, READ)
     * @param logger  Logger for operation tracking
     * @return TSVFile instance
     * @throws IOException if file cannot be opened
     */
    public static TSVFile open(final File file, final Column[] columns, final StandardOpenOption option,
            final Logger logger) throws IOException {
        switch (option) {
            case CREATE_NEW:
                return create(file, columns, logger);
            case CREATE:
                return create(file, columns, logger);
            case APPEND:
                final boolean exists = file.exists() && file.length() > 0;
                final TSVFile f = new TSVFile(file, null, columns, logger);
                f.open(OPEN_APPEND | Meta.OPEN_RDWR);
                if (!exists)
                    f.write(columns);
                return f;
            case READ:
            default:
                break;
        }
        return open(file, logger);
    }

    /**
     * Create new TSV/CSV file with specified columns and logger
     * 
     * @param file    The file to create
     * @param columns Column definitions for the file
     * @param logger  Logger for operation tracking
     * @return TSVFile instance for writing
     * @throws IOException if file cannot be created
     */
    public static TSVFile create(final File file, final Column[] columns, final Logger logger) throws IOException {
        final TSVFile f = new TSVFile(file, null, columns, logger);
        f.open(Meta.OPEN_RDWR);
        f.write(columns);
        return f;
    }

    /**
     * Create new TSV/CSV file with specified columns
     * 
     * @param file    The file to create
     * @param columns Column definitions for the file
     * @return TSVFile instance for writing
     * @throws IOException if file cannot be created
     */
    public static TSVFile create(final File file, final Column[] columns) throws IOException {
        return create(file, columns, new Logger.NullLogger());
    }

    /**
     * Buffered row formatter interface for TSV/CSV processing
     * 
     * Extends the base Formatter interface with additional capabilities for
     * handling
     * multi-line CSV records and providing metadata information.
     */
    static interface BUFFEREDROWFORMATTER extends Formatter<CharSequence, byte[]> {

        /**
         * check CSV multi line column
         * 
         * @param raw CSV/TSV content so far (may be a partial line)
         * @return
         */
        boolean completed(final CharSequence raw);

        /**
         * Get table metadata for this formatter
         * 
         * @return Table metadata containing column definitions
         */
        Meta meta();

        /**
         * Get the delimiter string used by this formatter
         * 
         * @return Delimiter string (e.g., "\t" for TSV, "," for CSV)
         */
        String delim();
    }

    /**
     * Format configuration for TSV/CSV files
     * 
     * Defines parsing and formatting rules including delimiters, quote characters,
     * null value representation, and header line handling.
     */
    static final class Format {
        private String NULL = "\\N";
        private char delimiter = '\t';
        private char quote = 0;
        private String name = "";

        public static Format TSV = new Format() //
                .setDelimiter('\t') //
                .setNull("\\N") //
                .setName("TSV");

        public static Format CSV = new Format() //
                .setDelimiter(',') //
                .setQuote('\"') //
                .setNull("NULL") //
                .setName("CSV");

        /**
         * Get format by name
         * 
         * @param name Format name ("TSV" or "CSV")
         * @return Format instance or null if not found
         */
        public static Format valueOf(String name) {
            switch (name) {
                case "TSV" -> {
                    return TSV;
                }
                case "CSV" -> {
                    return CSV;
                }
            }
            return null;
        }

        /**
         * Set format name
         * 
         * @param name Format name
         * @return This format instance for chaining
         */
        public Format setName(String name) {
            this.name = name;
            return this;
        }

        /**
         * Set null value representation
         * 
         * @param NULL String representation for null values
         * @return This format instance for chaining
         */
        public Format setNull(String NULL) {
            this.NULL = NULL;
            return this;
        }

        /**
         * Set field delimiter character
         * 
         * @param delimiter Delimiter character (e.g., '\t' for TSV, ',' for CSV)
         * @return This format instance for chaining
         */
        public Format setDelimiter(char delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        /**
         * Set quote character for escaping
         * 
         * @param escape Quote character (e.g., '"' for CSV)
         * @return This format instance for chaining
         */
        public Format setQuote(char escape) {
            this.quote = escape;
            return this;
        }

        String Null() {
            return NULL;
        }

        char delimiter() {
            return delimiter;
        }

        char quote() {
            return quote;
        }

        @Override
        public String toString() {
            return (name != null && !"".equals(name)) ? name : super.toString();
        }
    }

    /**
     * Text-based row formatter for TSV/CSV files
     * 
     * Handles parsing and formatting of delimited text files with support for
     * quotes, escaping, and various data types.
     */
    static final class TEXTROWFORMATTER implements BUFFEREDROWFORMATTER {
        private final Column[] columns;
        private final Meta meta;
        private final String NULL; // = "\\N";
        private final char DELIM; // = '\t';
        private final char QUOTE;// = '\"';
        // Reusable builder to minimize per-row allocations
        private final ThreadLocal<StringBuilder> tlBuilder = ThreadLocal.withInitial(() -> new StringBuilder(4096));

        /**
         * Constructor with predefined columns and format
         * 
         * @param name    Table name
         * @param columns Column definitions
         * @param format  Format specification
         */
        public TEXTROWFORMATTER(final Meta meta, final Format format) {
            this.columns = meta.columns();
            this.meta = meta;
            this.NULL = format.Null();
            this.DELIM = format.delimiter();
            this.QUOTE = format.quote();
            // System.err.printf("CSTR DELIM : %s\n", DELIM);
        }

        /**
         * Constructor with inferred columns (creates new Meta)
         * 
         * @param name    Table name
         * @param columns Inferred column definitions
         * @param format  Format specification
         */
        public TEXTROWFORMATTER(final String name, final Column[] columns, final Format format) {
            this.columns = columns;
            this.meta = new Meta(name).columns(columns);
            this.NULL = format.Null();
            this.DELIM = format.delimiter();
            this.QUOTE = format.quote();
            // System.err.printf("CSTR DELIM : %s\n", DELIM);
        }

        /**
         * Constructor with header parsing
         * 
         * @param name   Table name
         * @param head   Header line containing column names
         * @param format Format specification
         */
        private TEXTROWFORMATTER(final String name, final String head, final Format format) {
            this.NULL = format.Null();
            this.DELIM = format.delimiter();
            this.QUOTE = format.quote();
            this.columns = columns(format, head);
            this.meta = new Meta(name).columns(this.columns);
            // System.err.printf("CSTR DELIM : %s\n", DELIM);
        }

        @Override
        public void close() {
            // No-op for close
        }

        @Override
        public void release(final byte[] raw) throws IOException {
            // No-op for byte array release
        }

        /**
         * Parse column definitions from header line
         * 
         * @param format Format specification
         * @param head   Header line containing column names
         * @return Array of column definitions
         */
        Column[] columns(final Format format, final String head) {
            final String[] a = split(head);
            final Column[] v = new Column[a.length];
            for (int i = 0; i < a.length; i++) {
                v[i] = new Column(a[i].trim(), Column.TYPE_STRING, Short.MAX_VALUE, (byte) 0, false, null, null);
            }
            return v;
        }

        @Override
        public Meta meta() {
            return meta;
        }

        @Override
        public String delim() {
            return new String(new char[] { DELIM });
        }

        @Override
        public byte[] format(final Row row) throws IOException {
            final StringBuilder sb = tlBuilder.get();
            sb.setLength(0);

            for (int i = 0; i < columns.length; i++) {
                if (i > 0)
                    sb.append(DELIM);
                final Object v = row.get(i);
                if (v == null) {
                    sb.append(NULL);
                    continue;
                }

                switch (columns[i].type()) {
                    case Column.TYPE_DATE ->
                        sb.append((v instanceof java.util.Date) ? DATE_ISO.format((java.util.Date) v) : v.toString());
                    case Column.TYPE_TIME -> sb.append(
                            (v instanceof java.util.Date) ? DATETIME_ISO.format((java.util.Date) v) : v.toString());
                    case Column.TYPE_STRING -> {
                        final String str = v.toString();
                        appendEscaped(sb, str);
                    }
                    default -> sb.append(v.toString());
                }
            }
            sb.append('\n');
            return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        // Append with in-place escaping; avoids chained replace() allocations
        private void appendEscaped(final StringBuilder sb, final String s) {
            if (QUOTE == 0) {
                // Backslash-escaped mode: escape \\ \t \r \n and delimiter
                for (int i = 0, n = s.length(); i < n; i++) {
                    char ch = s.charAt(i);
                    if (ch == '\\') {
                        sb.append("\\\\");
                    } else if (ch == '\t') {
                        sb.append("\\t");
                    } else if (ch == '\n') {
                        sb.append("\\n");
                    } else if (ch == '\r') {
                        sb.append("\\r");
                    } else if (ch == DELIM) {
                        sb.append('\\').append(DELIM);
                    } else {
                        sb.append(ch);
                    }
                }
            } else {
                // CSV-style quoting: wrap if contains QUOTE/DELIM/newline
                boolean needsQuote = false;
                for (int i = 0, n = s.length(); i < n && !needsQuote; i++) {
                    char ch = s.charAt(i);
                    if (ch == QUOTE || ch == '\n' || ch == '\r' || ch == DELIM)
                        needsQuote = true;
                }
                if (!needsQuote) {
                    sb.append(s);
                    return;
                }
                sb.append(QUOTE);
                for (int i = 0, n = s.length(); i < n; i++) {
                    char ch = s.charAt(i);
                    if (ch == QUOTE) {
                        sb.append(QUOTE).append(QUOTE);
                    } else {
                        sb.append(ch);
                    }
                }
                sb.append(QUOTE);
            }
        }

        /**
         * Parse a text line into a Row object
         * 
         * @param raw Raw text line to parse
         * @return Parsed Row object
         * @throws IOException if parsing fails
         */
        @Override
        public Row parse(final CharSequence raw) throws IOException {
            final String[] a = split(raw);
            return Row.create(meta, a);
        }

        // Split variant that accepts CharSequence to avoid copying the line into a
        // char[] first
        private String[] split(final CharSequence raw) {
            final ArrayList<String> array = new ArrayList<>();
            final int L = raw.length();
            final char[] s = new char[L + 1]; // worst-case buffer
            final char BSLASH = '\\';
            int n = 0;
            int qoute = 0;
            int q = 0;

            for (int i = 0; i < L; i++) {
                final char ch = raw.charAt(i);
                final char next = ((i + 1) < L) ? raw.charAt(i + 1) : '\0';

                // Outside quotes, treat CR/LF as record terminators and stop collecting
                if (qoute == 0 && (ch == '\n' || ch == '\r')) {
                    break; // ignore trailing line break
                }

                if (qoute > 0 && QUOTE == ch && QUOTE == next) {
                    s[n++] = ch;
                    i++;
                } else if (qoute > 0 && QUOTE == ch) {
                    qoute = 0;
                } else if (QUOTE != 0 && QUOTE == ch) {
                    qoute = 1;
                    q = 1;
                } else if (qoute > 0) {
                    s[n++] = ch;
                } else if (BSLASH == ch) {
                    // Support backslash escapes in both CSV and TSV modes for robustness
                    // This allows inputs like a\,b to be treated as a,b in CSV as well
                    if (DELIM == next) {
                        s[n++] = DELIM;
                        i++;
                    } else if (BSLASH == next) {
                        s[n++] = BSLASH;
                        i++;
                    } else if ('n' == next) {
                        s[n++] = '\n';
                        i++;
                    } else if ('r' == next) {
                        s[n++] = '\r';
                        i++;
                    } else if ('t' == next) {
                        s[n++] = '\t';
                        i++;
                    } else {
                        // Unrecognized escape, keep the backslash literal
                        s[n++] = ch;
                    }
                } else if (DELIM == ch) {
                    final String v = new String(s, 0, n);
                    array.add((q == 0 && NULL.equals(v)) ? null : v);
                    n = 0;
                    q = 0;
                } else {
                    s[n++] = ch;
                }
            }
            // flush last field
            final String v = new String(s, 0, n);
            array.add((q == 0 && NULL.equals(v)) ? null : v);
            return array.toArray(String[]::new);
        }

        /**
         * Check if a CSV line is complete (handles multi-line quoted fields)
         * 
         * @param raw Raw text line to check
         * @return True if line is complete and ready for parsing
         */
        @Override
        public boolean completed(final CharSequence raw) {
            // Fast path: without quoting, each physical line is complete
            if (QUOTE == 0)
                return true;
            int qoute = 0;
            final int len = raw.length();
            for (int i = 0; i < len; i++) {
                final char ch = raw.charAt(i);
                final char next = (i + 1) < len ? raw.charAt(i + 1) : '\0';
                if (qoute > 0 && QUOTE == ch && QUOTE == next) {
                    // Escaped quote inside quoted field, skip the second quote
                    i++;
                } else if (qoute > 0 && QUOTE == ch) {
                    // End of quoted section
                    qoute = 0;
                } else if (QUOTE == ch) {
                    // Start of quoted section
                    qoute = 1;
                }
            }
            // Complete when not inside a quoted section at the end of buffer
            return qoute == 0;
        }
    }

    /**
     * ISO date formatter (yyyy-MM-dd)
     * 
     * Provides thread-safe date formatting and parsing for DATE columns.
     */
    static final class DATE_ISO {
        /**
         * Format a Date to ISO date string
         * 
         * @param d Date to format
         * @return ISO date string (yyyy-MM-dd)
         */
        static String format(final java.util.Date d) {
            return LocalDateTime.ofInstant(d.toInstant(), Row.ZONE_ID).format(Row.DATE_FORMAT);
        }

        static java.sql.Timestamp parse(final String s) throws java.text.ParseException {
            return (java.sql.Timestamp) Row.date(s);
        }
    }

    /**
     * ISO datetime formatter (yyyy-MM-dd HH:mm:ss)
     * 
     * Provides thread-safe datetime formatting and parsing for TIME columns.
     */
    static final class DATETIME_ISO {
        /**
         * Format a Date to ISO datetime string
         * 
         * @param d Date to format
         * @return ISO datetime string (yyyy-MM-dd HH:mm:ss)
         */
        static String format(final java.util.Date d) {
            return LocalDateTime.ofInstant(d.toInstant(), Row.ZONE_ID).format(Row.DATE_TIME_FORMAT);
        }

        static java.sql.Timestamp parse(final String s) throws java.text.ParseException {
            return (java.sql.Timestamp) Row.date(s);
        }
    }

    private final File file;
    private final Format format;
    private BUFFEREDROWFORMATTER rowformatter;
    private final Logger logger;
    private OutputStream ostream;

    /**
     * Constructor for TSVFile instance
     * 
     * @param file    File to handle
     * @param format  Format specification (can be null for auto-detection)
     * @param columns Column definitions (can be null)
     * @param logger  Logger for operation tracking
     */
    protected TSVFile(final File file, final Format format, final Column[] columns, final Logger logger) {
        this.file = file(file);
        this.format = format;
        this.rowformatter = columns == null ? null : formatter(file, format, new Meta(file.getName()).columns(columns));
        this.logger = logger;
    }

    /**
     * Get the actual file to use (checks for .gz variant)
     * 
     * @param file Original file
     * @return File to use (may be .gz version)
     */
    private static File file(final File file) {
        final File gz = new File(file.getParentFile(), file.getName() + ".gz");
        return (gz.exists()) ? gz : file;
    }

    /**
     * Get string representation of this TSVFile
     * 
     * @return File path string
     */
    @Override
    public String toString() {
        return file(file).toString();
    }

    /**
     * Close the TSVFile and clean up resources
     * 
     * @throws IOException if close operation fails
     */
    @Override
    public void close() throws IOException {
        logger.log("closed" //
                + ", file size : " + IO.readableBytesSize(file.length()) //
        );
        if (ostream != null) {
            ostream.close();
            ostream = null;
        }
    }

    /**
     * Drop (delete) the TSVFile and associated metadata
     * 
     * @throws IOException if delete operation fails
     */
    public void drop() throws IOException {
        close();
        GenericFile.drop(file);
    }

    /**
     * Get metadata file for a data file
     * 
     * @param file Data file
     * @return Corresponding metadata file
     */
    private static File metaFile(final File file) {
        final String name = file.getName();
        return new File(file.getParentFile(), name.concat(Meta.META_NAME_SUFFIX));
    }

    // /**
    // * Drop (delete) specified file and its metadata
    // *
    // * @param file File to delete
    // * @throws IOException if delete operation fails
    // */
    // public static void drop(final File file) throws IOException {
    // file(file).delete();
    // final File meta = metaFile(file);
    // if (meta.exists())
    // meta.delete();
    // }

    /**
     * Infer column types by reading sample rows from the file
     * 
     * @param reader        BufferedReader for the file
     * @param tempFormatter Temporary formatter for parsing
     * @param sampleSize    Number of rows to sample for type inference
     * @return Array of columns with inferred types
     * @throws IOException if reading fails
     */
    private Column[] inferTypesByReadingSampleRows(final java.io.BufferedReader reader,
            final TEXTROWFORMATTER tempFormatter, final int sampleSize) throws IOException {
        final Column[] originalColumns = tempFormatter.meta().columns();
        final int columnCount = originalColumns.length;
        final java.util.List<String[]> sampleRows = new java.util.ArrayList<>();

        // Read sample rows
        String line;
        StringBuilder sb = new StringBuilder();
        int rowsRead = 0;

        while ((line = reader.readLine()) != null && rowsRead < sampleSize) {
            sb.append(line);
            if (!tempFormatter.completed(sb)) {
                sb.append("\n");
                continue;
            }

            final Row row = tempFormatter.parse(sb);
            final String[] values = new String[columnCount];
            for (int i = 0; i < columnCount && i < row.size(); i++) {
                final Object value = row.get(i);
                values[i] = (value == null) ? null : value.toString();
            }
            sampleRows.add(values);
            rowsRead++;
            sb.setLength(0);
        }

        // Infer types for each column
        final Column[] inferredColumns = new Column[columnCount];
        for (int col = 0; col < columnCount; col++) {
            final String columnName = originalColumns[col].name();
            final short inferredType = inferColumnType(sampleRows, col);
            final short length = getTypeLength(inferredType);
            final byte scale = (inferredType == Column.TYPE_DECIMAL) ? (byte) 5 : (byte) 0; // DECIMAL(20,5)

            inferredColumns[col] = new Column(columnName, inferredType, length, scale, false, null, null);
        }

        return inferredColumns;
    }

    /**
     * Infer the appropriate data type for a specific column based on sample values
     * 
     * @param sampleRows  List of sample row data
     * @param columnIndex Index of the column to analyze
     * @return Inferred column type constant
     */
    private short inferColumnType(final java.util.List<String[]> sampleRows, final int columnIndex) {
        boolean canBeInt = true;
        boolean canBeLong = true;
        boolean canBeDouble = true;
        boolean canBeDate = true;
        boolean canBeTime = true;
        boolean foundValidValue = false;

        for (final String[] row : sampleRows) {
            if (columnIndex >= row.length)
                continue;
            final String value = row[columnIndex];
            if (value == null || value.trim().isEmpty())
                continue;

            foundValidValue = true;
            final String trimmed = value.trim();

            // Check if it can be a date first (yyyy-MM-dd format)
            if (canBeDate) {
                try {
                    DATE_ISO.parse(trimmed);
                    continue; // If it's a valid date, skip other checks
                } catch (ParseException e) {
                    canBeDate = false;
                }
            }

            // Check if it can be a datetime (yyyy-MM-dd HH:mm:ss format)
            if (canBeTime) {
                try {
                    DATETIME_ISO.parse(trimmed);
                    continue; // If it's a valid datetime, skip other checks
                } catch (ParseException e) {
                    canBeTime = false;
                }
            }

            // Check if it can be a long
            if (canBeLong) {
                try {
                    Long.parseLong(trimmed);
                } catch (NumberFormatException e) {
                    canBeLong = false;
                }
            }

            // Check if it can be a int
            if (canBeInt) {
                try {
                    Integer.parseInt(trimmed);
                } catch (NumberFormatException e) {
                    canBeInt = false;
                }
            }

            // Check if it can be a double (only if not long)
            if (canBeDouble && !canBeLong) {
                try {
                    Double.parseDouble(trimmed);
                } catch (NumberFormatException e) {
                    canBeDouble = false;
                }
            }
        }

        // If all values are null or empty, default to string for safety
        if (foundValidValue == false) {
            return Column.TYPE_STRING;
        }

        // Return the most specific type that fits all values
        // Priority: Date > Time > Long > Decimal > String
        if (canBeDate)
            return Column.TYPE_DATE;
        if (canBeTime)
            return Column.TYPE_TIME;
        // if (canBeInt) return Column.TYPE_INT;
        if (canBeLong || canBeInt)
            return Column.TYPE_INT64;
        if (canBeDouble)
            return Column.TYPE_DECIMAL;

        return Column.TYPE_STRING; // Default to string
    }

    /**
     * Get appropriate byte length for each data type
     * 
     * @param type Column type constant
     * @return Appropriate byte length for the type
     */
    private short getTypeLength(final short type) {
        return switch (type) {
            case Column.TYPE_INT64 -> 8;
            case Column.TYPE_DECIMAL -> 20;
            case Column.TYPE_DATE -> 10;
            case Column.TYPE_TIME -> 19;
            default -> Short.MAX_VALUE;
        }; // DECIMAL(20,5) - 20 total digits
        // For strings and others
    }

    /**
     * Open file with specified mode and setup streams
     * 
     * @param mode Open mode flags
     * @return True if successfully opened
     * @throws IOException if open operation fails
     */
    private boolean open(final int mode) throws IOException {
        logger.log("open " // + file //
                + "mode : " + ((Meta.OPEN_RDWR & mode) > 0 ? "rw" : "r") //
                + ", file size : " + IO.readableBytesSize(file.length()) //
        );
        if (ostream == null && (Meta.OPEN_RDWR & mode) > 0) {
            file.getParentFile().mkdirs();
            String name = file.getName();
            int p = name.lastIndexOf(".");
            this.ostream = new FileOutputStream(file);
            if (p > -1) {
                String ext = name.substring(p);
                if (".gz".equals(ext)) {
                    this.ostream = new java.util.zip.GZIPOutputStream(this.ostream, GZIP_BUFSZ);
                } else if (".zip".equals(ext)) {
                    name = name.substring(0, p);
                    java.util.zip.ZipOutputStream os = new java.util.zip.ZipOutputStream(this.ostream);
                    os.putNextEntry(new java.util.zip.ZipEntry(name.substring(0, p)));
                    this.ostream = os;
                }
            }
        }
        return true;
    }

    /**
     * Get file size in bytes
     * 
     * @return File size in bytes
     */
    @Override
    public long fileSize() {
        return file.length();
    }

    /**
     * Create input stream for file with compression support
     * 
     * @param file File to create stream for
     * @return InputStream with appropriate decompression
     * @throws IOException if stream creation fails
     */
    static java.io.InputStream stream(final File file) throws IOException {
        try {
            if (file.getName().endsWith(".gz")) {
                return new java.util.zip.GZIPInputStream((new java.io.FileInputStream(file)), GZIP_BUFSZ);
            } else if (file.getName().endsWith(".zip")) {
                final java.io.InputStream instream = new java.util.zip.ZipInputStream(
                        new java.io.FileInputStream(file));
                final java.util.zip.ZipEntry e = ((java.util.zip.ZipInputStream) instream).getNextEntry();
                // System.out.println("entry : " + e.getName());
                return e != null ? instream : null;
            } else if (file.getName().contains(".zip#")) {
                final int i = file.getName().indexOf('#');
                final String name = file.getName().substring(0, i);
                final String part = file.getName().substring(i + 1);
                // System.out.println("name : " + name + ", part : " + part);
                final java.util.zip.ZipInputStream instream = new java.util.zip.ZipInputStream(
                        new java.io.FileInputStream(new File(file.getParentFile(), name)));

                java.util.zip.ZipEntry e = null;
                while ((e = instream.getNextEntry()) != null) {
                    if (part.equals(e.getName()))
                        return instream;
                }
                instream.close();
                return null;
            } else {
                return new java.io.FileInputStream(file);
            }
        } catch (java.io.EOFException ex) {
            throw new java.io.EOFException("EOF " + file.getCanonicalPath());
        }
    }

    /**
     * Get table metadata for this TSV/CSV file
     * 
     * @return Table metadata with column definitions and format info
     * @throws IOException if metadata cannot be read or inferred
     */
    @Override
    public Meta meta() throws IOException {
        if (this.rowformatter != null)
            return this.rowformatter.meta();

        if (null != this.ostream)
            throw new RuntimeException("writing");

        final File metaFile = metaFile(file);
        if (metaFile.exists()) {
            final Meta meta = Meta.read(metaFile);
            this.rowformatter = formatter(file, format, meta);
            return meta;
        }

        final String name = file.getName().toLowerCase();
        try (final java.io.InputStream istream = stream(file)) {
            final java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(istream, StandardCharsets.UTF_8));
            final String head = reader.readLine(); // TSV/CSV HEADER
            if (head == null)
                throw new RuntimeException("No header - " + file);
            // System.out.println(head);

            // Predict types by reading first 10 rows
            Format inferredFormat = this.format;
            if (inferredFormat == null) {
                inferredFormat = (name.endsWith(".csv") || name.endsWith(".csv.gz") || name.endsWith(".csv.zip")) //
                        ? Format.CSV
                        : Format.TSV;
            }

            // First create a temporary formatter to parse the header and sample rows
            TEXTROWFORMATTER tempFormatter = new TEXTROWFORMATTER(file.getName(), head, inferredFormat);
            if (TYPE_PREDICT) {
                Column[] inferredColumns = inferTypesByReadingSampleRows(reader, tempFormatter, 10);
                this.rowformatter = new TEXTROWFORMATTER(file.getName(), inferredColumns, inferredFormat);
            } else {
                // Use the header to create the row formatter directly
                this.rowformatter = new TEXTROWFORMATTER(file.getName(), head, inferredFormat);
            }
        }

        return this.rowformatter.meta();
    }

    // /**
    // * Get table metadata for a file with specified format
    // *
    // * @param file File to analyze
    // * @param format Format specification
    // * @return Table metadata
    // * @throws IOException if metadata cannot be read
    // */
    // static Meta meta(final File file, final Format format) throws IOException {
    // final File metaFile = metaFile(file);
    // if (metaFile.exists()) {
    // final Meta meta = Meta.read(metaFile);
    // return meta;
    // }

    // try (var CLOSER = new IO.Closer()) {
    // final java.io.InputStream istream = CLOSER.register(stream(file));
    // final java.io.BufferedReader reader = new java.io.BufferedReader(new
    // java.io.InputStreamReader(istream, StandardCharsets.UTF_8));
    // final String head = reader.readLine(); // TSV/CSV HEADER
    // if (head == null)
    // throw new RuntimeException("No header - " + file);

    // final TEXTROWFORMATTER formatter = CLOSER.register(new
    // TEXTROWFORMATTER(file.getName(), head, format));
    // return formatter.meta();
    // }
    // }

    /**
     * Create appropriate row formatter for file and metadata
     * 
     * @param file   File being processed
     * @param format Format specification (can be null for auto-detection)
     * @param meta   Table metadata
     * @return Configured row formatter
     */
    static BUFFEREDROWFORMATTER formatter(final File file, final Format format, final Meta meta) {
        if (format != null)
            return new TEXTROWFORMATTER(meta, format);

        final String name = file.getName().toLowerCase();
        if (name.endsWith(".csv") || name.endsWith(".csv.gz") || name.endsWith(".csv.zip"))
            // return new CSVROWFORMATTER(file.getName(), meta.columns());
            return new TEXTROWFORMATTER(meta, Format.CSV);
        if (name.endsWith(".tsv") || name.endsWith(".tsv.gz") || name.endsWith(".tsv.zip")) //
            // return new TSVROWFORMATTER(file.getName(), meta.columns());
            return new TEXTROWFORMATTER(meta, Format.TSV);

        return new TEXTROWFORMATTER(meta, new Format() //
                .setDelimiter(meta.delimiter()) //
                .setQuote(meta.quote()) //
                .setNull(meta.nullString())//
        );
    }

    /**
     * Write column headers to output stream
     * 
     * @param columns Column definitions to write as headers
     * @throws IOException if write operation fails
     */
    private void write(final Column[] columns) throws IOException {
        final String DELIM = rowformatter.delim();
        final StringBuilder s = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            if (i > 0)
                s.append(DELIM);
            final Column c = columns[i];
            s.append(c.name());
        }
        s.append("\n");
        ostream.write(s.toString().getBytes());
    }

    /**
     * Write a data row to the output stream
     * 
     * @param row Row data to write
     * @return Always returns 0 (for compatibility)
     * @throws IOException if write operation fails
     */
    @Override
    public long write(final Row row) throws IOException {
        final byte[] bytes = rowformatter.format(row);
        ostream.write(bytes);
        return 0;
    }

    /**
     * Find rows matching specified criteria with streaming cursor
     * 
     * @param limit  Limit specification for result set
     * @param filter Filter criteria for row matching
     * @return Cursor for iterating through matching rows
     * @throws Exception if search operation fails
     */
    @Override
    public Cursor<Row> find(final Filter.Limit limit, final Comparable<Row> filter) throws Exception {
        if (null != this.ostream)
            throw new RuntimeException("writing");

        final Meta meta = this.meta();
        if (this.rowformatter == null)
            this.rowformatter = formatter(file, format != null ? format : Format.valueOf(meta.format()), meta);

        return new Cursor<Row>() {
            private java.io.InputStream inputStream;
            private java.io.BufferedReader reader;
            private final AtomicLong n = new AtomicLong(0);
            private StringBuilder sb = new StringBuilder();
            private boolean initialized = false;
            private boolean finished = false;

            private void initialize() throws Exception {
                if (!initialized) {
                    inputStream = stream(file);
                    reader = new java.io.BufferedReader(
                            new java.io.InputStreamReader(inputStream, java.nio.charset.StandardCharsets.UTF_8),
                            1 << 20);
                    if (!meta.absentHeader()) {
                        reader.readLine(); // Skip TSV/CSV HEADER
                    }
                    initialized = true;
                }
            }

            @Override
            public Row next() {
                if (finished)
                    return null;

                try {
                    initialize();

                    String l;
                    while ((l = reader.readLine()) != null) {
                        sb.append(l);
                        if (!rowformatter.completed(sb)) {
                            sb.append("\n");
                            continue;
                        }

                        final Row row = rowformatter.parse(sb);
                        sb.setLength(0);

                        if (filter.compareTo(row) == 0) {
                            if (!limit.skip()) {
                                if (!limit.remains()) {
                                    finished = true;
                                    return null;
                                }

                                row.id(n.get());
                                n.incrementAndGet();
                                return row;
                            }
                        }
                        n.incrementAndGet();
                    }

                    // // Process any remaining data in buffer (for last line without trailing newline
                    // // or CSV multi-line)
                    // if (sb.length() > 0) {
                    //     final Row row = rowformatter.parse(sb);
                    //     sb.setLength(0);

                    //     if (filter.compareTo(row) == 0) {
                    //         if (!limit.skip()) {
                    //             if (!limit.remains()) {
                    //                 finished = true;
                    //                 return null;
                    //             }

                    //             row.id(n.get());
                    //             n.incrementAndGet();
                    //             finished = true; // Mark finished after processing last row
                    //             return row;
                    //         }
                    //     }
                    //     n.incrementAndGet();
                    // }

                    // End of file reached
                    finished = true;
                    return null;
                } catch (Exception e) {
                    finished = true;
                    throw new RuntimeException("Error reading TSV file", e);
                }
            }

            @Override
            public void close() throws Exception {
                finished = true;
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (Exception e) {
                        // Ignore close errors
                    }
                }
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (Exception e) {
                        // Ignore close errors
                    }
                }
            }
        };
    }

    /**
     * Find all rows without filtering
     * 
     * @return Cursor for iterating through all rows
     * @throws Exception if search operation fails
     */
    @Override
    public Cursor<Row> find() throws Exception {
        return find(Filter.NOLIMIT, Filter.ALL);
    }

    /**
     * Find rows using SQL WHERE clause
     * 
     * @param where SQL WHERE clause string
     * @return Cursor for iterating through matching rows
     * @throws Exception if query parsing or execution fails
     */
    @Override
    public Cursor<Row> find(final String where) throws Exception {
        final SQL sql = SQL.parse(String.format("SELECT * FROM %s %s", file.getCanonicalPath(), where));
        final Comparable<Row>[] filter = Filter.compile(meta(), null, sql.where());
        return find(Filter.MaxLimit.parse(sql.limit()), filter[1]);
    }

    @Override
    public long rows(boolean force) throws IOException {
        if (!force) return -1;

        // Optimize row counting without full parsing
        long rowCount = 0;
        try (var CLOSER = new IO.Closer()) {
            final Meta meta = this.meta();
            final java.io.InputStream istream = CLOSER.register(stream(file));
            final java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(istream, StandardCharsets.UTF_8), 1 << 20);
            
            // Skip header if present
            if (!meta.absentHeader()) {
                reader.readLine();
            }

            // Get formatter to check for CSV multi-line support
            if (this.rowformatter == null) {
                this.rowformatter = formatter(file, format != null ? format : Format.valueOf(meta.format()), meta);
            }

            // For TSV (no quotes), just count newlines - fast path
            if (rowformatter.delim().equals("\t")) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isEmpty()) {
                        rowCount++;
                    }
                }
            } else {
                // For CSV with potential multi-line records, need to check completeness
                final StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                    if (rowformatter.completed(sb)) {
                        if (sb.length() > 0) {
                            rowCount++;
                        }
                        sb.setLength(0);
                    } else {
                        // Multi-line record, keep accumulating
                        sb.append('\n');
                    }
                }
                // Count any remaining data in buffer
                if (sb.length() > 0) {
                    rowCount++;
                }
            }
        } catch (Exception e) {
            throw new IOException("Failed to count rows", e);
        }
        return rowCount;
    }

    /**
     * Check if the file can be read as a TSV/CSV when no format is specified
     */
    static boolean canRead(final File file) {
        try (var CLOSER = new IO.Closer()) {
            var istream = CLOSER.register(stream(file));
            var reader = new java.io.BufferedReader(new java.io.InputStreamReader(istream, StandardCharsets.UTF_8));
            var head = reader.readLine(); // TSV header, CSV not supported here
            if (head == null || head.trim().length() == 0)
                return false;

            // check readable characters?
            for (int i = 0; i < Math.min(1024, head.length()); i++) {
                char ch = head.charAt(i);
                if (ch < 32 && ch != '\t' && ch != '\r' && ch != '\n')
                    return false;
            }

            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
