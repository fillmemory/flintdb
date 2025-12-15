/**
 * 
 */
package flint.db;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Table metadata class containing structure definition and configuration
 * 
 * Stores complete table schema including columns, indexes, storage settings,
 * and format options. Supports both binary and text file formats.
 */
public final class Meta {
    private final float version = 1.0f;
    private final String name;
    private String date; // yyyy-MM-dd
    private Integer compact;
    private String compressor;
    private String storage = Storage.TYPE_DEFAULT;
    private transient String directory;
    private String dictionary; // File
    private Integer increment;
    private Integer cacheSize;
    private String walMode; // WAL Mode: NONE, TRUNCATE, LOG
    private int walCheckpointInterval;
    private int walBatchSize;
    private int walCompressionThreshold;
    private int walPageData = 1; // 1=log page image for UPDATE/DELETE, 0=metadata-only

    private Index[] indexes = null;
    private Column[] columns = null;

    private String absentHeader; // TEXT File(TSV, CSV) Parser
    private char delimiter = '\t'; // TEXT File(TSV, CSV) Parser
    private char quote = '\0'; // TEXT File(TSV, CSV) Parser
    private String nullString = "\\N"; // TEXT File(TSV, CSV) Parser

    private String format = null;

    private transient Map<String, Integer> mc = null;

    public static final String PRODUCT_NAME = "FlintDB"; // TODO: Read from build config
    public static final String PRODUCT_NAME_LC = PRODUCT_NAME.toLowerCase();
    public static final String META_NAME_SUFFIX = ".desc";
    public static final String TABLE_NAME_SUFFIX = ".".concat(PRODUCT_NAME_LC); 

    public static final String WAL_OPT_OFF = "OFF";
    public static final String WAL_OPT_TRUNCATE = "TRUNCATE";
    public static final String WAL_OPT_LOG = "LOG";

    static final int INCREMENT_DEFAULT = (16 * 1024 * 1024);

    private final transient Map<String, Integer> mIndex = new java.util.LinkedHashMap<>();

    /**
     * Create metadata with specified table name
     * 
     * @param name Table name
     */
    public Meta(final String name) {
        this.name = name;
    }

    /**
     * Create metadata from file, using filename as table name
     * 
     * @param file File to extract name from
     */
    public Meta(final File file) {
        this.name = file.getName();
    }

    /**
     * Create metadata with empty table name
     */
    public Meta() {
        this.name = "";
    }

    /**
     * Get table name
     * 
     * @return Table name
     */
    public String name() {
        return name;
    }

    /**
     * Get table creation date
     * 
     * @return Date string (yyyy-MM-dd format)
     */
    public String date() {
        return date;
    }

    /**
     * Set table creation date
     * 
     * @param date Date string (yyyy-MM-dd format)
     * @return This metadata instance for chaining
     */
    public Meta date(final String date) {
        this.date = date;
        return this;
    }

    /**
     * Calculate total bytes required for a single row in binary format
     * 
     * Includes overhead for column count, type information, and variable-length
     * field size indicators.
     * 
     * @return Row size in bytes
     */
    public int rowBytes() {
        int n = Short.BYTES; // column count
        for (final Column c : columns) {
            final short ct = c.type();
            n += Short.BYTES; // type
            if (Column.TYPE_STRING == ct //
                    || Column.TYPE_DECIMAL == ct //
                    || Column.TYPE_BYTES == ct //
                    || Column.TYPE_BLOB == ct //
                    || Column.TYPE_OBJECT == ct //
            )
                n += Short.BYTES; // string length
            //
            n += c.bytes(); // max
        }
        return n;
        // }
    }

    /**
     * Set table indexes
     * 
     * Defines indexing structure for the table. First index must be primary key.
     * Maximum of 20 indexes allowed.
     * 
     * @param indexes Array of index definitions
     * @return This metadata instance for chaining
     * @throws IllegalArgumentException if indexes array is null or empty
     * @throws RuntimeException if more than 20 indexes or validation fails
     */
    public Meta indexes(final Index[] indexes) {
        if (indexes == null || indexes.length == 0)
            throw new IllegalArgumentException("indexes");
        if (indexes.length >= 20)
            throw new RuntimeException("Max indexes reached " + 20);

        validate(indexes);

        this.indexes = indexes;
        for (int i = 0; i < indexes.length; i++) {
            mIndex.put(indexes[i].name(), i);
        }
        return this;
    }

    /**
     * Get all table indexes
     * 
     * @return Array of index definitions
     */
    Index[] indexes() {
        return indexes;
    }

    /**
     * Get index by position
     * 
     * @param i Index position (0-based)
     * @return Index at specified position, or null if out of bounds
     */
    public Index index(final int i) {
        return i < indexes.length ? indexes[i] : null;
    }

    /**
     * Get index position by name
     * 
     * @param name Index name to search for
     * @return Index position (0-based)
     * @throws RuntimeException if index not found
     */
    public int index(final String name) {
        // System.err.println("indexId.alias" + alias);
        if (indexes != null) {
            // for (int i = 0; i < indexes.length; i++) {
            //     // System.err.println("indexes[i].name()" + indexes[i].name() + " " + i);
            //     if (indexes[i].name().equals(name))
            //         return i;
            // }
            if (Index.PRIMARY_NAME.equalsIgnoreCase(name))
                return Index.PRIMARY;
            final Integer n = mIndex.get(name);
            if (n != null)
                return n;
        }
        throw new RuntimeException("Index not found : " + name);
    }

    /**
     * Set table columns
     * 
     * Defines the table schema with column definitions including data types,
     * sizes, and constraints.
     * 
     * @param columns Array of column definitions
     * @return This metadata instance for chaining
     * @throws IllegalArgumentException if columns array is null or empty
     */
    public Meta columns(final Column[] columns) {
        if (columns == null || columns.length == 0)
            throw new IllegalArgumentException("columns cannot be null or empty");
        this.columns = columns;
        return this;
    }

    /**
     * Get all table columns
     * 
     * @return Array of column definitions
     */
    public Column[] columns() {
        return columns;
    }

    /**
     * Get column position by name
     * 
     * @param name Column name to search for (case insensitive)
     * @return Column position (0-based)
     * @throws RuntimeException if column not found
     */
    public int column(final String name) {
        if (mc == null) {
            final Map<String, Integer> m = new java.util.LinkedHashMap<>();
            for (int i = 0; i < columns.length; i++)
                m.put(columns[i].name(), i);
            this.mc = Collections.unmodifiableMap(m);
        }
        final Integer n = mc.get(Column.normalize(name));
        if (n == null || n == -1) {
            final StringBuilder s = new StringBuilder();
            for (final Column c : columns) {
                s.append(c.name());
                s.append(" ");
            }
            // System.err.println("Column[" + name + "] not found (" + s.toString().trim() + ")");
            throw new RuntimeException("Column[" + name + "] not found");
        }
        return n;
    }

    /**
     * Get metadata format version
     * 
     * @return Version number (currently 1.0)
     */
    public float version() {
        return version;
    }

    /**
     * Get compaction threshold for table storage
     * 
     * @return Compaction threshold in bytes, -1 if disabled
     */
    public int compact() {
        return compact == null ? -1 : compact;
    }

    /**
     * Set compaction threshold for table storage
     * 
     * When table grows beyond this size, automatic compaction may be triggered
     * to optimize storage efficiency.
     * 
     * @param compact Compaction threshold in bytes (0 to disable)
     * @return This metadata instance for chaining
     */
    public Meta compact(final int compact) {
        this.compact = compact > 0 ? compact : null;
        return this;
    }

    /**
     * Get compression algorithm used for data storage
     * 
     * @return Compression algorithm name, "none" if no compression
     */
    public String compressor() {
        return compressor == null ? "none" : compressor;
    }

    /**
     * Set compression algorithm for data storage
     * 
     * @param compressor Compression algorithm name (null for no compression)
     * @return This metadata instance for chaining
     */
    public Meta compressor(final String compressor) {
        this.compressor = compressor != null ? compressor : null;
        return this;
    }

    /**
     * Get storage type for the table
     * 
     * @return Storage type (e.g., "file", "memory")
     */
    public String storage() {
        return storage;
    }

    /**
     * Set storage type for the table
     * 
     * @param storage Storage type (null defaults to file storage)
     * @return This metadata instance for chaining
     */
    public Meta storage(final String storage) {
        this.storage = storage != null ? storage : Storage.TYPE_DEFAULT;
        if (null != storage && !Storage.supported(storage))
            throw new RuntimeException("Unsupported storage type : " + storage);
        
        return this;
    }

    /**
     * Get directory for table files
     * 
     * @return Directory path, current directory if not specified
     */
    public File directory() {
        return directory != null ? IO.path(directory) : new File(".");
    }

    /**
     * Set directory for table files
     * 
     * @param directory Directory path for table files
     * @return This metadata instance for chaining
     */
    public Meta directory(final String directory) {
        this.directory = directory;
        return this;
    }

    /**
     * Get dictionary file for data compression
     * 
     * @return Dictionary file or null if not used
     */
    public File dictionary() {
        return dictionary != null ? IO.path(dictionary) : null;
    }

    /**
     * Set dictionary file for data compression
     * 
     * @param dictionary Dictionary file path for compression
     * @return This metadata instance for chaining
     */
    public Meta dictionary(final String dictionary) {
        this.dictionary = dictionary;
        return this;
    }

    /**
     * Get cache size for table operations
     * 
     * @return Cache size in number of rows, -1 if not specified
     */
    public int cacheSize() {
        return cacheSize == null ? -1 : cacheSize;
    }

    /**
     * Set cache size for table operations
     * 
     * @param cacheSize Cache size in number of rows (0 to disable)
     * @return This metadata instance for chaining
     */
    public Meta cacheSize(final int cacheSize) {
        this.cacheSize = cacheSize > 0 ? cacheSize : null;
        return this;
    }

    /**
     * Check if WAL (Write-Ahead Logging) is enabled
     * 
     * @return true if WAL is enabled (TRUNCATE or LOG mode), false otherwise
     */
    public boolean walEnabled() {
        return walMode != null && !WAL_OPT_OFF.equalsIgnoreCase(walMode);
    }

    /**
     * Get WAL mode
     * 
     * @return WAL mode: OFF, TRUNCATE, or LOG (default: OFF)
     */
    public String walMode() {
        return walMode != null ? walMode.toUpperCase() : WAL_OPT_OFF;
    }

    public int walCheckpointInterval() {
        return walCheckpointInterval;
    }

    public int walBatchSize() {
        return walBatchSize;
    }

    public int walCompressionThreshold() {
        return walCompressionThreshold;
    }

    /**
     * Whether WAL should include page image (payload) for UPDATE/DELETE.
     *
     * 1: include payload (enables recovery replay of UPDATE/DELETE)
     * 0: metadata-only (smaller WAL, but UPDATE/DELETE replay is not possible)
     */
    public int walPageData() {
        return walPageData;
    }

    /**
     * Set WAL mode
     * 
     * @param mode WAL mode: OFF (disabled), TRUNCATE (auto-truncate), or LOG (keep all logs)
     * @return This metadata instance for chaining
     */
    public Meta walMode(final String mode) {
        if (mode != null) {
            String m = mode.toUpperCase();
            if (WAL_OPT_OFF.equals(m) || WAL_OPT_TRUNCATE.equals(m) || WAL_OPT_LOG.equals(m)) {
                this.walMode = m;
            } else {
                throw new IllegalArgumentException("Invalid WAL mode: " + mode + ". Must be OFF, TRUNCATE, or LOG");
            }
        } else {
            this.walMode = null;
        }
        return this;
    }

    public Meta walCheckpointInterval(final int interval) {
        this.walCheckpointInterval = interval;
        return this;
    }

    public Meta walBatchSize(final int size) {
        this.walBatchSize = size;
        return this;
    }

    public Meta walCompressionThreshold(final int threshold) {
        this.walCompressionThreshold = threshold;
        return this;
    }

    public Meta walPageData(final int enabled) {
        this.walPageData = enabled;
        return this;
    }

    /**
     * Get storage increment size for table growth
     * 
     * @return Increment size in bytes (default 16MB)
     */
    public int increment() {
        // return increment == null ? INCREMENT_DEFAULT : increment; // NO MORE USED
        return INCREMENT_DEFAULT;
    }

    /**
     * Set storage increment size for table growth
     * 
     * Determines how much space is allocated when table needs to grow.
     * 
     * @param increment Increment size in bytes (0 for default)
     * @return This metadata instance for chaining
     */
    public Meta increment(final int increment) {
        // this.increment = increment > 0 ? increment : null; // NO MORE USED
        return this;
    }

    // Text File

    /**
     * Check if header line is absent in text files
     * 
     * @return True if header is absent (first line is data, not header)
     */
    public boolean absentHeader() {
        return absentHeader != null && "ABSENT".equals(absentHeader);
    }

    /**
     * Set whether header line is absent in text files
     * 
     * @param absentHeader True if header is absent (first line is data, not header)
     * @return This metadata instance for chaining
     */
    public Meta absentHeader(final boolean absentHeader) {
        this.absentHeader = absentHeader ? "ABSENT" : null;
        return this;
    }

    /**
     * Get field delimiter character for text files
     * 
     * @return Delimiter character (default tab)
     */
    public char delimiter() {
        return delimiter;
    }

    /**
     * Set field delimiter character for text files
     * 
     * @param delimiter Delimiter character (e.g., '\t' for TSV, ',' for CSV)
     * @return This metadata instance for chaining
     */
    public Meta delimiter(final char delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    /**
     * Get quote character for text files
     * 
     * @return Quote character ('\0' if not used)
     */
    public char quote() {
        return quote;
    }

    /**
     * Set quote character for text files
     * 
     * @param quote Quote character for escaping (e.g., '"' for CSV)
     * @return This metadata instance for chaining
     */
    public Meta quote(final char quote) {
        this.quote = quote;
        return this;
    }

    /**
     * Get null value representation in text files
     * 
     * @return String representing null values (default "\\N")
     */
    public String nullString() {
        return nullString;
    }

    /**
     * Set null value representation in text files
     * 
     * @param nullString String to represent null values
     * @return This metadata instance for chaining
     */
    public Meta nullString(final String nullString) {
        this.nullString = nullString;
        return this;
    }

    /**
     * Set table format type
     * 
     * @param format Format type (e.g., "bin" for binary, "text" for text)
     * @return This metadata instance for chaining
     */
    public Meta format(final String format) {
        this.format = format;
        return this;
    }

    /**
     * Get table format type
     * 
     * @return Format type string
     */
    public String format() {
        return this.format;
    }


    /**
     * Get string representation of metadata in SQL format
     * 
     * @return SQL-formatted metadata string
     */
    @Override
    public String toString() {
        return SQL.trim_mws(SQL.stringify(this));
    }

    /**
     * Compare this metadata with another for equality
     * 
     * Compares all metadata fields including version, columns, indexes,
     * storage settings, and format options.
     * 
     * @param o Object to compare with
     * @return True if metadata are equivalent
     */
    @Override
    public boolean equals(Object o) {
        if (o != null && o instanceof Meta) {
            Meta x = (Meta) o;
            if (this.version != x.version)
                return FALSE("version");
            if (this.compact() != x.compact())
                return FALSE("compact");
            // if (this.increment() != x.increment())
            //     return FALSE("increment " + this.increment() + " != " + x.increment());

            if (this.indexes != null && x.indexes == null)
                return FALSE("indexes");
            if (this.indexes == null && x.indexes != null)
                return FALSE("indexes");
            if (this.indexes != null && x.indexes != null && this.indexes.length != x.indexes.length)
                return FALSE("indexes");
            if (this.columns.length != x.columns.length)
                return FALSE("columns.length");

            if (this.indexes != null)
                for (int i = 0; this.indexes != null && i < this.indexes.length; i++) {
                    if (!this.indexes[i].name().equals(x.indexes[i].name()))
                        return FALSE("index.name");
                    if (!this.indexes[i].type().equals(x.indexes[i].type()))
                        return FALSE("index.type");
                }

            for (int i = 0; i < this.columns.length; i++) {
                if (!this.columns[i].name().equals(x.columns[i].name()))
                    return FALSE("columns.name");
                if (this.columns[i].type() != x.columns[i].type())
                    return FALSE("columns.type");
                if (this.columns[i].bytes() != x.columns[i].bytes())
                    return FALSE("columns.bytes");
            }

            // TSV,CSV File
            if (this.delimiter != x.delimiter)
                return FALSE("delimiter");
            if (this.quote != x.quote)
                return FALSE("quote");
            if (this.nullString != null && !this.nullString.equals(x.nullString))
                return FALSE("nullString");
            if (this.nullString == null && x.nullString != null)
                return FALSE("nullString");
            // RESERVED FOR FUTURE USE
            if (this.format != null && !this.format.equals(x.format))
                return FALSE("format");
            if (this.format == null && x.format != null)
                return FALSE("format");
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    /**
     * Helper method for equality comparison error reporting
     * 
     * @param why Reason for inequality
     * @return Always returns false
     */
    static boolean FALSE(final String why) {
        System.err.println("WARN: " + why + " " + Thread.currentThread().getStackTrace()[2]);
        return false;
    }

    /**
     * Create a deep copy of this metadata
     * 
     * @return New metadata instance with same configuration
     */
    public Meta copy() {
        final Meta m = new Meta(name);
        m.date = date;
        m.compact = compact;
        m.compressor = compressor;
        m.storage = storage;
        m.directory = directory;
        m.dictionary = dictionary;
        m.increment = increment;
        m.cacheSize = cacheSize;
        m.indexes = indexes;
        m.columns = columns;
        m.absentHeader = absentHeader;
        m.delimiter = delimiter;
        m.quote = quote;
        m.nullString = nullString;
        m.format = format;

        m.walMode = walMode;
        m.walCheckpointInterval = walCheckpointInterval;
        m.walBatchSize = walBatchSize;
        m.walCompressionThreshold = walCompressionThreshold;
        
        return m;
    }

    /**
     * Create table data file and metadata file from metadata definition
     * 
     * Creates or updates the .desc metadata file and returns the data file reference.
     * Validates indexes and writes metadata in SQL format.
     * 
     * @param file Either data file or .desc metadata file
     * @param meta Metadata definition to write
     * @return Data file reference (without .desc extension)
     * @throws IOException if file creation or writing fails
     */
    public static File make(final File file, final Meta meta) throws IOException {
        if (Storage.TYPE_MEMORY.equalsIgnoreCase(meta.storage()) && file.getName().startsWith("@"))
            return file;

        if (file.getName().endsWith(Meta.META_NAME_SUFFIX)) {
            // System.out.println(metaFile);
            if (Formatter.storageFormat(file))
                validate(meta.indexes());

            if (!file.exists() || !meta.equals(Meta.read(file))) {
                final File dir = file.getParentFile();
                if (dir != null)
                    dir.mkdirs();
                try (java.io.OutputStream stream = new java.io.FileOutputStream(file)) {
                    stream.write(SQL.stringify(meta).getBytes());
                }
            }

            return new File(file.getParentFile(), file.getName().substring(0, file.getName().length() - Meta.META_NAME_SUFFIX.length()));
        } else {
            final File dir = file.getParentFile();
            final File metaFile = new File(dir, file.getName() + Meta.META_NAME_SUFFIX);
            return make(metaFile, meta);
        }
    }

    /**
     * Validate index array structure and constraints
     * 
     * Ensures first index is primary key and subsequent indexes are not primary keys.
     * 
     * @param indexes Array of indexes to validate
     * @throws RuntimeException if validation fails
     */
    private static void validate(final Index[] indexes) {
        if (indexes == null || indexes.length == 0)
            throw new RuntimeException("Index not found");

        final Index primary = indexes[0];
        if (!(primary instanceof Table.PrimaryKey))
            throw new RuntimeException("The first index must be a primary key");

        for (int i = 1; i < indexes.length; i++) {
            if (indexes[i] instanceof Table.PrimaryKey)
                throw new RuntimeException("The n-th index can't be a primary key");
        }
    }

    /**
     * Read metadata from file
     * 
     * Reads table metadata from .desc file or infers metadata file name
     * from data file name.
     * 
     * @param file Data file or .desc metadata file
     * @return Metadata loaded from file
     * @throws IOException if file not found or reading fails
     */
    public static Meta read(final File file) throws IOException {
        if (file.getName().endsWith(Meta.META_NAME_SUFFIX)) {
            if (!file.exists())
                throw new java.io.FileNotFoundException("FileTable.Meta.read(" + file);

            try (java.io.InputStream instream = new java.io.FileInputStream(file)) {
                return SQL.parse(instream).meta();
            } catch (IOException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        } else {
            final File dir = file.getParentFile();
            final File metaFile = new File(dir, name(file.getName()) + Meta.META_NAME_SUFFIX);
            if (!metaFile.exists())
                throw new java.io.FileNotFoundException("FileNotFoundException : " + metaFile);

            return read(metaFile);
        }
    }

    static boolean exists(final File file) {
        if (file.getName().endsWith(Meta.META_NAME_SUFFIX)) {
            return file.exists();
        } else {
            final File dir = file.getParentFile();
            final File metaFile = new File(dir, name(file.getName()) + Meta.META_NAME_SUFFIX);
            return metaFile.exists();
        }
    }

    /**
     * Extract base filename without .desc extensions
     * 
     * @param n Filename to process
     * @return Base filename without .desc extension
     */
    static String name(String n) {
        int p = n.lastIndexOf(".");
        if (p > -1) {
            String ext = n.substring(p);
            if (Meta.META_NAME_SUFFIX.equals(ext))
                return n.substring(0, p);
        }
        return n;
    }

    /**
     * Translate column names to column indexes
     * 
     * Converts array of column names to array of column positions for
     * index key mapping.
     * 
     * @param meta Table metadata containing column definitions
     * @param keys Array of column names to translate
     * @return Array of column indexes as bytes
     */
    static byte[] translate(final Meta meta, final String[] keys) {
        // System.out.println(Meta.toString(meta));
        final byte[] a = new byte[keys.length];
        for (int i = 0; i < a.length; i++) {
            final int n = meta.column(keys[i]);
            // System.out.println(keys[i] + " => " + n);
            a[i] = (byte) n;
        }
        return a;
    }
}
