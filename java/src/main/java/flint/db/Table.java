/**
 * FlintDB High-Performance Table Interface
 * 
 * Core interface for direct programmatic data access in FlintDB.
 * Provides high-speed data storage and retrieval without SQL parsing overhead.
 * 
 * @author FlintDB Project
 * @since 1.0
 */
package flint.db;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Lightweight Table Interface
 * 
 * Primary interface for direct data manipulation in FlintDB. Designed for zero SQL parsing overhead. 
 * Supports:
 * - Direct API access for production workloads
 * - B+ Tree and Hash indexing (in-memory and on-disk)
 * - Memory-efficient streaming operations
 * - Type-safe data operations
 * - Single-writer, multiple-reader concurrency model
 * 
 * @apiNote Case Sensitive Table
 * @apiNote Case Insensitive Column and Search
 * @apiNote Thread Safety: Read operations are thread-safe, write operations require synchronization
 */
public interface Table extends Closeable {

	/**
	 * Get total number of rows in the table
	 * 
	 * @return Number of rows
	 * @throws IOException if count operation fails
	 */
	long rows() throws IOException;

	/**
	 * Get total storage space used by the table in bytes
	 * 
	 * @return Storage size in bytes
	 */
	long bytes();

	/**
	 * Get table metadata including column definitions and indexes
	 * 
	 * @return Table metadata
	 * @throws IOException if metadata cannot be read
	 */
	Meta meta() throws IOException;

	/**
	 * Get table storage format
	 * 
	 * @return Format string (e.g., "bin" for binary format)
	 */
	String format();

	/**
	 * Add new row or update existing row
	 * 
	 * @param row Row data to insert or update
	 * @return Row ID (0 ~ Long.MAX_VALUE), -1 if error occurred
	 * @throws IOException if operation fails
	 */
	long apply(final Row row) throws IOException;

    /**
     * Add new row or update existing row with upsert option
     * @param row
     * @param upsert
     * @return
     * @throws IOException
     */
	long apply(final Row row, final boolean upsert) throws IOException;

	/**
	 * Update existing row at specified position
	 * 
	 * @param i Row ID to update
	 * @param row New row data
	 * @return Row ID of updated row
	 * @throws IOException if operation fails
	 */
	long apply(final long i, final Row row) throws IOException;

    /**
     * Batch insert or update rows from iterator
     * @param itr
     * @return
     * @throws IOException
     */
	long batch(final Iterator<Row> itr) throws IOException;

	/**
	 * Find rows using index-based search with filters
	 * 
	 * Provides direct access to index structures for maximum performance.
	 * No SQL parsing overhead.
	 * 
	 * @param index Index number to use (0 = primary key)
	 * @param dir Sort direction (Filter.ASCENDING / Filter.DESCENDING)
	 * @param limit Result limit specification
	 * @param filter Filter conditions [0] = indexed filters, [1] = non-indexed filters
	 * @return Cursor for iterating through matching row IDs
	 * @throws Exception if search operation fails
	 */
	Cursor<Long> find(int index, int dir, Filter.Limit limit, Comparable<Row>[] filter) throws Exception;

	/**
	 * Find rows using SQL-like WHERE clause
	 * 
	 * Provides SQL debugging capability for development purposes.
	 * For production use, prefer the direct index-based find() method.
	 * 
	 * <pre>
	 * USE INDEX(PRIMARY DESC) WHERE DT = '2023-12-22' LIMIT 0, 10
	 * </pre>
	 * 
	 * @param where SQL WHERE clause string
	 * @return Cursor for iterating through matching row IDs
	 * @throws Exception if query parsing or execution fails
	 */
	Cursor<Long> find(final String where) throws Exception;

	/**
	 * Get single row by index key lookup (fastest access method)
	 * 
	 * Direct primary key or index-based lookup with zero overhead.
	 * 
	 * @param index Index number to use (0 = primary key)
	 * @param key Map of column names to values for key lookup
	 * @return Row data or null if not found
	 * @throws Exception if lookup operation fails
	 */
	Row one(final int index, final Map<String, Object> key) throws Exception;

	/**
	 * Traverse all row IDs in the table
	 * 
	 * Efficiently iterate through all row identifiers without loading row data.
	 * Useful for bulk operations and statistics gathering.
	 * 
	 * @param visitor Visitor function to process each row ID
	 * @return Number of rows processed
	 * @throws Exception if traversal operation fails
	 */
	long traverse(final Consumer<Long> visitor) throws Exception;

	/**
	 * Read row data by row ID
	 * 
	 * Direct access to row data using internal row identifier.
	 * 
	 * @param i Row ID (0 ~ Long.MAX_VALUE)
	 * @return Row data
	 * @throws IOException if read operation fails
	 */
	Row read(final long i) throws IOException;

	/**
	 * Delete row by row ID
	 * 
	 * @param i Row ID to delete (0 ~ Long.MAX_VALUE)
	 * @return Number of affected rows (1 if successful, 0 if not found)
	 * @throws IOException if delete operation fails
	 */
	long delete(final long i) throws IOException;

	/**
	 * Drop (delete) the entire table and all associated files
	 * 
	 * @throws IOException if drop operation fails
	 */
	void drop() throws IOException;

	// Table Open Mode Constants
	
	/**
	 * Read-only access mode for maximum performance
	 */
	static final int OPEN_READ = (0);
	
	/**
	 * Read-write access mode for data modification
	 */
	static final int OPEN_WRITE = (1);
	

	// Index Algorithm Constants
	/**
	 * Hash-based indexing algorithm
	 */
	static final String ALG_HASH = "hash";
	
	/**
	 * B+ Tree indexing algorithm (recommended)
	 */
	static final String ALG_BPTREE = "bptree";
	
	/**
	 * Best performing algorithm (currently B+ Tree)
	 */
	static final String ALG_BEST = ALG_BPTREE;


	/**
	 * Create new table or open existing one with metadata and open for read-write access
	 * 
	 * @param file Data file location
	 * @param meta Table structure definition for creation
	 * @return Table instance ready for read-write operations
	 * @throws IOException if table creation fails
	 */
	static Table open( //
			final File file, //
			final Meta meta) throws IOException {
		return open(file, meta, new Logger.NullLogger());
	}
	
	/**
	 * Create new table or open existing one with metadata and open for read-write access
	 * 
	 * Creates table structure based on provided metadata. File size is calculated
	 * as rowBytes * maxRows for optimal performance.
	 * 
	 * @param file Data file location
	 * @param meta Table structure definition for creation
	 * @param logger Operation logger for tracking
	 * @return Table instance ready for read-write operations
	 * @throws IOException if table creation fails
	 */
	static Table open( //
			final File file, //
			final Meta meta, final Logger logger) throws IOException {
		return new TableImpl(Meta.make(file, meta), meta, OPEN_WRITE, logger);
	}

	/**
	 * Open existing table in read-only mode with null logger
	 * 
	 * @param file Data file location
	 * @return Table instance in read-only mode
	 * @throws IOException if table opening fails
	 */
	static Table open(final File file) throws IOException {
		return open(file, OPEN_READ, new Logger.NullLogger());
	}

	/**
	 * Open existing table with specified access mode and null logger
	 * 
	 * @param file Data file location
	 * @param mode Access mode (OPEN_READ | OPEN_WRITE)
	 * @return Table instance with specified access mode
	 * @throws IOException if table opening fails
	 */
	static Table open(final File file, final int mode) throws IOException {
		return open(file, mode, new Logger.NullLogger());
	}

	/**
	 * Open existing table with specified access mode
	 * 
	 * Opens an existing table for reading or writing. Table metadata is read
	 * from the .desc file.
	 * 
	 * @param file Data file location
	 * @param mode Access mode (OPEN_READ | OPEN_WRITE)
	 * @param logger Operation logger for tracking
	 * @return Table instance with specified access mode
	 * @throws IOException if table opening fails
	 */
	static Table open(final File file, final int mode, final Logger logger) throws IOException {
		final Meta meta = Meta.read(file);
		return new TableImpl(file, meta, mode, logger);
	}

	/**
	 * Drop (delete) table and all associated files
	 * 
	 * Removes the table data file, metadata file, and all index files.
	 * This operation is irreversible.
	 * 
	 * @param file Table file to drop
	 */
	static void drop(final File file) {
		// if (!file.exists()) {
		// 	System.err.println("Table file does not exist: " + file);
		// }
		if (file.getParentFile().exists()) {
			final File[] a = file.getParentFile().listFiles(
				(final File f) -> 
				f.isFile() 
				&& 
				(f.getName().startsWith(file.getName() + ".i.") //
                || f.getName().equals(file.getName() + Meta.META_NAME_SUFFIX) //
                || f.getName().equals(file.getName() + ".wal") //
                ) 
			);
			if (a != null) {
				for (File f : a) {
					f.delete();
				}
			}
			file.delete();
		}
	}

	/**
	 * Calculate total storage space used by table and indexes
	 * 
	 * @param tFile Table file to analyze
	 * @return Total space in bytes including data file, metadata, and indexes
	 */
	public static long space(final File tFile) {
		long bytes = tFile.length();
		if (tFile.exists()) {
			final File[] a = tFile.getParentFile().listFiles(
				(final File f) -> 
				f.isFile() && (f.getName().startsWith(tFile.getName() + ".i.") //
				|| f.getName().equals(tFile.getName() + Meta.META_NAME_SUFFIX) //
            ));
			if (a != null) {
				for (File f : a)
					bytes += f.length();
			}
		}
		return bytes;
	}

	/**
	 * Get row count for a table file without opening it
	 * 
	 * @param tFile Table file to analyze
	 * @return Number of rows, or -1 if file doesn't exist
	 * @throws IOException if reading fails
	 */
	public static long rows(final File tFile) throws IOException {
		if (tFile.exists()) {
			try (final Table table = open(tFile, OPEN_READ, new Logger.NullLogger())) {
				return table.rows();
			}
		}
		return -1L;
	}

	/**
	 * Abstract base class for index implementations
	 * 
	 * Provides common functionality for different index types including
	 * parameter validation and default algorithm selection.
	 */
	public static abstract class AbstractKey implements Index {
		final String name;
		final String type;

		final String algorithm;
		final String[] keys;
		// final int cacheSize;

		/**
		 * Constructor for abstract index
		 * 
		 * @param name Index name
		 * @param type Index type (e.g., "primary", "sort")
		 * @param algorithm Indexing algorithm
		 * @param cacheSize Cache size (currently unused)
		 * @param keys Column names forming the index key
		 * @throws RuntimeException if keys array is empty
		 */
		protected AbstractKey(final String name, final String type, final String algorithm, final int cacheSize, final String[] keys) {
			this.name = name;
			this.type = type;
			this.algorithm = ALG_BPTREE.equals(algorithm) ? null : algorithm;
			this.keys = keys;
			// System.out.println("Index(" + name + ", type : " + type + ", algorithm : " + algorithm + ", m : " + m);

			if (keys == null || keys.length == 0)
				throw new RuntimeException("The keys can't be empty");
		}

		@Override
		public String algorithm() {
			return algorithm == null ? ALG_BPTREE : algorithm;
		}

		@Override
		public String name() {
			return name;
		}

		@Override
		public String type() {
			return type;
		}

		@Override
		public String[] keys() {
			return keys;
		}

		@Override
		public String toString() {
			return name;
		}
	}

	/**
	 * Primary key index implementation
	 * 
	 * Represents the table's primary key which must be the first index (index 0).
	 * Uses B+ Tree algorithm for optimal performance.
	 */
	public final static class PrimaryKey extends AbstractKey {
		/**
		 * Create primary key with default branching factor
		 * 
		 * @param keys Column names forming the primary key
		 */
		public PrimaryKey(final String[] keys) {
			super(Index.PRIMARY_NAME, "primary", ALG_BPTREE, -1, keys);
		}
	}

	/**
	 * Secondary sort index implementation
	 * 
	 * Provides additional indexing on non-primary key columns for improved
	 * query performance. Uses B+ Tree algorithm.
	 */
	final static class SortKey extends AbstractKey {
		/**
		 * Create sort index with default branching factor
		 * 
		 * @param name Index name
		 * @param keys Column names forming the index key
		 */
		public SortKey(final String name, final String[] keys) {
			super(name, "sort", ALG_BPTREE, -1, keys);
		}
	}

	/**
	 * Transfer data from memory table to file-based table
	 * 
	 * Efficiently moves data from a memory-based table to persistent file storage.
	 * Useful for bulk data processing and temporary table operations.
	 * 
	 * @param memory Source memory table
	 * @param target Target file location
	 * @throws IOException if transfer operation fails or source is not memory-based
	 */
	static void transfer(final Table memory, final File target) throws IOException {
		if (!transferable(memory))
			throw new IOException("Table does not have memory storage");

		Table.drop(target);

		final TableImpl src = (TableImpl) memory;
		for (int i = 0; i < src.sorters().length; i++) {
			final File f = new File(target.getParentFile(), target.getName() + ".i." + src.sorters()[i].name());
			MemoryStorage.transfer((MemoryStorage) src.sorters()[i].storage(), f);
		}
		MemoryStorage.transfer((MemoryStorage) src.storage(), target);

		final Meta m = src.meta().copy();
		m.storage(Storage.TYPE_DEFAULT);
		Meta.make(target, m);
	}

	/**
	 * Check if table can be transferred (is memory-based)
	 * 
	 * @param memory Table to check
	 * @return True if table is memory-based and can be transferred
	 * @throws IOException if check operation fails
	 */
	static boolean transferable(final Table memory) throws IOException {
		return (((TableImpl) memory).storage() instanceof MemoryStorage);
	}


    /**
     * Check if file is supported table format
     * @param file
     * @return
     */
    static boolean supports(final File file) {
        if (!file.exists() || !file.isFile() || !file.canRead())
            return false;

        if (file.getName().endsWith(Meta.TABLE_NAME_SUFFIX))
            return true;

        try(IO.Closer CLOSER = new IO.Closer()) {
            var in = CLOSER.register(new java.io.FileInputStream(file));
            byte[] header = new byte[8];
            int read = in.read(header);
            if (read < 8) return false;
           
            int cmp = java.util.Arrays.compare(
                header, 0, 4, 
                "ITBL".getBytes(), 0, 4);
            int version = ByteBuffer.wrap(header, 4, 4).getInt();
            return cmp == 0 && version == 1;
        } catch (IOException ex) {
            return false;
        }
    }
}
