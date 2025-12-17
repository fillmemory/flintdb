/**
 * FlintDB HashTable Implementation
 * Provides hash-based table storage with high-performance key-value operations.
 */
package flint.db;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;


/**
 * Hash-based table implementation optimized for fast key-based lookups.
 * 
 * <p>HashTable provides a high-performance storage engine using hash-based indexing
 * for rapid key-value operations. It's particularly well-suited for scenarios requiring
 * fast primary key lookups and unique constraint enforcement.</p>
 * 
 * <p><strong>Key Features:</strong></p>
 * <ul>
 *   <li>Hash-based primary key indexing for O(1) average lookup time</li>
 *   <li>Memory-mapped file storage with optional compression</li>
 *   <li>Configurable cache layer for frequently accessed rows</li>
 *   <li>Automatic storage compaction and optimization</li>
 * </ul>
 * 
 * <p><strong>Storage Format:</strong></p>
 * <ul>
 *   <li>Signature: "HTBL" (Hash Table)</li>
 *   <li>Version: 1</li>
 *   <li>Hash-based index files with .i.primary extension</li>
 *   <li>Support for various compression algorithms</li>
 * </ul>
 */
public final class HashTable implements Table {
	/** File signature identifier for hash table format */
	private final String SIGNATURE = "HTBL";
	/** Data file containing the table data */
	private final File file;
	/** Table metadata including schema and configuration */
	private final Meta meta;
	/** Size in bytes of each row in the table */
	private final int rowBytes;
	/** Array of sorters/indexes for the table (currently only primary key) */
	private Sorter[] sorters;

	/** Storage engine for data persistence */
	private Storage storage;
	/** Cache for frequently accessed rows */
	private Cache<Long, Row> cache;
	/** Reader interface for row access operations */
	private Reader reader;
	/** Formatter for binary row serialization/deserialization */
	private Formatter<IoBuffer, IoBuffer> rowformatter;
	/** Logger for operations and error reporting */
	private final Logger logger;
	/** Current file access mode (read/write) */

	/**
	 * Creates a new hash table with the specified metadata and opens it for read/write access.
	 * 
	 * @param file data file where the table will be stored
	 * @param meta table metadata containing schema and configuration
	 * @param logger logger for operations and error reporting
	 * @return new HashTable instance ready for operations
	 * @throws IOException if file creation or initialization fails
	 */
	public static Table open( //
			final File file, //
			final Meta meta, final Logger logger) throws IOException {
		return new HashTable(Meta.make(file, meta), meta, OPEN_RDWR, logger);
	}

	/**
	 * Opens an existing hash table from file with the specified access mode.
	 * 
	 * @param file existing data file containing the table
	 * @param mode file access mode (OPEN_RDWR | OPEN_RDONLY or OPEN_RDONLY)
	 * @param logger logger for operations and error reporting
	 * @return HashTable instance for the existing table
	 * @throws IOException if file opening or metadata reading fails
	 */
	public static Table open(final File file, final int mode, final Logger logger) throws IOException {
		final Meta meta = Meta.read(file);
		return new HashTable(file, meta, mode, logger);
	}

	/**
	 * Constructs a new HashTable instance with the specified parameters.
	 * Initializes the storage engine, formatters, and opens the table for operations.
	 * 
	 * @param file data file for the table
	 * @param meta table metadata and configuration
	 * @param mode file access mode flags
	 * @param logger logger for operations and diagnostics
	 * @throws IOException if initialization fails
	 */
	HashTable(final File file, final Meta meta, final int mode, final Logger logger) throws IOException {
		this.file = file;
		this.meta = meta;
		this.rowBytes = meta.rowBytes();
		this.rowformatter = new Formatter.BINROWFORMATTER(meta.rowBytes(), meta);
		this.logger = logger;
		this.open(mode);
	}

	/**
	 * Returns the absolute path of the data file as string representation.
	 * 
	 * @return absolute file path
	 */
	@Override
	public String toString() {
		return file.getAbsolutePath();
	}

	/**
	 * Returns the table metadata containing schema and configuration information.
	 * 
	 * @return table metadata
	 */
	@Override
	public Meta meta() {
		return meta;
	}

	/**
	 * Returns the format identifier including signature and version information.
	 * 
	 * @return format string in the form "HTBL.1/version"
	 */
	@Override
	public String format() {
		return SIGNATURE + ".1/" + ((storage == null) ? Storage.version(file) : storage.version());
	}

	/**
	 * Initializes the hash table storage backend and cache system.
	 * 
	 * @param mode file access mode (read-only or read-write)
	 * @return true if initialization was successful
	 * @throws IOException if storage initialization fails
	 */
	private boolean open(final int mode) throws IOException { // , java.nio.channels.OverlappingFileLockException
		if (this.storage == null) {
			// System.out.println("FileTable open : " + mode + ", " + ((OPEN_RDWR & mode) > 0 ? "OPEN_RDWR" : "OPEN_RDONLY"));
			this.storage = Storage.create(new Storage.Options() //
					.file(file) //
					.mutable((OPEN_RDWR & mode) > 0) //
					.lock((OPEN_RDWR & mode) > 0) //
					// .headerBytes((short) 0) //
					.blockBytes((short) meta.rowBytes()) //
					.compact(meta.compact()) //
					.compress(meta.compressor()) //
					.dictionary(meta.dictionary()) //
					.increment(meta.increment()) //
					.storage(meta.storage()) //
			);

			if ((OPEN_RDWR & mode) > 0) {
				// HEADER
				final int HEAD_SZ = 4 + 4; // signature(4B) + version(4B)
				final IoBuffer h = storage.head(HEAD_SZ); /* MappedIoBuffer */
				final IoBuffer p = h.slice();
				p.getInt();
				if (0 == p.getInt()) {
					h.put(SIGNATURE.getBytes());
					h.putInt(1); // version
				}
			}

			this.cache = Cache.create((OPEN_RDWR & mode) > 0 ? meta.cacheSize() : 0);
			this.reader = (final long node) -> row(node);

			logger.log("open " // + file //
					+ " format : " + format() //
					+ ", mode : " + ((OPEN_RDWR & mode) > 0 ? "rw" : "r") //
					+ ", row bytes : " + new DecimalFormat("#,##0").format(meta.rowBytes()) + "B" //
					+ ", compact : " + new DecimalFormat("#,##0").format(meta.compact()) + "B"  //
					+ ", storage : " + meta.storage() + " " + IO.readableBytesSize(bytes()) //
					+ ", increment : " + (new DecimalFormat("#,##0").format(meta.increment())) //
					+ ", cache : " + new DecimalFormat("#,##0").format(meta.cacheSize()) //
			// + ", threads : " + ((OPEN_THREADS & mode) > 0 ? "YES" : "NO")//
			);

			final Index[] indexes = meta.indexes();
			this.sorters = new Sorter[indexes.length];
			for (int n = 0; n < indexes.length; n++) {
				final File f = new File(file.getParentFile(), String.format("%s.i.%s", file.getName(), indexes[0].name().toLowerCase()));
				final Sorter ix = sorters[n] = Sorter.sorter( //
						n == Index.PRIMARY ? "PRIMARY" : "", //
						f, //
						indexes[n], //
						meta, //
						mode, //
						Storage.TYPE_MEMORY.equals(meta.storage()), //
						reader, //
						logger);

				logger.log("index.open " + indexes[n].name() //
						+ ", algorithm : " + indexes[n].algorithm() //
						// + ", cache : " + new DecimalFormat("#,##0").format(indexes[n].cacheSize()) //
						+ ", storage : " + IO.readableBytesSize(f.length()) //
						+ ", rows : " + new DecimalFormat("#,##0").format(ix.count()) //
				);
			}
		}
		return true;
	}

	/**
	 * Safely closes a closeable resource with exception handling.
	 * 
	 * @param c the closeable resource to close
	 */
	private void close(final AutoCloseable c) {
		try {
			if (c != null)
				c.close();
		} catch (Exception ex) {
			// ex.printStackTrace();
			logger.error(file + " " + ex.getMessage());
		}
	}

	/**
	 * Closes the hash table, releasing all resources including storage, cache, and indexes.
	 * 
	 * @throws IOException if closing operations fail
	 */
	@Override
	public void close() throws IOException {
		logger.log("closing" //
				+ ", storage : " + IO.readableBytesSize(bytes()) //
				+ ", rows : " + new DecimalFormat("#,##0").format(rows()) //
		);

		final long start = System.currentTimeMillis();
		if (sorters != null) {
			for (final Sorter sorter : sorters) {
				if (sorter != null) {
					close(sorter);
					logger.log("close sorter : " + sorter);
				}
			}
			sorters = null;
		}

		if (cache != null)
			close(cache);
		cache = null;

		if (storage != null)
			close(storage);
		storage = null;

		if (rowformatter != null)
			close(rowformatter);
		rowformatter = null;

		final long elapsed = System.currentTimeMillis() - start;
		logger.log("closed" //
				+ ", storage : " + IO.readableBytesSize(bytes()) //
				+ ", elapsed : " + new DecimalFormat("#,##0").format(elapsed) + "ms" //
		);
	}

	/**
	 * Permanently deletes the hash table and all associated index files.
	 * 
	 * @throws IOException if deletion fails
	 */
	@Override
	public void drop() throws IOException {
		close();
		Table.drop(file);
	}

	/**
	 * Returns the total number of rows in the hash table.
	 * 
	 * @return row count or -1 if index is not available
	 * @throws IOException if count retrieval fails
	 */
	@Override
	public long rows() throws IOException {
		return (sorters == null || sorters[Index.PRIMARY] == null) ? -1 : sorters[Index.PRIMARY].count();
	}

	/**
	 * Returns the total storage size in bytes including data and index files.
	 * 
	 * @return total bytes used by the hash table
	 */
	@Override
	public long bytes() {
		long v = 0;
		try {
			v += (storage != null && storage.bytes() > 0) ? storage.bytes() : file.length();
			if (sorters != null)
				for (final Sorter sorter : sorters) {
					v += sorter.bytes();
				}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return v;
	}

	/**
	 * Returns the underlying data file reference.
	 * 
	 * @return file object for the hash table data
	 */
	public File file() {
		return file;
	}

	/**
	 * Returns the underlying storage engine instance.
	 * 
	 * @return storage backend for data persistence
	 */
	public Storage storage() {
		return storage;
	}

	/**
	 * Inserts a new row into the hash table and updates all indexes.
	 * 
	 * @param row the row data to insert
	 * @return the assigned row ID (node position)
	 * @throws IOException if insertion fails or column count mismatch
	 */
	@Override
	public long apply(Row row) throws IOException {
		if (meta().columns().length != row.array().length)
			throw new IOException("columns mismatch");
		final IoBuffer raw = rowformatter.format(row);
		final int remaining = raw.remaining();
		if (remaining > (meta().rowBytes()))
			throw new IOException("row bytes exceeded");

		final Sorter primary = sorters[Index.PRIMARY];
		final long exists = (row.id() > -1) //
				? row.id() //
				: primary.find(row);

		if (exists == -1) {
			final Long node = storage.write(raw); // insert
			row.id(node);

			primary.create(node);
			// for (int i = 1; i < sorters.length; i++) {
			// sorters[i].create(node);
			// }
			return node;
		} else {
			final Long node = exists;
			row.id(node);

			// for (int i = 1; i < sorters.length; i++) {
			// final Long ok = sorters[i].delete(node);
			// assert ok > -1 : ("sorter : " + sorters[i] + ", node : " + node + ", r : " + row + ", o : " + read(node));
			// if (ok < 0) {
			// logger.error(("apply.error : " + sorters[i] + ", node : " + node + ", r : " + row + ", o : " + read(node)));
			// }
			// }

			cache.remove(node);
			storage.write(node, raw); // update

			// for (int i = 1; i < sorters.length; i++) {
			// sorters[i].create(node);
			// }

			return node;
		}
	}

    @Override
    public long apply(final Row row, final boolean upsert) throws IOException {
        // upsert: not used in HashTable
        return apply(row);
    }

	/**
	 * Updates or inserts a row at a specific node position in the hash table.
	 * 
	 * @param node the target node position
	 * @param row the row data to apply
	 * @return the node position where the row was stored
	 * @throws IOException if update fails or column count mismatch
	 */
	@Override
	public long apply(final long node, final Row row) throws IOException {
		if (node < 0)
			return apply(row);

		if (meta().columns().length != row.array().length)
			throw new IOException("columns mismatch");
		final IoBuffer raw = rowformatter.format(row);
		final int remaining = raw.remaining();
		if (remaining > (meta().rowBytes()))
			throw new IOException("row bytes exceeded");

		row.id(node);
		// System.out.println("exists : " + node + " => " + row);
		// for (int i = 1; i < sorters.length; i++) {
		// final Long ok = sorters[i].delete(node);
		// assert ok > -1 : ("sorter : " + sorters[i] + ", node : " + node + ", r : " + row + ", o : " + read(node));
		// if (ok < 0) {
		// logger.error(("apply.error : " + sorters[i] + ", node : " + node + ", r : " + row + ", o : " + read(node)));
		// }
		// }

		// cache.put(node, row); // bug
		cache.remove(node); //
		storage.write(node, raw); // update

		// for (int i = 1; i < sorters.length; i++) {
		// sorters[i].create(node);
		// }

		return node;
	}

	/**
	 * Finds and returns a single row matching the given criteria using the specified index.
	 * 
	 * @param index the index number to use for search
	 * @param row search criteria as key-value map
	 * @return matching row or null if not found
	 * @throws Exception if search fails
	 */
	@Override
	public Row one(final int index, final Map<String, Object> row) throws Exception {
		final long i = sorters[index].find(Row.create(meta, row));
		return i < 0 ? null : read(i);
	}

	/**
	 * Finds rows using index with direction, limit, and filter criteria.
	 * Currently not implemented for hash tables.
	 * 
	 * @param index the index number to use
	 * @param dir search direction
	 * @param limit result limits
	 * @param filter row filters
	 * @return cursor over matching row IDs
	 * @throws Exception always - not implemented
	 */
	@Override
	public Cursor<Long> find(int index, int dir, Filter.Limit limit, Comparable<Row>[] filter) throws Exception {
		throw new UnsupportedOperationException("NOT IMPLEMENTED");
	}

	/**
	 * Finds rows using SQL WHERE clause.
	 * Currently not supported for hash tables.
	 * 
	 * @param where SQL WHERE clause
	 * @return cursor over matching row IDs
	 * @throws Exception always - not supported
	 */
	@Override
	public Cursor<Long> find(String where) throws Exception {
		throw new RuntimeException("NOT SUPPORTED");
	}

	/**
	 * Traverses all rows in the hash table using a visitor pattern.
	 * 
	 * @param visitor the visitor to process each row ID
	 * @return number of rows processed
	 * @throws Exception if traversal fails
	 */
	@Override
	public long traverse(final Consumer<Long> visitor) throws Exception {
		return sorters[0].traverse(visitor);
	}

	/**
	 * Deletes a row by its ID.
	 * Currently not implemented for hash tables.
	 * 
	 * @param i the row ID to delete
	 * @return deletion result
	 * @throws IOException always - not implemented
	 */
	@Override
	public long delete(long i) throws IOException {
		throw new UnsupportedOperationException("NOT IMPLEMENTED");
	}

	/**
	 * Retrieves a row from storage by its node ID, using cache for performance.
	 * This is an optimized method for internal row access.
	 * 
	 * @param node the node ID of the row to retrieve
	 * @return the row data or null if not found
	 */
	// @ForceInline
	private final Row row(final long node) {
		try {
			final Row r = cache.get(node);
			if (r != null)
				return r;

			IoBuffer bb = storage.read(node); // position - 1
			if (bb == null || bb.remaining() < rowBytes)
				bb = storage.read(node);

			if (bb != null) {
				final Row row = rowformatter.parse(bb);
				if (row != null) {
					row.id(node);
					cache.put(node, row);
					return row;
				}
			}
			logger.log("row(" + node + ") => null");
		} catch (Exception ex) {
			// ex.printStackTrace();
			logger.log("row(" + node + ") => " + ex.getMessage());
		}
		return null;
	}

	/**
	 * Reads a row from storage by its node ID without caching the result.
	 * This is the public interface for row retrieval.
	 * 
	 * @param node the node ID of the row to read
	 * @return the row data or null if not found
	 * @throws IOException if read operation fails
	 */
	@Override
	public final Row read(final long node) throws IOException {
		try {
			final Row r = cache.get(node);
			if (r != null)
				return r;

			IoBuffer bb = storage.read(node); // position - 1
			if (bb == null || bb.remaining() < rowBytes)
				bb = storage.read(node);

			if (bb != null) {
				final Row row = rowformatter.parse(bb);
				if (row != null) {
					row.id(node);
					return row;
				}
			}
			logger.log("read(" + node + ") => null");
		} catch (Exception ex) {
			// ex.printStackTrace();
			logger.log("read(" + node + ") => " + ex.getMessage());
		}
		return null;
	}

	/**
	 * Interface for reading rows by node ID.
	 * Used internally by sorters and indexes.
	 */
	static interface Reader {
		/**
		 * Reads a row by its node ID.
		 * 
		 * @param i the node ID
		 * @return the row data
		 */
		Row read(final long i);
	}

	/**
	 * Interface for index management and sorting operations.
	 * Provides methods for index maintenance, search, and traversal.
	 */
	static interface Sorter extends Closeable {
		/**
		 * Returns the number of indexed entries.
		 * 
		 * @return entry count
		 * @throws IOException if count retrieval fails
		 */
		long count() throws IOException;

		/**
		 * Returns the storage size of the index.
		 * 
		 * @return index size in bytes
		 * @throws IOException if size calculation fails
		 */
		long bytes() throws IOException;

		/**
		 * Returns the height of the index structure.
		 * 
		 * @return index height
		 * @throws IOException if height calculation fails
		 */
		long height() throws IOException;

		/**
		 * Creates a new index entry for the given node.
		 * 
		 * @param i the node ID to index
		 * @return the indexed value or null if creation failed
		 * @throws IOException if index creation fails
		 */
		Long create(final long i) throws IOException;

		/**
		 * Deletes an index entry for the given node.
		 * 
		 * @param i the node ID to remove from index
		 * @return the deleted value or null if not found
		 * @throws IOException if index deletion fails
		 */
		Long delete(final long i) throws IOException;

		/**
		 * Finds the node ID for a row matching the index criteria.
		 * 
		 * @param row the row to search for
		 * @return the node ID or -1 if not found
		 * @throws IOException if search fails
		 */
		long find(final Row row) throws IOException;

		// Cursor<Long> find(final int dir, final Filter.Limit limit, final Comparable<Row> filter) throws Exception;

		/**
		 * Traverses all entries in the index using a visitor pattern.
		 * 
		 * @param visitor the visitor to process each entry
		 * @return number of entries processed
		 * @throws Exception if traversal fails
		 */
		long traverse(final Consumer<Long> visitor) throws Exception;

		/**
		 * Returns the name of this index.
		 * 
		 * @return index name
		 */
		String name();

		/**
		 * Returns the underlying storage for this index.
		 * 
		 * @return storage instance
		 */
		Storage storage();

		/**
		 * Factory method to create a sorter instance based on index configuration.
		 * 
		 * @param object identifier string (unused)
		 * @param indexFile the index file
		 * @param index the index configuration
		 * @param meta table metadata
		 * @param mode file access mode
		 * @param memory whether to use memory storage
		 * @param reader row reader interface
		 * @param logger logger for operations
		 * @return new sorter instance
		 * @throws IOException if sorter creation fails
		 */
		private static Sorter sorter(final String object, //
				final File indexFile, final Index index, final Meta meta, //
				final int mode, final boolean memory, final Reader reader, //
				final Logger logger) throws IOException {
			return new HashKey(indexFile, index, meta, mode, memory, reader, logger);
		}
	}

	/**
	 * Hash-based primary key index implementation.
	 * Provides O(1) average lookup time for primary key operations.
	 */
	static final class HashKey implements Sorter {
		/** Index configuration for this hash key */
		private final Index index;
		/** Hash function for comparing and hashing rows */
		private final HashFile.HashFunction comparator;
		/** Underlying hash file storage */
		private final HashFile hash;
		/** Reader interface for accessing row data */
		private final Reader reader;
		/** Table metadata */
		private final Meta meta;

		/**
		 * Creates a new hash key index.
		 * 
		 * @param indexFile the file to store the index
		 * @param index index configuration
		 * @param meta table metadata
		 * @param mode file access mode
		 * @param memory whether to use memory storage
		 * @param reader row reader interface
		 * @param logger logger for operations
		 * @throws IOException if index creation fails
		 */
		public HashKey(final File indexFile, //
				final Index index, final Meta meta, final int mode, final boolean memory, //
				final Reader reader, //
				final Logger logger) throws IOException {
			this.index = index;
			this.reader = reader;
			this.meta = meta;

			this.comparator = new HashFile.HashFunction() {
				final byte[] keys = Meta.translate(meta, index.keys());

				@Override
				public int hash(long v) {
					// return (int) (v & Integer.MAX_VALUE);
					final Row r = reader.read(v);
					return rhash(r);
				}

				@Override
				public int compare(final Long o1, final Long o2) {
					final Row r1 = reader.read(o1);
					final Row r2 = reader.read(o2);
					return Row.compareTo(keys, r1, r2);
				}
			};

			this.hash = new HashFile(indexFile, 1024 * 1024, (OPEN_RDWR & mode) > 0, (1024 * 1024 * 5), comparator);
		}

		/**
		 * Closes the hash index and releases resources.
		 * 
		 * @throws IOException if close operation fails
		 */
		@Override
		public void close() throws IOException {
			// if (tree != null)
			((Closeable) hash).close();
		}

		/**
		 * Returns the number of entries in the hash index.
		 * 
		 * @return entry count
		 * @throws IOException if count retrieval fails
		 */
		@Override
		public long count() throws IOException {
			return hash.count();
		}

		/**
		 * Returns the storage size of the hash index.
		 * 
		 * @return index size in bytes
		 * @throws IOException if size calculation fails
		 */
		@Override
		public long bytes() throws IOException {
			return hash.bytes();
		}

		/**
		 * Returns the conceptual height of the hash structure (always 3 for hash tables).
		 * 
		 * @return hash structure height
		 * @throws IOException not thrown in this implementation
		 */
		@Override
		public long height() throws IOException {
			return 3;
		}

		/**
		 * Returns string representation of this index.
		 * 
		 * @return "PRIMARY"
		 */
		@Override
		public String toString() {
			return "PRIMARY";
		}

		/**
		 * Returns the name of this index.
		 * 
		 * @return "primary"
		 */
		@Override
		public String name() {
			return "primary";
		}

		/**
		 * Returns the underlying storage for this index.
		 * 
		 * @return storage instance
		 */
		@Override
		public Storage storage() {
			return hash.storage();
		}

		/**
		 * Creates a new index entry for the given row node.
		 * 
		 * @param i row node ID to index
		 * @return the indexed node ID
		 * @throws IOException if indexing fails
		 * @throws IndexOutOfBoundsException if node ID is invalid
		 */
		@Override
		public Long create(final long i) throws IOException {
			if (i < 0)
				throw new java.lang.IndexOutOfBoundsException("" + i);

			hash.put(i);
			return i;
		}

		/**
		 * Deletes an index entry for the given row node.
		 * Currently not implemented for hash indexes.
		 * 
		 * @param i row node ID to remove from index
		 * @return the deleted node ID
		 * @throws IOException always - not implemented
		 * @throws IndexOutOfBoundsException if node ID is invalid
		 */
		@Override
		public Long delete(final long i) throws IOException {
			if (i < 0)
				throw new java.lang.IndexOutOfBoundsException("" + i);

			// hash.delete(i);
			// return i;
			throw new UnsupportedOperationException("NOT IMPLEMENTED");
		}

		/**
		 * Computes hash code for a row based on the index key columns.
		 * 
		 * @param r the row to hash
		 * @return hash code for the row's key values
		 */
		public int rhash(final Row r) {
			final byte[] keys = Meta.translate(meta, index.keys());
			final Object[] a = new Object[keys.length];
			for (int i = 0; i < keys.length; i++)
				a[i] = r.get(keys[i]);
			return Arrays.hashCode(a);
		}

		@Override
		public long find(final Row row) throws IOException {
			final Long found = hash.find(new Comparable<Long>() {
				final byte[] keys = Meta.translate(meta, index.keys());

				@Override
				public int hashCode() {
					return rhash(row);
				}

				@Override
				public int compareTo(final Long o) {
					return Row.compareTo(keys, row, reader.read(o));
				}
			});
			// if (found != null && found == 0)
			// System.err.println("find : " + found + ", " + row + " <> " + reader.read(found));
			return found == null ? -1L : found;
		}

		@Override
		public long traverse(final Consumer<Long> visitor) throws Exception {
			return hash.traverse(visitor);
		}
	}
}
