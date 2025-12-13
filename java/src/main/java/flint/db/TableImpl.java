/**
 * 
 */
package flint.db;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Table Implementation 
 */
final class TableImpl implements Table {
	private final String SIGNATURE = "ITBL"; // Indexed Table

	private final File file;
	private final Meta meta;
	private final int rowBytes;
	private Sorter[] sorters;
	private final AtomicInteger lock = new AtomicInteger(0);

	private Storage storage;
    private WAL wal;
	private Cache<Long, Row> cache;
	private Reader reader;
	private Formatter<IoBuffer, IoBuffer> rowformatter;
	private final Logger logger;

	// Thread lock helpers - atomic spinlock
	private void tableLock() {
		int expected = 0;
		while (!lock.compareAndSet(expected, 1)) {
			expected = 0;
		}
	}

	private void tableUnlock() {
		lock.set(0);
	}

	TableImpl(final File file, final Meta meta, final int mode, final Logger logger) throws IOException {
		this.file = file;
		this.meta = meta;
		this.rowBytes = meta.rowBytes();
		this.rowformatter = new Formatter.BINROWFORMATTER(meta.rowBytes(), meta);
		this.logger = logger;
		this.open(mode);
	}

	@Override
	public String toString() {
		return file.getAbsolutePath();
	}

	@Override
	public Meta meta() {
		return meta;
	}

	@Override
	public String format() {
		return SIGNATURE + ".1/" + ((storage == null) ? Storage.version(file) : storage.version());
	}

	private boolean open(final int mode) throws IOException { // , java.nio.channels.OverlappingFileLockException
		if (this.storage == null) {
			//this.cache = Cache.create((OPEN_WRITE & mode) > 0 ? meta.cacheSize() : 0);
			this.cache = Cache.create((OPEN_WRITE & mode) > 0 ? meta.cacheSize() : meta.cacheSize() / 2);

            // Initialize WAL based on meta configuration
            if ((OPEN_WRITE & mode) > 0 && meta.walEnabled() && !Storage.TYPE_MEMORY.equalsIgnoreCase(meta.storage())) {
                final File walFile = new File(file.getParentFile(), file.getName() + ".wal");
                this.wal = WAL.open(walFile, meta.walMode());
            } else {
                this.wal = WAL.NONE; // WAL disabled or read-only mode
            }
			this.storage = WAL.wrap(wal, //
                    new Storage.Options() //
					.file(file) //
					.mutable((OPEN_WRITE & mode) > 0) //
					.lock((OPEN_WRITE & mode) > 0) //
					// .headerBytes((short) 0) //
					.blockBytes((short) meta.rowBytes()) //
					.compact(meta.compact()) //
					.compress(meta.compressor()) //
					.dictionary(meta.dictionary()) //
					.increment(meta.increment()) //
					.storage(meta.storage()), //
                    offset -> cache.remove(offset)
			);

			if ((OPEN_WRITE & mode) > 0) {
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

			// Use unlocked reader for internal comparators to avoid nested locks
			this.reader = (final long node) -> rowUnlocked(node);

			logger.log("open " // + file //
					+ " format : " + format() //
					+ ", mode : " + ((OPEN_WRITE & mode) > 0 ? "rw" : "r") //
					+ ", row bytes : " + new DecimalFormat("#,##0").format(meta.rowBytes()) + "B" //
					+ ", compact : " + new DecimalFormat("#,##0").format(meta.compact()) + "B" //
					+ ", storage : " + meta.storage() + " " + IO.readableBytesSize(bytes()) //
					+ ", increment : " + (new DecimalFormat("#,##0").format(meta.increment())) //
					+ ", cache : " + new DecimalFormat("#,##0").format(meta.cacheSize()) //
			// + ", threads : " + ((OPEN_THREADS & mode) > 0 ? "YES" : "NO")//
			);

			final Index[] indexes = meta.indexes();
			if (null == indexes || indexes.length == 0)
				throw new IllegalStateException("No indexes found - " + file);

			this.sorters = new Sorter[indexes.length];
			for (int n = 0; n < indexes.length; n++) {
				final File f = new File(file.getParentFile(),
						String.format("%s.i.%s", file.getName(), indexes[n].name().toLowerCase()));
				final Sorter ix = sorters[n] = Sorter.sorter(n == Index.PRIMARY ? "PRIMARY" : "", f, indexes[n], meta,
						mode, meta.storage(), reader, wal, logger);
				logger.log("index.open " + indexes[n].name() //
						+ ", algorithm : " + indexes[n].algorithm() //
						// + ", cache : " + new DecimalFormat("#,##0").format(indexes[n].cacheSize()) //
						+ ", storage : " + IO.readableBytesSize(f.length()) //
						+ ", height : " + new DecimalFormat("#,##0").format(ix.height()) //
						+ ", rows : " + new DecimalFormat("#,##0").format(ix.count()) //
				);
			}

            wal.recover();
		}
		return true;
	}

	private void close(final AutoCloseable c) {
		try {
			if (c != null)
				c.close();
		} catch (Exception ex) {
			// ex.printStackTrace();
			logger.error(file + " " + ex.getMessage());
		}
	}

	@Override
	public void close() throws IOException {
		logger.log("closing" //
				+ ", storage : " + IO.readableBytesSize(bytes()) //
				+ ", height : "
				+ new DecimalFormat("#,##0")
						.format(sorters == null || sorters.length == 0 ? 0 : sorters[Index.PRIMARY].height()) //
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

        if (wal != null)
            close(wal);
        wal = null;

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

	@Override
	public void drop() throws IOException {
		close();
		Table.drop(file);
	}

	@Override
	public long rows() throws IOException {
		return (sorters != null && sorters.length > 0 && sorters[Index.PRIMARY] != null)
				? sorters[Index.PRIMARY].count()
				: -1;
	}

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

	public File file() {
		return file;
	}

	public Storage storage() {
		return storage;
	}

	public Sorter[] sorters() {
		return sorters;
	}

	@Override
	public long apply(final Row row) throws IOException {
		return apply(row, true);
	}

	@Override
    public long apply(final Row row, final boolean upsert) throws IOException {
        if (meta().columns().length != row.array().length)
			throw new DatabaseException(ErrorCode.COLUMN_MISMATCH);

		final IoBuffer raw = rowformatter.format(row);
		final int remaining = raw.remaining();
		// if (remaining > (meta().rowBytes())) { // unneeded check
		// rowformatter.release(raw);
		// throw new IOException("row bytes exceeded");
		// }

		tableLock();
		final Primary primary = (Primary) sorters[Index.PRIMARY];
        final long transaction = wal.begin();
		try {
			final long exists = (row.id() > -1) //
					? row.id() //
					: primary.find(row);
			// System.out.println("primary.exists: " + exists);
			if (exists == -1) {
				final Long node = storage.write(raw); // insert
				row.id(node);
				// cache.put(node, row); // bug

				primary.create(node);
				for (int i = 1; i < sorters.length; i++) {
					sorters[i].create(node);
				}
                wal.commit(transaction);
				return node;
			} else {
                if (!upsert) 
                    throw new DatabaseException(ErrorCode.DUPLICATE_KEY, row);

				final Long node = exists;
				row.id(node);

				// TODO if nothing changed then skip update
				// System.out.println("exists : " + node + " => " + row);
				for (int i = 1; i < sorters.length; i++) {
					final Long ok = sorters[i].delete(node);
					assert ok > -1
							: ("sorter : " + sorters[i] + ", node : " + node + ", r : " + row + ", o : " + rowUnlocked(node));
					if (ok < 0) {
						logger.error(("apply.error : " + sorters[i] + ", node : " + node + ", r : " + row + ", o : "
								+ rowUnlocked(node)));
					}
				}

				cache.put(node, row); // unstable but fast
				// cache.remove(node); // stable but slow
				storage.write(node, raw); // update

				for (int i = 1; i < sorters.length; i++) {
					sorters[i].create(node);
				}

                wal.commit(transaction);
				return node;
			}
		} catch (DatabaseException ex) {
			// Preserve DatabaseException to maintain error codes
			// Don't log as error since these are expected business logic exceptions
			wal.rollback(transaction);
			throw ex;
		} catch (Exception ex) {
			// ex.printStackTrace();
			logger.error("apply(" + row + " => " + remaining + ", " + ex.getMessage());
			wal.rollback(transaction);
			throw new IOException(this + ".apply(" + row + " => " + remaining, ex);
		} finally {
			tableUnlock();
			rowformatter.release(raw);
		}
    }

	@Override
	public long apply(final long node, final Row row) throws IOException {
		if (node < 0)
			return apply(row);

		if (meta().columns().length != row.array().length)
			throw new DatabaseException(ErrorCode.COLUMN_MISMATCH);
			
		tableLock();
		final IoBuffer raw = rowformatter.format(row);
        final long transaction = wal.begin();
		try {
			final int remaining = raw.remaining();
			if (remaining > (meta().rowBytes()))
				throw new DatabaseException(ErrorCode.ROW_BYTES_EXCEEDED);

			// TODO if nothing changed then skip update
			row.id(node);
			// System.out.println("exists : " + node + " => " + row);
			for (int i = 1; i < sorters.length; i++) {
				final Long ok = sorters[i].delete(node);
				assert ok > -1
						: ("sorter : " + sorters[i] + ", node : " + node + ", r : " + row + ", o : " + rowUnlocked(node));
				if (ok < 0) {
					logger.error(("apply.error : " + sorters[i] + ", node : " + node + ", r : " + row + ", o : "
							+ rowUnlocked(node)));
				}
			}

			// cache.put(node, row); // bug
			cache.remove(node); //
			storage.write(node, raw); // update

			for (int i = 1; i < sorters.length; i++) {
				sorters[i].create(node);
			}

            wal.commit(transaction);
			return node;
        } catch (Exception ex) {
            // ex.printStackTrace();
            logger.error("apply(" + row + " => " + ex.getMessage());
            wal.rollback(transaction);
            throw new IOException(this + ".apply(" + row, ex);
		} finally {
			tableUnlock();
			rowformatter.release(raw);
		}
	}

    @Override
    public long batch(final Iterator<Row> itr) throws IOException {
        // for batch 
        throw new UnsupportedOperationException("Unimplemented method 'batch'");
    }

	// @ForceInline - internal unlocked version for use within comparators
	private final Row rowUnlocked(final long node) {
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

	// // Public locked version
	// private final Row row(final long node) {
	// 	tableLock();
	// 	try {
	// 		return rowUnlocked(node);
	// 	} finally {
	// 		tableUnlock();
	// 	}
	// }

	@Override
	public final Row read(final long node) throws IOException {
		tableLock();
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
		} finally {
			tableUnlock();
		}
		return null;
	}

	@Override
	public long delete(final long node) throws IOException {
		tableLock();
		final Primary primary = (Primary) sorters[Index.PRIMARY];
        final long transaction = wal.begin();
		try {
			final Row r = rowUnlocked(node);
			if (r != null) {
				final long exists = primary.find(r);
				// System.err.println("delete(" + node + " => rowid : " + r.rowid() + ", row : "
				// + r + ", exists : " + exists);
				if (exists > -1) {
					for (int i = 1; i < sorters.length; i++) {
						// System.err.println("delete.key(" + node); // + ", " + sorters[i].find(r)
						final Long ok = sorters[i].delete(node);
						assert ok > -1;
					}
					primary.delete(node); // primary key

					cache.remove(node);
					storage.delete(node);
                    wal.commit(transaction);
					return 1;
				}
			}
		} catch (Exception ex) {
			// ex.printStackTrace();
			logger.error("delete(" + node + ") => " + ex.getMessage());
            wal.rollback(transaction);
		} finally {
			tableUnlock();
		}
		return -1;
	}

	static Cursor<Long> limit(final Cursor<Long> cursor, final Filter.Limit limit) {
		for (; limit.skip();)
			if (cursor.next() == -1L)
				break;

		return new Cursor<Long>() {
			@Override
			public void close() throws Exception {
				cursor.close();
			}

			@Override
			public Long next() {
				return limit.remains() ? cursor.next() : -1L;
			}
		};
	}

	@Override
	public Cursor<Long> find(final int index, final int dir, final Filter.Limit limit, final Comparable<Row>[] filter)
			throws Exception {
		final Sorter sorter = sorters[index];
		final Cursor<Long> l0 = sorter.find(dir, filter[0]);
		final Cursor<Long> l1 = (filter.length < 2 || Filter.ALL.equals(filter[1]))
				? l0
				: new Cursor<Long>() {
					@Override
					public void close() throws Exception {
						l0.close();
					}

					@Override
					public Long next() {
						try {
							for (;;) {
								Long next = l0.next();
								if (next != null && next != -1) {
									final Row r = read(next);
									if (0 == filter[1].compareTo(r))
										return next;
								} else
									return -1L;
							}
						} catch (IOException ex) {
							return -1L;
						}
					}
				};
		return limit(l1, limit);
	}

	@Override
	public Cursor<Long> find(final String where) throws Exception {
		final SQL sql = SQL.parse(String.format("SELECT * FROM %s %s", file.getCanonicalPath(), where));
		final String[] hint = (sql.index() == null ? Index.PRIMARY_NAME : sql.index()).split(" ");
		final int index = meta().index(hint[0]);
		final int sort = hint.length > 1 && "DESC".equalsIgnoreCase(hint[1]) ? Filter.DESCENDING : Filter.ASCENDING;

		final Comparable<Row>[] filter = Filter.compile(meta(), meta().index(index), sql.where());
		return find(index, sort, Filter.MaxLimit.parse(sql.limit()), filter);
	}

	@Override
	public Row one(final int index, final Map<String, Object> row) throws Exception {
		final long i = sorters[index].find(Row.create(meta, row));
		return i < 0 ? null : read(i);
	}

	@Override
	public long traverse(final Consumer<Long> visitor) throws Exception {
		long rows = 0L;

		if (visitor == null)
			throw new NullPointerException("visitor is null");

		@SuppressWarnings("unchecked")
		final Comparable<Row>[] filter = new Comparable[] { Filter.ALL, Filter.ALL };
		try (final Cursor<Long> cursor = find(Index.PRIMARY, Filter.ASCENDING, Filter.NOLIMIT, filter)) {
			for (long i; (i = cursor.next()) > -1; rows++) {
				visitor.accept(i);
			}
		}
		return rows;
	}

	static interface Reader {
		Row read(final long i);
	}

	static interface Sorter extends Closeable {

		long count() throws IOException;

		long bytes() throws IOException;

		long height() throws IOException;

		Long create(final long i) throws IOException;

		Long delete(final long i) throws IOException;

		long find(final Row key) throws Exception;

		Cursor<Long> find(final int dir, final Comparable<Row> filter) throws Exception;

		String name();

		Storage storage();

		static String format(String s) {
			if (s == null || s.isEmpty() || Storage.compressed(s))
				return Storage.TYPE_DEFAULT;
			if (!Storage.supported(s))
				throw new IllegalArgumentException("Unsupported storage type: " + s);
			return s;
		}

		private static Sorter sorter(final String object, //
				final File indexFile, final Index index, final Meta meta, //
				final int mode, final String storage, final Reader reader, //
                final WAL wal, //
				final Logger logger) throws IOException {
			if (object != null && Index.PRIMARY_NAME.equalsIgnoreCase(object))
				return new Primary(indexFile, index, meta, mode, storage, reader, wal, logger);

			return new SorterImpl(indexFile, index, meta, mode, storage, reader, wal, logger);
		}

		static final int DEFAULT_KEY_CACHE_SIZE = 100 * 1024;
		static final int DEFAULT_STORAGE_INCREMENT = 16 * 1024 * 1024;
	}

	static final class Primary implements Sorter {
		private final Index primary;
		private final Comparator<Long> comparator;
		private final Tree tree;
		private final Reader reader;
		private final Meta meta;

		public Primary(final File indexFile, //
				final Index primary, final Meta meta, final int mode, final String storage, //
				final Reader reader, //
                final WAL wal, //
				final Logger logger) throws IOException {
			this.primary = primary;
			this.reader = reader;
			this.meta = meta;

			this.comparator = new Comparator<Long>() {
				final byte[] keys = Meta.translate(meta, primary.keys());

				@Override
				public int compare(final Long o1, final Long o2) {
					final Row r1 = reader.read(o1);
					final Row r2 = reader.read(o2);
					return Row.compareTo(keys, r1, r2);
				}
			};

			this.tree = Tree.newInstance( //
					indexFile, //
					(OPEN_WRITE & mode) > 0, //
					Sorter.format(storage), //
					DEFAULT_STORAGE_INCREMENT, //
					Math.max((int) (meta.cacheSize() * 0.75f), DEFAULT_KEY_CACHE_SIZE), //
					comparator,
                    wal
                );
		}

		@Override
		public void close() throws IOException {
			// if (tree != null)
			((Closeable) tree).close();
		}

		@Override
		public long count() throws IOException {
			return tree.count();
		}

		@Override
		public long bytes() throws IOException {
			return tree.bytes();
		}

		@Override
		public long height() throws IOException {
			return tree.height();
		}

		@Override
		public String toString() {
			return "PRIMARY";
		}

		@Override
		public String name() {
			return "primary";
		}

		@Override
		public Storage storage() {
			return tree.storage();
		}

		@Override
		public long find(final Row key) throws Exception {
			final Long found = tree.get(new Comparable<Long>() {
				final byte[] keys = Meta.translate(meta, primary.keys());

				@Override
				public int compareTo(final Long o) {
					return Row.compareTo(keys, key, reader.read(o));
				}
			});
			// if (found != null && found == 0)
			// System.err.println("find : " + found + ", " + row + " <> " +
			// reader.read(found));
			return found == null ? -1L : found;
		}

		/**
		 * Primary Index
		 * 
		 * @param i row index
		 * @return
		 * @throws IOException
		 */
		@Override
		public Long create(final long i) throws IOException {
			if (i < 0)
				throw new java.lang.IndexOutOfBoundsException("" + i);

			tree.put(i);
			return i;
		}

		@Override
		public Long delete(final long i) throws IOException {
			if (i < 0)
				throw new java.lang.IndexOutOfBoundsException("" + i);

			tree.delete(i);
			return i;
		}

		@Override
		public Cursor<Long> find(final int dir, final Comparable<Row> filter) throws Exception {
			return tree.find(dir, new Comparable<Long>() {
				@Override
				public int compareTo(final Long o) {
					return filter.compareTo(reader.read(o));
				}

				@Override
				public String toString() {
					return filter.toString();
				}
			});
		}
	}

	static final class SorterImpl implements Sorter {
		private final Comparator<Long> comparator;
		private final Index index;
		private final String name;
		private final Tree tree;
		private final Reader reader;
		private final File file;
		private final Meta meta;

		public SorterImpl(final File indexFile, //
				final Index index, final Meta meta, final int mode, final String storage, //
				final Reader reader, //
                final WAL wal, //
				final Logger logger) throws IOException {
			this.file = indexFile;
			this.index = index;
			this.name = index.name();
			this.reader = reader;
			this.meta = meta;
			this.comparator = new Comparator<Long>() {
				final byte[] keys = Meta.translate(meta, index.keys());

				@Override
				public int compare(final Long o1, final Long o2) {
					final Row r1 = reader.read(o1);
					final Row r2 = reader.read(o2);
					final int x = Long.compare(r1.id(), r2.id());
					int d = x;
					if (d != 0) {
						d = Row.compareTo(keys, r1, r2);
						if (d == 0) {
							d = x;
							if (d < 0)
								return -1;
							if (d > 0)
								return 1;
						}
					}
					return d;
				}
			};

			this.tree = Tree.newInstance( //
					indexFile, //
					(OPEN_WRITE & mode) > 0, //
					Sorter.format(storage), //
					DEFAULT_STORAGE_INCREMENT, //
					Math.max((int) (meta.cacheSize() * 0.75f), DEFAULT_KEY_CACHE_SIZE), //
					comparator, // 
                    wal
                );
		}

		@Override
		public void close() throws IOException {
			((Closeable) tree).close();
		}

		@Override
		public String toString() {
			return file.getName();
		}

		@Override
		public String name() {
			return name;
		}

		@Override
		public long count() throws IOException {
			return tree.count();
		}

		@Override
		public long bytes() throws IOException {
			return tree.bytes();
		}

		@Override
		public long height() throws IOException {
			return tree.height();
		}

		@Override
		public Storage storage() {
			return tree.storage();
		}

		@Override
		public long find(final Row key) throws Exception {
			final Long found = tree.get(new Comparable<Long>() {
				final byte[] keys = Meta.translate(meta, index.keys());

				@Override
				public int compareTo(final Long o) {
					return Row.compareTo(keys, key, reader.read(o));
				}
			});
			// if (found != null && found == 0)
			// System.err.println("find : " + found + ", " + row + " <> " +
			// reader.read(found));
			return found == null ? -1L : found;
		}

		@Override
		public Long create(final long i) throws IOException {
			if (i < 0)
				throw new java.lang.IndexOutOfBoundsException("" + i);
			tree.put(i);
			return i;
		}

		@Override
		public Long delete(final long i) throws IOException {
			if (i < 0)
				throw new java.lang.IndexOutOfBoundsException("" + i);
			return tree.delete(i) ? i : -1;
		}

		@Override
		public Cursor<Long> find(final int dir, final Comparable<Row> filter) throws Exception {
			return tree.find(dir, new Comparable<Long>() {
				@Override
				public int compareTo(final Long o) {
					return filter.compareTo(reader.read(o));
				}

				@Override
				public String toString() {
					return filter.toString();
				}
			});
		}
	}
}
