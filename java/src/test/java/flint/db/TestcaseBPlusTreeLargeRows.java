/**
 * 
 */
package flint.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.UUID;

/**
 * https://www.oracle.com/java/technologies/javase/vmoptions-jsp.html
 */
public class TestcaseBPlusTreeLargeRows {
	static final File randomintFile = new File("temp/randomint-tree2.bin");
	static final int CACHE_SIZE = 1 * 1024 * 1024;
	static final int INCREMENTS = 16 * 1024 * 1024;
	static final boolean MEMORY = false;

	/**
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Shuffle.make(randomintFile, 2 * 1024 * 1024, 1.0f);
		// Shuffle.make(randomintFile, 2 * 1024 * 1024, 0.5f);
		// Shuffle.make(randomintFile, 2 * 1024 * 1024, 0.0f);

		// testcase_random_input_hash(1024 * 1024);
		// testcase_random_input_hash(1024 * 1024);
		// testcase_random_search_hash();
		// testcase_random_search_hash();
		//
		// testcase_random_input_tree_v2();
		// testcase_random_input_tree_v2();
		// testcase_random_search_tree_v2();
		// testcase_random_search_tree_v2();

		testcase_random_input_HashTable();
		testcase_random_input_HashTable();
		testcase_random_input_BPlusTreeTable();
		testcase_random_input_BPlusTreeTable();
	}

	static void testcase_random_input_tree_v2() throws Exception {
		System.out.println("");
		System.out.println("-- testcase_random_input_tree_v2 --");
		// randomintFile.delete();
		if (!randomintFile.exists())
			throw new FileNotFoundException(randomintFile.getCanonicalPath());

		final File file = new File("temp/test2.tree");
		file.delete();

		final BPlusTree tree = new BPlusTree(file, //
				true, //
				Storage.TYPE_MEMORY, //
				(INCREMENTS), //
				(CACHE_SIZE), //
				new Comparator<Long>() {
					@Override
					public int compare(Long o1, Long o2) {
						return Long.compare(o1, o2);
					}
				}, WAL.NONE);

		final IO.StopWatch watch = new IO.StopWatch();
		long COUNT = 0L;
		try (final InputStream in = new FileInputStream(randomintFile)) {
			final byte[] bytes = new byte[4];

			long key = 0;
			int i = 1;
			for (i = 1;; i++) {
				if (in.read(bytes) <= 0)
					break;

                ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
				key = bb.getInt() & 0xFFFFFFFFL;
				tree.put(key);
				COUNT++;
			}
			System.out.println("put : " + key + ", i : " + (i - 1));
		}
		//
		// tree.put(0L);

		System.out.println(String.format("%sops, %s", watch.ops(COUNT), IO.StopWatch.humanReadableTime(watch.elapsed())));

		System.out.println("KEYS : " + tree.count() //
				+ ", HEIGHT : " + tree.height(tree.root()) //
				+ ", BYTES : " + IO.readableBytesSize(tree.bytes()) //
				+ ", LEAVES : " + Visualization.leaves(tree) //
				+ ", INTERNALS : " + Visualization.internals(tree) //
				+ ", BYTE_ALIGN : " + (BPlusTree.STORAGE_HEAD_BYTES + BPlusTree.NODE_BYTES) //
				+ ", LEAF_KEYS_MAX : " + BPlusTree.LEAF_KEYS_MAX //
				+ ", INTERNAL_KEYS_MAX : " + BPlusTree.INTERNAL_KEYS_MAX //
		);

		tree.close();
	}

	static void testcase_random_search_tree_v2() throws Exception {
		System.out.println("");
		System.out.println("-- testcase_random_search_tree_v2 --");
		final File file = new File("temp/test2.tree");
		final BPlusTree tree = new BPlusTree(file, //
				true, //
				Storage.TYPE_MEMORY, //
				(INCREMENTS), //
				(CACHE_SIZE), //
				new Comparator<Long>() {
					@Override
					public int compare(Long o1, Long o2) {
						return Long.compare(o1, o2);
					}
				}, WAL.NONE);

		int COUNT = 0;
		final IO.StopWatch watch = new IO.StopWatch();
		try (final InputStream in = new FileInputStream(randomintFile)) {
			final byte[] bytes = new byte[4];

			long key = 0;
			for (;;) {
				if (in.read(bytes) <= 0)
					break;

                ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
				key = bb.getInt() & 0xFFFFFFFFL;
				tree.get(key);
				COUNT++;
			}
			// System.out.println("put : " + key + ", i : " + (i - 1));
		}

		System.out.println(String.format("%sops, %s", watch.ops(COUNT), IO.StopWatch.humanReadableTime(watch.elapsed())));
		tree.close();
	}

	static void testcase_random_input_hash(final int m) throws Exception {
		System.out.println("");
		System.out.println("-- testcase_random_input_hash --");
		// randomintFile.delete();
		if (!randomintFile.exists())
			throw new FileNotFoundException(randomintFile.getCanonicalPath());

		final File file = new File("temp/test.hash");
		file.delete();

		final HashFile hash = new HashFile(file, m, true, (INCREMENTS), new HashFile.HashFunction() {
			@Override
			public int hash(long v) {
				return (int) (v & Integer.MAX_VALUE);
			}

			@Override
			public int compare(Long o1, Long o2) {
				return Long.compare(o1, o2);
			}
		});

		IO.StopWatch watch = new IO.StopWatch();
		long COUNT = 0L;
		try (final InputStream in = new FileInputStream(randomintFile)) {
			final byte[] bytes = new byte[4];
			long key = 0;
			int i = 1;
			for (i = 1;; i++) {
				if (in.read(bytes) <= 0)
					break;

                ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
				key = bb.getInt() & 0xFFFFFFFFL;
				hash.put(key);
				COUNT++;
			}
			System.out.println("put : " + key + ", i : " + (i - 1));
		}
		//
		// tree.put(0L);

		System.out.println(String.format("%sops, %s", watch.ops(COUNT), IO.StopWatch.humanReadableTime(watch.elapsed())));

		// new BPlusTreeTracer().trace(tree);
		System.out.println("KEYS : " + hash.count() + ", BYTES : " + hash.bytes());

		hash.close();
	}

	static void testcase_random_search_hash() throws Exception {
		System.out.println("");
		System.out.println("-- testcase_random_search_hash --");
		final File file = new File("temp/test.hash");
		final HashFile hash = new HashFile(file, (1024 * 1024), true, (INCREMENTS), new HashFile.HashFunction() {
			@Override
			public int hash(long v) {
				return (int) (v & Integer.MAX_VALUE);
			}

			@Override
			public int compare(Long o1, Long o2) {
				return Long.compare(o1, o2);
			}
		});

		int COUNT = 0;
		final IO.StopWatch watch = new IO.StopWatch();
		try (final InputStream in = new FileInputStream(randomintFile)) {
			final byte[] bytes = new byte[4];

			long key = 0;
			for (;;) {
				if (in.read(bytes) <= 0)
					break;

                ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
				key = bb.getInt() & 0xFFFFFFFFL;
				hash.get(key);
				COUNT++;
			}
			// System.out.println("put : " + key + ", i : " + (i - 1));
		}

		System.out.println(String.format("%sops, %s", watch.ops(COUNT), IO.StopWatch.humanReadableTime(watch.elapsed())));
		hash.close();
	}

	static Meta meta(final String name) {
		Meta meta = new Meta(name);
		meta.columns(new Column[] { //
				new Column.Builder("A", Column.TYPE_INT64).value(0).create(), //
				new Column.Builder("B", Column.TYPE_STRING).bytes(32).create(), //
				new Column.Builder("C", Column.TYPE_STRING).bytes(32).create(), //
		});
		meta.indexes(new Index[] { //
				new Table.PrimaryKey(new String[] { "A" }) //
		});
		meta.cacheSize(CACHE_SIZE);
		meta.increment(INCREMENTS);
		return meta;
	}

	static void testcase_random_input_HashTable() throws Exception {
		final File file = new File("temp/hashtable.db");
		final Meta meta = meta(file.getName());
		System.out.println(meta.toString());

		final IO.StopWatch watch = new IO.StopWatch();
		try (final Table table = new HashTable(file, meta, Table.OPEN_RDWR, new Logger.NullLogger())) {
			try (final InputStream in = new FileInputStream(randomintFile)) {
				final byte[] bytes = new byte[4];

				long key = 0;
				for (int i = 1; i <= (10 * 1024 * 1024); i++) {
					if (in.read(bytes) <= 0)
						break;

                    ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
					key = bb.getInt() & 0xFFFFFFFFL;
					table.apply(Row.create(meta, new Object[] { //
							key, //
							new UUID(key, key).toString().replace("-", ""), //
							// new BigDecimal(key + 0xFFFFFFFFFFFFFFFL).toBigInteger().toString(), //
							String.valueOf(key), //
					}));
				}
			}
			System.out.println("rows : " + table.rows() + ", ops : " + watch.ops(table.rows()) + ", time : " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", bytes : " + IO.readableBytesSize(table.bytes()));
		}
		Table.drop(file);
	}

	static void testcase_random_input_BPlusTreeTable() throws Exception {
		final File file = new File("temp/b+treetable.db");
		final Meta meta = meta(file.getName());
		System.out.println(meta.toString());

		final IO.StopWatch watch = new IO.StopWatch();
		try (final Table table = Table.open(file, meta, new Logger.NullLogger())) {
			try (final InputStream in = new FileInputStream(randomintFile)) {
				final byte[] bytes = new byte[4];

				long key = 0;
				for (int i = 1; i <= (10 * 1024 * 1024); i++) {
					if (in.read(bytes) <= 0)
						break;

					ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
					key = bb.getInt() & 0xFFFFFFFFL;
					table.apply(Row.create(meta, new Object[] { //
							key, //
							new UUID(key, key).toString().replace("-", ""), //
							String.valueOf(key), //
					}));
				}
			}

			System.out.println("rows : " + table.rows() + ", ops : " + watch.ops(table.rows()) + ", time : " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", bytes : " + IO.readableBytesSize(table.bytes()));
		}
		Table.drop(file);
	}
}
