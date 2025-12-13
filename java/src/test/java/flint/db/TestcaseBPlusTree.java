/**
 * 
 */
package flint.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class TestcaseBPlusTree {
	static final File randomintFile = new File("temp/randomint-tree.bin");

	/**
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		testcase_random_input_tree();
	}

	static void testcase_random_input_tree() throws Exception {
		// randomintFile.delete();
		if (!randomintFile.exists())
			make_random_file(1000, true);

		final int MAX = (int) (randomintFile.length() / Integer.BYTES);

		final File file = new File("temp/test.tree");
		file.delete();

		boolean TRAVERSE_FORWARD = false;
		boolean TRAVERSE_BACKWARD = false;
		boolean SEARCH_RANGE = true;
		boolean VALIDATE_KEYS = true;
		boolean TRAVERSE_NODE = false;

		final BPlusTree tree = new BPlusTree(file, //
				true, //
				Storage.TYPE_DEFAULT, //
				(1024), //
				(1024 * 1024), //
				new Comparator<Long>() {
					@Override
					public int compare(Long o1, Long o2) {
						return Long.compare(o1, o2);
					}
				}, WAL.NONE);

		IO.StopWatch watch = new IO.StopWatch();
		long COUNT = 0L;
		try (final InputStream in = new FileInputStream(randomintFile)) {
			final byte[] bytes = new byte[4];
			int BREAK = (12 * 1 + 1);
			BREAK = MAX;

			long key = 0;
			int i = 1;
			for (i = 1; i <= BREAK; i++) {
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

		// new BPlusTreeTracer().trace(tree);
		System.out.println("COUNT : " + tree.count() + ", HEIGHT : " + tree.height(tree.root()));

		if (VALIDATE_KEYS)
			for (long i = 1; i <= COUNT; i++) {
				final Object found = tree.get(i);
				if (found == null) {
					System.err.println("missing => " + i);
					TRAVERSE_NODE = true;
					break;
				} else {
					// System.out.println("found => " + found);
				}
			}

		if (TRAVERSE_FORWARD)
			try (final Cursor<Long> cursor = Visualization.front(tree)) {
				System.out.print("\nforward : ");
				int max = 2000 + 1;
				int i = 0;
				for (Long key = null; (key = cursor.next()) != null && (i < max); i++) {
					if (i % 12 == 0)
						System.out.println();
					System.out.print(String.format("%03d ", key));
				}
				System.out.println();
			}

		if (TRAVERSE_BACKWARD)
			try (final Cursor<Long> cursor = Visualization.tail(tree)) {
				System.out.print("\nbackward : ");
				int max = 2000 + 1;
				int i = 0;
				for (Long key = null; (key = cursor.next()) != null && (i < max); i++) {
					if (i % 12 == 0)
						System.out.println();
					System.out.print(String.format("%03d ", key));
				}
				System.out.println();
			}

		if (SEARCH_RANGE)
			try (final Cursor<Long> cursor = tree.find(Filter.DESCENDING, new Comparable<Long>() {
				@Override
				public int compareTo(Long v) {
					// return 0;
					int d = Long.compare(v, 10);
					if (d >= 0) {
						d = Long.compare(v, 40);
						return (d <= 0) ? 0 : -d;
					}
					return (d >= 0) ? 0 : -d;
				}
			})) {
				System.out.print("find range : ");
				int i = 0;
				for (long key; (key = cursor.next()) != -1 && (i < 1000); i++) {
					System.out.print(String.format("%03d ", key));
				}
				System.out.println();
			}

		if (TRAVERSE_NODE) {
			for (long offset = 1; offset <= 1000; offset++) {
				// System.out.println("OFFSET : " + offset);
				final BPlusTree.Node node = tree.read(offset);
				if (node == null)
					break;
				System.out.println("Node : " + node);
			}
			System.out.println("Root : " + tree.root());
			System.out.println("Height : " + tree.height(tree.root()));
		}

		tree.close();
	}

	static void make_random_file(final int sz, final boolean shuffle) throws Exception {
		final Integer[] a = new Integer[sz];
		for (int i = 0; i < sz; i++) {
			a[i] = i + 1;
		}

		if (shuffle)
			shuffle(a);

		try (final OutputStream out = new FileOutputStream(randomintFile)) {
			for (final Integer i : a) {
                ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                bb.putInt(i);
                bb.flip();
                out.write(bb.array());
			}
		}
	}

	static <T> void shuffle(T[] array) {
		Random rnd = ThreadLocalRandom.current();
		for (int i = array.length - 1; i > 0; i--) {
			int index = rnd.nextInt(i + 1);
			// Simple swap
			T a = array[index];
			array[index] = array[i];
			array[i] = a;
		}
	}

}
