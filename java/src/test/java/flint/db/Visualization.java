/**
 * Visualization.java
 */
package flint.db;

import java.awt.Desktop;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Visualization class for B+Tree structures.
 */
final class Visualization {

	/**
	 * Debug function
	 */
	static void open(final BPlusTree tree) throws Exception {
		final java.io.PrintStream out = System.out;
		final BPlusTree.Node root = tree.root();
		if (root == null) {
			out.println("ROOT == NULL");
			return;
		}

		final Set<BPlusTree.Node> a = new java.util.LinkedHashSet<>();
		a.add(root);
		final long max = (tree.bytes() - Storage.HEADER_BYTES) / (BPlusTree.NODE_BYTES);
		for (long offset = 1; offset <= max; offset++) {
			// System.out.println("OFFSET : " + offset);
			final BPlusTree.Node n = tree.read(offset);
			if (n != null && n.offset() != root.offset())
				a.add(n);
		}

		final File htmlFile = new File("./temp/b+tree.html");
		final File jsonFile = new File("./temp/b+tree.json.js");
		try (final IO.Closer CLOSER = new IO.Closer()) {
			final InputStream in = CLOSER.register(Visualization.class.getResourceAsStream("b+tree.html"));
			final OutputStream os = CLOSER.register(new FileOutputStream(htmlFile));
			IO.pipe(in, os);
		}
		try (final IO.Closer CLOSER = new IO.Closer()) {
			final String json = "var data = " + Json.stringify(a, 2) + ";";
			final InputStream in = new ByteArrayInputStream(json.getBytes());
			final OutputStream os = CLOSER.register(new FileOutputStream(jsonFile));
			IO.pipe(in, os);
		}

		final String URL = htmlFile.getCanonicalFile().toURI().toString();
		openURL(URL);
	}

	static void openURL(String URL) throws Exception {
		if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
			final String osname = System.getProperty("os.name");
			// System.out.println(osname);
			if (osname.toLowerCase().contains("mac os x")) {
				final ProcessBuilder pb = new ProcessBuilder();
				pb.command("open", URL);
				pb.start();
			} else if (osname.toLowerCase().contains("windows")) {
				final ProcessBuilder pb = new ProcessBuilder();
				pb.command("rundll32", "url.dll,FileProtocolHandle", URL);
				pb.start();
			} else {
				Desktop.getDesktop().browse(new URI(URL));
				// LINUX xdg-open http://stackoverflow.com
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

	static File make_random_file(final File randomintFile, final int sz, final boolean shuffle) throws Exception {
		final Integer[] a = new Integer[sz];
		for (int i = 0; i < sz; i++) {
			a[i] = i + 1;
		}

		if (shuffle)
			shuffle(a);

        ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
		try (final OutputStream out = new FileOutputStream(randomintFile)) {
			for (final Integer i : a) {
                bb.putInt(i);
                bb.flip();
                out.write(bb.array());
                bb.clear();
			}
		}
		return randomintFile;
	}

	/**
	 * Debug function
	 */
	static final Cursor<Long> front(final BPlusTree tree) throws IOException {
		BPlusTree.Leaf found = null;
		BPlusTree.Node n = tree.root();

		for (;;) {
			if (n instanceof BPlusTree.Leaf) {
				found = (BPlusTree.Leaf) n;
				break;
			}

			final BPlusTree.Internal node = (BPlusTree.Internal) n;
			final BPlusTree.KeyRef[] keys = node.keys;
			final BPlusTree.KeyRef k = keys[0];
			n = k.left();
		}

		final BPlusTree.Leaf front = found;
		return new Cursor<>() {
			BPlusTree.Leaf s = front;
			long[] keys = s == null ? null : s.keys;
			int i = 0;

			@Override
			public void close() throws Exception {
			}

			@Override
			public Long next() {
				try {
					if (null == keys)
						return null;
					if (i >= keys.length) {
						s = (BPlusTree.Leaf) tree.read(s.right());
						keys = s == null ? null : s.keys;
						i = 0;
					}
					if (null == keys)
						return null;
					return keys[i++];
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				return null;
			}
		};
	}

	/**
	 * Debug function
	 */
	static final Cursor<Long> tail(final BPlusTree tree) throws IOException {
		BPlusTree.Leaf found = null;
		BPlusTree.Node n = tree.root();

		for (;;) {
			if (n instanceof BPlusTree.Leaf) {
				found = (BPlusTree.Leaf) n;
				break;
			}

			if (n == null) {
				break;
			}

			final BPlusTree.Internal node = (BPlusTree.Internal) n;
			final BPlusTree.KeyRef[] keys = node.keys;
			final BPlusTree.KeyRef k = keys[keys.length - 1];
			n = k.right();
		}

		final BPlusTree.Leaf tail = found;
		return new Cursor<>() {
			BPlusTree.Leaf s = tail;
			long[] keys = s == null ? null : s.keys;
			int i = keys == null ? -1 : keys.length - 1;

			@Override
			public void close() throws Exception {
			}

			@Override
			public Long next() {
				try {
					if (null == keys)
						return null;
					if (i < 0) {
						s = (BPlusTree.Leaf) tree.read(s.left());
						keys = s == null ? null : s.keys;
						i = keys == null ? -1 : keys.length - 1;
					}
					if (null == keys)
						return null;
					return keys[i--];
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				return null;
			}
		};
	}

	static int internals(final BPlusTree tree) throws IOException {
		BPlusTree.Node root = tree.root();
		if (root == null)
			return 0;
		if (root instanceof BPlusTree.Leaf)
			return 0;
		return internals(tree, (BPlusTree.Internal) root);
	}

	static int internals(final BPlusTree tree, final BPlusTree.Internal node) throws IOException {
		int v = 1;
		for (int i = 0; i < node.keys.length; i++) {
			BPlusTree.Node left = node.keys[i].left();
			if (left instanceof BPlusTree.Leaf)
				break;
			v += internals(tree, (BPlusTree.Internal) left);
		}
		BPlusTree.Node right = node.keys[node.keys.length - 1].right();
		if (right instanceof BPlusTree.Internal)
			v += internals(tree, (BPlusTree.Internal) right);
		return v;
	}

	static int leaves(final BPlusTree tree) throws IOException {
		int v = 0;
		for (BPlusTree.Leaf n = tree.min(tree.root()); n != null; n = (BPlusTree.Leaf) tree.read(n.right()))
			v++;
		return v;
	}

	static enum DEBUG_TYPE {
		NONE, //
		ENABLE_KEY_DELETION_FORWARD, //
		ENABLE_KEY_DELETION_BACKWARD, //
		ENABLE_KEY_DELETION_RANDOM, //
		ENABLE_KEY_DELETION_BY_DIRECTION //
	}

	/**
	 */
	public static void testcase1(String[] args) throws Exception {
		File randomintFile = new File("temp/randomint-tree-n100.bin");
		final File file = new File("temp/test.b+tr");

		file.delete();
		// randomintFile.delete();

		if (!randomintFile.exists())
			randomintFile = make_random_file(new File("temp/randomint-tree-n100.bin"), 100, true);

		final int MAX = (int) (randomintFile.length() / Integer.BYTES);
		System.out.println("MAX : " + MAX);

		try (final IO.Closer CLOSER = new IO.Closer()) {
			final BPlusTree tree = CLOSER.register(new BPlusTree(file, //
					true, //
					Storage.TYPE_DEFAULT, //
					(1024 * 1024), //
					(1024), //
					new Comparator<Long>() {
						@Override
						public int compare(Long o1, Long o2) {
							return Long.compare(o1, o2);
						}
					}, WAL.NONE));

			// tree.DEBUG_KEY = 86;

			final IO.StopWatch watch = new IO.StopWatch();
			long COUNT = 0L;
			try (final InputStream in = new FileInputStream(randomintFile)) {
				final byte[] bytes = new byte[4];
				int BREAK = MAX;
				BREAK = 100;

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
			System.out.println(String.format("%sops, %s", watch.ops(COUNT), IO.StopWatch.humanReadableTime(watch.elapsed())));

			DEBUG_TYPE DEBUG = DEBUG_TYPE.NONE; // ENABLE_KEY_DELETION_RANDOM ENABLE_KEY_DELETION_BY_DIRECTION
			boolean GENERATION = false;

			if (DEBUG == DEBUG_TYPE.ENABLE_KEY_DELETION_RANDOM) {
				// System.out.println(tree.delete(101L));
				if (GENERATION) {
					final Integer[] a = new Integer[MAX];
					for (int i = 1; i <= 100; i++)
						a[i - 1] = i;
					shuffle(a);
					System.out.println("int[] array = new int[] " + Arrays.toString(a).replace("[", "{").replace("]", "};"));
				}
				int[] array = new int[] { //
						65, 99, 78, 77, 28, 32, 26, 3, 47, 92, //
						40, 84, 7, 1, 33, 43, 39, 62, 5, 59, // 1x
						44, 35, 67, 61, 96, 45, 83, 51, 75, 73, // 2x
						10, 79, 91, 9, 85, 93, 34, 81, 20, 82, // 3x
						22, 57, 66, 49, 29, 48, 72, 6, 88, 100, // 4x
						14, 11, 71, 25, 27, 70, 97, 60, 30, 36, // 5x
						53, 58, 98, 63, 17, 94, 50, 46, 42, 31, // 6x
						37, 64, 4, 89, 52, 95, 12, 55, 68, 54, // 7x
						56, 80, 21, 87, 69, 18, 8, 76, 74, 24,  // 8x
						2, 13, 90, 16, 86, 23, 41, 15, 38, 19  // 9x
				};
				long key = -1L;
				// 20 32 34 *53 64, 67, 68
				for (int i = 0; i < 85; i++) {
					key = (long) array[i];
					// System.out.println("delete(" + key);
					if (!tree.delete(key)) {
						System.err.println("i : " + i + ", key : " + key);
						break;
					}
					// System.out.println("delete(" + key);
				}

				// if (!tree.delete((key = 19L)))
				// System.err.println("key : " + key);
				// if (!tree.delete((key = 38L)))
				// System.err.println("key : " + key);

				System.out.println("delete(" + key);// for (key = 1; key <= 9; key++) {

			} else if (DEBUG == DEBUG_TYPE.ENABLE_KEY_DELETION_FORWARD) {
				long key = -1;
				for (key = 1L; key <= 98; key++) {
					if (!tree.delete(key)) {
						System.err.println("key : " + key);
						break;
					}
				}
				System.out.println("delete(" + key);
			} else if (DEBUG == DEBUG_TYPE.ENABLE_KEY_DELETION_BACKWARD) {
				long key = -1;
				for (key = 100L; key >= 8; key--) {
					// System.out.println("delete(" + key);
					tree.delete(key);
				}
				System.out.println("delete(" + key);
			} else if (DEBUG == DEBUG_TYPE.ENABLE_KEY_DELETION_BY_DIRECTION) {
				long key = -1L;
				for (key = 1L; key <= 7L; key++) {
					// System.out.println("delete(" + key);
					boolean ok = tree.delete(key);
					if (!ok)
						System.err.println("" + key);
				}
				for (key = 8L; key <= 13L; key++) {
					// System.out.println("delete(" + key);
					boolean ok = tree.delete(key);
					if (!ok)
						System.err.println("" + key);
				}
				for (key = 14L; key <= 46L; key++) {
					// System.out.println("delete(" + key);
					boolean ok = tree.delete(key);
					if (!ok)
						System.err.println("" + key);
				}

				for (key = 100L; key >= 88; key--) {
					// System.out.println("delete(" + key);
					boolean ok = tree.delete(key);
					if (!ok)
						System.err.println("" + key);
				}
				for (key = 87L; key >= 48; key--) {
					// System.out.println("delete(" + key);
					boolean ok = tree.delete(key);
					if (!ok)
						System.err.println("" + key);
				}
			}

			open(tree);
		}
	}

	public static void main(String[] args) throws Exception {
		final File file = new File("temp/sampledb.i.primary");

		try (final IO.Closer CLOSER = new IO.Closer()) {
			final BPlusTree tree = CLOSER.register(new BPlusTree(file, //
					true, //
					Storage.TYPE_DEFAULT, //
					(1024 * 1024), //
					(1024), //
					new Comparator<Long>() {
						@Override
						public int compare(Long o1, Long o2) {
							return Long.compare(o1, o2);
						}
					}, WAL.NONE));
			open(tree);
		}
	}
}
