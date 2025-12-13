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
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 *
 */
public class TestcaseHashFile {
	static final File randomintFile = new File("temp/randomint.bin");

	/**
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// testcase_join();
		// testcase_HashKey();

		// make_random_file(100);
		testcase_random_input_HashKey();
	}

	static void make_random_file(final int sz) throws Exception {
		final Integer[] a = new Integer[sz];
		for (int i = 0; i < sz; i++) {
			a[i] = i + 1;
		}
		shuffle(a);

		try (final OutputStream out = new FileOutputStream(randomintFile)) {
			for (final Integer i : a) {
                ByteBuffer bb = ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN);
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

	static void testcase_HashKey() throws Exception {
		final File file = new File("temp/hash.hkey");
		file.delete();

		final HashFile hash = new HashFile(file, (16), true, (1024 * 1024), new HashFile.HashFunction() {
			@Override
			public int hash(long v) {
				return 0;
			}

			@Override
			public int compare(Long o1, Long o2) {
				return Long.compare(o1, o2);
			}
		});

		final long MAX = 1000;
		for (long i = 1; i <= MAX; i++) {
			hash.put(i);
		}

		// System.out.println("found : " + hash.find(599L));
		for (long i = 1; i <= MAX; i++) {
			final Object found = hash.find(i);
			if (found == null) {
				System.out.println("missing  => " + i);
				break;
			} else {
				// System.out.println("found => " + found);
			}
		}

		hash.close();
	}

	static void testcase_random_input_HashKey() throws Exception {
		// randomintFile.delete();
		if (!randomintFile.exists())
			make_random_file(1000);

		final File file = new File("temp/hash.hkey");
		file.delete();

		final HashFile hash = new HashFile(file, (16), true, (1024 * 1024), new HashFile.HashFunction() {
			@Override
			public int hash(long v) {
				return 0;
			}

			@Override
			public int compare(Long o1, Long o2) {
				return Long.compare(o1, o2);
			}
		});

		IO.StopWatch watch = new IO.StopWatch();
		long MAX = 0L;
		try (final InputStream in = new FileInputStream(randomintFile)) {
			final byte[] bytes = new byte[4];
			final StringBuilder DEBUG = new StringBuilder("put => ");
			for (int i = 1; i <= 100000; i++) {
				if (in.read(bytes) <= 0)
					break;
				if (i < 0)
					continue;
                
                ByteBuffer bb = ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);
				final long key = bb.getInt() & 0xFFFFFFFFL;
				DEBUG.append(key + ", ");
				hash.put(key);
				MAX++;
				// if (key == 51)
				// hash.trace(String.format("%03d", key));
				if (key == 84) {
					// hash.trace(String.format("%03d", key));
					// break;
				}
			}
			// System.out.println(DEBUG);
		}

		hash.put(0L);
		System.out.println(String.format("%sops, %s", watch.ops(MAX), IO.StopWatch.humanReadableTime(watch.elapsed())));

		hash.trace("TRACE");
		System.out.println("COUNT : " + hash.count());

		for (long i = 1; i <= MAX; i++) {
			final Object found = hash.find(i);
			if (found == null) {
				System.err.println("missing => " + i);
				break;
			} else {
				// System.out.println("found => " + found);
			}
		}

		hash.traverse(new Consumer<Long>() {
			int i = 0;

			@Override
			public void accept(Long key) {
				if (i % 30 == 0)
					System.out.println();
				System.out.print(String.format("%03d ", key));

				i++;
			}
		});

		hash.close();
	}

	static void testcase_join() {
		final int ARRAY_COUNT_MAX = 3;

		final Runnable[] testcases = new Runnable[] { //
				new Runnable() {
					@Override
					public void run() {
						long[] source = new long[] { 19, 86 };
						long[] target = new long[source.length + 1];

						int offset = 1;
						int d = -1;
						long key = 67;

						long[] r = HashFile.join(ARRAY_COUNT_MAX, source, target, offset, d, key);

						System.out.println("target : " + Arrays.toString(target));
						if (r != null)
							System.out.println("split : " + Arrays.toString(r));

					}

				}, //
				new Runnable() {
					@Override
					public void run() {
						long[] source = new long[] { 19, 67, 86 };
						long[] target = new long[ARRAY_COUNT_MAX];

						int offset = 1;
						int d = 1;
						long key = 77;

						long[] r = HashFile.join(ARRAY_COUNT_MAX, source, target, offset, d, key);

						System.out.println("target : " + Arrays.toString(target));
						if (r != null)
							System.out.println("split : " + Arrays.toString(r));
					}
				}, //
				new Runnable() {
					@Override
					public void run() {
						long[] source = new long[] { 19, 67, 86 };
						long[] target = new long[ARRAY_COUNT_MAX];

						int offset = 0;
						int d = -1;
						long key = 1;

						long[] r = HashFile.join(ARRAY_COUNT_MAX, source, target, offset, d, key);

						System.out.println("target : " + Arrays.toString(target));
						if (r != null)
							System.out.println("split : " + Arrays.toString(r));
					}
				}, //
				new Runnable() {
					@Override
					public void run() {
						long[] source = new long[] { 19, 67, 86 };
						long[] target = new long[ARRAY_COUNT_MAX];

						int offset = 2;
						int d = 1;
						long key = 99;

						long[] r = HashFile.join(ARRAY_COUNT_MAX, source, target, offset, d, key);

						System.out.println("target : " + Arrays.toString(target));
						if (r != null)
							System.out.println("split : " + Arrays.toString(r));
					}
				}, //
				new Runnable() {
					@Override
					public void run() {
						long[] source = new long[] { 656, 894 };
						long[] target = new long[ARRAY_COUNT_MAX];

						int offset = 0;
						int d = -1;
						long key = 643;

						long[] r = HashFile.join(ARRAY_COUNT_MAX, source, target, offset, d, key);

						System.out.println("target : " + Arrays.toString(target));
						if (r != null)
							System.out.println("split : " + Arrays.toString(r));
					}
				}, //
				new Runnable() {
					@Override
					public void run() {
						long[] source = new long[] { 643, 656, 894 };
						long[] target = new long[ARRAY_COUNT_MAX];

						int offset = 2;
						int d = 1;
						long key = 999;

						long[] r = HashFile.join(ARRAY_COUNT_MAX, source, target, offset, d, key);

						System.out.println("target : " + Arrays.toString(target));
						if (r != null)
							System.out.println("split : " + Arrays.toString(r));
					}
				}, //
		};

		testcases[5].run();
	}

}
