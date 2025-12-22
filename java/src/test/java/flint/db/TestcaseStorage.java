/**
 * 
 */
package flint.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class TestcaseStorage {

	public static void main(String[] args) throws Exception {
		//testcase_mmap3_full();
		//testcase_mmap3_stress();

		// Usage examples:
		//   ./testcase.sh -c flint.db.TestcaseStorage
		//   ./testcase.sh -c flint.db.TestcaseStorage 2000000
		//   ./testcase.sh -c flint.db.TestcaseStorage boundary
		//   ./testcase.sh -c flint.db.TestcaseStorage boundary 2097152
		//   ./testcase.sh -c flint.db.TestcaseStorage boundary-mem 2097152
		//   ./testcase.sh -c flint.db.TestcaseStorage boundary-all 2097152
		String a0 = null;
		String a1 = null;
		if (args != null && args.length > 0) {
			// java/testcase.sh forwards all CLI args to main(), including its own flags.
			for (int i = 0; i < args.length; i++) {
				final String a = args[i];
				if (a == null) continue;
				if ("--build".equals(a)) continue;
				if ("-c".equals(a)) { // skip class name
					i++;
					continue;
				}
				if (a.startsWith("-")) continue;
				if (a0 == null) {
					a0 = a;
				} else if (a1 == null) {
					a1 = a;
					break;
				}
			}
		}

		if (a0 != null) {
			if ("boundary".equalsIgnoreCase(a0) || "boundary-mem".equalsIgnoreCase(a0) || "boundary-all".equalsIgnoreCase(a0)) {
				final boolean runAll = "boundary-all".equalsIgnoreCase(a0);
				final boolean useMemory = "boundary-mem".equalsIgnoreCase(a0);
				int max = 2 * 1024 * 1024;
				if (a1 != null) {
					max = Integer.parseInt(a1);
				}
				if (runAll) {
					testcase_storage_boundary(max, false);
					testcase_storage_boundary(max, true);
					return;
				}
				testcase_storage_boundary(max, useMemory);
				return;
			}
			// Backward-compatible: treat single numeric arg as max for testcase_storage1.
			int max = Integer.parseInt(a0);
			testcase_storage1(max);
			return;
		}

		testcase_storage1(1_000_000);
	}

	public static void testcase_mmap3_stress() throws Exception {
		final File file = new File("temp/test-mmap.storage");
		file.getParentFile().mkdirs();
		file.delete();

		final int MMAP_BYTES = 16 * 1024 * 1024; 
		final int BLOCK_BYTES = 1024;
		final int COMPACT_BYTES = -1; // BLOCK_BYTES / 2;
		final Storage storage = Storage.create( //
				new Storage.Options() //
						.file(file) //
						.compact(COMPACT_BYTES) //
						.blockBytes((short) BLOCK_BYTES) //
						.increment(MMAP_BYTES) //
						.mutable(true) //
		);

		final IO.StopWatch watch = new IO.StopWatch();

		final int DEFAULT_COUNT = 2 * 1024 * 1024;
		final IoBuffer b1 = IoBuffer.allocate(BLOCK_BYTES);
		for (int i = 1; i <= DEFAULT_COUNT; i++) {
			b1.rewind();
			b1.put(String.format("%0150dz", i).getBytes());
			b1.flip();
			storage.write(b1);
		}

		System.out.println(watch.elapsed() + "ms" + ", " + IO.StopWatch.humanReadableTime(watch.elapsed()) + ", " + watch.ops(DEFAULT_COUNT) + "ops");
		storage.status(System.out);
		storage.close();
	}

	public static void testcase_mmap3_full() throws Exception {
		final File file = new File("temp/test-mmap.storage");
		file.getParentFile().mkdirs();
		file.delete();

		final int MMAP_BYTES = 1 * 1024; 
		final int BLOCK_BYTES = 32;
		final int COMPACT_BYTES = BLOCK_BYTES / 2;
		final Storage storage = Storage.create( //
				new Storage.Options() //
						.file(file) //
						.compact(COMPACT_BYTES) //
						.blockBytes((short) BLOCK_BYTES) //
						.increment(MMAP_BYTES) //
						.mutable(true) //
		);

		IO.StopWatch watch = new IO.StopWatch();

		storage.read(0);
		// storage.read(8); // mmap 256
		// storage.read(16); // mmap 256
		// storage.read((MMAP_BYTES / 16) + 1); // mmap 20M

		System.out.println(watch.elapsed() + "ms" + ", " + IO.StopWatch.humanReadableTime(watch.elapsed()));

		storage.status(System.out);

		final int DEFAULT_COUNT = 10;
		for (int i = 1; i <= DEFAULT_COUNT; i++) {
			IoBuffer b1 = IoBuffer.allocate(4096);
			for (int x = 'A'; x <= 'Z'; x++) {
				char chr = (char) x;
				b1.put(String.format(chr + "%04dz", i).getBytes());
			}
			b1.flip();
			System.err.println("storage.write : " + b1.remaining() + ", " + (int) (Math.ceil(1f * b1.remaining() / COMPACT_BYTES)));
			storage.write(b1);
		}
		storage.status(System.out);

		boolean ok = true;
		if (ok) {
			final int i = 0;
			System.err.println("storage.stream : " + toString(storage.readAsStream(i)));

			IoBuffer bb = storage.read(i);
			byte[] a = new byte[bb.remaining()];
			bb.get(a);
			System.err.println("storage.read   : " + new String(a));
		}

		if (ok && DEFAULT_COUNT > 1) {
			final int i = 1;
			String s = "#1234567890";
			for (int x = 'A'; x <= 'Z'; x++) {
				char chr = (char) x;
				s += chr;
			}

			System.out.println("writeAsStream(" + s);
			storage.writeAsStream(i, new ByteArrayInputStream(s.getBytes()));
			IoBuffer bb = storage.read(i);
			byte[] a = new byte[bb.remaining()];
			bb.get(a);
			System.err.println("storage.read   : " + new String(a));
		}

		if (ok) { // update
			int i = 0;

			//
			IoBuffer b1 = IoBuffer.allocate(4096);
			for (int x = 'a'; x <= 'h'; x++) {
				char chr = (char) x;
				b1.put(String.format(chr + "%04dZ", i).getBytes());
			}
			b1.flip();
			System.err.println("storage.write : " + b1.remaining() + ", " + (int) (Math.ceil(1f * b1.remaining() / COMPACT_BYTES)));
			storage.write(i, b1);

			IoBuffer bb = storage.read(i);
			byte[] a = new byte[bb.remaining()];
			bb.get(a);
			System.err.println("storage.read : " + new String(a));
		}
		storage.status(System.out);

		for (int i = 0; i < 5; i++) {
			ok = storage.delete(i);
			System.err.println("storage.delete : " + i + ", " + ok);
		}

		for (int i = 1; i <= 10; i++) {
			IoBuffer b1 = IoBuffer.allocate(4096);
			for (int x = 'B'; x <= 'B'; x++) {
				char chr = (char) x;
				b1.put(String.format(chr + "%04dz", i).getBytes());
			}
			b1.flip();
			// System.err.println("storage.write : " + b1.remaining());
			storage.write(b1);
		}
		storage.status(System.out);

		IoBuffer b2 = IoBuffer.allocate(4096);
		b2.put(String.format("C%04d", 2).getBytes());
		b2.flip();
		storage.write(b2);

		// storage.delete(9);

		IoBuffer b3 = IoBuffer.allocate(4096);
		b3.put(String.format("D%04d", 3).getBytes());
		b3.flip();
		storage.write(b3);

		storage.status(System.out);
		storage.close();
	}

	private static String toString(InputStream in) throws IOException {
		if (in != null) {
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			byte[] bb = new byte[4096];
			for (int n = 0; (n = in.read(bb)) > -1;) {
				os.write(bb, 0, n);
			}
			return new String(os.toByteArray());
		}
		return null;
	}


	static void testcase_storage1(int max) throws Exception {
		var file = new File("temp/storage-j.bin");
		file.getParentFile().mkdirs();
		file.delete();

		try(var CLOSER = new IO.Closer()) {
			var watch = new IO.StopWatch();
			var s = Storage.create( //
					new Storage.Options() //
							.file(file) //
							.blockBytes((short) 512) //
							.mutable(true) //
			);
			CLOSER.register(s); 

			for(int i=0; i<max; i++) {
				var bb = IoBuffer.wrap(String.format("Hello, FlintDB! %07d", i).getBytes());
				s.write(bb);
			}

			for(int i=max-10; i<max; i++) {
				var bb = s.read(i);
				System.out.println("remaining : " + bb.remaining());
				var data = new byte[bb.remaining()];
				bb.get(data, 0, bb.remaining());
				System.out.println("storage.read : " + i + ", " + new String(data));
			}

			System.out.println("elapsed : " + watch.elapsed() + ", ops : " + watch.ops(max));
			System.out.println("count: " + s.count() + ", bytes: " + s.bytes());
		}
	}

	/**
	 * Boundary testcase to match the C testcase parameters closely:
	 * - data block bytes = 496 (so total block bytes = 512, incl. 16-byte header)
	 * - increment = 16MB
	 *
	 * With max = 2*1024*1024 blocks, total payload aligns with 64 * 16MB chunks.
	 */
	static void testcase_storage_boundary(int max, boolean useMemory) throws Exception {
		final File file = new File(useMemory ? "temp/storage-j-boundary-mem.bin" : "temp/storage-j-boundary.bin");
		file.getParentFile().mkdirs();
		file.delete();

		final int INCREMENT = 16 * 1024 * 1024;
		final short DATA_BLOCK_BYTES = (short) (512 - 16);

		try (var CLOSER = new IO.Closer()) {
			var watch = new IO.StopWatch();
			var opt = new Storage.Options()
					.file(file)
					.blockBytes(DATA_BLOCK_BYTES)
					.increment(INCREMENT)
					.mutable(true);
			if (useMemory) {
				opt.storage(Storage.TYPE_MEMORY);
			}
			var s = Storage.create(opt);
			CLOSER.register(s);

			for (int i = 0; i < max; i++) {
				// Keep per-record size small so it stays single-block.
				var bb = IoBuffer.wrap(String.format("Hello, PRODUCT_NAME! %07d", i + 1).getBytes());
				s.write(bb);
			}

			for (int i = max - 10; i < max; i++) {
				var bb = s.read(i);
				var data = new byte[bb.remaining()];
				bb.get(data, 0, bb.remaining());
				System.out.println("storage.read : " + i + ", " + new String(data));
			}

			final long chunkBytes = 64L * 16L * 1024L * 1024L;
			// Note: MemoryStorage.bytes() returns only allocated chunk bytes (header excluded).
			final long expectedBytes = useMemory ? chunkBytes : (16_384L + chunkBytes);
			System.out.println("elapsed : " + watch.elapsed() + ", ops : " + watch.ops(max));
			System.out.println("count: " + s.count() + ", bytes: " + s.bytes() + ", expected(boundary): " + expectedBytes + ", storage: " + (useMemory ? "MEMORY" : "MMAP"));
		}
	}
}
