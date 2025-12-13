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

		testcase_storage1();
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


	static void testcase_storage1() throws Exception {
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

			int max = 1_000_000;
			for(int i=0; i<max; i++) {
				var bb = IoBuffer.wrap(String.format("Hello, FlintDB! %03d", i).getBytes());
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
}
