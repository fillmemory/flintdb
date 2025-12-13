/**
 * 
 */
package flint.db;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public final class Shuffle {

	static void PRINT(final String fmt, final Object... argv) {
		final String d = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
		final String s = String.format(fmt, argv);
		System.out.println("LOG : " + d + " " + s);
	}

	static void make(final File file, final int sz, final float shuffle) throws Exception {
		final IO.StopWatch watch = new IO.StopWatch();

		final Set<java.nio.file.OpenOption> opt = new java.util.HashSet<>();
		opt.add(java.nio.file.StandardOpenOption.CREATE);
		opt.add(java.nio.file.StandardOpenOption.READ);
		opt.add(java.nio.file.StandardOpenOption.WRITE);
		opt.add(java.nio.file.StandardOpenOption.SYNC);

		file.delete();
		try (final FileChannel ch = (FileChannel) Files.newByteChannel(Paths.get(file.toURI()), opt)) {
			final ByteBuffer bb = ByteBuffer.allocate(4096);
			for (int i = 0; i < sz; i++) {
				bb.putInt(i + 1);
				if (bb.remaining() == 0) {
					bb.flip();
					ch.write(bb);
					bb.rewind();
				}
			}
			bb.flip();
			ch.write(bb);
			bb.rewind();

			if (shuffle > 0.0f) {
				PRINT("shuffle");
				shuffle(ch, sz, shuffle);
			}
		}
		PRINT("make %sBytes, time : %s", //
				IO.readableBytesSize(file.length()), //
				IO.StopWatch.humanReadableTime(watch.elapsed()) //
		);
	}

	private static void shuffle(final FileChannel ch, final int sz, final float shuffle) throws IOException {
		final Random rnd = ThreadLocalRandom.current();

		final int end = (int) ((sz) * Math.min(shuffle, 1.0f));
		for (int i = end - 1; i > 0; i--) {
			final int j = rnd.nextInt(i + 1);

			final ByteBuffer v1 = ByteBuffer.allocate(Integer.BYTES);
			final ByteBuffer v2 = ByteBuffer.allocate(Integer.BYTES);

			ch.read(v1, j * Integer.BYTES);
			ch.read(v2, i * Integer.BYTES);

			v1.flip();
			v2.flip();

			ch.write(v2, j * Integer.BYTES);
			ch.write(v1, i * Integer.BYTES);
		}
	}

	public static void main(String[] args) throws Exception {
		make(new File("temp/shuffle.bin"), 1024 * 1024, 1.0f);
	}
}
