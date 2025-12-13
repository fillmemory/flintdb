package flint.db;

import java.io.File;

public final class StoragePerf {
    public static void main(String[] args) throws Exception {
        final File file = new File("temp/storage-j.bin");
        file.getParentFile().mkdirs();
        file.delete();

        try (final IO.Closer closer = new IO.Closer()) {
            final IO.StopWatch watch = new IO.StopWatch();
            final Storage s = Storage.create(new Storage.Options()
                    .file(file)
                    .blockBytes((short) (512 - 16))
                    .mutable(true));
            closer.register(s);

            final int max = 2 * 1024 * 1024;
            for (int i = 0; i < max; i++) {
                final byte[] bytes = String.format("Hello, FlintDB! %03d", i).getBytes();
                final IoBuffer bb = IoBuffer.wrap(bytes);
                s.write(bb);
            }

            for (int i = max - 10; i < max; i++) {
                final IoBuffer bb = s.read(i);
                if (bb == null) {
                    System.out.println("read null at index: " + i);
                    continue;
                }
                final byte[] data = new byte[bb.remaining()];
                bb.get(data, 0, data.length);
                System.out.println("read at index: " + i + ", " + new String(data));
            }

            System.out.println("elapsed: " + watch.elapsed() + ", ops: " + watch.ops(max));
            System.out.println("count: " + s.count() + ", bytes: " + s.bytes());
        }
    }
}
