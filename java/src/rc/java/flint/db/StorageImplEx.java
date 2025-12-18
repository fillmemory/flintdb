package flint.db;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.zip.Deflater;
import java.util.zip.Inflater;


abstract class AbstractCompressionStorage implements Storage {
    final MMAPStorage mmap;
    final Options options;

    AbstractCompressionStorage(final Options options) throws IOException {
        this.mmap = new MMAPStorage(options);
        this.options = options;
    }

    @Override
    public void close() throws IOException {
        mmap.close();
    }

    abstract IoBuffer deflate(final IoBuffer bb) throws IOException;
    abstract IoBuffer inflate(final IoBuffer bb) throws IOException;

    @Override
    public long write(final IoBuffer bb) throws IOException {
        final IoBuffer output = deflate(bb);
        return mmap.write(output);
    }

    @Override
    public void write(final long index, final IoBuffer bb) throws IOException {
        final IoBuffer output = deflate(bb);
        mmap.write(index, output);
    }

    @Override
    public IoBuffer read(final long index) throws IOException {
        final IoBuffer input = mmap.read(index);
        return inflate(input);
    }

    @Override
    public boolean delete(final long index) throws IOException {
        return mmap.delete(index);
    }

    @Override
    public InputStream readAsStream(final long index) throws IOException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public long writeAsStream(final InputStream stream) throws IOException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void writeAsStream(final long index, final InputStream stream) throws IOException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public long count() throws IOException {
        return mmap.count();
    }

    @Override
    public long bytes() {
        return mmap.bytes();
    }

    @Override
    public IoBuffer head(final int size) throws IOException {
        return mmap.head(size);
    }

    @Override
    public IoBuffer head(final int offset, int size) throws IOException {
        return mmap.head(offset, size);
    }

    @Override
    public void lock() throws IOException {
        mmap.lock();
    }

    @Override
    public short version() {
        return mmap.version();
    }

    @Override
    public void status(final PrintStream out) throws IOException {
        mmap.status(out);
    }

    @Override
    public boolean readOnly() {
        return mmap.readOnly();
    }
}


final class ZStreamStorage extends AbstractCompressionStorage {
    final boolean nowrap = true;
    final byte[] dictionary;

    ZStreamStorage(final Options options) throws IOException {
        super(options);
        this.dictionary = options.dictionary != null ? dictionary(options.dictionary) : null;
    }

    private static byte[] dictionary(final File f) throws IOException {
        if (!f.exists())
            throw new java.io.FileNotFoundException();
        int remains = (int) f.length();
        if (remains <= 0)
            throw new java.io.IOException("The file '" + f + "' size must be greater than 0 byte");
        if (remains > 32768)
            throw new java.io.IOException("The file '" + f + "' size must be lesser than 32768 bytes");

        int offset = 0;
        final byte[] a = new byte[remains];
        try (final java.io.InputStream in = new java.io.FileInputStream(f)) {
            for (int n = 0; (n = in.read(a, offset, remains)) > -1;) {
                offset += n;
                remains -= n;
            }
        }
        return a;
    }

    @Override
    public void close() throws IOException {
        mmap.close();
    }

    final Deflater deflater = newDeflater();
    final Inflater inflater = newInflater();

    private Deflater newDeflater() {
        final Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, nowrap);
        if (dictionary != null) {
            deflater.setDictionary(dictionary);
        }
        return deflater;
    }

    private Inflater newInflater() {
        final Inflater inflater = new Inflater(nowrap);
        if (dictionary != null) {
            inflater.setDictionary(dictionary);
        }
        return inflater;
    }

    @Override
    IoBuffer deflate(final IoBuffer bb) throws IOException {
        final IoBuffer output = IoBuffer.allocate((int) (bb.remaining() * 1.5));
        deflater.setInput(bb.unwrap());
        deflater.finish();
        final int n = deflater.deflate(output.unwrap(), Deflater.FULL_FLUSH);
        deflater.end();
        assert n > 0;
        output.flip();
        return output;
    }

    @Override
    IoBuffer inflate(final IoBuffer bb) throws IOException {
        final IoBuffer output = IoBuffer.allocate((int) (bb.remaining() * 1.5));
        inflater.setDictionary(dictionary);
        inflater.setInput(bb.unwrap());
        try {
            final int n = inflater.inflate(output.unwrap());
            assert n > 0;
            inflater.end();
            output.flip();
        } catch (java.util.zip.DataFormatException ex) {
            throw new IOException("DataFormatException during inflate", ex);
        }
        return output;
    }
}


final class SnappyStorage extends AbstractCompressionStorage {
    SnappyStorage(Options options) throws IOException {
        super(options);
    }

    @Override
    IoBuffer deflate(final IoBuffer bb) throws IOException {
		final int BUFSZ = (int) Math.max(128, bb.remaining() * 0.8);
        try (var temp = new java.io.ByteArrayOutputStream(BUFSZ)) {
            try (var out = new org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream(temp)) {
                out.write(bb.slice().array());
            }
            return IoBuffer.wrap(temp.toByteArray());
        }
    }

    @Override
    IoBuffer inflate(final IoBuffer bb) throws IOException {
        try (var temp = new java.io.ByteArrayInputStream(bb.array())) {
            try (final org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream input = new org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream(temp)) {
                return IoBuffer.wrap(input.readAllBytes());
            }
        }
    }
}

final class LZ4Storage extends AbstractCompressionStorage {
    LZ4Storage(Options options) throws IOException {
        super(options);
    }

    @Override
    IoBuffer deflate(IoBuffer bb) throws IOException {
		final int BUFSZ = (int) Math.max(128, bb.remaining() * 0.8);
        try (var temp = new java.io.ByteArrayOutputStream(BUFSZ)) {
            try (final org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream out = new org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream(temp)) {
                out.write(bb.slice().array());
            }
            return IoBuffer.wrap(temp.toByteArray());
        }
    }

    @Override
    IoBuffer inflate(IoBuffer bb) throws IOException {
        try (var temp = new java.io.ByteArrayInputStream(bb.array())) {
            try (final org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream input = new org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream(temp)) {
                return IoBuffer.wrap(input.readAllBytes());
            }
        }
    }
}

final class ZSTDStorage extends AbstractCompressionStorage {
    ZSTDStorage(Options options) throws IOException {
        super(options);
    }

    @Override
    IoBuffer deflate(IoBuffer bb) throws IOException {
		final int BUFSZ = (int) Math.max(128, bb.remaining() * 0.8);
        try (var temp = new java.io.ByteArrayOutputStream(BUFSZ)) {
            try (final org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream out = new org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream(temp)) {
                out.write(bb.slice().array());
            }
            return IoBuffer.wrap(temp.toByteArray());
        }
    }

    @Override
    IoBuffer inflate(IoBuffer bb) throws IOException {
        try (var temp = new java.io.ByteArrayInputStream(bb.array())) {
            try (final org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream input = new org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream(temp)) {
                return IoBuffer.wrap(input.readAllBytes());
            }
        }
    }
}