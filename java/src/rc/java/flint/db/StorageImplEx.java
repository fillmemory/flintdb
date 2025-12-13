package flint.db;

import java.io.IOException;



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