/**
 * Storage.java
 */
package flint.db;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Paths;

/**
 * Storage
 * 
 * <pre>
FileFormat
16384B Custom Header (16384 - 64)
   8B Reserved (Block Count currently not used)
   8B The front of deleted blocks
   8B Reserved
   2B FileFormat Version ( 0 : Default, 1 : MMAP )
  28B Reserved
   2B Block Data Max Size (exclude Block Header)
   8B Data count
   +B Extra Header
      Block,....

BlockFormat
   1B Status ('+' : data exists, '-' : empty)
   1B Mark 'D' : 1st block data, 'N' : 1+n block data, 'X' : empty
   2B Data Length   (The current data length of this block)
   4B Total Length  (If data is overflowed then combine multiple block)
   8B Next ( -1 : No More, 0 >= Next block link )
      Data
      
- Index : The first block starts with 0

- Deletion
1. Move the indexed block
2. Set 0 to block header status 0
3. Set 0 to data and total length fields
4. If the next field != -1 then move to the next index block
5. Clean up with 0 to the data bytes
6. Last free block의 Next 에 삭제된 현재의 인덱스를 설정하고, 현재의 최종 Next를 Last free block에 업데이트 (링크드 리스트)

- Insertion
1. First free block 부터 추가 시작 (block이동)
2. Block Header Status '#' 으로 설정
3. Data + Total Length 를 설정
4. Next에는 오버플로우가 일어난 경우 다음 block 인덱스를 설정, 아닌경우 -1을 설정
5. Data 복사
6. First free block을 변경

- Modification
1. 기존의 데이터 보다 작은 경우 => 유지
2. 기존의 데이터 보다 큰 경우 => 기존의 block들을 모두 업데이트하고, 데이터 추가 로직과 동일
 * </pre>
 */
public interface Storage extends Closeable {

	public static Storage create(final Options options) throws IOException {
		switch (options.storage) {
		case TYPE_V1:
			return new MMAPStorage(options);
		case TYPE_MEMORY:
			return new MemoryStorage(options);
		// case TYPE_V2:
		// 	return new V2Storage(options);
		//#ifndef EXCLUDE (Do not remove this block. Internal use)
		case TYPE_Z:
			return new ZStreamStorage(options);
		case TYPE_LZ4:
			return new LZ4Storage(options);
		case TYPE_ZSTD:
			return new ZSTDStorage(options);
		case TYPE_SNAPPY:
			return new SnappyStorage(options);
		//#endif
		}
		throw new RuntimeException("STORAGE.TYPE : " + options.storage);
	}

	static void validate(final Options options) throws IOException {
		if (!options.mutable && !options.file.exists())
			throw new java.io.FileNotFoundException("file not found : " + options.file);
	}

	static boolean supported(final String type) {
		return TYPE_V1.equals(type) 
		|| TYPE_MEMORY.equals(type)
		// || TYPE_V2.equals(type)
		|| compressed(type);
	}

	static boolean compressed(final String compress) {
		return TYPE_Z.equals(compress) 
		|| TYPE_LZ4.equals(compress)
		|| TYPE_ZSTD.equals(compress) 
		|| TYPE_SNAPPY.equals(compress);
	}

	static final String TYPE_V1 = "MMAP";
	static final String TYPE_V2 = "V2"; // reserved for future use

	static final String TYPE_Z = "Z";
	static final String TYPE_LZ4 = "LZ4";
	static final String TYPE_ZSTD = "ZSTD";
	static final String TYPE_SNAPPY = "SNAPPY";

	static final String TYPE_MEMORY = "MEMORY"; // XX:MaxDirectMemorySize=20G
	static final String TYPE_DEFAULT = TYPE_V1;

	static final int HEADER_BYTES = (4096); // getpagesize()
	static final int COMMON_HEADER_BYTES = (8 + 8 + 8 + 2 + 4 + 24 + 2 + 8);
	static final int CUSTOM_HEADER_BYTES = (HEADER_BYTES - COMMON_HEADER_BYTES);
	static final int BLOCK_HEADER_BYTES = (1 + 1 + 2 + 4 + 8);
	static final int DEFAULT_INCREMENT_BYTES = 16 * 1024 * 1024;

	/**
	 * Options for configuring the storage.
	 */
	public static final class Options {
		File file;
		boolean mutable = true;
		boolean lock = false;
		short BLOCK_BYTES;
		int EXTRA_HEADER_BYTES = 0;
		int compact = -1;
		int increment = DEFAULT_INCREMENT_BYTES;
		String storage = TYPE_DEFAULT;
		String compress = null;
		File dictionary; // reserved for GZIP

		public Options file(File file) {
			this.file = file;
			if (file.getName().toUpperCase().startsWith("@MEMORY"))
				this.storage = TYPE_MEMORY;
			return this;
		}

		public Options compact(int compact) {
			this.compact = compact;
			return this;
		}

		public Options increment(int increment) {
			this.increment = increment;
			return this;
		}

		public Options storage(String storage) {
			this.storage = storage;
			return this;
		}

		public Options mutable(boolean mutable) {
			this.mutable = mutable;
			return this;
		}

		public Options lock(boolean lock) {
			this.lock = lock;
			return this;
		}

		public Options extraHeaderBytes(int EXTRA_HEADER_BYTES) {
			this.EXTRA_HEADER_BYTES = EXTRA_HEADER_BYTES;
			return this;
		}

		public Options blockBytes(short BLOCK_BYTES) {
			this.BLOCK_BYTES = BLOCK_BYTES;
			return this;
		}

		public Options compress(String compress) {
			this.compress = compressed(compress) ? compress : null;
			if (this.compress != null) {
				if (TYPE_Z.equals(compress))
					this.storage = TYPE_Z;
				else if (TYPE_LZ4.equals(compress))
					this.storage = TYPE_LZ4;
				else if (TYPE_ZSTD.equals(compress))
					this.storage = TYPE_ZSTD;
				else if (TYPE_SNAPPY.equals(compress))
					this.storage = TYPE_SNAPPY;
			}
			return this;
		}

		public Options dictionary(File dictionary) {
			this.dictionary = dictionary;
			return this;
		}
	}

	long write(final IoBuffer bb) throws IOException;

	void write(final long index, final IoBuffer bb) throws IOException;

	IoBuffer read(final long index) throws IOException;

	boolean delete(final long index) throws IOException;

	InputStream readAsStream(final long index) throws IOException;

	long writeAsStream(final InputStream stream) throws IOException;

	void writeAsStream(final long index, final InputStream stream) throws IOException;

	long count() throws IOException;

	long bytes();

	IoBuffer head(final int size) throws IOException;

	IoBuffer head(final int offset, int size) throws IOException;

	void lock() throws IOException;

	short version();

	void status(final PrintStream out) throws IOException;

	boolean readOnly();


    default void transaction(long id) {
        // do nothing
        // reserved for WAL
    }

	static FileLock lock(final FileChannel ch, final int timeout) throws IOException, InterruptedException {
		FileLock lock = null;
		final long limit = (System.currentTimeMillis() + timeout);
		while (System.currentTimeMillis() <= limit) {
			lock = ch.tryLock();
			if (lock != null)
				break;
			Thread.sleep(10);
		}
		return lock;
	}

	static int version(final File file) {
		if (file.exists() && file.length() >= CUSTOM_HEADER_BYTES) {
			try (final FileChannel ch = FileChannel.open(Paths.get(file.toURI()), java.nio.file.StandardOpenOption.READ)) {
				final IoBuffer bb = IoBuffer.allocate(COMMON_HEADER_BYTES);
				readFully(ch, bb, CUSTOM_HEADER_BYTES);
				bb.flip();

				bb.getLong(); // block count
				bb.getLong(); // front
				bb.getLong(); // tail
				final short v = bb.getShort(); // version
				return v == 8224 ? 0 : v;
			} catch (IOException ex) {
				ex.printStackTrace();
				return -1;
			}
		}
		return 1;
	}

	// static void writeFully(final FileChannel channel, final IoBuffer bb, final long position) throws IOException {
	// for (long offset = position; bb.remaining() > 0;) {
	// final int n = channel.write(bb, offset);
	// // System.err.println("n : " + n + ", " + bb.remaining() + ", " + channel);
	// if (n == -1)
	// break;
	// offset += n;
	// }
	// }

	static void readFully(final FileChannel channel, final IoBuffer bb, final long position) throws IOException {
		int n = 0;
		long offset = position;
		for (int sz = bb.capacity(); n < sz;) {
			n = channel.read(bb.unwrap(), offset);
			if (n == -1)
				break;
			offset += n;
		}
	}
}
