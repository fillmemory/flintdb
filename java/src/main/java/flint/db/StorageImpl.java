package flint.db;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * MemoryMappedFile with DirectBufferPool optimization
 */
final class MMAPStorage implements Storage {
    static final int O_SYNC = Integer.parseInt(System.getProperty("STORAGE.ATTR.O_SYNC", "0"));
    static final int O_DIRECT = Integer.parseInt(System.getProperty("STORAGE.ATTR.O_DIRECT", "0"));
    
    private final int EXTRA_HEADER_BYTES;
    private final int BLOCK_BYTES;
    private final int BLOCK_DATA_BYTES;
    private int MMAP_BYTES;
    static final byte STATUS_SET = '+';
    static final byte STATUS_EMPTY = '-';

    static final byte MARK_AS_DATA = 'D';
    static final byte MARK_AS_NEXT = 'N'; // When data is overflowed
    static final byte MARK_AS_UNUSED = 'X'; // When data is deleted or unused

    static final long NEXT_END = -1L;

    private final File file;
    private final FileChannel channel;
    private final IoBuffer HEADER; // 0 .. HEADER_BYTES mapping
    private final IoBuffer cmheader; // CUSTOM_HEADER_BYTES .. COMMON_HEADER_BYTES mapping
    private final Options options;

    // private volatile long blocks = 0;
    private volatile long count = 0;
    private volatile long free = 0;
    private volatile long dirty = 0;
    private final byte[] CLEAN; // filling w/ 0 to deleted block
    static final byte[] R24 = new byte[24]; // reserved 24 bytes in header

    
    // Direct IoBuffer Pool for temporary buffer allocation optimization
    private final DirectBufferPool bufferPool;

    private final Map<Integer, IoBuffer> mbb = new java.util.LinkedHashMap<>(64, 1f, false) {
    	private static final long serialVersionUID = 1L;
    	private final int MAX_CACHE_SIZE = 16;
        
    	@Override
    	protected boolean removeEldestEntry(Map.Entry<Integer, IoBuffer> eldest) {
    		if (size() > MAX_CACHE_SIZE) {
    			eldest.getValue().free();
    			return true;
    		}
    		return false;
    	}
    };

    MMAPStorage(final Options options) throws IOException {
        Storage.validate(options);
        this.file = options.file;
        this.options = options;
        this.EXTRA_HEADER_BYTES = options.EXTRA_HEADER_BYTES;
        this.BLOCK_BYTES = ((short) (BLOCK_HEADER_BYTES + (options.compact <= 0 ? (options.BLOCK_BYTES) : options.compact)));
        this.BLOCK_DATA_BYTES = BLOCK_BYTES - BLOCK_HEADER_BYTES;
        this.MMAP_BYTES = BLOCK_BYTES * (options.increment / BLOCK_BYTES);
        this.CLEAN = new byte[BLOCK_DATA_BYTES];

        // Initialize DirectBufferPool with optimal size for this storage
        this.bufferPool = new DirectBufferPool(BLOCK_BYTES, 64);

        final Set<java.nio.file.OpenOption> opt = new java.util.HashSet<>();
        opt.add(java.nio.file.StandardOpenOption.READ);
        if (options.mutable) {
            opt.add((file.exists() ? java.nio.file.StandardOpenOption.CREATE : java.nio.file.StandardOpenOption.CREATE_NEW));
            opt.add(java.nio.file.StandardOpenOption.WRITE);
            // ISSUE: O_DIRECT + O_SYNC performance is not good
            if (O_SYNC > 0)
                opt.add(java.nio.file.StandardOpenOption.SYNC); // O_SYNC
            // if (O_DIRECT > 0)
            //     opt.add(com.sun.nio.file.ExtendedOpenOption.DIRECT); // O_DIRECT + O_SYNC
        }
        this.channel = (FileChannel) Files.newByteChannel(Paths.get(file.toURI()), opt);

        if (options.mutable && options.lock)
            lock();

        if (channel.size() == 0) {
            HEADER = HEAD();
            cmheader = head(CUSTOM_HEADER_BYTES, COMMON_HEADER_BYTES);
            commit(true);
        } else {
            HEADER = HEAD();
            cmheader = head(CUSTOM_HEADER_BYTES, COMMON_HEADER_BYTES);
            final IoBuffer bb = cmheader.slice();

            /* this.blocks = */ bb.getLong();
            free = (bb.getLong()); // The front of deleted blocks
            bb.getLong(); // The tail of deleted blocks => not used in mmap
            bb.getShort(); // version 1
            int inc = bb.getInt(); // increment chunk size
            if (inc <= 0)
                throw new IOException("Invalid increment size: " + inc + ", " + options.file); // old version inc = 1024 * 1024 * 10;
            if (inc != (options.increment)) {
                options.increment = inc;
                this.MMAP_BYTES = BLOCK_BYTES * (options.increment / BLOCK_BYTES);
            }
            bb.position(bb.position() + R24.length); // reserved
            short blksize = bb.getShort(); // BLOCK Data Max Size (exclude BLOCK Header)
            if (blksize != BLOCK_DATA_BYTES) {
                throw new IOException(String.format("Block size mismatch: header=%d, opts=%d", blksize, BLOCK_DATA_BYTES));
            }
            this.count = bb.getLong();
            assert(this.count > -1L);
        }
    }

    @Override
    public short version() {
        return 1;
    }

    @Override
    public String toString() {
        return file.toString();
    }

    private void commit() throws IOException {
        commit(true);
    }

    private void commit(final boolean force) throws IOException {
        if (dirty < 0 && !force) {
            dirty++;
            return;
        }
        dirty = 0;


        final IoBuffer bb = cmheader.slice();
        bb.putLong(0L); // reserverd bb.putLong(blocks);
        bb.putLong(free); // The front of deleted blocks
        bb.putLong(0); // The tail of deleted blocks => not used in mmap
        bb.putShort(version()); // version 1
        bb.putInt(options.increment); // increment chunk size
        bb.put(R24); // reserved
        bb.putShort((short) BLOCK_DATA_BYTES); // BLOCK Data Max Size (exclude BLOCK Header)
        bb.putLong(count);
    }

    @Override
    public void close() throws IOException {
        // System.out.printf("MMAPStorage.close(): file=%s, count=%d, free=%d \n", file.getAbsolutePath(), count, free);
        if (channel != null && channel.isOpen()) {
            try (channel) {
                if (options.mutable)
                    commit(true);
                final Set<Entry<Integer, IoBuffer>> entrySet = mbb.entrySet();
                for (final Entry<Integer, IoBuffer> e : entrySet) {
                    final IoBuffer buffer = e.getValue();
                    buffer.free();
                }   entrySet.clear();
                HEADER.free();
                // Clear buffer pool
                bufferPool.clear();
                // channel = null;
                // System.err.println("close " + file);
            }
        }
    }

    @Override
    public boolean delete(final long index) throws IOException {
        final IoBuffer p = position(index);
        final IoBuffer c = p.slice();

        final byte status = c.get();
        c.get(); // mark
        c.getShort(); // Data Length
        c.getInt(); // Total Length
        final long next = c.getLong();

        if (STATUS_SET != status)  // empty
            return false;

        p.put(STATUS_EMPTY);
        p.put(MARK_AS_UNUSED);
        p.putShort((short) 0);
        p.putInt(0);
        p.putLong(free);
        p.put(CLEAN);

        free = index;
        count--;
        if (next > NEXT_END)
            delete(next);
        // System.err.println("free2 : " + free + ", next : " + next);
        commit();

        return true;
    }

    // @ForceInline // or -XX:CompileCommand=inline,your/class/Name,multiplyByTwo
    @Override
    public void write(final long index, final IoBuffer buffer) throws IOException {
        write(index, MARK_AS_DATA, buffer);
    }

    private void write(final long index, final byte mark, final IoBuffer buffer) throws IOException {
        final IoBuffer p = position(index);
        final IoBuffer c = p.slice();
        final int sz = buffer.remaining();
        final byte status = c.get();
        c.get();
        c.getShort(); // Data Length
        c.getInt(); // Total Length
        final long next = c.getLong();

        p.put(STATUS_SET);
        p.put(mark);
        p.putShort((short) Math.min(sz, BLOCK_DATA_BYTES));
        p.putInt(sz);

        if (STATUS_SET != status) {
            // 빈공간을 할당 받음
            count++;
            free = next; // relink
            // System.err.println("free1 : " + free);
        }

        // System.err.println("write : " + buffer.remaining());
        if ((sz - BLOCK_DATA_BYTES) > 0) {
            // System.err.println("next : " + next + ", index : " + index + ", count : " + count);
            
            // Use DirectBufferPool for temporary buffer allocation
            IoBuffer tempBuffer = null;
            try {
                tempBuffer = bufferPool.borrowBuffer(BLOCK_DATA_BYTES);
                
                // Copy data to temp buffer
                int remaining = Math.min(buffer.remaining(), BLOCK_DATA_BYTES);
                for (int copyCount = 0; copyCount < remaining; copyCount++) {
                    tempBuffer.put(buffer.get());
                }
                tempBuffer.flip();
                
                final long i = next > NEXT_END ? next : free;
                p.putLong(i);
                p.put(tempBuffer);
                
                write(i, MARK_AS_NEXT, buffer);
            } finally {
                if (tempBuffer != null)
                    bufferPool.returnBuffer(tempBuffer);
            }
        } else {
            p.putLong(NEXT_END);
            p.put(buffer);
            final int x = BLOCK_DATA_BYTES - sz;
            if (x > 0) {
                final byte[] xbb = new byte[x];
                p.put(xbb);
            }

            if (next > NEXT_END)
                delete(next);

            commit();
        }
    }

    @Override
    public long write(final IoBuffer buffer) throws IOException {
        final long index = free; // free == NEXT_NULL ? count : free;
        write(index, MARK_AS_DATA, buffer);
        return index;
    }

    @Override
    public IoBuffer read(final long index) throws IOException {
        final IoBuffer mbb = position(index);
        final byte status = mbb.get();
        if (STATUS_SET != status) // empty
            return null;

        final byte mark = mbb.get();
        if (MARK_AS_DATA != mark)
            return null;
        final short limit = mbb.getShort();
        final int length = mbb.getInt();
        long next = mbb.getLong();

        if (next > NEXT_END && length > BLOCK_DATA_BYTES) {
            // Use DirectBufferPool for multi-block reads to improve performance
            IoBuffer p = null;
            try {
                p = bufferPool.borrowBuffer(length);
                
                p.put(mbb);
                for (; next > NEXT_END;) {
                    final IoBuffer n = position(next);
                    if (STATUS_SET != n.get()) // empty
                        break;
                    n.get(); // == MARK_AS_NEXT
                    final short remains = n.getShort();
                    n.getInt();
                    next = n.getLong();
                    p.put(n.slice().limit(remains));
                }
                p.flip();
                
                // Create a copy for return since the pooled buffer will be reused
                // Using heap buffer to avoid memory leak
                IoBuffer result = IoBuffer.allocate(p.remaining());
                result.put(p);
                result.flip();
                return result;
            } finally {
                if (p != null) 
                    bufferPool.returnBuffer(p);
            }
        }

        final IoBuffer slice = mbb.slice();
        slice.limit(limit);
        return slice; // return slice.asReadOnlyBuffer();
    }

    @Override
    public InputStream readAsStream(final long index) throws IOException {
        final IoBuffer mbb = position(index);
        final byte status = mbb.get();
        if (STATUS_SET != status) // empty
            return null;

        mbb.get();
        final short limit = mbb.getShort();
        final int length = mbb.getInt();
        final long next = mbb.getLong();

        if (next > NEXT_END && length > BLOCK_DATA_BYTES) {
            return new LBBInputStream(length, BLOCK_DATA_BYTES) {
                @Override
                IoBuffer peek(final int i) throws IOException {
                    long next = index;
                    for (int r = 0; r <= i; r++) {
                        final IoBuffer mbb = position(next);
                        mbb.get();
                        mbb.get();
                        mbb.getShort();
                        mbb.getInt();
                        next = mbb.getLong();
                        if (r == i)
                            return mbb.slice();
                    }
                    return null;
                }
            };
        }

        return new LBBInputStream(length, BLOCK_DATA_BYTES) {
            @Override
            IoBuffer peek(int i) {
                final IoBuffer slice = mbb.slice();
                slice.limit(limit);
                return slice.asReadOnlyBuffer();
            }
        };
    }

    @Override
    public long writeAsStream(final InputStream stream) throws IOException {
        final long index = free; // free == NEXT_END ? count : free;
        writeAsStream(index, stream);
        return index;
    }

    @Override
    public void writeAsStream(final long index, final InputStream stream) throws IOException {
        byte[] bb =  new byte[BLOCK_DATA_BYTES];

        final IoBuffer h = position(index);
        IoBuffer p = h.slice();
        IoBuffer b = null;
        long next = NEXT_END;

        int sz = 0;
        for (int n = 0, i = 0; (n = stream.read(bb)) > -1; sz += n, i++) {
            if (b != null) {
                next = next > NEXT_END ? next : free;
                b.position(8);
                b.putLong(next);
            }

            final IoBuffer c = p.slice();
            final byte status = c.get();
            c.get(); // mark
            c.getShort(); // Data Length
            c.getInt(); // Total Length
            next = c.getLong();

            p.put(STATUS_SET);
            p.put(0 == i ? MARK_AS_DATA : MARK_AS_NEXT);
            p.putShort((short) Math.min(n, BLOCK_DATA_BYTES));
            p.putInt(0);

            if (STATUS_SET != status) {
                // 빈공간을 할당 받음
                count++;
                free = next; // relink
                // System.err.println("free1 : " + free);
            }

            p.putLong(NEXT_END);
            p.put(bb, 0, n);
            final int x = BLOCK_DATA_BYTES - n;
            if (x > 0) {
                final byte[] xbb = new byte[x];
                p.put(xbb);
            }

            b = p.flip();
            p = position(next);
        }

        h.position(4);
        h.putInt(sz);

        if (next > NEXT_END)
            delete(next);
        commit();
    }

    private IoBuffer position(final long index) throws IOException {
        final long absolute = BLOCK_BYTES * index;
        final int i = (int) (absolute / MMAP_BYTES);
        final int r = (int) (absolute % MMAP_BYTES);
        // System.err.println("position.i : " + i);

        IoBuffer p = mbb.get(i);
        if (p == null) {
            // try (final IO.Closer CLOSER = new IO.Closer(lock)) {
                p = mbb.get(i);
                if (p == null) {
                    final long offset = HEADER_BYTES + EXTRA_HEADER_BYTES + (1L * MMAP_BYTES * i);
                    final long sz = channel.size();
                    // System.err.println("channel.map => i : " + i + ", offset : " + offset + ", MMAP_BYTES : " + MMAP_BYTES + ", file : " + file);
                    p = IoBuffer.wrap(channel.map( //
                            readOnly() ? MapMode.READ_ONLY : MapMode.READ_WRITE, //
                            offset, //
                            MMAP_BYTES));
                    // p.load();
                    mbb.put(i, p);
                    // System.err.println("IoBuffer + " + i + ", " + (HEADER_BYTES + (MMAP_BYTES * i)) + ", mbb : " + mbb.size());

                    if (!readOnly() && sz < channel.size()) { // make new block as deleted
                        long next = 1L + (1L * i * (MMAP_BYTES / BLOCK_BYTES));
                        for (int x = 0; x < (MMAP_BYTES / BLOCK_BYTES); x++) {
                            // System.err.println("position.next : " + next);
                            final IoBuffer bb = (IoBuffer) p.slice(x * BLOCK_BYTES, BLOCK_BYTES);
                            bb.put(STATUS_EMPTY);
                            bb.put(MARK_AS_UNUSED);
                            bb.putShort((short) 0); // data length
                            bb.putInt(0); // total length
                            bb.putLong(next); // next
                            next++;
                        }
                        commit();
                    }
                }
            // } // try 
        }

        // return p.position(r);
        return (IoBuffer) p.slice(r, BLOCK_BYTES);
    }

    @Override
    public long count() throws IOException {
        return count;
    }

    @Override
    public long bytes() {
        return file.length();
    }

    @Override
    public IoBuffer head(final int size) throws IOException {
        assert(size > 0);
        return HEAD().slice(0, size);
    }

    @Override
    public IoBuffer head(final int offset, final int size) throws IOException {
        assert(offset >= 0);
        assert(size > 0);
        return HEAD().slice(offset, size);
    }

    /**
     * HEAD mapping
     * @param offset
     * @param size
     * @return
     * @throws IOException
     */
    private IoBuffer HEAD() throws IOException {
        if (HEADER != null)
            return HEADER;
        return IoBuffer.wrap((MappedByteBuffer)channel.map(readOnly() ? MapMode.READ_ONLY : MapMode.READ_WRITE, 0, HEADER_BYTES).load());
    }

    @Override
    public void lock() throws IOException {
        // for future use, not implemented yet
    }

    @Override
    public void status(final PrintStream out) throws IOException {
        out.println("file : " + file);
        out.println("version : " + version());
        out.println("count : " + count);
        out.println("next : " + free);
    }

    @Override
    public boolean readOnly() {
        return !options.mutable;
    }
}



/**
 * Memory
 */
final class MemoryStorage implements Storage {
    private final int EXTRA_HEADER_BYTES;
    private final int BLOCK_BYTES;
    private final int BLOCK_DATA_BYTES;
    private final int MBB_BYTES;
    static final byte STATUS_SET = '+';
    static final byte STATUS_EMPTY = '-';

    static final byte MARK_AS_DATA = 'D';
    static final byte MARK_AS_NEXT = 'N'; // When data is overflowed
    static final byte MARK_AS_UNUSED = 'X'; // When data is deleted or unused

    static final long NEXT_END = -1L;

    // private final File file;
    private final Options options;

    // private volatile long blocks = 0;
    private volatile long count = 0;
    private volatile long free = 0;
    private final byte[] CLEAN; // filling w/ 0 to deleted block
    private final IoBuffer HEADER;

    
    // Direct IoBuffer Pool for temporary buffer allocation optimization
    private final DirectBufferPool bufferPool;

    private final Map<Integer, IoBuffer> mbb = new java.util.LinkedHashMap<>();
    // private final Lock lock = new java.util.concurrent.locks.ReentrantLock();

    MemoryStorage(final Options options) throws IOException {
        // this.file = options.file;
        this.options = options;
        this.EXTRA_HEADER_BYTES = options.EXTRA_HEADER_BYTES;
        this.BLOCK_BYTES = ((short) (BLOCK_HEADER_BYTES + (options.compact <= 0 ? (options.BLOCK_BYTES) : options.compact)));
        this.BLOCK_DATA_BYTES = BLOCK_BYTES - BLOCK_HEADER_BYTES;
        this.MBB_BYTES = BLOCK_BYTES * (options.increment / BLOCK_BYTES);
        this.HEADER = IoBuffer.allocateDirect(CUSTOM_HEADER_BYTES + COMMON_HEADER_BYTES + EXTRA_HEADER_BYTES);
        this.CLEAN = new byte[BLOCK_DATA_BYTES];

        // Initialize DirectBufferPool with optimal size for this storage
        this.bufferPool = new DirectBufferPool(BLOCK_BYTES, 64);

        if (options.mutable)
            lock();

        commit();
        // System.out.println("MemoryStorage initialized: " + options.file.getAbsolutePath() + ", BLOCK_BYTES: " + BLOCK_BYTES + ", MBB_BYTES: " + MBB_BYTES); 
    }

    @Override
    public short version() {
        return 1;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    private void commit() throws IOException {
        final IoBuffer bb = HEADER.slice(CUSTOM_HEADER_BYTES, COMMON_HEADER_BYTES);
        bb.putLong(0L);	// reserved bb.putLong(blocks);
        bb.putLong(free); // The front of deleted blocks
        bb.putLong(0); // The tail of deleted blocks => not used in mmap
        bb.putShort(version()); // version 1
        bb.putInt(options.increment); // increment chunk size
        bb.put(MMAPStorage.R24); // reserved
        bb.putShort((short) BLOCK_DATA_BYTES); // BLOCK Data Max Size (exclude BLOCK Header)
        bb.putLong(count);
        bb.flip();
    }

    @Override
    public void close() throws IOException {
        if (options.mutable)
            commit();

        final Set<Entry<Integer, IoBuffer>> entrySet = mbb.entrySet();
        for (final Entry<Integer, IoBuffer> e : entrySet) {
            final IoBuffer buffer = e.getValue();
            buffer.free();
        }
        HEADER.free();
        entrySet.clear();
    }

    @Override
    public boolean delete(final long index) throws IOException {
        final IoBuffer p = position(index);
        final IoBuffer c = p.slice();

        final byte status = c.get();
        c.get(); // reserved
        c.getShort(); // Data Length
        c.getInt(); // Total Length
        final long next = c.getLong();

        if (STATUS_SET != status)  // empty
            return false;

        p.put(STATUS_EMPTY);
        p.put(MARK_AS_UNUSED);
        p.putShort((short) 0);
        p.putInt(0);
        p.putLong(free);
        p.put(CLEAN);

        free = index;
        count--;
        if (next > NEXT_END)
            delete(next);
        // System.err.println("free2 : " + free + ", next : " + next);
        commit();

        return true;
    }

    // @ForceInline // or -XX:CompileCommand=inline,your/class/Name,multiplyByTwo
    @Override
    public void write(final long index, final IoBuffer buffer) throws IOException {
        write(index, MARK_AS_DATA, buffer);
    }

    private void write(final long index, final byte mark, final IoBuffer buffer) throws IOException {
        final IoBuffer p = position(index);
        final IoBuffer c = p.slice();
        final int sz = buffer.remaining();
        final byte status = c.get();
        c.get(); // mark
        c.getShort(); // Data Length
        c.getInt(); // Total Length
        final long next = c.getLong();

        p.put(STATUS_SET);
        p.put(MARK_AS_DATA);
        p.putShort((short) Math.min(sz, BLOCK_DATA_BYTES));
        p.putInt(sz);

        if (STATUS_SET != status) {
            // 빈공간을 할당 받음
            count++;
            free = next; // relink
            // System.err.println("free1 : " + free);
        }


        // System.err.println("write : " + buffer.remaining());
        if ((sz - BLOCK_DATA_BYTES) > 0) {
            // System.err.println("next : " + next + ", index : " + index + ", count : " + count);
            
            // Use DirectBufferPool for temporary buffer allocation
            IoBuffer tempBuffer = null;
            try {
                tempBuffer = bufferPool.borrowBuffer(BLOCK_DATA_BYTES);
                
                // Copy data to temp buffer
                int remaining = Math.min(buffer.remaining(), BLOCK_DATA_BYTES);
                for (int copyCount = 0; copyCount < remaining; copyCount++) {
                    tempBuffer.put(buffer.get());
                }
                tempBuffer.flip();
                
                final long i = next > NEXT_END ? next : free;
                p.putLong(i);
                p.put(tempBuffer);
                
                write(i, MARK_AS_NEXT, buffer);
            } finally {
                if (tempBuffer != null) 
                    bufferPool.returnBuffer(tempBuffer);
            }
        } else {
            p.putLong(NEXT_END);
            p.put(buffer);
            final int x = BLOCK_DATA_BYTES - sz;
            if (x > 0) {
                final byte[] xbb = new byte[x];
                p.put(xbb);
            }

            if (next > NEXT_END)
                delete(next);

            commit();
        }
    }

    @Override
    public long write(final IoBuffer buffer) throws IOException {
        final long index = free; // free == NEXT_END ? count : free;
        write(index, MARK_AS_DATA, buffer);
        return index;
    }

    @Override
    public IoBuffer read(final long index) throws IOException {
        final IoBuffer mbb = position(index);
        final byte status = mbb.get();
        if (STATUS_SET != status) // empty
            return null;

        final byte mark = mbb.get();
        if (MARK_AS_DATA != mark)
            return null;
        final short limit = mbb.getShort();
        final int length = mbb.getInt();
        long next = mbb.getLong();

        if (next > NEXT_END && length > BLOCK_DATA_BYTES) {
            // Use DirectBufferPool for multi-block reads to improve performance
            IoBuffer p = null;
            try {
                p = bufferPool.borrowBuffer(length);
                
                p.put(mbb);
                for (; next > NEXT_END;) {
                    final IoBuffer n = position(next);
                    if (STATUS_SET != n.get()) // empty
                        break;
                    n.get(); // == MARK_AS_NEXT
                    final short remains = n.getShort();
                    n.getInt();
                    next = n.getLong();
                    p.put(n.slice().limit(remains));
                }
                p.flip();
                
                // Create a copy for return since the pooled buffer will be reused
                // Using heap buffer to avoid memory leak
                IoBuffer result = IoBuffer.allocate(p.remaining());
                result.put(p);
                result.flip();
                return result;
            } finally {
                if (p != null) 
                    bufferPool.returnBuffer(p);
            }
        }

        final IoBuffer slice = mbb.slice();
        slice.limit(limit);
        return slice; // .asReadOnlyBuffer();
    }

    @Override
    public InputStream readAsStream(final long index) throws IOException {
        final IoBuffer mbb = position(index);
        final byte status = mbb.get();
        if (STATUS_SET != status) // empty
            return null;

        mbb.get(); // reserved
        final short limit = mbb.getShort();
        final int length = mbb.getInt();
        final long next = mbb.getLong();

        if (next > NEXT_END && length > BLOCK_DATA_BYTES) {
            return new LBBInputStream(length, BLOCK_DATA_BYTES) {
                @Override
                IoBuffer peek(final int i) throws IOException {
                    long next = index;
                    for (int r = 0; r <= i; r++) {
                        final IoBuffer mbb = position(next);
                        mbb.get();
                        mbb.get();
                        mbb.getShort();
                        mbb.getInt();
                        next = mbb.getLong();
                        if (r == i)
                            return mbb.slice();
                    }
                    return null;
                }
            };
        }

        return new LBBInputStream(length, BLOCK_DATA_BYTES) {
            @Override
            IoBuffer peek(int i) {
                final IoBuffer slice = mbb.slice();
                slice.limit(limit);
                return slice.asReadOnlyBuffer();
            }
        };
    }

    @Override
    public long writeAsStream(final InputStream stream) throws IOException {
        final long index = free; // free == NEXT_END ? count : free;
        writeAsStream(index, stream);
        return index;
    }

    @Override
    public void writeAsStream(final long index, final InputStream stream) throws IOException {
        final byte[] bb = new byte[BLOCK_DATA_BYTES];

        final IoBuffer h = position(index);
        IoBuffer p = h.slice();
        IoBuffer b = null;
        long next = -1;

        int sz = 0;
        for (int n = 0, i = 0; (n = stream.read(bb)) > -1; sz += n, i++) {
            if (b != null) {
                next = next > NEXT_END ? next : free;
                b.position(8);
                b.putLong(next);
            }

            final IoBuffer c = p.slice();
            final byte status = c.get();
            c.get(); // Mark
            c.getShort(); // Data Length
            c.getInt(); // Total Length
            next = c.getLong();

            p.put(STATUS_SET);
            p.put(0 == i ? MARK_AS_DATA : MARK_AS_NEXT);
            p.putShort((short) Math.min(n, BLOCK_DATA_BYTES));
            p.putInt(0);

            if (STATUS_SET != status) {
                // 빈공간을 할당 받음
                count++;
                free = next; // relink
                // System.err.println("free1 : " + free);
            }

            p.putLong(NEXT_END);
            p.put(bb, 0, n);
            final int x = BLOCK_DATA_BYTES - n;
            if (x > 0) {
                final byte[] xbb = new byte[x];
                p.put(xbb);
            }

            b = p.flip();
            p = position(next);
        }

        h.position(4);
        h.putInt(sz);

        if (next > NEXT_END)
            delete(next);
        commit();
    }

    private IoBuffer position(final long index) throws IOException {
        final long absolute = BLOCK_BYTES * index;
        final int i = (int) (absolute / MBB_BYTES);
        final int r = (int) (absolute % MBB_BYTES);
        // System.err.println("position.i : " + i);

        IoBuffer p = mbb.get(i);
        if (p == null) {
            // try (final IO.Closer CLOSER = new IO.Closer(lock)) {
                p = mbb.get(i);
                if (p == null) {
                    p = IoBuffer.allocateDirect(MBB_BYTES);
                    mbb.put(i, p);

                    if (!readOnly()) { // make new block as deleted
                        long next = 1L + (1L * i * (MBB_BYTES / BLOCK_BYTES));
                        for (int x = 0; x < (MBB_BYTES / BLOCK_BYTES); x++) {
                            // System.err.println("position.next : " + next);
                            final IoBuffer bb = (IoBuffer) p.slice(x * BLOCK_BYTES, BLOCK_BYTES);
                            bb.put(STATUS_EMPTY);
                            bb.put(MARK_AS_UNUSED);
                            bb.putShort((short) 0); // data length
                            bb.putInt(0); // total length
                            bb.putLong(next); // next
                            next++;
                        }
                        commit();
                    }
                }
            }
        // } // try

        // return p.position(r);
        return p.slice(r, BLOCK_BYTES);
    }

    @Override
    public long count() throws IOException {
        return count;
    }

    @Override
    public long bytes() {
        return mbb.size() * MBB_BYTES;
    }

    @Override
    public IoBuffer head(final int size) throws IOException {
        return HEADER.slice(0, size);
    }

    @Override
    public IoBuffer head(final int offset, final int size) throws IOException {
        return HEADER.slice(offset, size);
    }

    @Override
    public void lock() throws IOException {
    }

    @Override
    public void status(final PrintStream out) throws IOException {
        out.println("version : " + version());
        out.println("count : " + count);
        out.println("next : " + free);
    }

    @Override
    public boolean readOnly() {
        return !options.mutable;
    }

    static void transfer(final MemoryStorage storage, final File dest) throws IOException {
        final Set<java.nio.file.OpenOption> opt = new java.util.HashSet<>();
        opt.add(java.nio.file.StandardOpenOption.CREATE_NEW);
        opt.add(java.nio.file.StandardOpenOption.WRITE);
        opt.add(java.nio.file.StandardOpenOption.SYNC);
        try (final FileChannel ch = FileChannel.open(Paths.get(dest.toURI()), opt)) {
            ch.write(storage.HEADER.slice().unwrap());
            for (final Map.Entry<Integer, IoBuffer> e : storage.mbb.entrySet()) {
                final IoBuffer bb = e.getValue().slice();
                ch.write(bb.unwrap());
            }
        }
    }
}

abstract class LBBInputStream extends InputStream {
    final int DATA_BYTES;
    int offset = 0;
    int p = 0;
    int i = 0;
    int l = 0;
    byte[] buf;

    LBBInputStream(final int DATA_BYTES, final int BLOCK_DATA_BYTES) {
        this.DATA_BYTES = DATA_BYTES;
        this.buf = new byte[BLOCK_DATA_BYTES];
    }

    abstract IoBuffer peek(final int i) throws IOException;

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int n = read(b, 0, 1);
        return n > -1 ? b[0] : -1;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int available() {
        return DATA_BYTES - offset;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (offset >= DATA_BYTES)
            return -1;

        if ((l - p) <= 0) {
            IoBuffer bb = peek(i);
            // System.err.println("peek : " + i + ", bb : " + bb);
            if (bb == null)
                return -1;
            l = bb.remaining();
            p = 0;
            bb.get(buf);
            i++;
        }

        int n = Math.min(l - p, len);
        // System.err.println("n : " + n + ", p : " + p + ", l : " + l + ", offset : " + offset + ", len : " + len);
        System.arraycopy(buf, p, b, off, n);
        p += n;

        offset += (off + n);
        return n;
    }
}
