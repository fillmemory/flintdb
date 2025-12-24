package flint.db;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A wrapper around ByteBuffer that enforces LITTLE_ENDIAN byte order.
 * This prevents common bugs where slice() operations lose the byte order
 * setting.
 * 
 * <pre>
 * * ByteBuffer does not have a way to enforce byte order on all derived buffers
 * </pre>
 */
final class IoBuffer {
    private static final MethodHandle CLEANER_CLEAN;
    private static final MethodHandle DIRECT_BUFFER_CLEANER;
    private static final boolean IS_ANDROID;
    
    static {
        // Android detection
        String vmVendor = System.getProperty("java.vm.vendor", "");
        String vendor = System.getProperty("java.vendor", "");
        IS_ANDROID = vmVendor.contains("Android") || vendor.contains("Android");
        
        MethodHandle cleanerMethod = null;
        MethodHandle cleanMethod = null;
        
        if (!IS_ANDROID) {
            try {
                // Use DirectBuffer.cleaner().clean() - this is the most compatible way
                // without deprecated warnings
                Class<?> directBufferClass = Class.forName("sun.nio.ch.DirectBuffer");
                Class<?> cleanerClass = Class.forName("jdk.internal.ref.Cleaner");
                
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                
                // Get DirectBuffer.cleaner() method
                cleanerMethod = lookup.findVirtual(
                    directBufferClass,
                    "cleaner",
                    MethodType.methodType(cleanerClass)
                );
                
                // Get Cleaner.clean() method
                cleanMethod = lookup.findVirtual(
                    cleanerClass,
                    "clean",
                    MethodType.methodType(void.class)
                );
            } catch (Throwable e) {
                // Fallback: not available
                cleanerMethod = null;
                cleanMethod = null;
            }
        }
        
        DIRECT_BUFFER_CLEANER = cleanerMethod;
        CLEANER_CLEAN = cleanMethod;
    }
    
    private final ByteBuffer buffer;

    private IoBuffer(ByteBuffer buffer) {
        this.buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public static IoBuffer wrap(ByteBuffer buffer) {
        return new IoBuffer(buffer);
    }

    public static IoBuffer wrap(byte[] array) {
        return new IoBuffer(ByteBuffer.wrap(array));
    }

    public static IoBuffer wrap(byte[] array, int offset, int length) {
        return new IoBuffer(ByteBuffer.wrap(array, offset, length));
    }

    public static IoBuffer allocate(int capacity) {
        return new IoBuffer(ByteBuffer.allocate(capacity));
    }

    public static IoBuffer allocateDirect(int capacity) {
        return new IoBuffer(ByteBuffer.allocateDirect(capacity));
    }

    public ByteBuffer unwrap() {
        return buffer;
    }

    public boolean isDirect() {
        return buffer.isDirect();
    }

    public boolean isMapped() {
        return buffer instanceof java.nio.MappedByteBuffer;
    }

    public boolean isReadOnly() {
        return buffer.isReadOnly();
    }

    public void free() {
        free(this);
    }

    public static void free(IoBuffer ioBuffer) {
        if (!ioBuffer.isDirect()) return;
        free(ioBuffer.unwrap());
    }

    /**
     * Releases the native memory of a direct ByteBuffer immediately.
     * Uses DirectBuffer.cleaner().clean() to avoid deprecated Unsafe warnings.
     * Falls back to GC cleanup if direct release is not available.
     */
    public static void free(final ByteBuffer cb) {
        if (cb == null || !cb.isDirect()) {
            return;
        }
        
        if (IS_ANDROID) {
            // On Android, let GC handle it
            return;
        }
        
        if (DIRECT_BUFFER_CLEANER != null && CLEANER_CLEAN != null) {
            try {
                // Call DirectBuffer.cleaner() to get the Cleaner
                Object cleaner = DIRECT_BUFFER_CLEANER.invoke(cb);
                if (cleaner != null) {
                    // Call Cleaner.clean()
                    CLEANER_CLEAN.invoke(cleaner);
                }
            } catch (Throwable e) {
                // Failed to clean, let GC handle it
            }
        }
        // If methods are null, just let GC handle cleanup
    }


    // Wrapper methods that return IoBuffer to maintain byte order
    public IoBuffer slice() {
        return new IoBuffer(buffer.slice());
    }
    
    public IoBuffer slice(int index, int length) {
        return new IoBuffer(buffer.slice(index, length));
    }

    // public IoBuffer slice(int index, int length) {
    //     // Java 11 compatible implementation (slice(int, int) added in Java 13)
    //     int oldPos = buffer.position();
    //     int oldLimit = buffer.limit();
    //     try {
    //         buffer.position(index);
    //         buffer.limit(index + length);
    //         return new IoBuffer(buffer.slice());
    //     } finally {
    //         buffer.position(oldPos);
    //         buffer.limit(oldLimit);
    //     }
    // }

    public IoBuffer duplicate() {
        return new IoBuffer(buffer.duplicate());
    }

    public IoBuffer clear() {
        buffer.clear();
        return this;
    }

    public IoBuffer rewind() {
        buffer.rewind();
        return this;
    }

    // Position and limit operations
    public IoBuffer position(int newPosition) {
        buffer.position(newPosition);
        return this;
    }

    public int position() {
        return buffer.position();
    }

    public IoBuffer limit(int newLimit) {
        buffer.limit(newLimit);
        return this;
    }

    public int limit() {
        return buffer.limit();
    }

    public IoBuffer flip() {
        buffer.flip();
        return this;
    }

    public int remaining() {
        return buffer.remaining();
    }

    public boolean hasRemaining() {
        return buffer.hasRemaining();
    }

    public int capacity() {
        return buffer.capacity();
    }

    // Get operations (automatically LITTLE_ENDIAN)
    public byte get() {
        return buffer.get();
    }

    public byte get(int index) {
        return buffer.get(index);
    }

    public short getShort() {
        return buffer.getShort();
    }

    public short getShort(int index) {
        return buffer.getShort(index);
    }

    public int getInt() {
        return buffer.getInt();
    }

    public int getInt(int index) {
        return buffer.getInt(index);
    }

    public long getLong() {
        return buffer.getLong();
    }

    public long getLong(int index) {
        return buffer.getLong(index);
    }

    public double getDouble() {
        return buffer.getDouble();
    }

    public double getDouble(int index) {
        return buffer.getDouble(index);
    }

    public float getFloat() {
        return buffer.getFloat();
    }

    public float getFloat(int index) {
        return buffer.getFloat(index);
    }

    public IoBuffer get(byte[] dst) {
        buffer.get(dst);
        return this;
    }

    public IoBuffer get(byte[] dst, int offset, int length) {
        buffer.get(dst, offset, length);
        return this;
    }

    public byte[] array() {
        return buffer.array();
    }

    // Put operations (automatically LITTLE_ENDIAN)
    public IoBuffer put(byte b) {
        buffer.put(b);
        return this;
    }

    public IoBuffer put(IoBuffer src) {
        buffer.put(src.unwrap());
        return this;
    }

    public IoBuffer put(int index, byte b) {
        buffer.put(index, b);
        return this;
    }

    public IoBuffer putShort(short value) {
        buffer.putShort(value);
        return this;
    }

    public IoBuffer putShort(int index, short value) {
        buffer.putShort(index, value);
        return this;
    }

    public IoBuffer putInt(int value) {
        buffer.putInt(value);
        return this;
    }

    public IoBuffer putInt(int index, int value) {
        buffer.putInt(index, value);
        return this;
    }

    public IoBuffer putLong(long value) {
        buffer.putLong(value);
        return this;
    }

    public IoBuffer putLong(int index, long value) {
        buffer.putLong(index, value);
        return this;
    }

    public IoBuffer putDouble(double value) {
        buffer.putDouble(value);
        return this;
    }

    public IoBuffer putDouble(int index, double value) {
        buffer.putDouble(index, value);
        return this;
    }

    public IoBuffer putFloat(float value) {
        buffer.putFloat(value);
        return this;
    }

    public IoBuffer putFloat(int index, float value) {
        buffer.putFloat(index, value);
        return this;
    }

    public IoBuffer put(byte[] src) {
        buffer.put(src);
        return this;
    }

    public IoBuffer put(byte[] src, int offset, int length) {
        buffer.put(src, offset, length);
        return this;
    }

    public IoBuffer put(ByteBuffer src) {
        buffer.put(src);
        return this;
    }


    public IoBuffer asReadOnlyBuffer() {
        return new IoBuffer(buffer.asReadOnlyBuffer());
    }

    // For compatibility with existing code
    public ByteOrder order() {
        return ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public String toString() {
        return "IoBuffer[pos=" + buffer.position() +
                " lim=" + buffer.limit() +
                " cap=" + buffer.capacity() + "]";
    }
}
