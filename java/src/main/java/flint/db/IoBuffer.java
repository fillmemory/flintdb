package flint.db;

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

    public static void free(final ByteBuffer cb) {
		if (cb == null || !cb.isDirect())
			return;
        
        // Android detection
        boolean isAndroid = System.getProperty("java.vm.vendor", "").contains("Android") 
                        || System.getProperty("java.vendor", "").contains("Android");
        
        if (isAndroid) {
            // On Android, just let GC handle it
            // Direct buffers are automatically cleaned up by the GC
            return;
        }

		// we could use this type cast and call functions without reflection code,
		// but static import from sun.* package is risky for non-SUN virtual machine.
		// try { ((sun.nio.ch.DirectBuffer)cb).cleaner().clean(); } catch (Exception ex) { }

		// JavaSpecVer: 1.6, 1.7, 1.8, 9, 10
		boolean isOldJDK = System.getProperty("java.specification.version", "99").startsWith("1.");
		try {
			if (isOldJDK) {
				java.lang.reflect.Method cleaner = cb.getClass().getMethod("cleaner");
				cleaner.setAccessible(true);
				java.lang.reflect.Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
				clean.setAccessible(true);
				clean.invoke(cleaner.invoke(cb));
			} else {
				Class<?> unsafeClass;
				try {
					unsafeClass = Class.forName("sun.misc.Unsafe");
				} catch (Exception ex) {
					// jdk.internal.misc.Unsafe doesn't yet have an invokeCleaner() method,
					// but that method should be added if sun.misc.Unsafe is removed.
					unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
				}
				java.lang.reflect.Method clean = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
				clean.setAccessible(true);
				java.lang.reflect.Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
				theUnsafeField.setAccessible(true);
				Object theUnsafe = theUnsafeField.get(null);
				clean.invoke(theUnsafe, cb);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}


    // Wrapper methods that return IoBuffer to maintain byte order
    public IoBuffer slice() {
        return new IoBuffer(buffer.slice());
    }

    public IoBuffer slice(int index, int length) {
        return new IoBuffer(buffer.slice(index, length));
    }

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
