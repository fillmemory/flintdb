package flint.db;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A wrapper around ByteBuffer that enforces LITTLE_ENDIAN byte order.
 * This prevents common bugs where slice() operations lose the byte order setting.
 */
final class LittleEndianBuffer {
    private final ByteBuffer buffer;

    private LittleEndianBuffer(ByteBuffer buffer) {
        this.buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public static LittleEndianBuffer wrap(ByteBuffer buffer) {
        return new LittleEndianBuffer(buffer);
    }

    public ByteBuffer unwrap() {
        return buffer;
    }

    // Wrapper methods that return LittleEndianBuffer to maintain byte order
    public LittleEndianBuffer slice() {
        return new LittleEndianBuffer(buffer.slice());
    }

    public LittleEndianBuffer slice(int index, int length) {
        return new LittleEndianBuffer(buffer.slice(index, length));
    }

    public LittleEndianBuffer duplicate() {
        return new LittleEndianBuffer(buffer.duplicate());
    }

    // Position and limit operations
    public LittleEndianBuffer position(int newPosition) {
        buffer.position(newPosition);
        return this;
    }

    public int position() {
        return buffer.position();
    }

    public LittleEndianBuffer limit(int newLimit) {
        buffer.limit(newLimit);
        return this;
    }

    public int limit() {
        return buffer.limit();
    }

    public LittleEndianBuffer flip() {
        buffer.flip();
        return this;
    }

    public int remaining() {
        return buffer.remaining();
    }

    public boolean hasRemaining() {
        return buffer.hasRemaining();
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

    public LittleEndianBuffer get(byte[] dst) {
        buffer.get(dst);
        return this;
    }

    public LittleEndianBuffer get(byte[] dst, int offset, int length) {
        buffer.get(dst, offset, length);
        return this;
    }

    // Put operations (automatically LITTLE_ENDIAN)
    public LittleEndianBuffer put(byte b) {
        buffer.put(b);
        return this;
    }

    public LittleEndianBuffer put(int index, byte b) {
        buffer.put(index, b);
        return this;
    }

    public LittleEndianBuffer putShort(short value) {
        buffer.putShort(value);
        return this;
    }

    public LittleEndianBuffer putShort(int index, short value) {
        buffer.putShort(index, value);
        return this;
    }

    public LittleEndianBuffer putInt(int value) {
        buffer.putInt(value);
        return this;
    }

    public LittleEndianBuffer putInt(int index, int value) {
        buffer.putInt(index, value);
        return this;
    }

    public LittleEndianBuffer putLong(long value) {
        buffer.putLong(value);
        return this;
    }

    public LittleEndianBuffer putLong(int index, long value) {
        buffer.putLong(index, value);
        return this;
    }

    public LittleEndianBuffer put(byte[] src) {
        buffer.put(src);
        return this;
    }

    public LittleEndianBuffer put(byte[] src, int offset, int length) {
        buffer.put(src, offset, length);
        return this;
    }

    public LittleEndianBuffer put(ByteBuffer src) {
        buffer.put(src);
        return this;
    }

    // For compatibility with existing code
    public ByteOrder order() {
        return ByteOrder.LITTLE_ENDIAN;
    }

    @Override
    public String toString() {
        return "LittleEndianBuffer[pos=" + buffer.position() + 
               " lim=" + buffer.limit() + 
               " cap=" + buffer.capacity() + "]";
    }
}
