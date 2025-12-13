package flint.db;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * DirectBufferPool - Pre-allocated Slice-based Direct IoBuffer Pool
 * 
 * GC-friendly direct buffer pool that pre-allocates a master buffer
 * and slices it into smaller buffers for reuse. This avoids frequent
 * allocations and deallocations, reducing GC pressure and improving
 * performance for applications that require frequent direct buffer usage.
 */
final class DirectBufferPool implements AutoCloseable {
    
    private final ConcurrentLinkedQueue<IoBuffer> availableBuffers;
    private final IoBuffer masterBuffer; // Pre-allocated master buffer 
    private final int defaultBufferSize;
    private final int maxPoolSize;
    private volatile int currentPoolSize = 0;
    
    /**
     * Constructor
     *
     * @param defaultBufferSize Default buffer size (in bytes)
     * @param maxPoolSize Maximum pool size
     */
    public DirectBufferPool(int defaultBufferSize, int maxPoolSize) {
        if (defaultBufferSize <= 0) 
            throw new IllegalArgumentException("Buffer size must be positive: " + defaultBufferSize);
        if (maxPoolSize <= 0) 
            throw new IllegalArgumentException("Max pool size must be positive: " + maxPoolSize);
        
        this.defaultBufferSize = defaultBufferSize;
        this.maxPoolSize = maxPoolSize;
        this.availableBuffers = new ConcurrentLinkedQueue<>();
        
        // Allocate the total size at once
        long totalSize = (long) defaultBufferSize * maxPoolSize;
        if (totalSize > Integer.MAX_VALUE) 
            throw new IllegalArgumentException("Total buffer size exceeds maximum: " + totalSize);
        
        this.masterBuffer = IoBuffer.allocateDirect((int) totalSize);
        
        // Pre-create slices and add them to the pool (Little Endian for C compatibility)
        for (int i = 0; i < maxPoolSize; i++) {
            int offset = i * defaultBufferSize;
            IoBuffer slice = masterBuffer.slice(offset, defaultBufferSize);
            availableBuffers.offer(slice);
            currentPoolSize++;
        }
    }
    
    /**
     * Create DirectBufferPool with default settings
     * - Default buffer size: 64KB
     * - Maximum pool size: 32
     */
    public DirectBufferPool() {
        this(64 * 1024, 32);
    }
    
    /**
     * Borrows a buffer from the pool.
     *
     * @param requestedSize Requested buffer size
     * @return Available IoBuffer (sliced direct buffer)
     */
    public IoBuffer borrowBuffer(int requestedSize) {
        // Try to reuse from pool if requested size is similar to default size
        if (requestedSize <= defaultBufferSize) {
            IoBuffer pooled = availableBuffers.poll();
            if (pooled != null) {
                currentPoolSize--;
                pooled.clear();
                pooled.limit(requestedSize);
                return pooled;
            }

            // Must fix when you meet this message, maybe buffer is not returned properly
            // or defaultBufferSize, maxPoolSize are not configured correctly
            System.err.println("No available buffer in pool, allocating new buffer: " + requestedSize);
        }
        
        // If pool is empty or requested size is large, allocate separately
        // System.out.println("No available buffer in pool or requested size is large. Allocating new buffer: " + requestedSize);
        return IoBuffer.allocate(requestedSize);
    }
    
    /**
     * Returns a used buffer back to the pool.
     *
     * @param buffer IoBuffer to return
     */
    public void returnBuffer(IoBuffer buffer) {
        if (buffer != null && 
            buffer.isDirect() &&
            buffer.capacity() == defaultBufferSize && 
            currentPoolSize < maxPoolSize) {
            
            // Simple check if buffer is a slice of the master buffer
            buffer.clear();
            if (availableBuffers.offer(buffer)) {
                currentPoolSize++;
            }
        }
        // Buffers that do not meet the conditions are left to GC
    }
    
    /**
     * Clear the pool and re-create slices from the master buffer
     */
    public void clear() {
        availableBuffers.clear();
        // Re-create slices from the master buffer (Little Endian)
        for (int i = 0; i < maxPoolSize; i++) {
            int offset = i * defaultBufferSize;
            IoBuffer slice = masterBuffer.slice(offset, defaultBufferSize);
            availableBuffers.offer(slice);
        }
        currentPoolSize = maxPoolSize;
    }

    @Override
    public void close() {
        clear();
        // Storage.free(masterBuffer);
        masterBuffer.free();
    }
}
