/**
 * HyperLogLog.java
 */
package flint.db;

import java.util.Arrays;

/**
 * HyperLogLog is a probabilistic data structure used for cardinality estimation.
 * It provides an approximation of the number of distinct elements in a dataset
 * with very low memory usage and good accuracy.
 * 
 * <pre>
 * b = 4 → 16 bytes
 * b = 14 → 2^14 = 16,384 bytes (~16 KB)
 * b = 16 → 65,536 bytes (~64 KB)
 * </pre>
 */
public final class HyperLogLog {
    
    private final int b; // number of bits for bucket index
    private final int m; // number of buckets (2^b)
    private final byte[] buckets; // storage for leading zero counts
    private final double alphaMM; // bias correction constant
    
    /**
     * Creates a HyperLogLog with the specified precision parameter.
     * @param b precision parameter (4 <= b <= 16). Higher values give better accuracy but use more memory.
     */
    public HyperLogLog(int b) {
        if (b < 4 || b > 16) {
            throw new IllegalArgumentException("b must be between 4 and 16");
        }
        
        this.b = b;
        this.m = 1 << b; // 2^b
        this.buckets = new byte[m];
        this.alphaMM = getAlpha(m) * m * m;
    }

    private HyperLogLog(int b, byte[] buckets, double alphaMM) {
        this.b = b;
        this.m = 1 << b; // 2^b
        this.buckets = buckets;
        this.alphaMM = alphaMM;
    }

    public static HyperLogLog fromByteArray(byte[] buf) {
        if (buf == null || buf.length == 0) {
            throw new IllegalArgumentException("Invalid byte array");
        }

        int b = 14; // default precision
        int m = 1 << b;
        byte[] buckets = new byte[m];
        double alphaMM = getAlpha(m) * m * m;

        // Deserialize the HyperLogLog from the byte array
        for (int i = 0; i < m; i++) {
            buckets[i] = buf[i];
        }

        return new HyperLogLog(b, buckets, alphaMM);
    }

    /**
     * Creates a HyperLogLog with default precision (b=14, giving ~0.81% standard error).
     */
    public HyperLogLog() {
        this(14);
    }
    
    /**
     * Adds an element to the HyperLogLog.
     * @param value the value to add
     */
    public void add(Object value) {
        if (value == null) {
            return;
        }
        addHash(hash(value.toString()));
    }

    
    /**
     * Adds a hash value to the HyperLogLog.
     * @param hash the hash value to add
     */
    private void addHash(long hash) {
        // Get bucket index from first b bits
        int bucketIndex = (int)(hash & ((1L << b) - 1));
        
        // Get remaining bits for leading zero count
        long w = hash >>> b;
        
        // Count leading zeros in w, plus 1
        int leadingZeros = Long.numberOfLeadingZeros(w) - (64 - (64 - b)) + 1;
        
        // Update bucket with maximum leading zero count
        buckets[bucketIndex] = (byte) Math.max(buckets[bucketIndex], leadingZeros);
    }
    
    /**
     * Estimates the cardinality (number of distinct elements).
     * @return estimated cardinality
     */
    public long cardinality() {
        double rawEstimate = alphaMM / sum();
        
        // Apply bias correction and range corrections
        if (rawEstimate <= 2.5 * m) {
            // Small range correction
            int zeros = countZeros();
            if (zeros != 0) {
                return Math.round(m * Math.log(m / (double) zeros));
            }
        }
        
        if (rawEstimate <= (1.0/30.0) * (1L << 32)) {
            // No correction needed
            return Math.round(rawEstimate);
        } else {
            // Large range correction
            return Math.round(-1L * (1L << 32) * Math.log(1 - rawEstimate / (1L << 32)));
        }
    }
    
    /**
     * Merges another HyperLogLog into this one.
     * Both HyperLogLogs must have the same precision parameter.
     * @param other the HyperLogLog to merge
     */
    public void merge(HyperLogLog other) {
        if (this.b != other.b) {
            throw new IllegalArgumentException("Cannot merge HyperLogLogs with different precision parameters");
        }
        
        for (int i = 0; i < m; i++) {
            buckets[i] = (byte) Math.max(buckets[i], other.buckets[i]);
        }
    }
    
    /**
     * Clears all data from the HyperLogLog.
     */
    public void clear() {
        Arrays.fill(buckets, (byte) 0);
    }
    
    /**
     * Returns the size in bytes of this HyperLogLog.
     * @return size in bytes
     */
    public int sizeInBytes() {
        return m; // Each bucket is 1 byte
    }
    
    /**
     * Returns the number of buckets.
     * @return number of buckets
     */
    public int getBucketCount() {
        return m;
    }
    
    /**
     * Returns the precision parameter.
     * @return precision parameter b
     */
    public int getPrecision() {
        return b;
    }
    
    // Private helper methods
    
    private double sum() {
        double sum = 0;
        for (byte bucket : buckets) {
            sum += Math.pow(2, -bucket);
        }
        return sum;
    }
    
    private int countZeros() {
        int count = 0;
        for (byte bucket : buckets) {
            if (bucket == 0) {
                count++;
            }
        }
        return count;
    }
    
    private static double getAlpha(int m) {
        switch (m) {
            case 16:
                return 0.673;
            case 32:
                return 0.697;
            case 64:
                return 0.709;
            default:
                return 0.7213 / (1 + 1.079 / m);
        }
    }
    
    private static long hash(Object obj) {
        // Simple hash function - in production, use a better hash like MurmurHash
        long hash = obj.hashCode();
        hash ^= (hash >>> 32);
        hash *= 0x9e3779b97f4a7c15L;
        hash ^= (hash >>> 32);
        hash *= 0x9e3779b97f4a7c15L;
        hash ^= (hash >>> 32);
        return hash;
    }

    public byte[] toByteArray() {
        byte[] buf = new byte[m];
        for (int i = 0; i < m; i++) {
            buf[i] = buckets[i];
        }
        return buf;
    }

    @Override
    public String toString() {
        return String.format("HyperLogLog(b=%d, m=%d, estimated_cardinality=%d)", 
                           b, m, cardinality());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        HyperLogLog other = (HyperLogLog) obj;
        return b == other.b && Arrays.equals(buckets, other.buckets);
    }
    
    @Override
    public int hashCode() {
        return Arrays.hashCode(buckets) * 31 + b;
    }
}
