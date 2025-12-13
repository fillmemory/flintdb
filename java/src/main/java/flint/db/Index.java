/**
 * 
 */
package flint.db;

/**
 * Index interface for table indexing strategies
 * 
 * Defines the contract for table indexes including primary keys and secondary indexes.
 * Supports various algorithms like B+ Tree and Hash indexing.
 */
public interface Index {
    /** Primary key index identifier */
    static final int PRIMARY = 0;
    static final String PRIMARY_NAME = "PRIMARY";

    /**
     * Get column names that form this index key
     * 
     * @return Array of column names
     */
    String[] keys();

    /**
     * Get indexing algorithm used
     * 
     * @return Algorithm name (e.g., "bptree", "hash")
     */
    String algorithm();

    /**
     * Get index name
     * 
     * @return Index name
     */
    String name();

    /**
     * Get index type
     * 
     * @return Index type (e.g., "primary", "sort")
     */
    String type();
}
