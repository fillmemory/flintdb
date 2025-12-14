/**
 * 
 */
package flint.db;

/**
 * Error codes for database operations
 */
public enum ErrorCode {
    // Constraint violations (-1000 to -1999)
    NOT_FOUND(-1, "Record not found"),
    DUPLICATE_KEY(-1000, "Duplicate key found"),
    UNIQUE_CONSTRAINT_VIOLATION(-1001, "Unique constraint violation"),
    FOREIGN_KEY_VIOLATION(-1002, "Foreign key constraint violation"),
    CHECK_CONSTRAINT_VIOLATION(-1003, "Check constraint violation"),
    
    // Data integrity errors (-2000 to -2999)
    COLUMN_MISMATCH(-2000, "Column count mismatch"),
    ROW_BYTES_EXCEEDED(-2001, "Row bytes exceeded maximum size"),
    INVALID_DATA_TYPE(-2002, "Invalid data type"),
    
    // Table/Index errors (-3000 to -3999)
    TABLE_NOT_FOUND(-3000, "Table not found"),
    INDEX_NOT_FOUND(-3001, "Index not found"),
    NO_INDEXES(-3002, "No indexes found"),
    
    // Storage errors (-4000 to -4999)
    STORAGE_READ_ERROR(-4000, "Storage read error"),
    STORAGE_WRITE_ERROR(-4001, "Storage write error"),
    STORAGE_DELETE_ERROR(-4002, "Storage delete error"),
    STORAGE_FULL(-4003, "Storage full"),
    
    // Lock/Transaction errors (-5000 to -5999)
    LOCK_TIMEOUT(-5000, "Lock acquisition timeout"),
    DEADLOCK_DETECTED(-5001, "Deadlock detected"),
    TRANSACTION_FAILED(-5002, "Transaction failed"),
    TRANSACTION_NOT_STARTED(-5003, "Transaction not started"),
    
    // General errors (-9000 to -9999)
    INVALID_OPERATION(-9000, "Invalid operation"),
    RESOURCE_NOT_AVAILABLE(-9001, "Resource not available"),
    INTERNAL_ERROR(-9002, "Internal error");
    
    private final int code;
    private final String message;
    
    ErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }
    
    public int getCode() {
        return code;
    }
    
    public String getMessage() {
        return message;
    }
}
