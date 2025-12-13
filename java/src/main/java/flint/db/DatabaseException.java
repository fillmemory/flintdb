/**
 * 
 */
package flint.db;

import java.io.IOException;

/**
 * Database exception with error code classification.
 * This allows callers to distinguish different types of database errors.
 */
public class DatabaseException extends IOException {
    
    private final ErrorCode errorCode;
    private final Object context;
    
    public DatabaseException(ErrorCode errorCode) {
        super(errorCode.getMessage());
        this.errorCode = errorCode;
        this.context = null;
    }

    // public DatabaseException(String additionalMessage) {
    //     super(additionalMessage);
    //     this.errorCode = ErrorCode.INTERNAL_ERROR;
    //     this.context = null;
    // }
    
    public DatabaseException(ErrorCode errorCode, String additionalMessage) {
        super(errorCode.getMessage() + " - " + additionalMessage);
        this.errorCode = errorCode;
        this.context = null;
    }
    
    public DatabaseException(ErrorCode errorCode, Object context) {
        super(errorCode.getMessage() + " - " + context);
        this.errorCode = errorCode;
        this.context = context;
    }
    
    public DatabaseException(ErrorCode errorCode, String additionalMessage, Throwable cause) {
        super(errorCode.getMessage() + " - " + additionalMessage, cause);
        this.errorCode = errorCode;
        this.context = null;
    }
    
    public DatabaseException(ErrorCode errorCode, Object context, Throwable cause) {
        super(errorCode.getMessage() + " - " + context, cause);
        this.errorCode = errorCode;
        this.context = context;
    }
    
    /**
     * Get the error code for this exception
     * @return the error code
     */
    public ErrorCode getErrorCode() {
        return errorCode;
    }
    
    /**
     * Get the context object associated with this exception
     * @return the context object, or null if not available
     */
    public Object getContext() {
        return context;
    }
    
    /**
     * Check if this exception has a specific error code
     * @param code the error code to check
     * @return true if the error code matches
     */
    public boolean isErrorCode(ErrorCode code) {
        return this.errorCode == code;
    }

    public String toString() {
        return "DatabaseException{" +
                "errorCode=" + errorCode +
                ", context=" + context +
                '}';
    }
}
