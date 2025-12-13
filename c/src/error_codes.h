#ifndef ERROR_CODES_H
#define ERROR_CODES_H

#include <stdio.h>
#include <string.h>

/**
 * Database error codes for structured error handling
 */
enum db_error_code {
    // Constraint violations
    DB_ERR_DUPLICATE_KEY = -1000,
    DB_ERR_UNIQUE_CONSTRAINT_VIOLATION,
    DB_ERR_FOREIGN_KEY_VIOLATION,
    DB_ERR_CHECK_CONSTRAINT_VIOLATION,
    
    // Data integrity errors
    DB_ERR_COLUMN_MISMATCH = -2000,
    DB_ERR_ROW_BYTES_EXCEEDED,
    DB_ERR_INVALID_DATA_TYPE,
    
    // Table/Index errors
    DB_ERR_TABLE_NOT_FOUND = -3000,
    DB_ERR_INDEX_NOT_FOUND,
    DB_ERR_NO_INDEXES,
    
    // Storage errors
    DB_ERR_STORAGE_READ_ERROR = -4000,
    DB_ERR_STORAGE_WRITE_ERROR,
    DB_ERR_STORAGE_DELETE_ERROR,
    DB_ERR_STORAGE_FULL,
    
    // Lock/Transaction errors
    DB_ERR_LOCK_TIMEOUT = -5000,
    DB_ERR_DEADLOCK_DETECTED,
    DB_ERR_TRANSACTION_FAILED,
    
    // General errors
    DB_ERR_INVALID_OPERATION = -9000,
    DB_ERR_RESOURCE_NOT_AVAILABLE,
    DB_ERR_INTERNAL_ERROR
};

#endif // ERROR_CODES_H
