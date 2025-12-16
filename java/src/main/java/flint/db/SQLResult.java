package flint.db;

/**
 * SQL execution result
 * 
 * UPDATE/INSERT/DELETE: affected rows
 * SELECT: columns and cursor
 */
public final class SQLResult implements AutoCloseable {
    final long affected;

    final String[] columns;
    final Cursor<Row> cursor;
    final Transaction transaction;

    /**
     * Get number of affected rows
     * @return number of affected rows, or -1 if result is a cursor or not applicable
     */
    public long getAffected() {
        return affected;
    }

    /**
     * Get column names of result set
     * @return
     */
    public String[] getColumns() {
        return columns;
    }

    /**
     * Get cursor of result set
     * @return
     */
    public Cursor<Row> getCursor() {
        return cursor;
    }

    /**
     * Get transaction associated with this result (for BEGIN TRANSACTION)
     * @return transaction or null
     */
    public Transaction getTransaction() {
        return transaction;
    }

    /**
     * Close the result and release resources
     */
    @Override
    public void close() throws Exception {
        if (cursor != null) {
            cursor.close();
        }
    }

    /**
     * Constructor for affected rows result
     * @param affected
     */
    public SQLResult(long affected) {
        this.affected = affected;
        this.columns = null;
        this.cursor = null;
        this.transaction = null;
    }

    /**
     * Constructor for cursor result
     * @param columns
     * @param cursor
     */
    public SQLResult(String[] columns, Cursor<Row> cursor) {
        this.affected = -1;
        this.columns = columns;
        this.cursor = cursor;
        this.transaction = null;
    }

    /**
     * Constructor with transaction (for BEGIN TRANSACTION result)
     * @param affected
     * @param transaction
     */
    public SQLResult(long affected, Transaction transaction) {
        this.affected = affected;
        this.columns = null;
        this.cursor = null;
        this.transaction = transaction;
    }
}
