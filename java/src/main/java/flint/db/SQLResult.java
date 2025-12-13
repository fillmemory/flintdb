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
    }
}
