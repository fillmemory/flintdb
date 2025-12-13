/**
 * FlintDB Cursor Implementation
 * Provides forward-only iteration over query results with automatic resource management.
 */
package flint.db;

/**
 * Interface for iterating over database query results in a forward-only manner.
 * Implements AutoCloseable for proper resource management and cleanup.
 * 
 * <p>The Cursor provides a simple iteration mechanism for accessing query results
 * one at a time, supporting both row ID iteration and direct row object access
 * depending on the generic type parameter.</p>
 * 
 * <p><strong>Usage Examples:</strong></p>
 * <pre>{@code
 * // Example 1: Iterate over row IDs
 * Cursor<Long> cursor = table.find("USE INDEX(PRIMARY DESC) WHERE A = '2023-01-01'");
 * try {
 *     for (long id; (id = cursor.next()) > -1;) {
 *         final Row row = table.read(id);
 *         // Process row data
 *     }
 * } finally {
 *     cursor.close(); // or use try-with-resources
 * }
 * 
 * // Example 2: Using try-with-resources (recommended)
 * try (Cursor<Long> cursor = table.find("SELECT * FROM users WHERE active = 1")) {
 *     for (long id; (id = cursor.next()) > -1;) {
 *         final Row row = table.read(id);
 *         System.out.println(row.toString(","));
 *     }
 * }
 * }</pre>
 * 
 * @param <T> the type of objects returned by this cursor (typically Long for row IDs or Row for direct access)
 */
public interface Cursor<T> extends AutoCloseable {
	/**
	 * Advances the cursor to the next element in the result set.
	 * 
	 * <p>This method moves the cursor forward one position and returns the current element.
	 * The cursor starts before the first element, so the first call to {@code next()}
	 * returns the first element in the result set.</p>
	 * 
	 * <p><strong>Return Value Semantics:</strong></p>
	 * <ul>
	 *   <li>For {@code Cursor<Long>}: Returns the row ID (positive value) or -1 when no more rows</li>
	 *   <li>For {@code Cursor<Row>}: Returns the Row object or null when no more rows</li>
	 * </ul>
	 * 
	 * @return the next element of type T, or a sentinel value indicating end of results
	 *         (typically -1 for numeric types, null for object types)
	 */
	T next();
}
