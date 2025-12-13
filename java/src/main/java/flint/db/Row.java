/**
 * FlintDB Row Implementation
 * Provides row-level data access and manipulation functionality for database operations.
 */
package flint.db;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;

/**
 * Interface representing a database row with typed column access and manipulation capabilities.
 * Provides comprehensive data type conversion, casting, and comparison operations for database rows.
 * 
 * This interface defines the contract for row-level database operations including:
 * - Row creation from various data sources (maps, arrays, other rows)
 * - Type-safe column value access and modification
 * - Data type conversion and casting between database types
 * - Row comparison and string representation
 */
public interface Row {
	static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	static final DateTimeFormatter DATE_TIME_FORMAT_MS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
	static final DateTimeFormatter DATE_TIME_FORMAT_MS3 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	static final ZoneId ZONE_ID = ZoneId.systemDefault();


	/**
	 * Creates a new Row instance from a map of column names to values.
	 * 
	 * @param meta the table metadata defining column structure and types
	 * @param va map containing column name-value pairs
	 * @return new Row instance with properly typed column values
	 */
	public static Row create(final Meta meta, final Map<String, Object> va) {
		final Column[] columns = meta.columns();
		final Object[] array = new Object[columns.length];
		for (int i = 0; i < array.length; i++) {
			final Column c = columns[i];
			final String name = Column.normalize(c.name());
			final Object v = va.containsKey(name) ? va.get(name) : c.value();
			array[i] = cast(v, c.type(), c.precision());
			// System.out.println(v + ", " + name + ", " + c.type());
		}
		return new RowImpl(meta, array);
	}

	/**
	 * Creates a new Row instance from an object array.
	 * Values are cast to appropriate types based on column definitions.
	 * 
	 * @param meta the table metadata defining column structure and types
	 * @param array object array containing column values in order
	 * @return new Row instance with properly typed column values
	 */
	public static Row create(final Meta meta, final Object[] array) {
		final Column[] columns = meta.columns();
		final Object[] a = new Object[columns.length];
		final int n = Math.min(columns.length, array.length);
		// if (columns.length != array.length)
		// System.err.println("columns.length : " + columns.length + ", array.length : " + array.length);
		for (int i = 0; i < n; i++) {
			final Column c = columns[i];
			final Object v = array[i];
			a[i] = cast(v, c.type(), c.precision());
		}
		return new RowImpl(meta, a);
	}

	/**
	 * Creates a new Row instance by copying column values from another row.
	 * Performs type casting based on the target table metadata.
	 * 
	 * @param meta the table metadata defining target column structure and types
	 * @param r source row to copy values from
	 * @return new Row instance with values copied and cast from source row
	 */
	public static Row create(final Meta meta, final Row r) {
		final Column[] columns = meta.columns();
		final Object[] array = new Object[columns.length];
		for (int i = 0; i < columns.length; i++) {
			final Column c = columns[i];
			final String name = Column.normalize(c.name());
			final Object v = r.get(name);
			array[i] = cast(v, c.type(), c.precision());
		}
		return new RowImpl(meta, array);
	}

	/**
	 * Creates a new Row instance with default column values.
	 * Uses default values defined in the table metadata for each column.
	 * 
	 * @param meta the table metadata defining column structure, types, and default values
	 * @return new Row instance with default column values
	 */
	public static Row create(final Meta meta) {
		final Column[] columns = meta.columns();
		final Object[] array = new Object[columns.length];
		for (int i = 0; i < array.length; i++) {
			final Column c = columns[i];
			final Object v = c.value();
			array[i] = cast(v, c.type(), c.precision());
		}
		return new RowImpl(meta, array);
	}

	/**
	 * Converts a variable argument list to a LinkedHashMap for row creation.
	 * Arguments should be provided as alternating key-value pairs.
	 * 
	 * @param va variable arguments as alternating column names and values
	 * @return LinkedHashMap with normalized column names as keys and values
	 */
	public static Map<String, Object> mapify(final Object... va) {
		final Map<String, Object> m = new java.util.LinkedHashMap<>();
		final int l = (va.length >> 1); // final int l = (int) Math.floor(va.length / 2);
		for (int i = 0; i < l; i++) {
			m.put(Column.normalize(va[i * 2].toString()), va[i * 2 + 1]);
		}
		return m;
	}

	/**
	 * Returns the internal object array containing all column values.
	 * 
	 * @return object array with column values in column order
	 */
	Object[] array();

	/**
	 * Returns the number of columns in this row.
	 * 
	 * @return column count
	 */
	int size();

	/**
	 * Gets the unique row identifier.
	 * 
	 * @return row id
	 */
	long id();

	/**
	 * Sets the unique row identifier.
	 * 
	 * @param id row id
	 */
	void id(final long id);

	/**
	 * Returns the table metadata associated with this row.
	 * 
	 * @return table metadata containing column definitions
	 */
	Meta meta();

	/**
	 * Creates a deep copy of this row.
	 * 
	 * @return new Row instance with identical values
	 */
	Row copy();

	/**
	 * Populates a map with this row's column values.
	 * 
	 * @param dest destination map to populate
	 * @return the populated destination map
	 */
	<T extends Map<String, Object>> T map(final T dest);


	Map<String, Object> map();

	/**
	 * Checks if this row contains a column with the specified name.
	 * 
	 * @param name column name to check
	 * @return true if column exists, false otherwise
	 */
	boolean contains(final String name);

	/**
	 * Gets the value of the column at the specified index.
	 * 
	 * @param i column index
	 * @return column value
	 */
	Object get(final int i);

	/**
	 * Gets the value of the column with the specified name.
	 * 
	 * @param name column name
	 * @return column value
	 */
	Object get(final String name);

	/**
	 * Sets the value of the column at the specified index.
	 * 
	 * @param i column index
	 * @param value new column value
	 */
	void set(final int i, final Object value);

	/**
	 * Sets the value of the column with the specified name.
	 * 
	 * @param name column name
	 * @param value new column value
	 */
	void set(final String name, final Object value);

	/**
	 * Gets the String value of the column at the specified index.
	 * 
	 * @param i column index
	 * @return String value or null
	 */
	String getString(final int i);

	/**
	 * Gets the String value of the column with the specified name.
	 * 
	 * @param name column name
	 * @return String value or null
	 */
	String getString(final String name);

	/**
	 * Gets the Integer value of the column at the specified index.
	 * 
	 * @param i column index
	 * @return Integer value or null
	 */
	Integer getInt(final int i);

	/**
	 * Gets the Integer value of the column with the specified name.
	 * 
	 * @param name column name
	 * @return Integer value or null
	 */
	Integer getInt(final String name);

	/**
	 * Gets the Long value of the column at the specified index.
	 * 
	 * @param i column index
	 * @return Long value or null
	 */
	Long getLong(final int i);

	/**
	 * Gets the Long value of the column with the specified name.
	 * 
	 * @param name column name
	 * @return Long value or null
	 */
	Long getLong(final String name);

	/**
	 * Gets the Double value of the column at the specified index.
	 * 
	 * @param i column index
	 * @return Double value or null
	 */
	Double getDouble(final int i);

	/**
	 * Gets the Double value of the column with the specified name.
	 * 
	 * @param name column name
	 * @return Double value or null
	 */
	Double getDouble(final String name);

	/**
	 * Gets the Float value of the column at the specified index.
	 * 
	 * @param i column index
	 * @return Float value or null
	 */
	Float getFloat(final int i);

	/**
	 * Gets the Float value of the column with the specified name.
	 * 
	 * @param name column name
	 * @return Float value or null
	 */
	Float getFloat(final String name);

	/**
	 * Gets the Short value of the column at the specified index.
	 * 
	 * @param i column index
	 * @return Short value or null
	 */
	Short getShort(final int i);

	/**
	 * Gets the Short value of the column with the specified name.
	 * 
	 * @param name column name
	 * @return Short value or null
	 */
	Short getShort(final String name);

	/**
	 * Gets the Byte value of the column at the specified index.
	 * 
	 * @param i column index
	 * @return Byte value or null
	 */
	Byte getByte(final int i);

	/**
	 * Gets the Byte value of the column with the specified name.
	 * 
	 * @param name column name
	 * @return Byte value or null
	 */
	Byte getByte(final String name);

	/**
	 * Gets the BigDecimal value of the column at the specified index.
	 * 
	 * @param i column index
	 * @return BigDecimal value or null
	 */
	BigDecimal getBigDecimal(final int i);

	/**
	 * Gets the BigDecimal value of the column with the specified name.
	 * 
	 * @param name column name
	 * @return BigDecimal value or null
	 */
	BigDecimal getBigDecimal(final String name);

	/**
	 * Gets the Date value of the column at the specified index.
	 * 
	 * @param i column index
	 * @return Date value or null
	 */
	Date getDate(final int i);

	/**
	 * Gets the Date value of the column with the specified name.
	 * 
	 * @param name column name
	 * @return Date value or null
	 */
	Date getDate(final String name);

	/**
	 * Gets the byte array value of the column at the specified index.
	 * 
	 * @param i column index
	 * @return byte array or null
	 */
	byte[] getBytes(final int i);

	/**
	 * Gets the byte array value of the column with the specified name.
	 * 
	 * @param name column name
	 * @return byte array or null
	 */
	byte[] getBytes(final String name);

	//

	/**
	 * Casts an object value to the specified database type with precision.
	 * Handles type conversion between Java types and database column types.
	 * 
	 * @param v the value to cast
	 * @param type the target database type constant
	 * @param precision the precision for decimal types
	 * @return the cast value in the appropriate Java type
	 * @throws IllegalArgumentException if the cast cannot be performed
	 */
	// @ForceInline
	static Object cast(final Object v, final short type, final short precision) {
		// Null check - most frequent early exit
		if (v == null) return null;

		// Fast path: if already correct type, return as-is for most common cases
		final Class<?> vClass = v.getClass();
		
		try {
			switch (type) {
			// Most frequently used types first with optimized conversions
			case Column.TYPE_STRING:
				return v; // No conversion needed, just return the object

			case Column.TYPE_INT:
				if ("".equals(v)) return null; // Handle empty string case
				if (vClass == Integer.class) return v;
				if (vClass == Long.class) return ((Long) v).intValue();
				if (vClass == Double.class) return ((Double) v).intValue();
				if (vClass == Float.class) return ((Float) v).intValue();
				if (vClass == Short.class) return ((Short) v).intValue();
				if (vClass == Byte.class) return ((Byte) v).intValue();
				return new BigDecimal(v.toString()).intValue();

			case Column.TYPE_INT64:
				if ("".equals(v)) return null; // Handle empty string case
				if (vClass == Long.class) return v;
				if (vClass == Integer.class) return ((Integer) v).longValue();
				if (vClass == Double.class) return ((Double) v).longValue();
				if (vClass == Float.class) return ((Float) v).longValue();
				if (vClass == Short.class) return ((Short) v).longValue();
				if (vClass == Byte.class) return ((Byte) v).longValue();
				return new BigDecimal(v.toString()).longValue();

			case Column.TYPE_UINT:
				if ("".equals(v)) return null; // Handle empty string case
				if (vClass == Long.class) return v;
				if (vClass == Integer.class) return ((Integer) v).longValue();
				return new BigDecimal(v.toString()).longValue();

			case Column.TYPE_DOUBLE:
				if ("".equals(v)) return null; // Handle empty string case
				if (vClass == Double.class) return v;
				if (vClass == Float.class) return ((Float) v).doubleValue();
				if (vClass == Long.class) return ((Long) v).doubleValue();
				if (vClass == Integer.class) return ((Integer) v).doubleValue();
				return new BigDecimal(v.toString()).doubleValue();

			case Column.TYPE_FLOAT:
				if ("".equals(v)) return null; // Handle empty string case
				if (vClass == Float.class) return v;
				if (vClass == Double.class) return ((Double) v).floatValue();
				if (vClass == Long.class) return ((Long) v).floatValue();
				if (vClass == Integer.class) return ((Integer) v).floatValue();
				if (vClass == String.class) return Float.parseFloat((String) v);
				return new BigDecimal(v.toString()).floatValue();

			case Column.TYPE_INT16:
				if ("".equals(v)) return null; // Handle empty string case
				if (vClass == Short.class) return v;
				if (vClass == Integer.class) return ((Integer) v).shortValue();
				if (vClass == Long.class) return ((Long) v).shortValue();
				if (vClass == Byte.class) return ((Byte) v).shortValue();
				return new BigDecimal(v.toString()).shortValue();

			case Column.TYPE_UINT16:
				if ("".equals(v)) return null; // Handle empty string case
				if (vClass == Integer.class) return v;
				if (vClass == Short.class) return ((Short) v).intValue() & 0xFFFF;
				if (vClass == Long.class) return ((Long) v).intValue();
				return new BigDecimal(v.toString()).intValue();

			case Column.TYPE_INT8:
				if ("".equals(v)) return null; // Handle empty string case
				if (vClass == Byte.class) return v;
				if (vClass == Short.class) return ((Short) v).byteValue();
				if (vClass == Integer.class) return ((Integer) v).byteValue();
				return new BigDecimal(v.toString()).byteValue();

			case Column.TYPE_UINT8:
				if ("".equals(v)) return null; // Handle empty string case
				if (vClass == Short.class) return v;
				if (vClass == Byte.class) return (short) (((Byte) v) & 0xFF);
				if (vClass == Integer.class) return ((Integer) v).shortValue();
				return new BigDecimal(v.toString()).shortValue();

			// Date/Time types
			case Column.TYPE_DATE:
			case Column.TYPE_TIME:
				if (vClass == java.sql.Timestamp.class)
					return v;
				if (vClass == java.sql.Date.class || vClass == java.util.Date.class) 
					return new java.sql.Timestamp(((Date)v).getTime());
				return date(v.toString());

			// Complex types
			case Column.TYPE_UUID:
			case Column.TYPE_IPV6:
				if (vClass == ULONGLONG.class) return v;
				return ULONGLONG.decodeHexString(v.toString());

			case Column.TYPE_DECIMAL:
				if ("".equals(v)) return null; // Handle empty string case
				if (vClass == BigDecimal.class)
					return ((BigDecimal) v).setScale(precision, RoundingMode.FLOOR);
				if (vClass == BigInteger.class)
					return new BigDecimal((BigInteger) v).setScale(precision, RoundingMode.FLOOR);
				if (vClass == ULONGLONG.class)
					return ((ULONGLONG) v).asBigDecimal().setScale(precision, RoundingMode.FLOOR);
				if (v instanceof byte[])
					return new BigDecimal(new BigInteger((byte[]) v)).setScale(precision, RoundingMode.FLOOR);
				if (v instanceof long[])
					return new BigDecimal(new BigInteger(new ULONGLONG((long[]) v).asBytes())).setScale(precision, RoundingMode.FLOOR);
				// For numeric types, avoid string conversion
				if (vClass == Long.class || vClass == Integer.class || vClass == Double.class || vClass == Float.class)
					return BigDecimal.valueOf(((Number) v).doubleValue()).setScale(precision, RoundingMode.FLOOR);
				return new BigDecimal(v.toString()).setScale(precision, RoundingMode.FLOOR);

			case Column.TYPE_BYTES:
				if (v instanceof byte[]) return v;
				return IO.Hex.decode(v.toString());

			case Column.TYPE_BLOB:
			case Column.TYPE_OBJECT:
				throw new RuntimeException("NOT SUPPORTED YET " + Column.typename(type));
			}

			return v;
		} catch (NumberFormatException nfe) {
			// More specific error for number format issues
			throw new IllegalArgumentException("Invalid number format for type [" + Column.typename(type) + "]: " + v, nfe);
		} catch (Throwable ex) {
			throw new IllegalArgumentException("Type Cast : [" + Column.typename(type) + "] => " + v + " (" + v.getClass() + ")" + "]", ex);
		}
	}

	/**
	 * Converts an object value to its string representation based on the specified database type.
	 * Handles proper string formatting for different data types including dates and numeric values.
	 * 
	 * @param v the value to convert to string
	 * @param type the database type constant for proper formatting
	 * @return string representation of the value
	 * @throws IllegalArgumentException if the conversion cannot be performed
	 */
	static String string(final Object v, final short type) {
		// throw new RuntimeException("CAST");
		if (v == null)
			return null;

		try {
			switch (type) {
			case Column.TYPE_INT:
				return v.toString();

			case Column.TYPE_UINT:
				return new BigDecimal(v.toString()).toString();

			case Column.TYPE_INT8:
				return new BigDecimal(v.toString()).toString();
			case Column.TYPE_UINT8:
				return new BigDecimal(v.toString()).toString();

			case Column.TYPE_INT16:
				return new BigDecimal(v.toString()).toString();
			case Column.TYPE_UINT16:
				return new BigDecimal(v.toString()).toString();

			case Column.TYPE_INT64:
				return new BigDecimal(v.toString()).toString();
			case Column.TYPE_DOUBLE:
				return new BigDecimal(v.toString()).toString();
			case Column.TYPE_FLOAT:
				return new BigDecimal(v.toString()).toString();

			case Column.TYPE_DATE:
				return (v instanceof Date) ? LocalDateTime.ofInstant(((Date) v).toInstant(), ZONE_ID).format(DATE_FORMAT) : (v.toString());
			case Column.TYPE_TIME:
				return (v instanceof Date) ? LocalDateTime.ofInstant(((Date) v).toInstant(), ZONE_ID).format(DATE_TIME_FORMAT) : (v.toString());
			case Column.TYPE_UUID:
				return v.toString();

			case Column.TYPE_IPV6:
				return (v instanceof ULONGLONG) ? ((ULONGLONG) v).asIPV6().toString() : v.toString();

			case Column.TYPE_STRING:
				return v.toString();
			case Column.TYPE_DECIMAL:
				return (v instanceof BigDecimal) ? v.toString() : v.toString();
			case Column.TYPE_BYTES:
				return (v instanceof byte[]) ? IO.Hex.encode((byte[]) v) : v.toString();

			case Column.TYPE_BLOB:
			case Column.TYPE_OBJECT:
				throw new RuntimeException("NOT SUPPORTED YET " + Column.typename(type));
			}

			return v.toString();
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new IllegalArgumentException("Type String Cast : [" + Column.typename(type) + "] => " + v + " (" + (v == null ? "<NULL>" : v.getClass()) + ")" + "]");
		}
	}

	/**
	 * Parses a date string into a Date object using various supported formats.
	 * Supports formats: yyyy-MM-dd, yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm:ss.S, yyyy-MM-dd HH:mm:ss.SSS
	 * 
	 * @param s the date string to parse
	 * @return Date object as Timestamp or null if format not recognized
	 * @throws RuntimeException if parsing fails
	 */
	static Date date(final String s) { 
		try {
			switch (s.length()) {
			case 10:
				// return new java.sql.Timestamp(LocalDate.parse(s, DATE_FORMAT).atStartOfDay(ZONE_ID).toInstant().toEpochMilli());
				return java.sql.Timestamp.from(LocalDate.parse(s, DATE_FORMAT).atStartOfDay(ZONE_ID).toInstant());
			case 19:
				// return new java.sql.Timestamp(LocalDateTime.parse(s, DATE_TIME_FORMAT).atZone(ZONE_ID).toInstant().toEpochMilli());
				return java.sql.Timestamp.from(LocalDateTime.parse(s, DATE_TIME_FORMAT).atZone(ZONE_ID).toInstant());
			case 21:
				// return new java.sql.Timestamp(LocalDateTime.parse(s, DATE_TIME_FORMAT_MS).atZone(ZONE_ID).toInstant().toEpochMilli());
				return java.sql.Timestamp.from(LocalDateTime.parse(s, DATE_TIME_FORMAT_MS).atZone(ZONE_ID).toInstant());
			case 23:
				// return new java.sql.Timestamp(LocalDateTime.parse(s, DATE_TIME_FORMAT_MS3).atZone(ZONE_ID).toInstant().toEpochMilli());
				return java.sql.Timestamp.from(LocalDateTime.parse(s, DATE_TIME_FORMAT_MS3).atZone(ZONE_ID).toInstant());
			}
			return null;
		} catch (Exception ex) {
			throw new RuntimeException("date(" + s + ") : " + ex.getMessage());
		}
	}


	/**
	 * Compares two rows based on the specified column indices for sorting purposes.
	 * ULTRA-OPTIMIZED VERSION: Maximum performance with minimal overhead
	 * 
	 * @param keys array of column indices to compare
	 * @param r1 first row to compare
	 * @param r2 second row to compare
	 * @return negative if r1 < r2, zero if equal, positive if r1 > r2
	 * @throws Exception if comparison fails
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static int compareTo(final byte[] keys, final Row r1, final Row r2) {
		// Minimize virtual dispatch by direct array access
		final Object[] a1 = r1.array();
		final Object[] a2 = r2.array();
		for (int i = 0, n = keys.length; i < n; i++) {
			final Object v1 = a1[keys[i]];
			final Object v2 = a2[keys[i]];

			// Same reference or both null
			if (v1 == v2) continue;

			// Null handling: null is greater (sorted last)
			if (v1 == null) return 1;
			if (v2 == null) return -1;

			final Class<?> c1 = v1.getClass();
			final Class<?> c2 = v2.getClass();

			// Fast path: exact same wrapper class
			if (c1 == c2) {
				if (c1 == Integer.class) {
					final int x = (Integer) v1, y = (Integer) v2;
					if (x != y) return (x < y) ? -1 : 1;
					continue;
				}
				if (c1 == String.class) {
					final int cmp = ((String) v1).compareTo((String) v2);
					if (cmp != 0) return cmp;
					continue;
				}
				if (c1 == BigDecimal.class) {
					final int cmp = ((BigDecimal) v1).compareTo((BigDecimal) v2);
					if (cmp != 0) return cmp;
					continue;
				}
				if (c1 == Long.class) {
					final long x = (Long) v1, y = (Long) v2;
					if (x != y) return (x < y) ? -1 : 1;
					continue;
				}
				if (c1 == Short.class) {
					final short x = (Short) v1, y = (Short) v2;
					if (x != y) return (x < y) ? -1 : 1;
					continue;
				}
				if (c1 == Double.class) {
					final int cmp = Double.compare((Double) v1, (Double) v2);
					if (cmp != 0) return cmp;
					continue;
				}
				if (c1 == Float.class) {
					final int cmp = Float.compare((Float) v1, (Float) v2);
					if (cmp != 0) return cmp;
					continue;
				}
				if (c1 == byte[].class) {
					final int cmp = java.util.Arrays.compare((byte[]) v1, (byte[]) v2);
					if (cmp != 0) return cmp;
					continue;
				}
				if (v1 instanceof Comparable) {
					final int cmp = ((Comparable) v1).compareTo(v2);
					if (cmp != 0) return cmp;
					continue;
				}
			}

			// Mixed numeric types fast path (e.g., Integer vs Long vs Double)
			if (v1 instanceof Number && v2 instanceof Number) {
				final boolean fp = (v1 instanceof Double) || (v1 instanceof Float) || (v2 instanceof Double) || (v2 instanceof Float)
						|| (v1 instanceof BigDecimal) || (v2 instanceof BigDecimal);
				if (fp) {
					final BigDecimal b1 = (v1 instanceof BigDecimal) ? (BigDecimal) v1 : BigDecimal.valueOf(((Number) v1).doubleValue());
					final BigDecimal b2 = (v2 instanceof BigDecimal) ? (BigDecimal) v2 : BigDecimal.valueOf(((Number) v2).doubleValue());
					final int cmp = b1.compareTo(b2);
					if (cmp != 0) return cmp;
				} else {
					final long x = ((Number) v1).longValue();
					final long y = ((Number) v2).longValue();
					if (x != y) return (x < y) ? -1 : 1;
				}
				continue;
			}

			// Byte array comparison (in case of different array implementations)
			if (v1 instanceof byte[] && v2 instanceof byte[]) {
				final int cmp = java.util.Arrays.compare((byte[]) v1, (byte[]) v2);
				if (cmp != 0) return cmp;
				continue;
			}

			// Generic Comparable fallback (handle Date, custom types)
			if (v1 instanceof Comparable && v2 instanceof Comparable) {
				try {
					final int cmp = ((Comparable) v1).compareTo(v2);
					if (cmp != 0) return cmp;
					continue;
				} catch (ClassCastException ignore) {
					// fall through to string compare
				}
			}

			// Last resort: string comparison (stable but slower)
			final int cmp = v1.toString().compareTo(v2.toString());
			if (cmp != 0) return cmp;
		}
		return 0;
	}
	
	/**
	 * Returns a string representation of this row using the specified delimiter.
	 * 
	 * @param delimiter the delimiter to use between column values
	 * @return string representation of the row
	 */
	public abstract String toString(final String delimiter);


    /**
     * Validates the row data against the table metadata constraints.
     * @return true if the row is valid, false otherwise
     */
    public boolean validate();
}
