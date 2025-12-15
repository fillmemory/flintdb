/**
 * Provides aggregation functions and grouping capabilities
 * Implements SQL GROUP BY and aggregate functions (SUM, COUNT, AVG, etc.)
 */
package flint.db;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;


/**
 * Main class for aggregation operations (Function and Groupby processing)
 * Processes database aggregation queries in memory and returns results
 */
public final class Aggregate implements AutoCloseable {
	/** Identifier for the aggregation operation */
	private final String id;
	/** Grouping criteria columns */
	private final Groupby[] groupby;
	/** Aggregate functions to execute */
	private final Function[] exprs;
	/** Set to store group keys */
	private Set<GROUPKEY> keys = new TreeSet<>();
	/** Lock for concurrency control */
	private final Lock lock = new java.util.concurrent.locks.ReentrantLock();

	/**
	 * Aggregate constructor (using default ID)
	 * @param groupby Array of grouping criteria
	 * @param exprs Array of aggregate functions
	 */
	public Aggregate(final Groupby[] groupby, final Function[] exprs) {
		this("", groupby, exprs);
	}

	/**
	 * Aggregate constructor (with specified ID)
	 * @param id Aggregation operation identifier
	 * @param groupby Array of grouping criteria
	 * @param exprs Array of aggregate functions
	 */
	public Aggregate(final String id, final Groupby[] groupby, final Function[] exprs) {
		this.id = id;
		this.groupby = groupby;
		this.exprs = exprs;
	}

	/**
	 * Returns the aggregation operation ID
	 * @return Aggregation ID
	 */
	public String id() {
		return id;
	}

	/**
	 * Resource cleanup (calls close method of all functions)
	 */
	@Override
	public void close() {
		for (Function f : exprs) {
			try {
				f.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * String representation of aggregation information
	 * @return String containing GROUP BY and FUNCTIONS information
	 */
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		if (groupby != null && groupby.length > 0) {
			s.append("GROUP BY [");
			for (int i = 0; i < groupby.length; i++) {
				if (i > 0)
					s.append(", ");
				s.append(groupby[i].alias);
			}
			s.append("]");
		}

		s.append(" FUNCTIONS [");
		for(int i = 0; i < exprs.length; i++) {
			if (i > 0)
				s.append(", ");
			s.append(exprs[i].alias());
		}
		s.append("]");
		return s.toString().trim();
	}

	/**
	 * Returns temporary directory path
	 * @return Temporary directory for aggregation operations
	 */
	public static File tempdir() {
		return new File("temp", Aggregate.class.getSimpleName().toLowerCase());
	}

	/**
	 * Generates a random positive integer
	 * @return Random integer between 0 and Integer.MAX_VALUE
	 */
	private static int randomUInt() {
		final int min = 0;
		final int max = Integer.MAX_VALUE;
		return new SecureRandom().ints(min, max).findFirst().getAsInt();
	}

	/**
	 * Class defining grouping criteria
	 * Defines columns and processing methods used in SQL GROUP BY clause
	 */
	public static class Groupby {
		/** Alias used in results */
		protected String alias;
		/** Actual column name */
		protected String column;
		/** Data type */
		protected short type;
		/** Handler for value processing */
		protected final Handler handler;

		/**
		 * Groupby constructor (using default handler)
		 * @param column Column name (also used as alias)
		 * @param type Data type
		 */
		public Groupby(final String column, final short type) {
			this(column, column, type, DEFAULT);
		}

		/**
		 * Groupby constructor (with specified alias, using default handler)
		 * @param alias Alias used in results
		 * @param column Actual column name
		 * @param type Data type
		 */
		public Groupby(final String alias, final String column, final short type) {
			this(alias, column, type, DEFAULT);
		}

		/**
		 * Groupby constructor (with specified handler)
		 * @param column Column name (also used as alias)
		 * @param type Data type
		 * @param handler Value processing handler
		 */
		public Groupby(final String column, final short type, final Handler handler) {
			this(column, column, type, handler);
		}

		/**
		 * Groupby constructor (with all parameters specified)
		 * @param alias Alias used in results
		 * @param column Actual column name
		 * @param type Data type
		 * @param handler Value processing handler
		 */
		public Groupby(final String alias, final String column, final short type, final Handler handler) {
			this.alias = alias;
			this.column = Column.normalize(column);
			this.type = type;
			this.handler = handler != null ? handler : DEFAULT;
		}

		/**
		 * Returns column name (using alias)
		 * @return Column alias
		 */
		String column() {
			// return column;
			return alias;
		};

		/**
		 * Returns data type
		 * @return Column data type
		 */
		short type() {
			return type;
		}

		/**
		 * Gets the value of this column from a row
		 * @param r Data row
		 * @return Column value
		 */
		public Object get(final Row r) {
			// return r.get(column);
			return handler.get(column, r);
		}

		/**
		 * Handler interface for value processing
		 */
		public interface Handler {
			/**
			 * Extracts value of a specific column from a row
			 * @param column Column name
			 * @param r Data row
			 * @return Extracted value
			 */
			Object get(final String column, final Row r);
		}

		/** Default handler - simply returns column value */
		public static final Handler DEFAULT = (final String column, final Row r) -> r.get(column);

		/**
		 * Handler for time-based grouping
		 * Formats dates to specified format for use as grouping keys
		 */
		public static final class Time implements Handler {
			/** Date formatter */
			protected final DateTimeFormatter fmt;
			protected final ZoneId zoneId = ZoneId.systemDefault();

			/**
			 * Time handler constructor
			 * @param fmt Date format string
			 */
			public Time(final String fmt) {
				this.fmt = DateTimeFormatter.ofPattern(fmt);
			}

			/**
			 * Converts date column to formatted string
			 * @param column Column name
			 * @param r Data row
			 * @return Formatted date string or "<NULL>"
			 */
			@Override
			public Object get(final String column, final Row r) {
				Date d = r.getDate(column);
				return d == null ? "<NULL>" : LocalDateTime.ofInstant(d.toInstant(), zoneId).format(fmt);
			}
		}

		@Override
		public String toString() {
			return "Groupby [alias=" + alias + ", column=" + column + ", type=" + Column.typename(type) + "]";
		}
	}

	/**
	 * Interface for condition checking
	 * Used to define and evaluate WHERE clause conditions
	 */
	public static interface Condition {
		/**
		 * Checks if the given row satisfies the condition
		 * @param r Row to check
		 * @return Whether condition is satisfied
		 */
		boolean ok(Row r);

		/** Condition that always returns true */
		static final Condition True = (final Row r) -> true;

		/**
		 * Connects multiple conditions with AND
		 * @param va Conditions to connect
		 * @return Condition that returns true only when all conditions are true
		 */
		static Condition and(final Condition... va) {
			return (final Row r) -> {
                            for (final Condition c : va) {
                                if (!c.ok(r))
                                    return false;
                            }
                            return va.length > 0;
                        };
		}

		/**
		 * Connects multiple conditions with OR
		 * @param va Conditions to connect
		 * @return Condition that returns true if any condition is true
		 */
		static Condition or(final Condition... va) {
			return (final Row r) -> {
                            for (final Condition c : va) {
                                if (c.ok(r))
                                    return true;
                            }
                            return false;
                        };
		}
	}

	/**
	 * Checks if a date is within the specified range
	 * @param d Date to check
	 * @param s Start date
	 * @param e End date
	 * @return Whether date is within range
	 */
	public static boolean between(final Date d, final Date s, final Date e) {
		final long ts = d.getTime();
		return s.getTime() <= ts && ts <= e.getTime();
	}

	/**
	 * Checks if a string is within the specified range (lexicographic order)
	 * @param d String to check
	 * @param s Start string
	 * @param e End string
	 * @return Whether string is within range
	 */
	public static boolean between(final String d, final String s, final String e) {
		return d.compareTo(s) >= 0 && d.compareTo(e) <= 0;
	}

	/**
	 * Joins array elements with delimiter to create string
	 * @param delim Delimiter
	 * @param a Object array to join
	 * @return Joined string
	 */
	private static String join(final String delim, final Object[] a) {
		StringBuilder s = new StringBuilder();
		for (int i = 0; i < a.length; i++) {
			if (i > 0)
				s.append(delim);
			s.append(a[i]);
		}
		return s.toString();
	}

	/**
	 * Abstract base class for aggregate functions
	 * Parent class for concrete aggregate functions like SUM, COUNT, AVG, etc.
	 */
	public static abstract class Function implements AutoCloseable {
		/** Target columns */
		String[] columns;
		/** Result column alias */
		String alias;
		/** Result data type */
		short type;
		/** Application condition */
		Condition condition;

		/**
		 * Function constructor
		 * @param alias Result column alias
		 * @param columns Target column array
		 * @param type Result data type
		 * @param condition Application condition
		 */
		public Function(final String alias, final String[] columns, final short type, final Condition condition) {
			this.alias = alias;
			this.columns = (columns == null || columns.length == 0 || columns[0] == null) ? new String[] { alias } : normalize(columns);
			this.type = type;
			this.condition = condition != null ? condition : new Condition() {
				@Override
				public boolean ok(final Row r) {
					return true;
				}
			};
		}

		/**
		 * Normalizes column names
		 * @param columns Original column name array
		 * @return Normalized column name array
		 */
		private static String[] normalize(String[] columns) {
			String[] normalized = new String[columns.length];
			for (int i = 0; i < columns.length; i++) {
				normalized[i] = Column.normalize(columns[i]);
			}
			return normalized;
		}

		/** Returns alias */
		protected String alias() {
			return alias;
		}

		/** Returns column array */
		protected String[] columns() {
			return columns;
		}

		/** Returns type */
		protected short type() {
			return type;
		}

		/** Checks condition */
		protected boolean condition(final Row r) {
			return condition.ok(r);
		}

		/**
		 * Processes row data (overridden in each implementation)
		 * @param key Group key
		 * @param r Row to process
		 */
		public abstract void row(final GROUPKEY key, final Row r);

		/**
		 * Calculates aggregate result (overridden in each implementation)
		 * @param key Group key
		 * @return Calculated aggregate value
		 */
		public abstract Object compute(final GROUPKEY key);

		public Object object(final GROUPKEY key) {
			return null;
		}

		/**
		 * Returns decimal precision
		 * @return Default precision 5
		 */
		public short precision() {
			return 5; // Default precision for decimal values
		}

		/**
		 * Returns default value
		 * @return null
		 */
		public Object defaultValue() {
			return null;
		}

		/**
		 * Converts null value to 0
		 * @param v BigDecimal value
		 * @return 0 if null, otherwise original value
		 */
		public static BigDecimal NOTNULL(final BigDecimal v) {
			return v == null ? BigDecimal.ZERO : v;
		}

		/**
		 * Converts null value to default value
		 * @param v Value to check
		 * @param d Default value
		 * @return Default value if null, otherwise original value
		 */
		public static <T> T IFNULL(T v, T d) {
			return v == null ? d : v;
		}

		/** Cleanup operation (default implementation is empty) */
		void purge() {
		}

		@Override
		public String toString() {
			return "Function [alias=" + alias + ", columns=" + Arrays.toString(columns) + ", type=" + Column.typename(type) + "]";
		}
	}

	/**
	 * Abstract class for custom aggregate functions
	 */
	public static abstract class CUSTOM extends Function {
		/**
		 * CUSTOM function constructor
		 * @param alias Alias
		 * @param type Result type
		 */
		public CUSTOM(String alias, short type) {
			super(alias, null, type, null);
		}
	}

	/**
	 * SUM aggregate function - calculates sum of numeric column
	 */
	public static class SUM extends Function {
		/** @deprecated Multi-column support discontinued */
		@Deprecated
		private SUM(final String alias, final String[] columns, final Condition condition) {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
		}

		/**
		 * SUM function constructor
		 * @param alias Result column alias
		 * @param column Column name to sum
		 * @param condition Application condition
		 */
		public SUM(final String alias, final String column, final Condition condition) {
			super(alias, new String[] { column }, Column.TYPE_DECIMAL, condition);
		}

		/**
		 * SUM function constructor (alias and column name are the same)
		 * @param column Column name to sum
		 * @param condition Application condition
		 */
		public SUM(final String column, final Condition condition) {
			super(column, new String[] { column }, Column.TYPE_DECIMAL, condition);
		}

		/** Map storing sum values by group */
		private Map<GROUPKEY, BigDecimal> values = new HashMap<>();

		/**
		 * Processes row data - adds value to the group's sum
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
			try {
				// if (condition(r)) {
				Object v = r.get(columns()[0]);
				if (v == null)
					v = BigDecimal.ZERO;

				final BigDecimal p = NOTNULL(values.get(key));
				values.put(key, p.add(new BigDecimal(v.toString())));
				// }
			} catch (Exception ex) {
			}
		}

		/**
		 * Returns sum value by group
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			return NOTNULL(values.get(key));
		}

		/**
		 * Default value is 0
		 */
		@Override
		public Object defaultValue() {
			return BigDecimal.ZERO;
		}

		/**
		 * Resource cleanup
		 */
		@Override
		public void close() throws Exception {
			values.clear();
		}
	}

	/**
	 * AVG aggregate function - calculates average of numeric column
	 */
	public static class AVG extends Function {
		/**
		 * AVG function constructor
		 * @param alias Result column alias
		 * @param column Column name to average
		 * @param condition Application condition
		 */
		public AVG(final String alias, final String column, final Condition condition) {
			super(alias, new String[] { column }, Column.TYPE_DECIMAL, condition);
		}
		
		/** Map storing sum values by group */
		private Map<GROUPKEY, BigDecimal> values = new HashMap<>();
		/** Map storing counts by group */
		private Map<GROUPKEY, BigDecimal> counts = new HashMap<>();

		/**
		 * Processes row data - increments the group's sum and count
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
			try {
				Object v = r.get(columns()[0]);
				if (v == null)
					v = BigDecimal.ZERO;

				final BigDecimal p = NOTNULL(values.get(key));
				values.put(key, p.add(new BigDecimal(v.toString())));

				final BigDecimal c = NOTNULL(counts.get(key));
				counts.put(key, c.add(BigDecimal.ONE));
			} catch (Exception ex) {
			}
		}

		/**
		 * Calculates average value by group (sum / count)
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			final BigDecimal sum = NOTNULL(values.get(key));
			final BigDecimal count = NOTNULL(counts.get(key));
			final BigDecimal v = count.compareTo(BigDecimal.ZERO) == 0 
			? BigDecimal.ZERO 
			: sum.divide(count, 5, RoundingMode.HALF_UP);
			// System.out.println("AVG compute: " + key + " sum=" + sum + ", count=" + count + ", avg=" + v);
			return v;
		}

		/**
		 * Default value is 0
		 */
		@Override
		public Object defaultValue() {
			return BigDecimal.ZERO;
		}

		/**
		 * Resource cleanup
		 */
		@Override
		public void close() throws Exception {
			values.clear();
			counts.clear();
		}
	}

	/**
	 * COUNT aggregate function - calculates number of rows
	 */
	public static class COUNT extends Function {
		/**
		 * COUNT function constructor (total row count)
		 * @param alias Result column alias
		 * @param condition Application condition
		 */
		public COUNT(String alias, Condition condition) {
			super(alias, null, Column.TYPE_DECIMAL, condition);
		}

		/**
		 * COUNT function constructor (based on specific column)
		 * @param alias Result column alias
		 * @param column Column name to count
		 * @param condition Application condition
		 */
		public COUNT(String alias, String column, Condition condition) {
			super(alias, new String[] { column }, Column.TYPE_DECIMAL, condition);
		}

		/** Map storing counts by group */
		private Map<GROUPKEY, BigDecimal> values = new HashMap<>();

		/**
		 * Processes row data - increments the group's count by 1
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
			final BigDecimal p = NOTNULL(values.get(key));
			values.put(key, p.add(BigDecimal.ONE));
		}

		/**
		 * Returns count by group
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			return NOTNULL(values.get(key));
		}

		/**
		 * COUNT does not require precision
		 */
		@Override
		public short precision() {
			return 0; // COUNT does not require precision
		}

		/**
		 * Default value is 0
		 */
		@Override
		public Object defaultValue() {
			return BigDecimal.ZERO;
		}

		/**
		 * Resource cleanup
		 */
		@Override
		public void close() throws Exception {
			values.clear();
		}
	}

	/**
	 * Handler interface for distinct processing
	 * Used to efficiently handle duplicate values in DISTINCT COUNT, etc.
	 */
	static interface DISTINCT_HANDLER extends AutoCloseable {
		/** File-based memory map mode */
		static final int MM_FILE = 0;
		/** Auto selection mode */
		static final int MM_AUTO = 1;

		/**
		 * Processes row data
		 * @param key Group key
		 * @param distinct Values to check for duplicates
		 * @throws Exception Processing error
		 */
		void row(final GROUPKEY key, final Object[] distinct) throws Exception;

		/**
		 * Returns map of distinct counts by group
		 * @return Map of distinct counts
		 */
		Map<GROUPKEY, BigDecimal> values();

		/**
		 * Creates handler instance
		 * @param prefix File name prefix
		 * @param columns Target columns
		 * @param mm Memory map mode
		 * @return Created handler
		 * @throws IOException File creation error
		 */
		static DISTINCT_HANDLER create(final String prefix, final String[] columns, final int mm) throws IOException {
			final DISTINCT_HANDLER h = new DISTINCT_HANDLER_HASH(prefix, columns);
			return h;
		}
	}
	
	/**
	 * 
	 */
	private static final class DISTINCT_HANDLER_HASH implements DISTINCT_HANDLER {
		private final Map<GROUPKEY, BigDecimal> values = new HashMap<>();
		private final File file;
		private final HashFile h;

		DISTINCT_HANDLER_HASH(final String prefix, final String[] columns) throws IOException {
			final File temp = tempdir();
			this.file = new File(temp, //
					String.format("%s-DISTINCT-HASH-%s-%s-%d.aggregate", 
							LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")), 
							prefix, 
							randomUInt(), 
							ProcessHandle.current().pid() 
					)
			);
			temp.mkdirs();
			// file.deleteOnExit();

			final int BTREE_INCREMENT_SIZE = Integer.parseInt(System.getProperty(Meta.PRODUCT_NAME_LC + ".aggregate.increment.size", String.valueOf(1 * 1024 * 1024)));

			this.h = new HashFile(file, 1024 * 1024, true, BTREE_INCREMENT_SIZE, new HashFile.HashFunction() {
				@Override
				public int compare(Long key1, Long key2) {
					return Long.compare(key1, key2);
				}

				@Override
				public int hash(long key) {
					return (int) (key & Integer.MAX_VALUE);
				}
			});
		}

		@Override
		public void row(final GROUPKEY key, final Object[] distinct) throws Exception {
			final String s1 = join("-", distinct);
			// final ULONGLONG hash = new ULONGLONG(MurMurHash3.hash128x64(s1.getBytes()));
			// final long hash = s1.hashCode();
			final long hash = MurMurHash2.hash64(s1.getBytes());
			if (h.get(hash) == null) {
				h.put(hash);
				final BigDecimal p = Function.NOTNULL(values.get(key));
				values.put(key, p.add(BigDecimal.ONE));
			}
		}

		@Override
		public Map<GROUPKEY, BigDecimal> values() {
			return values;
		}

		@Override
		public void close() throws Exception {
			values.clear();
			if (h != null) {
				h.close();
				// System.out.println("DISTINCT close (" + file + " : " + IO.readableFileSize(file.length()));
				file.delete();
			}
		}
	}

	/**
	 * DISTINCT_COUNT aggregate function - counts distinct values in specified columns
	 * Uses hash-based deduplication to efficiently count unique value combinations
	 */
	public static class DISTINCT_COUNT extends Function {
		/** Handler for distinct value processing */
		private final DISTINCT_HANDLER handler;
		/** Set to track exception messages to avoid duplicate error logging */
		private final Set<String> exceptions = new java.util.HashSet<>();

		/**
		 * DISTINCT_COUNT function constructor (with auto memory mode)
		 * @param alias Result column alias
		 * @param columns Columns to count distinct values
		 * @param condition Application condition
		 * @throws IOException File creation error
		 */
		public DISTINCT_COUNT(final String alias, final String[] columns, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
			handler = DISTINCT_HANDLER.create(alias, columns, DISTINCT_HANDLER.MM_AUTO);
		}

		/**
		 * DISTINCT_COUNT function constructor (with specified memory mode)
		 * @param alias Result column alias
		 * @param columns Columns to count distinct values
		 * @param mm Memory map mode
		 * @param condition Application condition
		 * @throws IOException File creation error
		 */
		protected DISTINCT_COUNT(final String alias, final String[] columns, final int mm, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
			handler = DISTINCT_HANDLER.create(alias, columns, mm);
		}

		/**
		 * Processes row data - adds distinct value combination to handler
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
			try {
				// handler.row(key, r);
				final Object[] distinct = new Object[key.length() + columns.length];
				int i = 0;
				for (; i < key.length(); i++) {
					distinct[i] = key.get(i);
				}
				for (int j = 0; j < columns.length; j++, i++) {
					final String column = columns[j];
					distinct[i] = r.get(column);
				}
				handler.row(key, distinct);
			} catch (Exception ex) {
				if (!exceptions.contains(ex.getMessage())) {
					exceptions.add(ex.getMessage());
					ex.printStackTrace();
					// System.exit(0);
				}
			}
		}

		/**
		 * Returns distinct count by group
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			return NOTNULL(handler.values().get(key));
		}

		/**
		 * DISTINCT_COUNT does not require precision
		 */
		@Override
		public short precision() {
			return 0; // COUNT does not require precision
		}

		/**
		 * Default value is 0
		 */
		@Override
		public Object defaultValue() {
			return BigDecimal.ZERO;
		}

		/**
		 * Resource cleanup - closes handler
		 */
		@Override
		public void close() throws Exception {
			handler.close();
		}

		/**
		 * Cleanup operation - closes handler for temporary file cleanup
		 */
		@Override
		void purge() {
			try {
				handler.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 * DISTINCT_COALESCE_COUNT aggregate function - counts distinct values using COALESCE logic
	 * Similar to DISTINCT_COUNT but uses the first non-null value from multiple columns
	 */
	public static class DISTINCT_COALESCE_COUNT extends Function {
		/** Handler for distinct value processing */
		private final DISTINCT_HANDLER handler;
		/** Set to track exception messages to avoid duplicate error logging */
		private final Set<String> exceptions = new java.util.HashSet<>();

		/**
		 * DISTINCT_COALESCE_COUNT function constructor (with auto memory mode)
		 * @param alias Result column alias
		 * @param columns Columns to count distinct values (uses first non-null)
		 * @param condition Application condition
		 * @throws IOException File creation error
		 */
		public DISTINCT_COALESCE_COUNT(final String alias, final String[] columns, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
			handler = DISTINCT_HANDLER.create(alias, columns, DISTINCT_HANDLER.MM_AUTO);
		}

		/**
		 * DISTINCT_COALESCE_COUNT function constructor (with specified memory mode)
		 * @param alias Result column alias
		 * @param columns Columns to count distinct values (uses first non-null)
		 * @param mm Memory map mode
		 * @param condition Application condition
		 * @throws IOException File creation error
		 */
		protected DISTINCT_COALESCE_COUNT(final String alias, final String[] columns, final int mm, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
			handler = DISTINCT_HANDLER.create(alias, columns, mm);
		}

		/**
		 * Processes row data - uses first non-null value from the specified columns
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
			try {
				final Object[] distinct = new Object[key.length() + 1];
				int i = 0;
				for (; i < key.length(); i++) {
					distinct[i] = key.get(i);
				}
				for (int j = 0; j < columns.length; j++) {
					final String column = columns[j];
					final Object v = r.get(column);
					if (v != null) {
						distinct[distinct.length - 1] = v;
						break;
					}
				}
				handler.row(key, distinct);
			} catch (Exception ex) {
				if (!exceptions.contains(ex.getMessage())) {
					exceptions.add(ex.getMessage());
					ex.printStackTrace();
					// System.exit(0);
				}
			}
		}

		/**
		 * Returns distinct count by group
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			return NOTNULL(handler.values().get(key));
		}

		/**
		 * Default value is 0
		 */
		@Override
		public Object defaultValue() {
			return BigDecimal.ZERO;
		}

		/**
		 * Resource cleanup - closes handler
		 */
		@Override
		public void close() throws Exception {
			handler.close();
		}

		/**
		 * Cleanup operation - closes handler for temporary file cleanup
		 */
		@Override
		void purge() {
			try {
				handler.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 * DISTINCT_SET aggregate function - collects all distinct values in a set
	 * Returns a Set containing all unique values from the specified column
	 */
	public static class DISTINCT_SET extends Function {
		/**
		 * DISTINCT_SET function constructor
		 * @param alias Result column alias
		 * @param column Column to collect distinct values
		 * @param condition Application condition
		 * @throws IOException File creation error (not used in current implementation)
		 */
		public DISTINCT_SET(final String alias, final String column, final Condition condition) throws IOException {
			super(alias, new String[] { column }, Column.TYPE_STRING, condition);
		}

		/** Map storing sets of distinct values by group */
		private Map<GROUPKEY, Set<Object>> values = new java.util.TreeMap<>();

		/**
		 * Resource cleanup
		 */
		@Override
		public void close() throws Exception {
			values.clear();
		}

		/**
		 * Processes row data - adds value to the group's distinct set
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
			Object v = r.get(columns()[0]);
			try {
				Set<Object> p = values.get(key);
				if (p == null) {
					p = new TreeSet<>();
					values.put(key, p);
				}
				p.add(v);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		/**
		 * Returns set of distinct values by group
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			return values.get(key);
		}
	}

	/**
	 * DISTINCT_APPROX_COUNT aggregate function - approximate distinct count using HyperLogLog
	 * Provides approximate distinct count with lower memory usage than exact counting
	 */
	public static class DISTINCT_APPROX_COUNT extends Function {
		/** Map storing HyperLogLog instances by group */
		private final Map<GROUPKEY, HyperLogLog> values = new HashMap<>();
		/** Precision parameter for HyperLogLog (higher = more accurate but more memory) */
		private final int precision = 16;
		/** Set to track exception messages to avoid duplicate error logging */
		private final Set<String> exceptions = new java.util.HashSet<>();

		/**
		 * DISTINCT_APPROX_COUNT function constructor
		 * @param alias Result column alias
		 * @param columns Columns to count approximate distinct values
		 * @param condition Application condition
		 * @throws IOException File creation error (not used with HyperLogLog)
		 */
		public DISTINCT_APPROX_COUNT(final String alias, final String[] columns, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
		}

		/**
		 * DISTINCT_APPROX_COUNT function constructor (memory mode parameter ignored)
		 * @param alias Result column alias
		 * @param columns Columns to count approximate distinct values
		 * @param mm Memory map mode (ignored for HyperLogLog)
		 * @param condition Application condition
		 * @throws IOException File creation error (not used with HyperLogLog)
		 */
		protected DISTINCT_APPROX_COUNT(final String alias, final String[] columns, final int mm, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
		}

		/**
		 * Resource cleanup - closes HyperLogLog handler
		 */
		@Override
		public void close() throws Exception {
			values.clear();
		}

		/**
		 * Processes row data - adds distinct value combination to HyperLogLog estimator
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
			try {
				final Object[] distinct = new Object[key.length() + columns.length];
				int i = 0;
				for (; i < key.length(); i++) {
					distinct[i] = key.get(i);
				}
				for (int j = 0; j < columns.length; j++, i++) {
					final String column = columns[j];
					distinct[i] = r.get(column);
				}
				row(key, distinct);
			} catch (Exception ex) {
				if (!exceptions.contains(ex.getMessage())) {
					exceptions.add(ex.getMessage());
					ex.printStackTrace();
					// System.exit(0);
				}
			}
		}

		private void row(final GROUPKEY key, final Object[] distinct) throws Exception {
			final HyperLogLog hll = values.computeIfAbsent(key, k -> new HyperLogLog(precision));
			hll.add(distinct);
		}

		/**
		 * Returns approximate distinct count by group
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			var hll = (HyperLogLog)object(key);
			return hll != null ? new BigDecimal(hll.cardinality()) : BigDecimal.ZERO;
		}

		@Override
		public Object object(final GROUPKEY key) {
			return values.get(key);
		}

		/**
		 * DISTINCT_APPROX_COUNT does not require precision
		 */
		@Override
		public short precision() {
			return 0; // COUNT does not require precision
		}

		/**
		 * Default value is 0
		 */
		@Override
		public Object defaultValue() {
			return BigDecimal.ZERO;
		}
	}

	/**
	 * DISTINCT_APPROX_COUNT_SUM aggregate function - approximate distinct sum using HyperLogLog
	 * Provides approximate distinct sum with lower memory usage than exact summation
	 */
	public static class DISTINCT_APPROX_COUNT_SUM extends Function {
		/** Map storing sum values by group */
		private Map<GROUPKEY, HyperLogLog> values = new HashMap<>();
		private final Set<String> exceptions = new java.util.HashSet<>();

		public DISTINCT_APPROX_COUNT_SUM(final String alias, final String[] columns, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
		}

		protected DISTINCT_APPROX_COUNT_SUM(final String alias, final String[] columns, final int mm, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
		}

		@Override
		public void close() throws Exception {
			values.clear();
		}


		@Override
		public void row(GROUPKEY key, Row r) {
			try {
				byte[] bb = r.getBytes(columns[0]);
				HyperLogLog hll = HyperLogLog.fromByteArray(bb);
				values.computeIfAbsent(key, k -> new HyperLogLog()).merge(hll);
			} catch (Exception ex) {
				if (!exceptions.contains(ex.getMessage())) {
					exceptions.add(ex.getMessage());
					ex.printStackTrace();
					// System.exit(0);
				}
			}
		}

		@Override
		public Object compute(GROUPKEY key) {
			HyperLogLog hll = values.get(key);
			return (hll != null) ? new BigDecimal(hll.cardinality()) : BigDecimal.ZERO;
		}

		@Override
		public short precision() {
			return 0; // COUNT does not require precision
		}

		@Override
		public Object defaultValue() {
			return BigDecimal.ZERO;
		}
	}

	/**
	 * DISTINCT_ROARING_BITMAP_COUNT aggregate function - approximate distinct count using RoaringBitmap
	 * Provides approximate distinct count with lower memory usage than exact counting
	 */
	public static class DISTINCT_ROARING_BITMAP_COUNT extends Function {
		private final Map<GROUPKEY, RoaringBitmap> values = new HashMap<>();
		private final Set<String> exceptions = new java.util.HashSet<>();

		public DISTINCT_ROARING_BITMAP_COUNT(final String alias, final String[] columns, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
		}

		protected DISTINCT_ROARING_BITMAP_COUNT(final String alias, final String[] columns, final int mm, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
		}

		@Override
		public void close() throws Exception {
			values.clear();
		}

		@Override
		public void row(GROUPKEY key, Row r) {
			try {
				final Object[] distinct = new Object[key.length() + columns.length];
				int i = 0;
				for (; i < key.length(); i++) {
					distinct[i] = key.get(i);
				}
				for (int j = 0; j < columns.length; j++, i++) {
					final String column = columns[j];
					distinct[i] = r.get(column);
				}
				row(key, distinct);
			} catch (Exception ex) {
				if (!exceptions.contains(ex.getMessage())) {
					exceptions.add(ex.getMessage());
					ex.printStackTrace();
					// System.exit(0);
				}
			}
		}

		void row(final GROUPKEY key, final Object[] distinct) throws Exception {
			var rb = values.computeIfAbsent(key, k -> new RoaringBitmap());
			// Build a stable string representation and hash it, then map to 31-bit non-negative int
			// final String s1 = join("\u0001", distinct);
			// final long h64 = MurMurHash2.hash64(s1.getBytes());
			// int hash = (int) (h64 & 0x7FFFFFFFL); // 31-bit non-negative
			int hash = Arrays.hashCode(distinct) & 0x7FFFFFFF;
			assert hash > -1;
			rb.add(hash);
		}

		@Override
		public Object compute(final GROUPKEY key) {
			var bitmap = (RoaringBitmap) object(key);
			return (bitmap != null) ? new BigDecimal(bitmap.cardinality()) : BigDecimal.ZERO;
		}

		@Override
		public Object object(final GROUPKEY key) {
			return values.get(key);
		}

		@Override
		public short precision() {
			return 0; // COUNT does not require precision
		}

		@Override
		public Object defaultValue() {
			return BigDecimal.ZERO;
		}
	}

	static class DISTINCT_ROARING_BITMAP_SUM extends Function { // Experimental
		/** Map storing sum values by group */
		private Map<GROUPKEY, RoaringBitmap> values = new HashMap<>();
		private final Set<String> exceptions = new java.util.HashSet<>();

		public DISTINCT_ROARING_BITMAP_SUM(final String alias, final String[] columns, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
		}

		protected DISTINCT_ROARING_BITMAP_SUM(final String alias, final String[] columns, final int mm, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, condition);
		}

		@Override
		public void close() throws Exception {
			values.clear();
		}

		@Override
		public void row(GROUPKEY key, Row r) {
			try {
				byte[] bb = r.getBytes(columns[0]);
				RoaringBitmap bitmap = RoaringBitmap.fromByteArray(bb);
				values.computeIfAbsent(key, k -> new RoaringBitmap()).or(bitmap);
			} catch (Exception ex) {
				if (!exceptions.contains(ex.getMessage())) {
					exceptions.add(ex.getMessage());
					ex.printStackTrace();
					// System.exit(0);
				}
			}
		}

		@Override
		public Object compute(GROUPKEY key) {
			RoaringBitmap bitmap = values.get(key);
			return (bitmap != null) ? new BigDecimal(bitmap.cardinality()) : BigDecimal.ZERO;
		}

		@Override
		public short precision() {
			return 0; // COUNT does not require precision
		}

		@Override
		public Object defaultValue() {
			return BigDecimal.ZERO;
		}
	}

	/**
	 * MAX aggregate function - finds maximum value in a column
	 * Returns the largest value among all values in the specified column for each group
	 */
	public static class MAX extends Function {
		/**
		 * MAX function constructor
		 * @param alias Result column alias
		 * @param column Column name to find maximum value
		 * @param type Data type of the column
		 * @param condition Application condition
		 */
		public MAX(String alias, String column, short type, Condition condition) {
			super(alias, new String[] { column }, type, condition);
		}

		/** Map storing maximum values by group */
		private Map<GROUPKEY, Object> values = new HashMap<>();
		/** Comparator for value comparison */
		private TComparator comparator = new TComparator();

		/**
		 * Processes row data - updates maximum value for the group
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
			Object v = r.get(columns()[0]);
			if (v != null) {
				try {
					Object p = values.get(key);
					if (p == null) {
						values.put(key, v);
					} else {
						if (comparator.compare(v, p) > 0) {
							values.put(key, v);
						}
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}

		/**
		 * Returns maximum value by group
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			return values.get(key);
		}

		/**
		 * Resource cleanup
		 */
		@Override
		public void close() throws Exception {
			values.clear();
		}
	}

	/**
	 * MIN aggregate function - finds minimum value in a column
	 * Returns the smallest value among all values in the specified column for each group
	 */
	public static class MIN extends Function {
		/**
		 * MIN function constructor
		 * @param alias Result column alias
		 * @param column Column name to find minimum value
		 * @param type Data type of the column
		 * @param condition Application condition
		 */
		public MIN(String alias, String column, short type, Condition condition) {
			super(alias, new String[] { column }, type, condition);
		}

		/** Map storing minimum values by group */
		private Map<GROUPKEY, Object> values = new HashMap<>();
		/** Comparator for value comparison */
		private TComparator comparator = new TComparator();

		/**
		 * Processes row data - updates minimum value for the group
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
			Object v = r.get(columns()[0]);
			if (v != null) {
				try {
					Object p = values.get(key);
					if (p == null) {
						values.put(key, v);
					} else {
						if (comparator.compare(v, p) < 0) {
							values.put(key, v);
						}
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}

		/**
		 * Returns minimum value by group
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			return values.get(key);
		}

		/**
		 * Resource cleanup
		 */
		@Override
		public void close() throws Exception {
			values.clear();
		}
	}

	/**
	 * FIRST aggregate function - returns the first value encountered
	 * Returns the first non-null value in the specified column for each group
	 */
	public static class FIRST extends Function {
		/**
		 * FIRST function constructor
		 * @param alias Result column alias
		 * @param column Column name to get first value
		 * @param type Data type of the column
		 * @param condition Application condition
		 */
		public FIRST(String alias, String column, short type, Condition condition) {
			super(alias, new String[] { column }, type, condition);
		}

		/** Map storing first values by group */
		private Map<GROUPKEY, Object> values = new HashMap<>();

		/**
		 * Extracts value from row
		 * @param r Data row
		 * @return Column value
		 */
		protected Object value(final Row r) {
			return r.get(columns()[0]);
		}

		/**
		 * Processes row data - stores first non-null value for the group
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
			// if (condition(r)) {
			final Object v = value(r);
			if (v != null) {
				try {
					if (!values.containsKey(key)) {
						values.put(key, v);
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
			// }
		}

		/**
		 * Returns first value by group
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			return values.get(key);
		}

		/**
		 * Resource cleanup
		 */
		@Override
		public void close() throws Exception {
			values.clear();
		}
	}

	/**
	 * ZERO aggregate function - always returns 0
	 * Utility function that returns constant value 0 regardless of input
	 */
	public static class ZERO extends Function {
		/**
		 * ZERO function constructor
		 * @param column Column name (not actually used)
		 * @param condition Application condition
		 */
		public ZERO(String column, Condition condition) {
			super(column, new String[] { column }, Column.TYPE_INT, condition);
		}

		/**
		 * Row processing - no operation (always returns 0)
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
		}

		/**
		 * Returns constant value 0
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			return 0;
		}

		/**
		 * Resource cleanup - no operation
		 */
		@Override
		public void close() throws Exception {
		}
	}

	/**
	 * NULL aggregate function - always returns null
	 * Utility function that returns constant value null regardless of input
	 */
	public static class NULL extends Function {
		/**
		 * NULL function constructor
		 * @param column Column name (not actually used)
		 * @param condition Application condition
		 */
		public NULL(String column, Condition condition) {
			super(column, new String[] { column }, Column.TYPE_INT, condition);
		}

		/**
		 * Row processing - no operation (always returns null)
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
		}

		/**
		 * Returns constant value null
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			return null;
		}

		/**
		 * Resource cleanup - no operation
		 */
		@Override
		public void close() throws Exception {
		}
	}

	/**
	 * LAST aggregate function - returns the last value encountered
	 * Returns the last non-null value in the specified column for each group
	 */
	public static class LAST extends Function {
		/**
		 * LAST function constructor
		 * @param alias Result column alias
		 * @param column Column name to get last value
		 * @param type Data type of the column
		 * @param condition Application condition
		 */
		public LAST(String alias, String column, short type, Condition condition) {
			super(alias, new String[] { column }, type, condition);
		}

		/** Map storing last values by group */
		private Map<GROUPKEY, Object> values = new HashMap<>();

		/**
		 * Processes row data - updates last value for the group
		 */
		@Override
		public void row(final GROUPKEY key, final Row r) {
			// if (condition(r)) {
			Object v = r.get(columns()[0]);
			if (v != null) {
				try {
					values.put(key, v);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
			// }
		}

		/**
		 * Returns last value by group
		 */
		@Override
		public Object compute(final GROUPKEY key) {
			return values.get(key);
		}

		/**
		 * Resource cleanup
		 */
		@Override
		public void close() throws Exception {
			values.clear();
		}
	}

	/**
	 * ROWID aggregate function - generates sequential row numbers
	 * Returns incremental row numbers starting from 1 for each compute call
	 */
	public static final class ROWID extends Function {
		/** Current row ID value */
		private BigDecimal v = BigDecimal.ZERO;

		/**
		 * ROWID function constructor (starting from 0)
		 * @param column Column name (used as alias)
		 */
		public ROWID(final String column) {
			super(column, new String[] { column }, Column.TYPE_DECIMAL, Condition.True);
		}

		/**
		 * ROWID function constructor (starting from specified value)
		 * @param column Column name (used as alias)
		 * @param v Starting value
		 */
		public ROWID(final String column, final BigDecimal v) {
			super(column, new String[] { column }, Column.TYPE_DECIMAL, Condition.True);
			this.v = NOTNULL(v);
		}

		/**
		 * Resource cleanup - no operation
		 */
		@Override
		public void close() throws Exception {
		}

		/**
		 * Row processing - no operation (ROWID is independent of row data)
		 */
		@Override
		public void row(GROUPKEY key, Row r) {
		}

		/**
		 * Returns incremented row ID
		 */
		@Override
		public Object compute(GROUPKEY key) {
			v = v.add(BigDecimal.ONE);
			return v;
		}
	}

	/**
	 * HASH aggregate function - generates hash values for specified columns
	 * Creates hash values based on concatenated column values
	 */
	public static final class HASH extends Function {
		/** Map storing hash values by group */
		private Map<GROUPKEY, Long> values = new HashMap<>();

		/**
		 * HASH function constructor
		 * @param alias Result column alias
		 * @param columns Columns to hash
		 * @param condition Application condition (ignored, always uses Condition.True)
		 * @throws IOException File operation error (not used in current implementation)
		 */
		public HASH(final String alias, final String[] columns, final Condition condition) throws IOException {
			super(alias, columns, Column.TYPE_DECIMAL, Condition.True);
		}

		/**
		 * Resource cleanup - no operation
		 */
		@Override
		public void close() throws Exception {
		}

		/**
		 * Processes row data - calculates hash value for specified columns
		 */
		@Override
		public void row(GROUPKEY key, Row r) {
			final String[] columns = columns();
			String s = "";
			if (columns != null) {
				for (int i = 0; i < columns.length; i++) {
					final Object v = r.get(columns[i]);
					if (v != null) {
						s += "#" + v;
					}
				}
			}
			final long hash = MurMurHash2.hash64(s.getBytes());
			values.put(key, hash);
		}

		/**
		 * Returns hash value by group
		 */
		@Override
		public Object compute(GROUPKEY key) {
			return values.get(key);
		}
	}

	/**
	 * Class representing group keys
	 * Used to identify unique groups by combining values from multiple columns
	 */
	public static final class GROUPKEY implements Comparable<GROUPKEY> {
		/** Array of values that constitute the group */
		private final String[] array;

		/**
		 * Creates group key with string array
		 * @param array String array to constitute the group key
		 */
		public GROUPKEY(final String... array) {
			this.array = array;
		}

		/**
		 * Creates group key with object array (converted to strings)
		 * @param array Object array to constitute the group key
		 */
		public GROUPKEY(final Object... array) {
			this.array = safe(array);
		}

		/**
		 * Safely converts object array to string array
		 * @param a Object array to convert
		 * @return String array
		 */
		private static String[] safe(final Object[] a) {
			final String[] array = new String[a.length];
			for (int i = 0; i < a.length; i++) {
				array[i] = a[i] == null ? null : a[i].toString();
			}
			return array;
		}

		/**
		 * Returns length of group key
		 * @return Array length
		 */
		public int length() {
			return array.length;
		}

		/**
		 * Returns value at specified index
		 * @param n Index
		 * @return Value at that position
		 */
		public Object get(final int n) {
			return array[n];
		}

		/**
		 * Calculates hash code
		 */
		@Override
		public int hashCode() {
			return Arrays.hashCode(array);
		}

		/**
		 * Compares equality with another GROUPKEY
		 */
		@Override
		public boolean equals(final Object o) {
			return Arrays.equals(array, ((GROUPKEY) o).array);
		}

		/**
		 * String representation (connected with hyphens)
		 */
		@Override
		public String toString() {
			StringBuilder s = new StringBuilder();
			for (int i = 0; i < array.length; i++) {
				if (i > 0)
					s.append("-");
				s.append(array[i]);
			}
			return s.toString();
		}

		/**
		 * Generates unique ID using CRC32
		 * @return ID converted from CRC32 value to string
		 */
		public String id() {
			java.util.zip.CRC32 x = new java.util.zip.CRC32();
			x.update(toString().getBytes());
			return String.valueOf(x.getValue());
		}

		@Override
		public int compareTo(final GROUPKEY o) {
			return Arrays.compare(array, o.array, new Comparator<Object>() {
				@SuppressWarnings({ "rawtypes", "unchecked", "null" })
				@Override
				public int compare(final Object o1, final Object o2) {
					if (eq(o1, o2))
						return 0;
					if (o1 == null && o2 != null)
						return 1;
					if (o2 == null && o1 != null)
						return -1;
					if ("".equals(o1))
						return 1;
					if ("".equals(o2))
						return -1;
                    assert(o1 != null && o2 != null);
					return ((Comparable) o1).compareTo(o2);
				}
			});
		}

		static boolean eq(final Object o1, final Object o2) {
			return (o1 == o2) || (o1 != null && o1.equals(o2)) || (o2 != null && o2.equals(o1));
		}
	}

	/**
	 * Adds one row of data to aggregation
	 * Uses lock for thread safety and synchronization
	 * @param r Data row to process
	 */
	public void row(final Row r) {
		try (final IO.Closer CLOSER = new IO.Closer(lock)) {
			if (r != null) {
				final GROUPKEY key = key(r);
				// keys.add(key);
				for (final Function f : exprs) {
					if (f.condition(r)) {
						if (!keys.contains(key))
							keys.add(key);

						f.row(key, r);
					}
				}
			}
		}
	}

	/**
	 * Creates group key from row
	 * @param r Data row
	 * @return Generated group key
	 */
	private GROUPKEY key(final Row r) {
		final Object[] key = (groupby != null) ? new String[groupby.length] : new String[0];
		for (int i = 0; i < key.length; i++) {
			// final Object v = r.get(groupby[i].column());
			final Object v = groupby[i].get(r);
			if (v != null)
				key[i] = v.toString();
			else
				key[i] = null;
		}
		return new GROUPKEY(key);
	}

	/** Variable caching calculated results */
	private Row[] compute = null;

	/**
	 * Calculates and returns aggregation results
	 * @return Array of aggregated result rows
	 */
	public Row[] compute() {
		if (compute == null)
			compute = refresh();
		return compute;
	}

	/**
	 * Returns aggregation results sorted
	 * @param c Sorting criteria comparator
	 * @return Array of sorted aggregated result rows
	 */
	public Row[] compute(final Comparator<Row> c) {
		if (compute == null)
			compute = sort(refresh(), c);
		return compute;
	}

	// public Row[] compute(final boolean refresh) {
	// if (compute == null && refresh)
	// compute = refresh();
	// return compute;
	// }
	//
	// public Row[] compute(final boolean refresh, final Comparator<Row> c) {
	// if (compute == null && refresh)
	// compute = sort(refresh(), c);
	// return compute;
	// }

	/**
	 * Array sorting utility method
	 * @param a Array to sort
	 * @param c Comparator
	 * @return Sorted array
	 */
	public static <T> T[] sort(T[] a, Comparator<T> c) {
		Arrays.sort(a, c);
		return a;
	}

	/**
	 * Exports aggregation results to TSV file
	 * @param outFile Output file
	 * @throws IOException File writing error
	 */
	public void export(final File outFile) throws IOException {
		final Meta meta = meta();
		try (final TSVFile out = TSVFile.create( //
				outFile, //
				meta.columns(), //
				new Logger.NullLogger())) {

			final Row[] rows = compute();
			for (final Row r : rows) {
				out.write(r);
			}
		}
	}

	/**
	 * Creates table metadata
	 * @return Metadata for aggregation results
	 */
	public Meta meta() {
		final int cc = (groupby != null ? groupby.length : 0) + (exprs != null ? exprs.length : 0);
		final Column[] columns = new Column[cc];
		int i = 0;
		if (groupby != null) {
			for (int j = 0; j < groupby.length; j++) {
				columns[i++] = new Column(groupby[j].column(), groupby[j].type(), (short) 32, (short) -1, false, null, null);
			}
		}

		if (exprs != null) {
			for (int j = 0; j < exprs.length; j++) {
				// System.out.println("exprs[" + j + "] = " + exprs[j]);
				columns[i++] = new Column(exprs[j].alias(), exprs[j].type(), (short) 32, exprs[j].precision(), false, null, null);
			}
		}

		final Meta meta = new Meta("").columns(columns);
		return meta;
	}

	/**
	 * Returns result column header array
	 * @return Column name array
	 */
	public String[] head() {
		final int cc = (groupby != null ? groupby.length : 0) + (exprs != null ? exprs.length : 0);
		final String[] columns = new String[cc];
		int i = 0;
		if (groupby != null) {
			for (int j = 0; j < groupby.length; j++) {
				columns[i++] = groupby[j].column();
			}
		}
		if (exprs != null) {
			for (int j = 0; j < exprs.length; j++) {
				columns[i++] = exprs[j].alias();
			}
		}
		return columns;
	}

	/**
	 * Cleans up temporary files
	 * Cleans up temporary files used by each aggregate function
	 */
	public void purge() {
		for (int j = 0; j < exprs.length; j++) {
			exprs[j].purge();
		}
	}

	/**
	 * Calculates actual aggregation results and generates row array
	 * Executes aggregate functions for each group key to generate result rows
	 * @return Array of calculated result rows
	 */
	private Row[] refresh() {
		final Meta meta = meta();
		final int cc = meta.columns().length;
		final Row[] a = new Row[keys.size()];
		int x = 0;
		for (final GROUPKEY k : keys) {
			final Object[] array = new Object[cc];

			int n = 0;
			for (; n < k.length(); n++) {
				array[n] = k.get(n);
			}
			for (int j = 0; j < exprs.length; j++) {
				array[n++] = exprs[j].compute(k);
			}
			a[x++] = Row.create(meta, array);
		}

		// Generate default row when no groups exist and result is empty
		if (a.length == 0 && (groupby == null || groupby.length == 0)) {
			final Object[] va = new Object[cc];
			for (int j = 0; j < exprs.length; j++) {
				va[j] = exprs[j].defaultValue();
			}
			final Row r = Row.create(meta, va);
			return new Row[] { r };
		}

		return a;
	}

	/**
	 * MurmurHash2 hash algorithm implementation
	 * Original source: https://commons.apache.org/proper/commons-codec/jacoco/org.apache.commons.codec.digest/MurmurHash2.java
	 * High-performance hash function used for deduplication, etc.
	 */
	static final class MurMurHash2 {
		private static final long M64 = 0xc6a4a7935bd1e995L;
		private static final int R64 = 47;

		/**
		 * Calculates 64-bit hash (using default seed)
		 * @param data Data to hash
		 * @return 64-bit hash value
		 */
		public static long hash64(final byte[] data) {
			return hash64(data, data.length, 0x9747b28c);
		}

		/**
		 * Calculates 64-bit hash
		 * @param data Data to hash
		 * @param length Data length
		 * @param seed Seed value
		 * @return 64-bit hash value
		 */
		public static long hash64(final byte[] data, int length, long seed) {
			long h = (seed & 0xffffffffL) ^ (length * M64);

			final int nblocks = length >> 3;

			// body
			for (int i = 0; i < nblocks; i++) {
				final int index = (i << 3);
				long k = getLittleEndianLong(data, index);

				k *= M64;
				k ^= k >>> R64;
				k *= M64;

				h ^= k;
				h *= M64;
			}

			final int index = (nblocks << 3);
			switch (length - index) {
			case 7:
				h ^= ((long) data[index + 6] & 0xff) << 48;
			case 6:
				h ^= ((long) data[index + 5] & 0xff) << 40;
			case 5:
				h ^= ((long) data[index + 4] & 0xff) << 32;
			case 4:
				h ^= ((long) data[index + 3] & 0xff) << 24;
			case 3:
				h ^= ((long) data[index + 2] & 0xff) << 16;
			case 2:
				h ^= ((long) data[index + 1] & 0xff) << 8;
			case 1:
				h ^= ((long) data[index] & 0xff);
				h *= M64;
			}

			h ^= h >>> R64;
			h *= M64;
			h ^= h >>> R64;

			return h;
		}

		/**
		 * Extracts long value in little-endian format
		 * @param data Data array
		 * @param index Starting index
		 * @return Extracted long value
		 */
		private static long getLittleEndianLong(final byte[] data, final int index) {
			return (((long) data[index] & 0xff)) | //
					(((long) data[index + 1] & 0xff) << 8) | //
					(((long) data[index + 2] & 0xff) << 16) | //
					(((long) data[index + 3] & 0xff) << 24) | //
					(((long) data[index + 4] & 0xff) << 32) | //
					(((long) data[index + 5] & 0xff) << 40) | //
					(((long) data[index + 6] & 0xff) << 48) | //
					(((long) data[index + 7] & 0xff) << 56); //
		}
	}

	/**
	 * MurmurHash3 hash algorithm implementation
	 * Original source: https://commons.apache.org/proper/commons-codec/jacoco/org.apache.commons.codec.digest/MurmurHash3.java
	 * 
	 * @see https://en.wikipedia.org/wiki/MurmurHash
	 */
	static final class MurMurHash3 {
		private static final long C1 = 0x87c37b91114253d5L;
		private static final long C2 = 0x4cf5ad432745937fL;
		private static final int R1 = 31;
		private static final int R2 = 27;
		private static final int R3 = 33;
		private static final int M = 5;
		private static final int N1 = 0x52dce729;
		private static final int N2 = 0x38495ab5;

		/**
		 * Returns 128-bit hash as 64-bit array
		 * @param data Data to hash
		 * @return Long array [2] representing 128-bit hash
		 */
		public static long[] hash128x64(final byte[] data) {
			return hash128x64Internal(data, 0, data.length, 104729 & 0xffffffffL);
		}

		private static long[] hash128x64Internal(final byte[] data, final int offset, final int length, final long seed) {
			long h1 = seed;
			long h2 = seed;
			final int nblocks = length >> 4;

			// body
			for (int i = 0; i < nblocks; i++) {
				final int index = offset + (i << 4);
				long k1 = getLittleEndianLong(data, index);
				long k2 = getLittleEndianLong(data, index + 8);

				// mix functions for k1
				k1 *= C1;
				k1 = Long.rotateLeft(k1, R1);
				k1 *= C2;
				h1 ^= k1;
				h1 = Long.rotateLeft(h1, R2);
				h1 += h2;
				h1 = h1 * M + N1;

				// mix functions for k2
				k2 *= C2;
				k2 = Long.rotateLeft(k2, R3);
				k2 *= C1;
				h2 ^= k2;
				h2 = Long.rotateLeft(h2, R1);
				h2 += h1;
				h2 = h2 * M + N2;
			}

			// tail
			long k1 = 0;
			long k2 = 0;
			final int index = offset + (nblocks << 4);
			switch (offset + length - index) {
			case 15:
				k2 ^= ((long) data[index + 14] & 0xff) << 48;
			case 14:
				k2 ^= ((long) data[index + 13] & 0xff) << 40;
			case 13:
				k2 ^= ((long) data[index + 12] & 0xff) << 32;
			case 12:
				k2 ^= ((long) data[index + 11] & 0xff) << 24;
			case 11:
				k2 ^= ((long) data[index + 10] & 0xff) << 16;
			case 10:
				k2 ^= ((long) data[index + 9] & 0xff) << 8;
			case 9:
				k2 ^= data[index + 8] & 0xff;
				k2 *= C2;
				k2 = Long.rotateLeft(k2, R3);
				k2 *= C1;
				h2 ^= k2;

			case 8:
				k1 ^= ((long) data[index + 7] & 0xff) << 56;
			case 7:
				k1 ^= ((long) data[index + 6] & 0xff) << 48;
			case 6:
				k1 ^= ((long) data[index + 5] & 0xff) << 40;
			case 5:
				k1 ^= ((long) data[index + 4] & 0xff) << 32;
			case 4:
				k1 ^= ((long) data[index + 3] & 0xff) << 24;
			case 3:
				k1 ^= ((long) data[index + 2] & 0xff) << 16;
			case 2:
				k1 ^= ((long) data[index + 1] & 0xff) << 8;
			case 1:
				k1 ^= data[index] & 0xff;
				k1 *= C1;
				k1 = Long.rotateLeft(k1, R1);
				k1 *= C2;
				h1 ^= k1;
			}

			// finalization
			h1 ^= length;
			h2 ^= length;

			h1 += h2;
			h2 += h1;

			h1 = fmix64(h1);
			h2 = fmix64(h2);

			h1 += h2;
			h2 += h1;

			return new long[] { h1, h2 };
		}

		private static long fmix64(long hash) {
			hash ^= (hash >>> 33);
			hash *= 0xff51afd7ed558ccdL;
			hash ^= (hash >>> 33);
			hash *= 0xc4ceb9fe1a85ec53L;
			hash ^= (hash >>> 33);
			return hash;
		}

		private static long getLittleEndianLong(final byte[] data, final int index) {
			return (((long) data[index] & 0xff)) | //
					(((long) data[index + 1] & 0xff) << 8) | //
					(((long) data[index + 2] & 0xff) << 16) | //
					(((long) data[index + 3] & 0xff) << 24) | //
					(((long) data[index + 4] & 0xff) << 32) | //
					(((long) data[index + 5] & 0xff) << 40) | //
					(((long) data[index + 6] & 0xff) << 48) | //
					(((long) data[index + 7] & 0xff) << 56); //
		}
	}

	/**
	 * xxHash64 hash algorithm implementation
	 * Fast performance 64-bit hash function
	 * @param data Data to hash
	 * @param seed Seed value
	 * @return 64-bit hash value
	 */
	public static long xxHash64(byte[] data, long seed) {
		final long PRIME1 = 0x9E3779B185EBCA87L;
		final long PRIME2 = 0xC2B2AE3D27D4EB4FL;
		final long PRIME3 = 0x165667B19E3779F9L;
		final long PRIME4 = 0x85EBCA77C2B2AE63L;
		final long PRIME5 = 0x27D4EB2F165667C5L;
	
		long hash = seed + PRIME5 + data.length;
	
		for (byte b : data) {
			hash ^= (b & 0xFF) * PRIME1;
			hash = Long.rotateLeft(hash, 31) * PRIME2;
		}
	
		hash ^= hash >>> 33;
		hash *= PRIME3;
		hash ^= hash >>> 29;
		hash *= PRIME4;
		hash ^= hash >>> 32;
	
		return hash;
	}

	/**
	 * FarmHash64 hash algorithm implementation (simple FNV hash based)
	 * High-speed hash function optimized for string hashing
	 * @param data Data to hash
	 * @return 64-bit hash value
	 */
	public static long farmHash64(byte[] data) {
		long hash = 0xcbf29ce484222325L; // FNV offset basis
		for (byte b : data) {
			hash ^= b;
			hash *= 0x100000001b3L; // FNV prime
		}
		return hash;
	}

	/**
	 * Legacy comparator class for backward compatibility
	 * // deprecated Use Filter.compare() method instead
	 */
	private static final class TComparator {
		/**
		 * Compares two objects with type compatibility handling
		 * @param v1 First value
		 * @param v2 Second value
		 * @return Comparison result (-1, 0, 1)
		 */
		public int compare(final Object v1, final Object v2) {
			// System.err.println("TComparator.compare");
			final int d = diff(v1, v2);
			if (d < 0)
				return -1;
			if (d > 0)
				return 1;
			return d;
		}

		/**
		 * Internal difference calculation with enhanced type handling
		 * Handles numeric conversions, date parsing, and string comparisons
		 */
		@SuppressWarnings({ "rawtypes", "unchecked" })
		private int diff(final Object v1, final Object v2) {
			if (v1 == v2)
				return 0;
			if (v1 != null && v2 == null)
				return -1;
			if (v1 == null && v2 != null)
				return 1;
			
			// At this point both v1 and v2 are non-null
			assert v1 != null && v2 != null;

			int n = 0;
			try {
				if (v1 instanceof Number && v2 instanceof Number)
					return new java.math.BigDecimal(v1.toString()).compareTo(new java.math.BigDecimal(v2.toString()));
				if (v1 instanceof Number && v2 instanceof CharSequence)
					return new java.math.BigDecimal(v1.toString()).compareTo(new java.math.BigDecimal(v2.toString()));
				if (v1 instanceof java.util.Date && v2 instanceof CharSequence)
					return ((java.util.Date) v1).compareTo(Row.date(v2.toString()));
				if (v1 instanceof byte[] && v2 instanceof byte[])
					return java.util.Arrays.compare((byte[]) v1, (byte[]) v2);
				if ((v1 instanceof Comparable && v2 instanceof Comparable) && (v1.getClass() == v2.getClass()))
					return ((Comparable) v1).compareTo((Comparable) v2);

				n = String.valueOf(v1).compareTo(String.valueOf(v2));
				if (n != 0)
					return n;
			} catch (Exception ex) {
				System.err.println("" + v1 + " <> " + v2 + " " + (v1 != null ? v1.getClass() : null) + ", " + (v2 != null ? v2.getClass() : null) + " " + ex.getMessage());
				throw ex;
			}
			return n;
		}
	}
}
