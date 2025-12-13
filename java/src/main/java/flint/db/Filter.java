/**
 * Provides filtering, sorting, and limiting capabilities for database queries
 * Implements SQL WHERE clause parsing and row filtering functionality
 */
package flint.db;

import java.util.ArrayList;
import java.util.List;

/**
 * Main class for query filtering operations
 * Handles SQL WHERE clause compilation, row comparison, and result limiting
 */
public final class Filter {

	/** Constant for ascending sort order */
	public static int ASCENDING = 1;
	/** Constant for descending sort order */
	/** Constant for descending sort order */
	public static int DESCENDING = -1;

	/**
	 * Interface for implementing result limiting (LIMIT/OFFSET)
	 * Controls how many rows to return and how many to skip
	 */
	public static interface Limit {
		/**
		 * Checks if more rows can be returned
		 * @return true if more rows can be processed
		 */
		boolean remains();

		/**
		 * Checks if current row should be skipped (for OFFSET)
		 * @return true if current row should be skipped
		 */
		boolean skip(); // move to offset
	}

	/**
	 * Implementation of Limit interface with maximum count and offset support
	 * Handles SQL LIMIT and OFFSET clauses
	 */
	public static final class MaxLimit implements Limit {
		/** Current remaining count (decremented as rows are processed) */
		private volatile int n;
		/** Current remaining offset (decremented as rows are skipped) */
		private volatile int o;

		/** Maximum number of rows to return */
		private final int limit;
		/** Number of rows to skip from the beginning */
		private final int offset;

		/**
		 * MaxLimit constructor with limit only (no offset)
		 * @param limit Maximum number of rows to return
		 */
		public MaxLimit(final int limit) {
			this(0, limit);
		}

		/**
		 * MaxLimit constructor with offset and limit
		 * @param offset Number of rows to skip
		 * @param limit Maximum number of rows to return
		 */
		public MaxLimit(final int offset, final int limit) {
			this.limit = this.n = (limit == -1 ? Integer.MAX_VALUE : limit);
			this.offset = this.o = offset;
		}

		/**
		 * Checks if more rows can be returned and decrements counter
		 */
		@Override
		public boolean remains() {
			return n-- > 0;
		}

		/**
		 * Checks if current row should be skipped and decrements offset counter
		 */
		@Override
		public boolean skip() {
			return (o > 0 ? o-- : o) > 0;
		}

		/**
		 * Returns string representation of LIMIT clause
		 */
		@Override
		public String toString() {
			if (offset == 0 && limit == Integer.MAX_VALUE)
				return "";
			if (offset == 0 && limit > 0)
				return String.format("LIMIT %d", limit);
			return String.format("LIMIT %d,%d", offset, limit);
		}

		/**
		 * Parses LIMIT string and creates MaxLimit instance
		 * @param limit String representation of LIMIT clause
		 * @return Limit instance or NOLIMIT if empty
		 */
		public static Limit parse(final String limit) {
			if (limit == null || "".equals(limit))
				return NOLIMIT;
			final String[] a = limit.split("[,]", 2);
			if (a.length == 2)
				return new MaxLimit(Integer.parseInt(a[0].trim()), Integer.parseInt(a[1].trim()));
			return new MaxLimit(Integer.parseInt(a[0]));
		}
	}

	/**
	 * No-limit implementation that allows all rows to pass through
	 * Used when no LIMIT clause is specified
	 */
	public static final Limit NOLIMIT = new Limit() {
		/**
		 * Always returns true (no limit)
		 */
		@Override
		public boolean remains() {
			return true;
		}

		/**
		 * Always returns false (no offset)
		 */
		@Override
		public boolean skip() {
			return false;
		}

		@Override
		public String toString() {
			return "NOLIMIT";
		}
	};

	/**
	 * Universal filter that accepts all rows (no filtering)
	 * Used when no WHERE clause is specified
	 */
	public static final Comparable<Row> ALL = new Comparable<>() {
		@Override
		public int compareTo(final Row o) {
			return 0; // no filter
		}

		@Override
		public String toString() {
			return "ALL";
		}
	};
	

	/**
	 * Separates WHERE conditions into indexable and non-indexable parts
	 * Analyzes SQL WHERE clause to determine which conditions can use indexes
	 * @param meta Table metadata
	 * @param index Table index information
	 * @param q WHERE condition string
	 * @return [0]: indexable conditions, [1]: non-indexable conditions (TSV, CSV, or other file formats must be used, or table 2nd match condition must be applied)
	 */
	@SuppressWarnings("unchecked")
	public static Comparable<Row>[] compile(final Meta meta, final Index index, final String q) {
		if (q == null || q.trim().isEmpty())
			return new Comparable[]{ALL, ALL};

		// Simple implementation: only basic comparison operators (=, >, <, >=, <=) are considered indexable
		final String trimmed = SQL.trim_mws(q);
		
		// OR conditions make index usage difficult
		if (trimmed.toUpperCase().contains(" OR ")) {
			Comparable<Row> condition = compileSimple(meta, trimmed);
			return new Comparable[]{ALL, condition};
		}
		
		// Analyze AND conditions to separate indexable from non-indexable
		final String[] andParts = trimmed.split("(?i)\\s+AND\\s+");
		final List<String> indexableParts = new ArrayList<>();
		final List<String> nonIndexableParts = new ArrayList<>();
		
		for (String part : andParts) {
			String cleanPart = part.trim();
			if (isSimpleIndexableCondition(meta, index, cleanPart)) {
				indexableParts.add(cleanPart);
			} else {
				nonIndexableParts.add(cleanPart);
			}
		}
		
		// Combine indexable conditions
		Comparable<Row> indexableCondition = ALL;
		if (!indexableParts.isEmpty()) {
			String indexableExpr = String.join(" AND ", indexableParts);
			indexableCondition = compileSimple(meta, indexableExpr);
		}
		
		// Combine non-indexable conditions
		Comparable<Row> nonIndexableCondition = ALL;
		if (!nonIndexableParts.isEmpty()) {
			String nonIndexableExpr = String.join(" AND ", nonIndexableParts);
			nonIndexableCondition = compileSimple(meta, nonIndexableExpr);
		}
		
		return new Comparable[]{indexableCondition, nonIndexableCondition};
	}

	/**
	 * Find the best index for given WHERE and ORDER BY clauses
	 * 
	 * Selection priority:
	 * 1. Indexes that match both WHERE and ORDER BY columns
	 * 2. Indexes that match WHERE columns
	 * 3. Indexes that match ORDER BY columns
	 * 4. Return -1 if no suitable index found
	 * 
	 * @param meta Table metadata for column lookup
	 * @param where WHERE clause string (without "WHERE" keyword)
	 * @param orderby ORDER BY clause string (without "ORDER BY" keyword)
	 * @return Best index to use (0-based), or -1 if none found
	 */
	public static int getBestIndex(final Meta meta, final String where, final String orderby) {
		if (meta == null) {
			throw new IllegalArgumentException("meta is NULL");
		}
		
		final Index[] indexes = meta.indexes();
		if (indexes == null || indexes.length == 0) {
			return -1;
		}
		
		// No WHERE and no ORDER BY: use primary index if available
		if ((where == null || where.trim().isEmpty()) && (orderby == null || orderby.trim().isEmpty())) {
			return indexes.length > 0 ? 0 : -1;
		}
		
		// Parse WHERE clause to get filter conditions
		Comparable<Row> whereFilter = null;
		if (where != null && !where.trim().isEmpty()) {
			whereFilter = compileSimple(meta, SQL.trim_mws(where));
		}
		
		// Parse ORDER BY clause to extract column names
		final String[] orderbyColumns = parseOrderByColumns(orderby);
		
		int bestIndex = -1;
		int bestScore = 0;
		
		// Evaluate each index
		for (int idx = 0; idx < indexes.length; idx++) {
			final Index index = indexes[idx];
			int score = 0;
			
			// Check WHERE clause compatibility
			if (whereFilter != null && where != null) {
				final boolean whereMatch = isWhereIndexable(meta, index, where);
				if (whereMatch) {
					score += 100; // WHERE match is most important
				}
			}
			
			// Check ORDER BY compatibility
			if (orderbyColumns != null && orderbyColumns.length > 0) {
				final boolean orderbyMatch = isOrderByIndexable(index, orderbyColumns);
				if (orderbyMatch) {
					score += 50; // ORDER BY match is secondary
					score += orderbyColumns.length; // Prefer indexes matching more ORDER BY columns
				}
			}
			
			// Additional scoring: prefer indexes with fewer total columns (more specific)
			if (score > 0 && index.keys() != null) {
				score += (10 - index.keys().length); // Slight preference for narrower indexes
			}
			
			// Update best index
			if (score > bestScore) {
				bestScore = score;
				bestIndex = idx;
			}
		}
		
		return bestIndex;
	}
	
	/**
	 * Parse ORDER BY clause to extract column names
	 * @param orderby ORDER BY clause string
	 * @return Array of column names, or null if empty
	 */
	private static String[] parseOrderByColumns(final String orderby) {
		if (orderby == null || orderby.trim().isEmpty()) {
			return null;
		}
		
		final List<String> columns = new ArrayList<>();
		final String trimmed = SQL.trim_mws(orderby);
		final String[] parts = trimmed.split(",");
		
		for (String part : parts) {
			String col = part.trim();
			// Remove ASC/DESC if present
			if (col.toUpperCase().endsWith(" ASC")) {
				col = col.substring(0, col.length() - 4).trim();
			} else if (col.toUpperCase().endsWith(" DESC")) {
				col = col.substring(0, col.length() - 5).trim();
			}
			
			// Normalize column name
			col = Column.normalize(col);
			if (!col.isEmpty()) {
				columns.add(col);
			}
		}
		
		return columns.isEmpty() ? null : columns.toArray(new String[0]);
	}
	
	/**
	 * Check if WHERE clause can use the given index
	 * @param meta Table metadata
	 * @param index Index to check
	 * @param where WHERE clause string
	 * @return true if WHERE can use this index
	 */
	private static boolean isWhereIndexable(final Meta meta, final Index index, final String where) {
		if (index == null || index.keys() == null || where == null) {
			return false;
		}
		
		final String trimmed = SQL.trim_mws(where);
		final String upperWhere = trimmed.toUpperCase();
		
		// OR conditions make index usage difficult
		if (upperWhere.contains(" OR ")) {
			return false;
		}
		
		// Analyze AND conditions
		final String[] andParts = trimmed.split("(?i)\\s+AND\\s+");
		
		for (String part : andParts) {
			if (isSimpleIndexableCondition(meta, index, part.trim())) {
				return true; // At least one condition can use the index
			}
		}
		
		return false;
	}
	
	/**
	 * Check if ORDER BY columns match index prefix
	 * @param index Index to check
	 * @param orderbyColumns Parsed ORDER BY column names
	 * @return true if ORDER BY can use this index
	 */
	private static boolean isOrderByIndexable(final Index index, final String[] orderbyColumns) {
		if (index == null || index.keys() == null || orderbyColumns == null) {
			return false;
		}
		
		final String[] indexKeys = index.keys();
		
		// Check if ORDER BY columns match index prefix
		for (int i = 0; i < orderbyColumns.length && i < indexKeys.length; i++) {
			final String orderbyCol = Column.normalize(orderbyColumns[i]);
			final String indexKey = Column.normalize(indexKeys[i]);
			
			if (!orderbyCol.equals(indexKey)) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Legacy compatibility method (does not use indexes)
	 * Compiles WHERE conditions without index optimization
	 */
	private static Comparable<Row> compileSimple(final Meta meta, final String q) {
		if (q == null || q.trim().isEmpty())
			return ALL;

		return parseExpression(meta, SQL.trim_mws(q));
	}
	
	/**
	 * Checks if a condition can use simple index optimization
	 * Determines whether a WHERE condition can benefit from index usage
	 */
	private static boolean isSimpleIndexableCondition(final Meta meta, final Index index, final String condition) {
		if (index == null) {
			return false;
		}
		
		final String upperCondition = condition.toUpperCase();
		
		// NOT, LIKE, BETWEEN, IN are complex so treat as non-indexable for now
		if (upperCondition.contains("NOT ") || 
			upperCondition.contains(" LIKE ") ||
			upperCondition.contains(" BETWEEN ") ||
			upperCondition.contains(" IN ")) {
			return false;
		}
		
		// Check only basic comparison operators
		for (String op : new String[]{">=", "<=", "!=", "<>", "=", ">", "<"}) {
			int opIndex = upperCondition.indexOf(" " + op + " ");
			if (opIndex > 0) {
				String columnName = condition.substring(0, opIndex).trim();
				String normalizedColumn = Column.normalize(columnName);
				
				// Check if column exists
				int columnIndex = meta.column(normalizedColumn);
				if (columnIndex >= 0) {
					// != and <> have limited index effectiveness
					if ("!=".equals(op) || "<>".equals(op)) {
						return false;
					}
					
					// Check if column is included in index
					if (isColumnInIndex(index, normalizedColumn)) {
						return true; // =, >, <, >=, <= can use index
					}
				}
			}
		}
		
		return false;
	}
	
	/**
	 * Checks if a column is included in the index
	 * @param index Table index to check
	 * @param columnName Column name to look for
	 * @return true if column is part of the index
	 */
	private static boolean isColumnInIndex(final Index index, final String columnName) {
		if (index == null || index.keys() == null) {
			return false;
		}
		
		for (String indexKey : index.keys()) {
			if (Column.normalize(indexKey).equals(columnName)) {
				return true;
			}
		}
		
		return false;
	}
	
	// Use existing parsing logic as-is
	/**
	 * Parses SQL expression into comparable filter
	 * Main entry point for WHERE clause parsing
	 */
	private static Comparable<Row> parseExpression(final Meta meta, final String expr) {
		// OR conditions have lowest precedence, so handle first
		if (expr.toUpperCase().contains(" OR ")) {
			return parseOrExpression(meta, expr);
		}
		// Handle AND conditions
		return parseAndExpression(meta, expr);
	}
	
	/**
	 * Parses OR expressions (disjunction)
	 * Creates filter that passes if any sub-condition is true
	 */
	private static Comparable<Row> parseOrExpression(final Meta meta, final String expr) {
		final List<Comparable<Row>> orConditions = new ArrayList<>();
		final String[] orParts = expr.split("(?i)\\s+OR\\s+");
		
		for (String part : orParts) {
			orConditions.add(parseAndExpression(meta, part.trim()));
		}
		
		return new Comparable<Row>() {
			@Override
			public int compareTo(final Row o) {
				// OR condition: returns 0 if any condition is satisfied
				for (final Comparable<Row> condition : orConditions) {
					if (condition.compareTo(o) == 0) {
						return 0;
					}
				}
				return -1; // Filtered out if no condition is satisfied
			}

			@Override
			public String toString() {
				return expr;
			}
		};
	}
	
	/**
	 * Parses AND expressions (conjunction)
	 * Creates filter that passes only if all sub-conditions are true
	 */
	private static Comparable<Row> parseAndExpression(final Meta meta, final String expr) {
		final List<Comparable<Row>> andConditions = new ArrayList<>();
		final String[] andParts = expr.split("(?i)\\s+AND\\s+");
		
		for (String part : andParts) {
			andConditions.add(parseCondition(meta, part.trim()));
		}
		
		return new Comparable<Row>() {
			@Override
			public int compareTo(final Row o) {
				// AND condition: all conditions must be satisfied
				for (final Comparable<Row> condition : andConditions) {
					int result = condition.compareTo(o);
					if (result != 0) {
						return result;
					}
				}
				return 0;
			}

			@Override
			public String toString() {
				return expr;
			}
		};
	}
	
	/**
	 * Parses individual conditions (atomic conditions)
	 * Handles NOT, BETWEEN, IN, LIKE, and basic comparison operators
	 */
	private static Comparable<Row> parseCondition(final Meta meta, final String condition) {
		final String trimmed = condition.trim();
		
		// NOT handling
		if (trimmed.toUpperCase().startsWith("NOT ")) {
			final Comparable<Row> innerCondition = parseCondition(meta, trimmed.substring(4).trim());
			return new Comparable<Row>() {
				@Override
				public int compareTo(final Row o) {
					return innerCondition.compareTo(o) == 0 ? -1 : 0;
				}
				
				@Override
				public String toString() {
					return "NOT " + innerCondition.toString();
				}
			};
		}
		
		// BETWEEN handling
		if (trimmed.toUpperCase().contains(" BETWEEN ")) {
			return parseBetweenCondition(meta, trimmed);
		}
		
		// IN handling
		if (trimmed.toUpperCase().contains(" IN ")) {
			return parseInCondition(meta, trimmed);
		}
		
		// LIKE handling
		if (trimmed.toUpperCase().contains(" LIKE ")) {
			return parseLikeCondition(meta, trimmed);
		}
		
		// Basic comparison operator handling
		return parseBasicCondition(meta, trimmed);
	}
	
	/**
	 * Parses basic comparison conditions (=, >, <, >=, <=, !=, <>)
	 * Creates filter for simple column-value comparisons
	 */
	private static Comparable<Row> parseBasicCondition(final Meta meta, final String condition) {
		for (String op : new String[]{">=", "<=", "!=", "<>", "=", ">", "<"}) {
			int index = condition.toUpperCase().indexOf(" " + op + " ");
			if (index > 0) {
				String lv = condition.substring(0, index).trim();
				String rv = condition.substring(index + op.length() + 2).trim();
				return compile(meta, Column.normalize(lv), op, parseValue(meta, lv, rv));
			}
		}
		throw new IllegalArgumentException("Invalid condition: " + condition);
	}
	
	/**
	 * Parses BETWEEN conditions (column BETWEEN value1 AND value2)
	 * Creates filter for range checking
	 */
	private static Comparable<Row> parseBetweenCondition(final Meta meta, final String condition) {
		final String[] parts = condition.split("(?i)\\s+BETWEEN\\s+");
		if (parts.length != 2) {
			throw new IllegalArgumentException("Invalid BETWEEN condition: " + condition);
		}
		
		final String columnName = Column.normalize(parts[0].trim());
		final String[] values = parts[1].split("(?i)\\s+AND\\s+");
		if (values.length != 2) {
			throw new IllegalArgumentException("Invalid BETWEEN condition: " + condition);
		}
		
		final Object minValue = parseValue(meta, columnName, values[0].trim());
		final Object maxValue = parseValue(meta, columnName, values[1].trim());
		final int columnIndex = meta.column(columnName);
		
		return new Comparable<Row>() {
			@Override
			public int compareTo(final Row o) {
				Object value = o.get(columnIndex);
				return (compare(minValue, value) <= 0 && compare(value, maxValue) <= 0) ? 0 : -1;
			}
			
			@Override
			public String toString() {
				return condition;
			}
		};
	}
	
	/**
	 * Parses IN conditions (column IN (value1, value2, ...))
	 * Creates filter for membership testing
	 */
	private static Comparable<Row> parseInCondition(final Meta meta, final String condition) {
		final String[] parts = condition.split("(?i)\\s+IN\\s+");
		if (parts.length != 2) {
			throw new IllegalArgumentException("Invalid IN condition: " + condition);
		}
		
		final String columnName = Column.normalize(parts[0].trim());
		final String valueList = parts[1].trim();
		
		if (!valueList.startsWith("(") || !valueList.endsWith(")")) {
			throw new IllegalArgumentException("IN values must be enclosed in parentheses: " + condition);
		}
		
		final String[] valueStrings = valueList.substring(1, valueList.length() - 1).split(",");
		final List<Object> values = new ArrayList<>();
		for (String valueStr : valueStrings) {
			values.add(parseValue(meta, columnName, valueStr.trim()));
		}
		
		final int columnIndex = meta.column(columnName);
		
		return new Comparable<Row>() {
			@Override
			public int compareTo(final Row o) {
				Object value = o.get(columnIndex);
				for (Object inValue : values) {
					if (compare(value, inValue) == 0) {
						return 0;
					}
				}
				return -1;
			}
			
			@Override
			public String toString() {
				return condition;
			}
		};
	}
	
	/**
	 * Parses LIKE conditions (column LIKE pattern)
	 * Creates filter for pattern matching using SQL LIKE syntax
	 */
	private static Comparable<Row> parseLikeCondition(final Meta meta, final String condition) {
		final String[] parts = condition.split("(?i)\\s+LIKE\\s+");
		if (parts.length != 2) {
			throw new IllegalArgumentException("Invalid LIKE condition: " + condition);
		}
		
		final String columnName = Column.normalize(parts[0].trim());
		final String pattern = parseValue(meta, columnName, parts[1].trim()).toString();
		final int columnIndex = meta.column(columnName);
		
		// Convert SQL LIKE pattern to Java regex
		final String regex = pattern.replace("%", ".*").replace("_", ".");
		
		return new Comparable<Row>() {
			@Override
			public int compareTo(final Row o) {
				Object value = o.get(columnIndex);
				if (value == null) return -1;
				return value.toString().matches(regex) ? 0 : -1;
			}
			
			@Override
			public String toString() {
				return condition;
			}
		};
	}
	
	/**
	 * Parses string values into appropriate data types
	 * Converts string representations to typed values based on column metadata
	 */
	private static Object parseValue(final Meta meta, final String columnName, final String valueStr) {
		final int columnIndex = meta.column(columnName);
		if (columnIndex == -1) {
			throw new IllegalArgumentException("Unknown column: " + columnName);
		}
		
		final Column column = meta.columns()[columnIndex];
		final String trimmed = valueStr.trim();
		
		if ("NULL".equalsIgnoreCase(trimmed)) {
			return null;
		}
		
		if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
			return Row.cast(trimmed.substring(1, trimmed.length() - 1), column.type(), column.precision());
		}
		
		return Row.cast(trimmed, column.type(), column.precision());
	}

	/**
	 * Compiles individual condition into comparable filter
	 * Creates specific filter implementation based on operator type
	 */
	private static Comparable<Row> compile(final Meta meta, final String LV, final String OP, final Object RV) {
		final String expr = LV + " " + OP + " " + RV;
		final int key = meta.column(LV);
		
		switch (OP) {
			case "=":
				return new Comparable<>() {
					@Override
					public int compareTo(final Row o) {
						return compare(RV, o.get(key));
					}

					@Override
					public String toString() {
						return expr;
					}
				};
			case "<=":
				return new Comparable<>() {
					@Override
					public int compareTo(final Row o) {
						return compare(RV, o.get(key)) >= 0 ? 0 : -1;
					}

					@Override
					public String toString() {
						return expr;
					}
				};
			case "<":
				return new Comparable<>() {
					@Override
					public int compareTo(final Row o) {
						return compare(RV, o.get(key)) > 0 ? 0 : -1;
					}

					@Override
					public String toString() {
						return expr;
					}
				};
			case ">=":
				return new Comparable<>() {
					@Override
					public int compareTo(final Row o) {
						return -compare(RV, o.get(key)) >= 0 ? 0 : 1;
					}

					@Override
					public String toString() {
						return expr;
					}
				};
			case ">":
				return new Comparable<>() {
					@Override
					public int compareTo(final Row o) {
						return -compare(RV, o.get(key)) > 0 ? 0 : 1;
					}

					@Override
					public String toString() {
						return expr;
					}
				};
			case "!=":
			case "<>":
				return new Comparable<>() {
					@Override
					public int compareTo(final Row o) {
						return compare(RV, o.get(key)) != 0 ? 0 : -1;
					}

					@Override
					public String toString() {
						return expr;
					}
				};
			default:
				throw new java.lang.UnsupportedOperationException(expr);
		}
	}

	/**
	 * General-purpose comparison method for objects
	 * Handles null values and different data types safely
	 * @param v1 First value to compare
	 * @param v2 Second value to compare
	 * @return Comparison result (-1, 0, 1)
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static int compare(final Object v1, final Object v2) {
		int d = 0;
		if (v1 == v2)
			return 0;
		if (v1 != null && v2 == null)
			return -1;
		if (v1 == null && v2 != null)
			return 1;
		try {
			if ((v1 instanceof Comparable && v2 instanceof Comparable)) {
				d = ((Comparable) v1).compareTo((Comparable) v2);
				return d;
			}

			if (v1 instanceof byte[] && v2 instanceof byte[]) {
				d = java.util.Arrays.compare((byte[]) v1, (byte[]) v2);
				return d;
			}
		} catch (Exception ex) {
			System.err.println("" + v1 + " <> " + v2 + " " + (v1 != null ? v1.getClass() : null) + ", " + (v2 != null ? v2.getClass() : null) + " " + ex.getMessage());
			throw ex;
		}
		return d;
	}
}
