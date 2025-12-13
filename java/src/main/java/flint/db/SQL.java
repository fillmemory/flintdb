/**
 * 
 */
package flint.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * SQL utility class for parsing SQL statements.
 * This class provides methods to parse SQL statements, extract relevant components,
 * and support backtick-quoted identifiers.
 * 
 * @apiNote This is not a full-featured SQL engine; it only parses simple SQL statements.
 */
final class SQL {
	/**
	 * Parses the given SQL string and returns an SQL object containing the parsed components.
	 * @param sql the SQL string to parse
	 * @return an SQL object containing the parsed components
	 */
	public static SQL parse(final String sql) {
		final String[] tokens = tokenize(trim_mws(sql));
		final Map<String, String> m = statements(tokens);
		return new SQL(sql, m);
	}

	/**
	 * Parses the given InputStream and returns an SQL object containing the parsed components.
	 * @param stream the InputStream to parse
	 * @return an SQL object containing the parsed components
	 * @throws IOException if an I/O error occurs
	 */
	public static SQL parse(final java.io.InputStream stream) throws IOException {
		final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
		final byte[] bb = new byte[4096];
		int n = 0;
		while ((n = stream.read(bb)) > 0) {
			out.write(bb, 0, n);
		}
		final String s = new String(out.toByteArray());
		// System.out.println(s);
		return parse(s);
	}

	public String origin() {
		return sql;
	}

	public String statement() {
		return stmt;
	}

	public String table() {
		return table;
	}

	public String connect() {
		return connect;
	}

	public String object() {
		return object;
	}

	public String index() {
		return index;
	}

	public String[] columns() {
		return columns;
	}

	public String[] values() {
		return values;
	}

	public String where() {
		return where;
	}

	public String limit() {
		return limit;
	}

	public String from() {
		return from;
	}

	public String into() {
		return into;
	}

	public String orderby() {
		return orderby;
	}

	public String groupby() {
		return groupby;
	}

	public String having() {
		return having;
	}

	public boolean distinct() {
		return distinct;
	}

	public String[] definition() {
		return definition;
	}

	public String option() {
		return option;
	}

	private final String sql;
	private final String stmt;
	private final String table;
	private final String connect;
	private final String object;
	private final String[] columns;
	private final String index;
	private final String where;
	private final String limit;
	private final String[] values;
	private final String ignore;
	private final String from;
	private final String into;
	private final String orderby;
	private final String groupby;
	private final String having;
	private final boolean distinct;
	//
	private final String[] definition;
	private final String dictionary;
	private final String compressor;
	private final String compact;
	private final String increment;
	private final String cache;
	private final String walEnabled;
	private final String date;
	private final String directory;
	private final String storage;
	//
	private final String option;
	//
	private final String header;
	private final String delimiter;
	private final String quote;
	private final String nullString;
	//
	private final String format;

	/**
	 * 
	 * @param sql
	 * @param stmt
	 */
	private SQL(final String sql, final Map<String, String> stmt) {
		this.sql = sql;
		this.stmt = stmt.get("stmt");
		this.table = unwrap(stmt.get("table"));
		this.connect = stmt.get("connect");
		this.columns = unwrapArray(split(stmt.get("columns"), COMMA));
		this.index = stmt.get("index");
		this.where = stmt.get("where");
		this.limit = stmt.get("limit");
		this.values = values(stmt.get("values"));
		this.ignore = stmt.get("ignore");
		this.from = stmt.get("from");
		this.into = stmt.get("into");
		this.orderby = stmt.get("orderby");
		this.groupby = stmt.get("groupby");
		this.having = stmt.get("having");
		this.distinct = stmt.get("distinct") != null;
		this.object = stmt.get("object");
		//
		this.definition = split(stmt.get("definition"), COMMA);
		this.dictionary = stmt.get("dictionary");
		this.compressor = stmt.get("compressor");
		this.compact = stmt.get("compact");
		this.increment = stmt.get("increment");
		this.cache = stmt.get("cache");
		this.walEnabled = stmt.get("walEnabled");
		this.date = stmt.get("date");
		this.directory = stmt.get("directory");
		this.storage = stmt.get("storage");
		//
		this.option = stmt.get("option");

		//
		this.header = stmt.get("header");
		this.delimiter = stmt.get("delimiter");
		this.quote = stmt.get("quote");
		this.nullString = stmt.get("nullString");

		this.format = stmt.get("format");
	}

	@Override
	public String toString() {
		final StringBuilder s = new StringBuilder();
		s.append(stmt);
		if (null != ignore)
			s.append(" ").append(ignore);
		if (null != table)
			s.append(", TABLE : ").append(table);
		if (null != connect)
			s.append(", CONNECT : ").append(connect);
		if (null != object)
			s.append(", OBJECT : ").append(object);
		if (null != index)
			s.append(", INDEX : ").append(index);
		if (null != where)
			s.append(", WHERE : ").append(where);
		if (null != groupby)
			s.append(", GROUP BY : ").append(groupby);
		if (null != having)
			s.append(", HAVING : ").append(having);	
		if (null != orderby)
			s.append(", ORDER BY : ").append(orderby);
		if (null != limit)
			s.append(", LIMIT : ").append(limit);
		if (null != columns)
			s.append(", COLUMNS : ").append(Arrays.toString(columns));
		if (null != values)
			s.append(", VALUES : ").append(Arrays.toString(values));
		if (null != from)
			s.append(", FROM : ").append(from);
		if (null != into)
			s.append(", INTO : ").append(into);
		// if (null != definition)
		// s.append(", DEF : ").append(definition);
		if (null != sql)
			s.append(", SQL : ").append(sql);

		return s.toString();
	}

	Meta meta() {
		if (!"CREATE".equalsIgnoreCase(stmt))
			throw new RuntimeException(stmt);
		if (definition == null || definition.length == 0)
			throw new RuntimeException(stmt);

		final List<Column> columns = new ArrayList<>();
		final List<Index> indexes = new ArrayList<>();
		String part = null;
		for (int i = 0; i < definition.length; i++) {
			// System.out.println("> " + definition[i]);
			final FIFO<String> a = new FIFO<>(tokenize(definition[i]));
			String s = a.poll();
			if ("PRIMARY".equalsIgnoreCase(s) && "KEY".equalsIgnoreCase(a.peek())) {
				a.poll(); // skip "KEY"
				final String[] keys = strip(a.poll(), "(", ")").replace(" ", "").split(",");
				indexes.add(new Table.PrimaryKey(keys));
				part = "KEY";
			} else if ("KEY".equalsIgnoreCase(s) && "KEY".equals(part)) {
				final String name = a.poll();
				final String[] keys = strip(a.poll(), "(", ")").replace(" ", "").split(",");
				indexes.add(new Table.SortKey(name, keys));
			} else {
				final String name = s;
				final short type = Column.valueOf(a.poll());
				final String[] bytes = (a.peek().startsWith("(") && a.peek().endsWith(")")) //
						? strip(a.poll(), "(", ")").replace(" ", "").split(",", 2) //
						: null;
				String def = null;
                boolean notnull = false;
				String comment = null;

				for (String x = null; (x = a.poll()) != null;) {
					if ("NOT".equalsIgnoreCase(x) && "NULL".equalsIgnoreCase(a.poll())) {
                        notnull = true;
					} else if ("NULL".equalsIgnoreCase(x)) {
						// DO NOTHING
					} else if ("DEFAULT".equalsIgnoreCase(x)) {
						def = unescape(strip(a.poll(), "'", "'"));
					} else if ("COMMENT".equalsIgnoreCase(x)) {
						comment = unescape(strip(a.poll(), "'", "'"));
					}
				}

				final Column c = new Column(name, type, (bytes != null) ? Short.parseShort(bytes[0]) : -1, //
						(bytes != null && bytes.length > 1) ? Short.parseShort(bytes[1]) : -1, //
                        notnull, //
						def, comment);
				columns.add(c);
			}
		}

		final Meta meta = new Meta((table == null || "".equals(table)) ? "*" : table);
		meta.columns(columns.toArray(Column[]::new));
		if (!indexes.isEmpty())
			meta.indexes(indexes.toArray(Index[]::new));

		if (directory != null)
			meta.directory(directory);
		if (storage != null)
			meta.storage(storage);
		if (dictionary != null)
			meta.dictionary(dictionary);
		if (compressor != null)
			meta.compressor(compressor);
		if (compact != null)
			meta.compact(parseBytes(compact));
		if (increment != null)
			meta.increment(parseBytes(increment));
		if (cache != null)
			meta.cacheSize(parseBytes(cache));
		if (walEnabled != null)
			meta.walMode(walEnabled);
		if (date != null)
			meta.date(date);

		if (header != null)
			meta.absentHeader("ABSENT".equals(header.toUpperCase()) || "SKIP".equals(header.toUpperCase()));
		if (delimiter != null && delimiter.length() == 1)
			meta.delimiter(delimiter.charAt(0));
		if (quote != null && quote.length() == 1)
			meta.quote(quote.charAt(0));
		if (nullString != null)
			meta.nullString(nullString);

		if (format != null)
			meta.format(format);

		// System.err.println("storage : " + meta.storage());
		return meta;
	}

	public static String stringify(final Meta meta) {
		final String EOL = "\n";
		final String INDENT = "\t";
		final String EXTRA_INDENT = " ";
		final StringBuilder s = new StringBuilder();
		final String tablename = (meta.name() == null || "".equals(meta.name())) ? "*" : meta.name();
		// System.err.println("meta " + meta.name());
		s.append("CREATE TABLE ").append(tablename).append(" (").append(EOL);
		final Column[] columns = meta.columns();
		for (int i = 0; i < columns.length; i++) {
			if (i > 0)
				s.append(COMMA).append(EOL);

			final Column c = columns[i];
			s.append(INDENT);
			s.append(c.name()).append(" ");
			s.append(Column.typename(c.type()).substring(5));

			final boolean vt = (c.type() == Column.TYPE_STRING) //
					|| (c.type() == Column.TYPE_DECIMAL) //
					|| (c.type() == Column.TYPE_BYTES) //
					|| (c.type() == Column.TYPE_BLOB) //
					|| (c.type() == Column.TYPE_OBJECT) //
			;
			if (vt) {
				s.append("(");
				s.append(c.bytes());
				if (c.precision() > 0)
					s.append(",").append(c.precision());
				s.append(")");
			}

            if (c.notnull())
                s.append(" NOT NULL");

			if (c.value() != null)
				s.append(" DEFAULT '").append(escape(c.value().toString())).append("'");

			if (c.comment() != null)
				s.append(" COMMENT '").append(escape(c.comment())).append("'");
		}

		final Index[] indexes = meta.indexes();
		if (indexes != null) {
			for (int i = 0; i < Math.min(1, indexes.length); i++) {
				s.append(COMMA).append(EOL);
				final Index index = indexes[i];
				s.append(INDENT);
				s.append("PRIMARY KEY (").append(join(index.keys(), COMMA)).append(")");
			}
			for (int i = 1; i < (indexes.length); i++) {
				s.append(COMMA).append(EOL);
				final Index index = indexes[i];
				s.append(INDENT);
				s.append("KEY ").append(index.name()).append(" (").append(join(index.keys(), COMMA)).append(")");
			}
		}
		s.append("").append(EOL);
		s.append(")");

		int extras = 0;
		if (meta.storage() != null && !Storage.TYPE_DEFAULT.equalsIgnoreCase(meta.storage())) {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("STORAGE=").append(meta.storage().toUpperCase());
			extras++;
		}
		if (meta.increment() > 0 && Meta.INCREMENT_DEFAULT != meta.increment()) {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("INCREMENT=").append(meta.increment());
			extras++;
		}
		if (meta.compact() > 0) {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("COMPACT=").append(meta.compact());
			extras++;
		}
		if (meta.cacheSize() > 0) {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("CACHE=").append(meta.cacheSize());
			extras++;
		}
		if (meta.walEnabled()) {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("WAL=").append(meta.walMode());
			extras++;
		}
		if (meta.compressor() != null && !"none".equalsIgnoreCase(meta.compressor())) {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("COMPRESSOR=").append(meta.compressor());
			extras++;
		}
		if (meta.dictionary() != null) {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("DICTIONARY=").append(meta.dictionary());
			extras++;
		}

		// Text File
		if (meta.absentHeader()) {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("HEADER=").append("ABSENT");
			extras++;
		}
		if (meta.delimiter() != '\t') {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("DELIMITER=").append(meta.delimiter());
			extras++;
		}
		if (meta.quote() != '\0') {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("QUOTE=").append(meta.quote());
			extras++;
		}
		if (meta.nullString() != null && !"\\N".equals(meta.nullString())) {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("NULL=").append(meta.nullString());
			extras++;
		}
		if (meta.format() != null) {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("FORMAT=").append(meta.format());
			extras++;
		}

		if (meta.date() != null) {
			s.append((extras > 0) ? COMMA : "").append(EXTRA_INDENT).append("DATE=").append(meta.date());
			extras++;
		}

		// s.append(EOL);
		// s.append("-- row bytes:").append(meta.rowBytes());

		return s.toString();
	}

	/**
	 * Checks if a token is a valid SQL keyword that can appear after FROM clause
	 * @param token the token to check
	 * @return true if it's a valid keyword, false otherwise
	 */
	private static boolean isValidSQLKeywordAfterFrom(final String token) {
		return "WHERE".equalsIgnoreCase(token) || 
		       "LIMIT".equalsIgnoreCase(token) || 
		       "ORDER".equalsIgnoreCase(token) || 
		       "GROUP".equalsIgnoreCase(token) || 
		       "HAVING".equalsIgnoreCase(token) || 
		       "INTO".equalsIgnoreCase(token) ||
		       "USE".equalsIgnoreCase(token) ||
		       "INDEX".equalsIgnoreCase(token) ||
		       "CONNECT".equalsIgnoreCase(token) ||
		       SQL_TERMINATOR.equals(token);
	}

	private static Map<String, String> statements(final String[] a) {
		String part = null;
		String table = null;
		String columns = null;
		boolean distinct = false;
		String index = null;
		String connect = null;
		String where = null;
		String orderby = null;
		String groupby = null;
		String having = null;
		String limit = null;
		String ignore = null;
		String values = null;
		String from = null;
		String into = null;

		if ("SELECT".equalsIgnoreCase(a[0])) {
			final ArrayList<String> c = new ArrayList<>();
			for (int i = 1; i < a.length; i++) {
				final String s = a[i];

				if (SQL_TERMINATOR.equals(s)) {
					break;
				} else if ("DISTINCT".equalsIgnoreCase(s)) {
					distinct = true;
				} else if ("FROM".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					table = a[i + 1];
					columns = join(c, " ");
					
					// Check for missing WHERE clause: detect if next token after table looks like a condition
					if (i + 2 < a.length) {
						String nextToken = a[i + 2];
						// If next token is not a SQL keyword and looks like it could be a column name or condition
						if (!isValidSQLKeywordAfterFrom(nextToken) && 
						    (nextToken.contains("=") || nextToken.contains(">") || nextToken.contains("<") || 
						     (!nextToken.startsWith("'") && !nextToken.startsWith("\"") && 
						      i + 3 < a.length && (a[i + 3].equals("=") || a[i + 3].equals(">") || a[i + 3].equals("<"))))) {
							throw new IllegalArgumentException(
								"SQL syntax error: Missing WHERE keyword before condition. " +
								"Did you mean: SELECT ... FROM " + table + " WHERE " + nextToken + " ... ?"
							);
						}
					}
				} else if ("USE".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
				} else if ("INDEX".equalsIgnoreCase(s)) {
					final String v = a[i + 1].trim();
					index = v.substring(1, v.length() - 1);
					
					// Check for missing WHERE clause after USE INDEX
					if (i + 2 < a.length) {
						String nextToken = a[i + 2];
						if (!isValidSQLKeywordAfterFrom(nextToken) && 
						    (nextToken.contains("=") || nextToken.contains(">") || nextToken.contains("<") || 
						     (!nextToken.startsWith("'") && !nextToken.startsWith("\"") && 
						      i + 3 < a.length && (a[i + 3].equals("=") || a[i + 3].equals(">") || a[i + 3].equals("<"))))) {
							throw new IllegalArgumentException(
								"SQL syntax error: Missing WHERE keyword before condition. " +
								"Did you mean: ... USE INDEX(" + index + ") WHERE " + nextToken + " ... ?"
							);
						}
					}
				} else if ("CONNECT".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					connect = strip(seek(a, i + 1), "(", ")");
				} else if ("WHERE".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					where = seek(a, i + 1);
				} else if ("LIMIT".equalsIgnoreCase(s)) {
					limit = seek(a, i + 1);
				} else if ("ORDER".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
				} else if ("GROUP".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
				} else if ("BY".equalsIgnoreCase(s)) {
					if ("ORDER".equalsIgnoreCase(part))
						orderby = seek(a, i + 1);
					else if ("GROUP".equalsIgnoreCase(part))
						groupby = seek(a, i + 1);
				} else if ("HAVING".equalsIgnoreCase(s)) {
					having = seek(a, i + 1);
				} else if ("INTO".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					final String v = a[i + 1].trim();
					into = v; // .substring(1, v.length() - 1);
				}

				if (part == null)
					c.add(s);
			}

			final Map<String, String> m = new java.util.LinkedHashMap<>();
			m.put("stmt", a[0]);
			m.put("table", table);
			m.put("index", index);
			m.put("columns", columns);
			m.put("distinct", distinct ? "DISTINCT" : null);
			m.put("connect", connect);
			m.put("where", where);
			m.put("limit", limit);
			m.put("into", into);
			m.put("groupby", groupby);
			m.put("orderby", orderby);
			m.put("having", having);
			return m;
		}

		if ("DELETE".equalsIgnoreCase(a[0])) {
			for (int i = 1; i < a.length; i++) {
				final String s = a[i];

				if (SQL_TERMINATOR.equals(s)) {
					break;
				} else if ("FROM".equalsIgnoreCase(s)) {
					table = a[i + 1];
				} else if ("USE".equalsIgnoreCase(s)) {
				} else if ("INDEX".equalsIgnoreCase(s)) {
					final String v = a[i + 1].trim();
					index = v.substring(1, v.length() - 1);
				} else if ("WHERE".equalsIgnoreCase(s)) {
					where = seek(a, i + 1);
				} else if ("LIMIT".equalsIgnoreCase(s)) {
					limit = seek(a, i + 1);
				}
			}

			final Map<String, String> m = new java.util.LinkedHashMap<>();
			m.put("stmt", a[0]);
			m.put("table", table);
			m.put("index", index);
			m.put("where", where);
			m.put("limit", limit);
			return m;
		}

		if ("UPDATE".equalsIgnoreCase(a[0])) {
			final ArrayList<String> c = new ArrayList<>();
			table = a[1];

			for (int i = 2; i < a.length; i++) {
				final String s = a[i];

				if (SQL_TERMINATOR.equals(s)) {
					break;
				} else if ("SET".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					continue;
				} else if ("USE".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
				} else if ("INDEX".equalsIgnoreCase(s)) {
					final String v = a[i + 1].trim();
					index = v.substring(1, v.length() - 1);
				} else if ("WHERE".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					where = seek(a, i + 1);
				} else if ("LIMIT".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					limit = seek(a, i + 1);
				}

				if ("SET".equals(part))
					c.add(s);
			}

			if (part != null && columns == null) {
				for (int n = 0; n < c.size();) {
					String column = c.get(n++);
					String op = c.get(n++);
					String value = c.get(n++);

					// System.err.println("x : " + n + " => " + column + " " + op + " " + value + " " + value.getClass());
					// System.err.println("x1 : " + columns + " / " + values);
					columns = (columns == null) ? column : (columns + (", ") + (column));
					values = (values == null) ? value : (values + (value)); // value ends with ','
					// System.err.println("x2 : " + columns + " / " + values);

					if (!"=".equals(op))
						throw new RuntimeException("operator " + op);
				}
				// System.err.println("x3 : " + columns + " / " + values);
			}

			final Map<String, String> m = new java.util.LinkedHashMap<>();
			m.put("stmt", a[0]);
			m.put("table", table);
			m.put("columns", columns);
			m.put("values", values);
			m.put("index", index);
			m.put("where", where);
			m.put("limit", limit);
			return m;
		}

		if ("INSERT".equalsIgnoreCase(a[0]) || "REPLACE".equalsIgnoreCase(a[0])) {
			for (int i = 1; i < a.length; i++) {
				final String s = a[i];

				if (SQL_TERMINATOR.equals(s)) {
					break;
				} else if ("IGNORE".equalsIgnoreCase(s)) {
					ignore = s.toUpperCase();
				} else if ("INTO".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					table = a[++i];
				} else if ("VALUES".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
				} else if ("FROM".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					// from = seek(a, i + 1);
					from = a[++i]; // INSERT INTO temp/tpch_lineitem.desc FROM temp/tpch/lineitem.tbl.gz
					// break;
				} else if ("INTO".equals(part) && s.startsWith("(") && s.endsWith(")")) {
					// System.err.println("C1 : [" + s + "]" + " <= " + part);
					columns = s.substring(1, s.length() - 1);
				} else if ("VALUES".equals(part) && s.startsWith("(") && s.endsWith(")")) {
					// System.err.println("C2 : [" + s + "]" + " <= " + part);
					values = s.substring(1, s.length() - 1);
				} else if ("LIMIT".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					limit = seek(a, i + 1); // INSERT INTO temp/tpch_lineitem.desc FROM temp/tpch/lineitem.tbl.gz LIMIT 1000000
				} else if ("WHERE".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					where = seek(a, i + 1); // INSERT INTO temp/tpch_lineitem.desc FROM temp/tpch/lineitem.tbl.gz WHERE l_orderkey = 1
				} else {
				// 	throw new java.lang.UnsupportedOperationException(part + ": [" + s + "]");
				}
			}

			// System.err.println("C3 : [" + values + "]" + " <= " + part);
			final Map<String, String> m = new java.util.LinkedHashMap<>();
			m.put("stmt", a[0]);
			m.put("table", table);
			m.put("ignore", ignore);
			m.put("columns", columns);
			m.put("values", values);
			m.put("from", from);
			m.put("where", where);
			m.put("limit", limit);
			return m;
		}

		if ("DESC".equalsIgnoreCase(a[0]) || "META".equalsIgnoreCase(a[0])) {
			table = a[1];
			final Map<String, String> m = new java.util.LinkedHashMap<>();
			m.put("stmt", a[0]);
			m.put("table", table);

			for (int i = 2; i < a.length; i++) {
				final String s = a[i];
				if ("CONNECT".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					connect = seek(a, i + 1);
					m.put("connect", strip(connect, "(", ")"));
				} else if ("INTO".equalsIgnoreCase(s)) {
					part = s.toUpperCase();
					final String v = a[i + 1].trim();
					into = v; // .substring(1, v.length() - 1);
					m.put("into", into);
				}
			}
			return m;
		}

		if ("SHOW".equalsIgnoreCase(a[0])) {
			final Map<String, String> m = new java.util.LinkedHashMap<>();
			m.put("stmt", a[0]);
			m.put("object", a[1].toUpperCase());

			for (int i = 2; i < a.length; i++) {
				final String s = a[i];
				if ("WHERE".equalsIgnoreCase(s)) {
					where = seek(a, i + 1);
					m.put("where", where);
				} else if ("OPTION".equalsIgnoreCase(s)) {
					m.put("option", seek(a, i + 1));
				}
			}
			return m;
		}

		if ("DROP".equalsIgnoreCase(a[0]) && a.length >= 3 && "TABLE".equalsIgnoreCase(a[1])) {
			final Map<String, String> m = new java.util.LinkedHashMap<>();
			m.put("stmt", a[0]);
			m.put("table", a[2]);
			return m;
		}

		if ("CREATE".equalsIgnoreCase(a[0]) && a.length >= 3) {
			// for (int i = 0; i < a.length; i++)
			// System.out.println(String.format("%02d, %s", i, a[i]));

			int i = 2;
			final Map<String, String> m = new java.util.LinkedHashMap<>();
			m.put("stmt", a[0]);

			if ("TEMPORARY".equalsIgnoreCase(a[1]) && "TABLE".equals(a[2]))
				i = 3;
			else if ("TABLE".equals(a[1]))
				i = 2;

			m.put("table", a[i++]);

			final String def = a[i++];

			for (; i < a.length; i++) {
				final String s = endsWith(a[i], COMMA) ? a[i].substring(0, a[i].length() - 1) : a[i];
				final String[] nv = split(s, '=');
				// System.out.println(s);
				if (SQL_TERMINATOR.equals(s)) {
					break;
				} else if ("DIRECTORY".equalsIgnoreCase(nv[0])) {
					m.put("directory", nv[1]);
				} else if ("STORAGE".equalsIgnoreCase(nv[0])) {
					m.put("storage", nv[1]);
				} else if ("DICTIONARY".equalsIgnoreCase(nv[0])) {
					m.put("dictionary", nv[1]);
				} else if ("COMPRESSOR".equalsIgnoreCase(nv[0])) {
					m.put("compressor", nv[1].toLowerCase());
				} else if ("COMPACT".equalsIgnoreCase(nv[0])) {
					m.put("compact", nv[1].toUpperCase());
				} else if ("INCREMENT".equalsIgnoreCase(nv[0])) {
					m.put("increment", nv[1].toUpperCase());
				} else if ("CACHE".equalsIgnoreCase(nv[0])) {
					m.put("cache", nv[1].toUpperCase());
				} else if ("WAL".equalsIgnoreCase(nv[0]) || "WALENABLED".equalsIgnoreCase(nv[0])) {
					m.put("walEnabled", nv[1]);
				} else if ("DATE".equalsIgnoreCase(nv[0])) {
					m.put("date", nv[1].toUpperCase());

				} else if ("HEADER".equalsIgnoreCase(nv[0])) { // Text File
					m.put("header", nv[1]);
				} else if ("DELIMITER".equalsIgnoreCase(nv[0])) {
					m.put("delimiter", nv[1]);
				} else if ("QUOTE".equalsIgnoreCase(nv[0])) {
					m.put("quote", nv[1]);
				} else if ("NULL".equalsIgnoreCase(nv[0])) {
					m.put("nullString", nv[1]);

				} else if ("FORMAT".equalsIgnoreCase(nv[0])) {
					m.put("format", nv[1]);
				} else if ("MAX".equalsIgnoreCase(nv[0])) {
					m.put("max", nv[1]);
				}
			}

			m.put("definition", strip(def, "(", ")"));
			// final String[] COLUMNS = columns(strip(def, "(", ")")); // parseCommaStrings columns
			// for (i = 0; i < COLUMNS.length; i++) {
			// System.out.println(String.format("%02d %s", i, COLUMNS[i]));
			// }
			// System.out.println("---------");
			return m;
		}

		throw new java.lang.UnsupportedOperationException(a[0]);
	}

	private static String seek(final String[] a, final int offset) {
		String where = "";
		for (int i = offset; i < a.length; i++) {
			if (SQL_TERMINATOR.equalsIgnoreCase(a[i]))
				break;
			if ("LIMIT".equalsIgnoreCase(a[i]))
				break;
			if ("INTO".equalsIgnoreCase(a[i]))
				break;
			if ("CONNECT".equalsIgnoreCase(a[i]))
				break;
			if ("USE".equalsIgnoreCase(a[i]))
				break;
			if ("ORDER".equalsIgnoreCase(a[i]) && i + 1 < a.length && "BY".equalsIgnoreCase(a[i + 1]))
				break;
			if ("GROUP".equalsIgnoreCase(a[i]) && i + 1 < a.length && "BY".equalsIgnoreCase(a[i + 1]))
				break;
			if ("HAVING".equalsIgnoreCase(a[i]))
				break;
			if ("OPTION".equalsIgnoreCase(a[i]))
				break;

			if (i > (offset)) // (offset + 1)
				where += " ";
			where += a[i];
		}
		return where;
	}

	static String join(final List<String> a, final String delim) {
		String v = "";
		for (int i = 0; i < a.size(); i++) {
			// System.err.println("join : " + a.get(i));
			if (i > 0)
				v += delim;
			v += a.get(i);
		}
		return v;
	}

	static String join(final String[] a, final char delim) {
		String v = "";
		for (int i = 0; i < a.length; i++) {
			// System.err.println("join : " + a.get(i));
			if (i > 0)
				v += delim;
			v += a[i];
		}
		return v;
	}

	static String strip(final String s, final String begin, final String end) {
		return (s.startsWith(begin) && s.endsWith(end)) ? s.substring(begin.length(), s.length() - end.length()) : s;
	}

	static int parseBytes(final String s) {
		int m = 1;
		String i = s.toUpperCase();
		if (i.charAt(i.length() - 1) == 'K') {
			m = 1024;
			i = i.substring(0, i.length() - 1);
		} else if (i.charAt(i.length() - 1) == 'M') {
			m = 1024 * 1024;
			i = i.substring(0, i.length() - 1);
		} else if (i.charAt(i.length() - 1) == 'G') {
			m = 1024 * 1024 * 1024;
			i = i.substring(0, i.length() - 1);
		}
		return Integer.parseInt(i) * m;
	}

	/**
	 * Unwrap surrounding quotes ('|"|`) from a string if present
	 * 
	 * @param s Input string to unwrap
	 * @return Unwrapped string (null if input is null)
	 */
	static String unwrap(String s) {
		if (s == null || s.length() < 2) {
			return s;
		}
		
		char first = s.charAt(0);
		char last = s.charAt(s.length() - 1);
		
		if ((first == '\'' && last == '\'') ||
			(first == '\"' && last == '\"') ||
			(first == '`' && last == '`')) {
			return s.substring(1, s.length() - 1);
		}
		
		return s;
	}

	/**
	 * Unwrap each string in an array
	 * 
	 * @param array Input string array
	 * @return Array with unwrapped strings (null if input is null)
	 */
	static String[] unwrapArray(String[] array) {
		if (array == null) {
			return null;
		}
		
		String[] result = new String[array.length];
		for (int i = 0; i < array.length; i++) {
			result[i] = unwrap(array[i]);
		}
		return result;
	}

	static final class FIFO<T> {
		final T[] array;
		int i = 0;

		public FIFO(final T[] array) {
			this.array = array;
		}

		public T poll() {
			return i < array.length ? array[i++] : null;
		}

		public T peek() {
			return i < array.length ? array[i] : null;
		}

		public int length() {
			return array.length;
		}

		// public void rewind() {
		// i = 0;
		// }
	}

	static final char PARENTHESE_START = '(';
	static final char PARENTHESE_END = ')';
	static final char[] QOUTES = new char[] { '\'', '`' };
	static final char NULL = '\0';
	static final char SPACE = ' ';
	static final char COMMA = ',';
	static final char BSLASH = '\\';
	static final String SQL_TERMINATOR = "<END>";

	static final char[][] COMMENT_SINGLELINE = new char[][] { {'-', '-'}, {'\n'} }; // START, END 
	static final char[][] COMMENT_MULTILINE = new char[][] { {'/', '*'}, {'*', '/'} }; // START, END 

	private static <T> boolean QOUTE(final char ch) {
		return Arrays.binarySearch(QOUTES, ch) > -1;
	}

	/**
	 * <pre>
	 * * CASE 1
	 * 
	 * * CASE 2
	 * AA STRING (128) NOT NULL COMMENT 'CMT A',
	 * BB STRING (64) NOT NULL DEFAULT '' COMMENT 'CMT B',
	 * CC DECIMAL (10,3) DEFAULT 0,
	 * PRIMARY KEY (AA, BB),
	 * KEY IX_CC (CC, BB)
	 * </pre>
	 * 
	 * @param string
	 * @return
	 */
	private static String[] split(final String string, final char delim) {
		if (string == null)
			return null;

		final ArrayList<String> array = new ArrayList<>();
		final char[] a = concat(string.toCharArray(), delim);
		final char[] s = new char[a.length];
		int n = 0;

		int qoute = 0;
		int parentheses = 0;
		char prev = NULL;
		for (int i = 0; i < a.length; i++) {
			final char ch = a[i];

			if (parentheses > 0 && PARENTHESE_END == ch) {
				parentheses--;
				s[n++] = ch;
			} else if (PARENTHESE_START == ch) {
				parentheses++;
				s[n++] = ch;
			} else if (parentheses > 0) {
				s[n++] = ch;
			} else if (qoute > 0 && BSLASH != prev && QOUTE(ch)) {
				qoute = 0;
				s[n++] = ch;
			} else if (qoute > 0) {
				s[n++] = ch;
			} else if (QOUTE(ch)) {
				qoute = 1;
				s[n++] = ch;
			} else if (delim == ch) {
				if (n > 0)
					array.add(new String(s, 0, n));
				n = 0;
			} else if (!(n == 0 && ch == SPACE)) {
				s[n++] = ch;
			}

			prev = ch;
		}

		return array.toArray(String[]::new);
	}

	private static String[] values(final String string) {
		if (string == null)
			return null;

		final ArrayList<String> array = new ArrayList<>();
		final char delim = COMMA;
		final char[] a = concat(string.toCharArray(), delim);
		final char[] s = new char[a.length];
		int n = 0;

		int parentheses = 0;
		for (int i = 0; i < a.length; i++) {
			final char ch = a[i];
			if (parentheses > 0 && PARENTHESE_END == ch) {
				parentheses--;
				s[n++] = ch;
			} else if (parentheses > 0) {
				s[n++] = ch;
			} else if (PARENTHESE_START == ch) {
				parentheses++;
				s[n++] = ch;
			} else if (delim == ch) {
				final String v = new String(s, 0, n).trim();
				if ("NULL".equalsIgnoreCase(v))
					array.add(null);
				else if (startsWith(v, QOUTES[0]) && endsWith(v, QOUTES[0]))
					array.add(unescape(v.substring(1, v.length() - 1)));
				else
					array.add(v);
				n = 0;
			} else {
				s[n++] = ch;
			}
		}

		return array.toArray(String[]::new);
	}

	private static String escape(final String s) {
		return s //
				.replace("\\", "\\\\") //
				.replace("'", "''") //
				.replace("\n", "\\n") //
				.replace("\r", "\\r") //
				.replace("\t", "\\t") //
		;
	}

	private static String unescape(final String s) {
		return s //
				.replace("''", "'") //
				.replace("\'", "'") //
				.replace("\\n", "\n") //
				.replace("\\r", "\r") //
				.replace("\\t", "\t") //
				.replace("\\\\", "\\") //
		;
	}

	private static boolean startsWith(final String s, final char ch) {
		return s.charAt(0) == ch;
	}

	private static boolean endsWith(final String s, final char ch) {
		return s.charAt(s.length() - 1) == ch;
	}

	static char[] concat(final char[] s, final char ch) {
		final char[] a = new char[s.length + 1];
		for (int i = 0; i < s.length; i++)
			a[i] = s[i];
		a[a.length - 1] = ch;
		return a;
	}

	/**
	 * change multiple whitespaces to single space
	 * 
	 * @param string
	 * @return
	 */
	static String trim_mws(final String string) {
		// First, remove comments
		String cleaned = removeComments(string);
		
		final char[] a = cleaned.toCharArray();
		final char[] s = new char[(int) (a.length * 1.5)];
		int n = 0;

		int qoute = 0;
		char prev = NULL;
		for (int i = 0; i < a.length; i++) {
			final char ch = a[i] < SPACE ? SPACE : a[i];
			// System.out.println("case0 : " + ch + " " + ((int) ch) + " " + qoute);

			if (qoute > 0 && BSLASH != prev && QOUTE(ch)) {
				qoute = 0;
				s[n++] = ch;
			} else if (qoute > 0) {
				s[n++] = ch;
			} else if (QOUTE(ch)) {
				qoute = 1;
				s[n++] = ch;
			} else if (COMMA == ch && SPACE == prev) {
				s[n - 1] = ch;
			} else if (!(SPACE == ch && SPACE == prev)) {
				if (SPACE == ch && prev == PARENTHESE_START) {
					s[n] = ch;
				} else if (PARENTHESE_END == ch && prev == SPACE) {
					s[n - 1] = ch;
				} else if (PARENTHESE_START == ch && prev != SPACE) {
					s[n++] = SPACE;
					s[n++] = ch;
				} else {
					s[n++] = ch;
				}
			}

			prev = ch;
		}

		return new String(s, 0, n).trim();
	}

	/**
	 * Remove comments from SQL string
	 */
	static String removeComments(final String string) {
		final char[] a = string.toCharArray();
		final StringBuilder result = new StringBuilder(a.length);
		
		int qoute = 0;
		char prev = NULL;
		char[] comment_end = null;

		for (int i = 0; i < a.length; i++) {
			final char ch = a[i];

			// Skip comment detection when inside quotes
			if (qoute == 0 && comment_end == null) {
				// Check for comment start
				if (COMMENT_SINGLELINE[0][0] == ch && i + 1 < a.length
					&& COMMENT_SINGLELINE[0][1] == a[i + 1]) {
					// single line comment start
					comment_end = COMMENT_SINGLELINE[1];
					i++; // skip next char
					continue;
				} else if (COMMENT_MULTILINE[0][0] == ch && i + 1 < a.length
					&& COMMENT_MULTILINE[0][1] == a[i + 1]) {
					// multi line comment start
					comment_end = COMMENT_MULTILINE[1];
					i++; // skip next char
					continue;
				}
			}
			
			if (comment_end != null) {
				// in comment mode - check for comment end
				if (comment_end.length == 1) {
					// single line comment end
					if (comment_end[0] == ch) {
						comment_end = null; // end comment mode
						result.append(' '); // replace comment with space
					}
				} else if (comment_end.length == 2) {
					// multi line comment end
					if (comment_end[0] == ch && i + 1 < a.length
						&& comment_end[1] == a[i + 1]) {
						comment_end = null; // end comment mode
						i++; // skip next char
						result.append(' '); // replace comment with space
					}
				}
				continue; // skip all chars in comment mode
			}

			// Track quotes
			if (qoute > 0 && BSLASH != prev && QOUTE(ch)) {
				qoute = 0;
				result.append(ch);
			} else if (qoute > 0) {
				result.append(ch);
			} else if (QOUTE(ch)) {
				qoute = 1;
				result.append(ch);
			} else {
				result.append(ch);
			}

			prev = ch;
		}

		return result.toString();
	}

	static String[] tokenize(final String string) {
		final ArrayList<String> array = new ArrayList<>();
		final char[] a = concat(string.toCharArray(), SPACE);
		final char[] s = new char[a.length];
		int n = 0;

		int qoute = 0;
		int parentheses = 0;
		char prev = NULL;

		for (int i = 0; i < a.length; i++) {
			final char ch = a[i];

			if (parentheses > 0 && PARENTHESE_END == ch) {
				parentheses--;
				s[n++] = ch;

				if (0 == parentheses) {
					if (n > 0)
						array.add(new String(s, 0, n));
					
					n = 0;
				}
			} else if (PARENTHESE_START == ch) {
				parentheses++;
				
				s[n++] = ch;
			} else if (parentheses > 0) {
				s[n++] = ch;
			} else if (qoute > 0 && BSLASH != prev && QOUTE(ch)) {
				qoute = 0;
				s[n++] = ch;
			} else if (qoute > 0) {
				s[n++] = ch;
			} else if (QOUTE(ch)) {
				qoute = 1;
				s[n++] = ch;
			} else if (SPACE == ch) {
				if (n > 0)
					array.add(new String(s, 0, n));
				n = 0;
			} else if (!(SPACE == ch && SPACE == prev)) {
				s[n++] = ch;
			}

			prev = ch;
		}

		array.add(SQL_TERMINATOR);
		return array.toArray(String[]::new);
	}

	/**
	 * Parse GROUP BY column names
	 */
	static String[] parseGroupByColumns(String groupby) {
		if (groupby == null || groupby.trim().isEmpty()) {
			return new String[0];
		}
		
		String[] parts = groupby.split(",");
		String[] result = new String[parts.length];
		for (int i = 0; i < parts.length; i++) {
			result[i] = parts[i].trim();
		}
		return result;
	}

	/**
	 * Parse aggregate functions from SELECT clause
	 */
	static Aggregate.Function[] parseAggregateFunctions(SQL q, String[] groupCols, Meta meta) throws Exception {
		java.util.List<Aggregate.Function> funcs = new java.util.ArrayList<>();
		
		for (String expr : q.columns()) {
			// Skip group columns
			boolean isGroupCol = false;
			for (String gc : groupCols) {
				if (expr.equals(gc)) {
					isGroupCol = true;
					break;
				}
			}
			if (isGroupCol) continue;
			
			// Parse aggregate function - remove all whitespace first
			String normalized = expr.replaceAll("\\s+", " ").trim();
			String upper = normalized.toUpperCase().replaceAll("\\s+", "");
			int openParen = upper.indexOf('(');
			int closeParen = upper.lastIndexOf(')');
			
			if (openParen < 0 || closeParen < 0 || closeParen <= openParen) {
				throw new java.sql.SQLException("Malformed aggregate expression: " + expr);
			}
			
			String funcName = upper.substring(0, openParen);
			// Use normalized (not upper) to preserve column name case
			String colName = normalized.substring(normalized.indexOf('(') + 1, normalized.lastIndexOf(')')).trim();
			
			// Extract alias
			String alias = extractAlias(expr);
			if (alias == null) {
				alias = expr.trim();
			}
			
			// Create function based on type
			Aggregate.Function func = null;
			Aggregate.Condition cond = Aggregate.Condition.True;
			
			// Determine column type from metadata
			short colType = Column.TYPE_STRING;
			int colIdx = -1;
			
			// For COUNT(*) or COUNT(1), don't lookup column
			if (!"*".equals(colName) && !"1".equals(colName) && !"0".equals(colName)) {
				colIdx = meta.column(colName);
				if (colIdx >= 0) {
					colType = meta.columns()[colIdx].type();
				}
			}
			
			if ("COUNT".equals(funcName)) {
				// COUNT(*) or COUNT(1) - use "*" as column name
				if ("*".equals(colName) || "1".equals(colName) || "0".equals(colName)) {
					func = new Aggregate.COUNT(alias, "*", cond);
				} else {
					func = new Aggregate.COUNT(alias, colName, cond);
				}
			} else if ("SUM".equals(funcName)) {
				func = new Aggregate.SUM(alias, colName, cond);
			} else if ("AVG".equals(funcName)) {
				func = new Aggregate.AVG(alias, colName, cond);
			} else if ("MIN".equals(funcName)) {
				func = new Aggregate.MIN(alias, colName, colType, cond);
			} else if ("MAX".equals(funcName)) {
				func = new Aggregate.MAX(alias, colName, colType, cond);
			} else if ("FIRST".equals(funcName)) {
				func = new Aggregate.FIRST(alias, colName, colType, cond);
			} else if ("LAST".equals(funcName)) {
				func = new Aggregate.LAST(alias, colName, colType, cond);
			} else {
				throw new java.sql.SQLException("Unknown aggregate function: " + funcName);
			}
			
			funcs.add(func);
		}
		
		if (funcs.isEmpty()) {
			throw new java.sql.SQLException("No aggregate functions found in SELECT list");
		}
		
		return funcs.toArray(new Aggregate.Function[0]);
	}

	/**
	 * Parse ORDER BY clause
	 */
	static OrderSpec[] parseOrderBy(String orderby, Meta meta) {
		String[] parts = orderby.split(",");
		OrderSpec[] specs = new OrderSpec[parts.length];
		
		for (int i = 0; i < parts.length; i++) {
			String part = parts[i].trim();
			boolean desc = false;
			
			if (part.toUpperCase().endsWith(" DESC")) {
				desc = true;
				part = part.substring(0, part.length() - 5).trim();
			} else if (part.toUpperCase().endsWith(" ASC")) {
				part = part.substring(0, part.length() - 4).trim();
			}
			
			int colIndex = -1;
			Column[] columns = meta.columns();
			for (int j = 0; j < columns.length; j++) {
				if (columns[j].name().equals(part)) {
					colIndex = j;
					break;
				}
			}
			
			if (colIndex < 0) {
				throw new IllegalArgumentException("ORDER BY column not found: " + part);
			}
			
			specs[i] = new OrderSpec(colIndex, desc);
		}
		
		return specs;
	}

	/**
	 * OrderSpec class for ORDER BY parsing
	 */
	static class OrderSpec {
		final int colIndex;
		final boolean descending;
		
		OrderSpec(int colIndex, boolean descending) {
			this.colIndex = colIndex;
			this.descending = descending;
		}
	}

	/**
	 * Extract alias from expression (e.g., "COUNT(*) AS total" -> "total")
	 */
	static String extractAlias(String expr) {
		if (expr == null || expr.isEmpty()) {
			return null;
		}
		
		// Look for AS keyword
		String upper = expr.toUpperCase();
		int asIdx = upper.lastIndexOf(" AS ");
		
		if (asIdx >= 0) {
			// Found AS keyword
			String alias = expr.substring(asIdx + 4).trim();
			// Remove quotes if present
			if ((alias.startsWith("`") && alias.endsWith("`")) ||
				(alias.startsWith("\"") && alias.endsWith("\""))) {
				alias = alias.substring(1, alias.length() - 1);
			}
			return alias;
		}
		
		return null;
	}

}
