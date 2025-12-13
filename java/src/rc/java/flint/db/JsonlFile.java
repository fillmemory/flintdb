package flint.db;

import java.io.IOException;
import java.time.LocalDateTime;

/**
 * JSONL File Handler (using Gson)
 */
final class JsonlFile implements GenericFile {
	private final java.io.File file;
	private final Logger logger;
	private java.io.BufferedWriter writer; // non-null when opened for writing
	private Meta cachedMeta; // lazily resolved for reading

	private final com.google.gson.Gson gson = new com.google.gson.Gson();

	private static final int SAMPLE_ROWS = 32;
	private static final boolean TYPE_PREDICT = TSVFile.TYPE_PREDICT; // reuse global switch

	private JsonlFile(final java.io.File file, final Meta meta, final Logger logger, final boolean write) throws java.io.IOException {
		this.file = file;
		this.cachedMeta = meta; // may be null when reading
		this.logger = (logger != null ? logger : new Logger.NullLogger());
		if (write) openWriter();
		else this.logger.log("open r, file size : " + IO.readableBytesSize(file.length()));
	}

	// --------- static helpers ---------

	static JsonlFile open(final java.io.File file) throws java.io.IOException { return open(file, new Logger.NullLogger()); }
	static JsonlFile open(final java.io.File file, final Logger logger) throws java.io.IOException { return new JsonlFile(file, null, logger, false); }

	static JsonlFile create(final java.io.File file, final Column[] columns) throws java.io.IOException { return create(file, columns, new Logger.NullLogger()); }
	static JsonlFile create(final java.io.File file, final Column[] columns, final Logger logger) throws java.io.IOException {
		if (columns == null || columns.length == 0) throw new IllegalArgumentException("columns");
		final Meta meta = new Meta(file.getName()).columns(columns);
		Meta.make(file, meta);
		return new JsonlFile(file, meta, logger, true);
	}

	// --------- lifecycle ---------

	@Override
	public void close() throws java.io.IOException {
		if (writer != null) {
			try { writer.flush(); } catch (Exception ignore) {}
			try { writer.close(); } finally { writer = null; }
		}
		logger.log("closed, file size : " + IO.readableBytesSize(file.length()));
	}

	void drop() throws java.io.IOException { close(); file.delete(); final java.io.File meta = new java.io.File(file.getParentFile(), Meta.name(file.getName()) + Meta.META_NAME_SUFFIX); if (meta.exists()) meta.delete(); }

	@Override
	public long fileSize() { return file.length(); }

	// --------- metadata ---------

	@Override
	public Meta meta() throws java.io.IOException {
		if (cachedMeta != null) return cachedMeta;
		// Try .desc
		try { cachedMeta = Meta.read(file); return cachedMeta; } catch (Exception ignore) {}
		// Infer from first line(s)
		try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(instream(file), java.nio.charset.StandardCharsets.UTF_8))) {
			java.util.List<java.util.Map<String, Object>> samples = new java.util.ArrayList<>();
			String line; int n=0;
			while (n < SAMPLE_ROWS && (line = br.readLine()) != null) {
				line = line.trim();
				if (line.isEmpty()) continue;
				try {
					@SuppressWarnings("unchecked") java.util.Map<String,Object> m = gson.fromJson(line, java.util.Map.class);
					if (m != null) { samples.add(m); n++; }
				} catch (Exception ex) { /* skip */ }
			}
			if (samples.isEmpty()) throw new java.io.IOException("empty jsonl: " + file);
			final Column[] columns = inferColumns(samples);
			cachedMeta = new Meta(file.getName()).columns(columns);
			return cachedMeta;
		}
	}

	private Column[] inferColumns(final java.util.List<java.util.Map<String, Object>> samples) {
		// Collect all keys preserving insertion order
		final java.util.LinkedHashMap<String, java.util.List<Object>> values = new java.util.LinkedHashMap<>();
		for (var m : samples) {
			for (var e : m.entrySet()) {
				final String k = Column.normalize(e.getKey());
				values.computeIfAbsent(k, _k -> new java.util.ArrayList<>()).add(e.getValue());
			}
		}
		final java.util.List<Column> cols = new java.util.ArrayList<>();
		for (var e : values.entrySet()) {
			final String name = e.getKey();
			final short type = (TYPE_PREDICT ? inferType(e.getValue()) : Column.TYPE_STRING);
			final short bytes = inferredBytes(type);
			cols.add(new Column(name, type, bytes, (short) (type == Column.TYPE_DECIMAL ? 5 : 0), false, null, null));
		}
		return cols.toArray(Column[]::new);
	}

	private short inferType(final java.util.List<Object> vs) {
		boolean has = false, allInt=true, allLong=true, allDouble=true, allDate=true, allDateTime=true;
		for (Object o : vs) {
			if (o == null) continue; has = true;
			final String s = o.toString().trim(); if (s.isEmpty()) continue;
			// date
			if (allDate) { try { LocalDateTime.parse(s, Row.DATE_FORMAT); } catch(Exception ex){ allDate=false; } }
			if (allDateTime) { try { LocalDateTime.parse(s, Row.DATE_TIME_FORMAT); } catch(Exception ex){ allDateTime=false; } }
			if (allLong) { try { Long.parseLong(s); } catch(Exception ex){ allLong=false; } }
			if (allInt) { try { Integer.parseInt(s); } catch(Exception ex){ allInt=false; } }
			if (allDouble) { try { Double.parseDouble(s); } catch(Exception ex){ allDouble=false; } }
		}
		if (!has) return Column.TYPE_STRING;
		if (allDate) return Column.TYPE_DATE;
		if (allDateTime) return Column.TYPE_TIME; // time = datetime
		if (allLong || allInt) return Column.TYPE_INT64;
		if (allDouble) return Column.TYPE_DECIMAL;
		return Column.TYPE_STRING;
	}

	private short inferredBytes(final short type) {
		return switch (type) {
			case Column.TYPE_INT64 -> 8;
			case Column.TYPE_DECIMAL -> 20;
			case Column.TYPE_DATE -> 10;
			case Column.TYPE_TIME -> 19;
			default -> Short.MAX_VALUE;
		};
	}

	private static java.io.InputStream instream(final java.io.File f) throws java.io.IOException {
		final String n = f.getName().toLowerCase();
		if (n.endsWith(".gz") || n.endsWith(".gzip")) return new java.util.zip.GZIPInputStream(new java.io.FileInputStream(f), 65535);
		return new java.io.FileInputStream(f);
	}

	private void openWriter() throws java.io.IOException {
		file.getParentFile().mkdirs();
		final boolean gzip = file.getName().endsWith(".gz") || file.getName().endsWith(".gzip");
		java.io.OutputStream os = new java.io.FileOutputStream(file);
		if (gzip) os = new java.util.zip.GZIPOutputStream(os, 8192);
		this.writer = new java.io.BufferedWriter(new java.io.OutputStreamWriter(os, java.nio.charset.StandardCharsets.UTF_8));
		logger.log("open w, file : " + file);
	}

	// --------- write ---------
	@Override
	public long write(final Row row) throws java.io.IOException {
		if (writer == null) throw new java.io.IOException("writer not opened");
		if (cachedMeta == null) cachedMeta = row.meta();
		final Column[] cols = cachedMeta.columns();
		final com.google.gson.JsonObject o = new com.google.gson.JsonObject();
		for (int i=0;i<cols.length;i++) {
			final Column c = cols[i];
			final Object v = row.get(i);
			if (v == null) { o.add(c.name(), com.google.gson.JsonNull.INSTANCE); continue; }
			switch (c.type()) {
			case Column.TYPE_DATE: {
				o.addProperty(c.name(), Row.string(v, c.type()));
				break;
			}
			case Column.TYPE_TIME: {
				o.addProperty(c.name(), Row.string(v, c.type()));
				break;
			}
			case Column.TYPE_INT:
			case Column.TYPE_INT8:
			case Column.TYPE_UINT8:
			case Column.TYPE_INT16:
			case Column.TYPE_UINT16:
			case Column.TYPE_INT64:
			case Column.TYPE_UINT: {
				o.addProperty(c.name(), ((Number) Row.cast(v, c.type(), c.precision())).longValue());
				break;
			}
			case Column.TYPE_DOUBLE: {
				o.addProperty(c.name(), ((Number) Row.cast(v, c.type(), c.precision())).doubleValue());
				break;
			}
			case Column.TYPE_FLOAT: {
				o.addProperty(c.name(), ((Number) Row.cast(v, c.type(), c.precision())).floatValue());
				break;
			}
			case Column.TYPE_DECIMAL: {
				o.addProperty(c.name(), Row.cast(v, c.type(), c.precision()).toString());
				break;
			}
			case Column.TYPE_BYTES: {
				Object casted = Row.cast(v, c.type(), c.precision());
				if (casted instanceof byte[] b) o.addProperty(c.name(), IO.Hex.encode(b));
				else if (casted != null) o.addProperty(c.name(), casted.toString());
				break;
			}
			default: {
				o.addProperty(c.name(), Row.cast(v, c.type(), c.precision()).toString());
			}
			}
		}
		writer.write(gson.toJson(o));
		writer.write('\n');
		return 0L;
	}

	// --------- read ---------
	@Override
	public Cursor<Row> find() throws Exception { return find(Filter.NOLIMIT, Filter.ALL); }

	@Override
	public Cursor<Row> find(final String where) throws Exception {
		final SQL sql = SQL.parse(String.format("SELECT * FROM %s %s", file.getCanonicalPath(), where));
		final Comparable<Row>[] filter = Filter.compile(meta(), null, sql.where());
		return find(Filter.MaxLimit.parse(sql.limit()), filter[1]);
	}

	@Override
	public Cursor<Row> find(final Filter.Limit limit, final Comparable<Row> filter) throws Exception {
		if (writer != null) throw new RuntimeException("writing");
		final Meta m = meta();
		return new Cursor<Row>() {
			private java.io.BufferedReader br;
			private boolean finished = false;
			private final java.util.concurrent.atomic.AtomicLong n = new java.util.concurrent.atomic.AtomicLong(0);
			@SuppressWarnings("resource")
            private final IO.Closer closer = new IO.Closer();

			private void init() throws java.io.IOException {
				if (br != null) return;
				br = closer.register(new java.io.BufferedReader(
					closer.register(new java.io.InputStreamReader(
						closer.register(instream(file)), 
					java.nio.charset.StandardCharsets.UTF_8)), 
					1<<20));
			}

			@Override
			public Row next() {
				if (finished) return null;
				try {
					init();
					String line;
					while ((line = br.readLine()) != null) {
						line = line.trim();
						if (line.isEmpty()) continue;
						Row row = parseLine(m, line);
						if (filter.compareTo(row) == 0) {
							if (!limit.skip()) {
								if (!limit.remains()) { finished = true; return null; }
								row.id(n.get());
								n.incrementAndGet();
								return row;
							}
						}
						n.incrementAndGet();
					}
					finished = true; return null;
				} catch (Exception ex) {
					finished = true; throw new RuntimeException("Error reading JSONL file", ex);
				}
			}

			@Override
			public void close() throws Exception { 
				closer.close();
				finished = true;
			}
		};
	}

    @Override
    public long rows(boolean force) throws IOException {
        if (!force) return -1;

        long rowCount = 0;
        try (var CLOSER = new IO.Closer()) {
            final java.io.InputStream istream = CLOSER.register(instream(file));
            final java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(istream, java.nio.charset.StandardCharsets.UTF_8), 1 << 20);
            
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    rowCount++;
                }
            }
        } catch (Exception e) {
            throw new IOException("Failed to count rows", e);
        }
        return rowCount;
    }

	@SuppressWarnings("unchecked")
	private Row parseLine(final Meta meta, final String line) {
		var map = gson.fromJson(line, java.util.LinkedHashMap.class);
		return Row.create(meta, map);
	}
}
