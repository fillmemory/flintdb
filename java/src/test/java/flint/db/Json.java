/**
 * Json.java
 */
package flint.db;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.stream.JsonWriter;

/**
 * Json utility class for serialization and deserialization.
 */
final class Json {

	private static final class IndexDeserializer implements JsonDeserializer<Index> {
		@Override
		public Index deserialize(final JsonElement json, final Type typeOfT, final JsonDeserializationContext context) throws JsonParseException {
			// System.out.println("json : " + json);
			final JsonObject o = json.getAsJsonObject();
			final JsonElement t = o.get("type");
			if ("primary".equals(t.getAsString()))
				return context.deserialize(json, Table.PrimaryKey.class);
			// if ("unique".equals(t.getAsString()))
			// return context.deserialize(json, Table.UniqueKey.class);
			if ("sort".equals(t.getAsString()))
				return context.deserialize(json, Table.SortKey.class);
			return null;
		}
	}

	private static final class ColumnDeserializer implements JsonDeserializer<Column> {
		@Override
		public Column deserialize(final JsonElement json, final Type typeOfT, final JsonDeserializationContext context) throws JsonParseException {
			// System.out.println("json : " + json);
			final JsonObject o = json.getAsJsonObject();
			final String name = o.get("name").getAsString();
			final short type = Column.valueOf(o.get("type").getAsString());
			final short bytes = !o.has("bytes") ? -1 : o.get("bytes").getAsShort();
			final short precision = !o.has("precision") ? -1 : o.get("precision").getAsShort();
			final Object value = !o.has("value") ? -1 : (o.get("value").getAsJsonPrimitive().isString() ? o.get("value").getAsString() : o.get("value").getAsBigDecimal());
			return new Column(name, type, bytes, precision, false, value, null);
		}
	}

	public static String stringify(final Object o) {
		return stringify(o, false);
	}

	public static String stringify(final Object o, boolean pretty) {
		return stringify(o, 3);
	}

	public static String stringify(final Object o, final int pretty) {
		// https://stackoverflow.com/questions/42064140/gson-how-to-skip-serialization-of-specific-default-values
		// https://stackoverflow.com/questions/31239190/avoid-primitive-data-types-default-value-in-json-mapping-using-gson
		final GsonBuilder gb = new GsonBuilder() //
				.setDateFormat("yyyy-MM-dd HH:mm:ss") //
				.registerTypeAdapter(Double.class, new JsonSerializer<Double>() {
					@Override
					public JsonElement serialize(Double src, Type typeOfSrc, JsonSerializationContext context) {
						if (src == src.longValue())
							return new JsonPrimitive(src.longValue());
						return new JsonPrimitive(src);
					}
				}) //
				.registerTypeAdapter(BigDecimal.class, new JsonSerializer<BigDecimal>() {
					@Override
					public JsonElement serialize(BigDecimal src, Type typeOfSrc, JsonSerializationContext context) {
						if (src.stripTrailingZeros().scale() <= 0)
							return new JsonPrimitive(src.longValue());

						return new JsonPrimitive(src.setScale(3, RoundingMode.DOWN));
					}
				}) //
				.registerTypeAdapter(Column.class, new JsonSerializer<Column>() {
					@Override
					public JsonElement serialize(Column c, Type typeOfSrc, JsonSerializationContext context) {
						final JsonObject o = new JsonObject();
						o.addProperty("name", c.name());
						o.addProperty("type", Column.typename(c.type()).replace("TYPE_", ""));
						o.addProperty("bytes", c.bytes());
						if (c.precision() > 0)
							o.addProperty("precision", c.precision());
						if (c.value() != null) {
							if (c.value() instanceof Number)
								o.addProperty("value", new BigDecimal(c.value().toString()));
							else
								o.addProperty("value", c.value().toString());
						}
						return o;
					}
				}) //
		;

		if (pretty > 0) {
			gb.setPrettyPrinting(); //
			final Gson g = gb.create();

			final StringWriter out = new StringWriter();
			final JsonWriter jsonWriter = new PrettyJsonWriter(pretty, out);
			g.toJson(g.toJsonTree(o), jsonWriter);
			return out.toString();
		} else {
			final Gson g = gb.create();
			return g.toJson(o);
		}
	}

	static final class PrettyJsonWriter extends JsonWriter {
		private int level;
		private final int prettyDepth;

		public PrettyJsonWriter(final int prettyDepth, final Writer out) {
			super(out);
			this.prettyDepth = prettyDepth;
		}

		@Override
		public JsonWriter beginArray() throws IOException {
			final JsonWriter jsonWriter = super.beginArray();
			level++;
			adjustIndent();
			return jsonWriter;
		}

		@Override
		public JsonWriter endArray() throws IOException {
			final JsonWriter jsonWriter = super.endArray();
			level--;
			adjustIndent();
			return jsonWriter;
		}

		@Override
		public JsonWriter beginObject() throws IOException {
			final JsonWriter jsonWriter = super.beginObject();
			level++;
			adjustIndent();
			return jsonWriter;
		}

		@Override
		public JsonWriter endObject() throws IOException {
			final JsonWriter jsonWriter = super.endObject();
			level--;
			adjustIndent();
			return jsonWriter;
		}

		// handle whatever rules you want
		private void adjustIndent() {
			final String indent;
			if (level <= prettyDepth)
				indent = "\t";
			else
				indent = "";
			setIndent(indent);
		}
	}

	public static <T> T fromJson(final InputStream instream, final Type type) throws IOException {
		final GsonBuilder builder = new GsonBuilder();
		builder.registerTypeAdapter(Index.class, new Json.IndexDeserializer());
		builder.registerTypeAdapter(Column.class, new Json.ColumnDeserializer());
		return builder.create().fromJson(new InputStreamReader(instream, "UTF-8"), type);
	}

	public static <T> T fromJson(final File file, final Type type) throws IOException {
		try (final InputStream instream = instream(file)) {
			return fromJson(instream, type);
		}
	}

	static InputStream instream(File file) throws IOException {
		String n = file.getName().toLowerCase();
		if (n.endsWith(".gz") || n.endsWith(".gzip")) {
			return new java.util.zip.GZIPInputStream(new java.io.FileInputStream(file), 65535);
		}
		return new java.io.FileInputStream(file);
	}
}
