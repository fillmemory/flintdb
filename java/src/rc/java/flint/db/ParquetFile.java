package flint.db;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;

/**
 * FlintDB Parquet File Handler (using parquet-avro)
 *
 * Provides high-performance streaming read/write of Parquet files with
 * schema mapping from/to FlintDB Meta and Row.
 */
public final class ParquetFile implements GenericFile {

    private final File file;
    private final Logger logger;

    private volatile Meta cachedMeta;
    private volatile Schema avroSchema;

    private ParquetReader<GenericRecord> reader;
    private ParquetWriter<GenericRecord> writer;

    private final CompressionCodecName compressionCodec;

    private ParquetFile(final File file, final Meta meta, final Logger logger,
            final CompressionCodecName compressionCodec) {
        this.file = file;
        this.cachedMeta = meta; // may be null and lazily resolved
        this.logger = (logger != null ? logger : new Logger.NullLogger());
        this.compressionCodec = (compressionCodec != null ? compressionCodec : CompressionCodecName.SNAPPY);
    }

    // ---------- Static open/create helpers ----------

    /** Open Parquet file for reading. */
    public static ParquetFile open(final File file) throws IOException {
        return open(file, new Logger.NullLogger());
    }

    /** Open Parquet file for reading with logger. */
    public static ParquetFile open(final File file, final Logger logger) throws IOException {
        final ParquetFile f = new ParquetFile(file, null, logger, null);
        // Lazy reader/open when find() is called; just log now.
        logger.log("open r, file size : " + IO.readableBytesSize(file.length()));
        return f;
    }

    /**
     * Create Parquet file for writing using provided columns. Also writes .desc
     * meta.
     */
    public static ParquetFile create(final File file, final Column[] columns) throws IOException {
        return create(file, columns, new Logger.NullLogger());
    }

    /**
     * Create Parquet file for writing using provided columns. Also writes .desc
     * meta.
     */
    public static ParquetFile create(final File file, final Column[] columns, final Logger logger) throws IOException {
        return create(file, columns, logger, null);
    }

    /**
     * Create Parquet file for writing using provided columns. Also writes .desc
     * meta.
     */
    public static ParquetFile create(final File file, final Column[] columns, final Logger logger,
            final CompressionCodecName compressionCodec) throws IOException {
        if (columns == null || columns.length == 0)
            throw new IllegalArgumentException("columns");

        // synthetic primary key if not provided (first column)
        final Meta meta = new Meta(file.getName()).columns(columns);
        try {
            meta.indexes(new Index[] { new Table.PrimaryKey(new String[] { columns[0].name() }) });
        } catch (RuntimeException ignore) {
            /* if already validated elsewhere */ }

        final ParquetFile f = new ParquetFile(file, meta, logger, compressionCodec);
        f.openWriter();
        return f;
    }

    // ---------- Lifecycle ----------

    @Override
    public void close() throws IOException {
        IOException first = null;
        try {
            if (reader != null)
                reader.close();
        } catch (IOException ex) {
            first = ex;
        } finally {
            reader = null;
        }
        try {
            if (writer != null)
                writer.close();
        } catch (IOException ex) {
            if (first == null)
                first = ex;
        } finally {
            writer = null;
        }

        logger.log("closed, file size : " + IO.readableBytesSize(file.length()));
        if (first != null)
            throw first;

        // delete .<filename>.parquet.crc
        File crcFile = new File(file.getParentFile(), "." + file.getName() + ".crc");
        if (crcFile.exists()) {
            if (!crcFile.delete()) {
                logger.log("Failed to delete CRC file: " + crcFile);
            }
        }
    }

    public void drop() throws IOException {
        close();
        GenericFile.drop(file);
    }

    @Override
    public long fileSize() {
        return file.length();
    }

    public static long rows(final File parquetFile) {
        try (ParquetFileReader fr = ParquetFileReader.open(new NioInputFile(parquetFile))) {
            final ParquetMetadata footer = fr.getFooter();
            return footer.getBlocks().stream().mapToLong(b -> b.getRowCount()).sum();
        } catch (Exception ignore) {
        }
        return -1;
    }

    // ---------- Metadata ----------

    @Override
    public Meta meta() throws IOException {
        if (cachedMeta != null)
            return cachedMeta;

        // Try to read schema from Parquet metadata first
        try (ParquetFileReader fr = ParquetFileReader.open(localInput())) {
            final ParquetMetadata footer = fr.getFooter();
            final java.util.Map<String, String> kv = footer.getFileMetaData().getKeyValueMetaData();
            final String schemaSql = kv.get("flintdb.schema");
            if (schemaSql != null) {
                this.cachedMeta = SQL.parse(schemaSql).meta();
                return cachedMeta;
            }
        } catch (Exception ignore) {
            // Fallback to inference
        }

        // Fallback: infer from Parquet schema
        final Schema s = readSchemaDirect();
        if (s == null)
            throw new IOException("Cannot read Parquet schema: " + file);

        this.avroSchema = s;
        this.cachedMeta = new Meta(file.getName()).columns(columnsFromAvro(s));
        return cachedMeta;
    }

    // ---------- Read ----------

    @Override
    public Cursor<Row> find() throws Exception {
        return find(Filter.NOLIMIT, Filter.ALL);
    }

    @Override
    public Cursor<Row> find(final String where) throws Exception {
        final SQL sql = SQL.parse(String.format("SELECT * FROM %s %s", file.getCanonicalPath(), where));
        final Comparable<Row>[] filter = Filter.compile(meta(), null, sql.where());
        return find(Filter.MaxLimit.parse(sql.limit()), filter[1]);
    }

    @Override
    public Cursor<Row> find(final Filter.Limit limit, final Comparable<Row> filter) throws Exception {
        if (writer != null)
            throw new RuntimeException("writing");

        final Meta m = meta();
        if (this.avroSchema == null)
            this.avroSchema = ensureAvroSchema(m);

        return new Cursor<Row>() {
            private boolean initialized = false;
            private boolean finished = false;
            private final AtomicLong n = new AtomicLong(0);

            private void init() throws IOException {
                if (!initialized) {
                    reader = AvroParquetReader.<GenericRecord>builder(localInput()).build();
                    initialized = true;
                }
            }

            @Override
            public Row next() {
                if (finished)
                    return null;
                try {
                    init();
                    GenericRecord rec;
                    while ((rec = reader.read()) != null) {
                        final Row row = recordToRow(m, rec);
                        if (filter.compareTo(row) == 0) {
                            if (!limit.skip()) {
                                if (!limit.remains()) {
                                    finished = true;
                                    return null;
                                }
                                row.id(n.get());
                                n.incrementAndGet();
                                return row;
                            }
                        }
                        n.incrementAndGet();
                    }
                    finished = true;
                    return null;
                } catch (Exception ex) {
                    finished = true;
                    throw new RuntimeException("Error reading Parquet file", ex);
                }
            }

            @Override
            public void close() throws Exception {
                finished = true;
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (Exception ignore) {
                    }
                }
            }
        };
    }

    @Override
    public long rows(boolean force) throws IOException {
        return ParquetFile.rows(file);
    }

    // ---------- Write ----------

    @Override
    public long write(final Row row) throws IOException {
        if (writer == null)
            throw new IOException("writer not opened");
        final GenericRecord rec = rowToRecord(cachedMeta, row);
        writer.write(rec);
        return 0L;
    }

    // ---------- Internals ----------

    private void openWriter() throws IOException {
        final Meta m = (cachedMeta != null) ? cachedMeta : meta();
        this.avroSchema = ensureAvroSchema(m);

        final java.util.Map<String, String> metadata = new java.util.HashMap<>();
        metadata.put("flintdb.schema", SQL.stringify(m));
        metadata.put("flintdb.version", "1.0");

        this.writer = AvroParquetWriter.<GenericRecord>builder(localOutput())
                .withSchema(avroSchema)
                .withCompressionCodec(compressionCodec)
                .withExtraMetaData(metadata)
                .build();
        logger.log("open w, file : " + file);
    }

    private InputFile localInput() {
        return new NioInputFile(file);
    }

    private OutputFile localOutput() {
        return new NioOutputFile(file);
    }

    private Schema ensureAvroSchema(final Meta meta) {
        if (this.avroSchema != null)
            return this.avroSchema;
        this.avroSchema = buildAvroSchema(meta);
        return this.avroSchema;
    }

    private Schema readSchemaDirect() {
        // Try reading footer schema without materializing a record
        try (ParquetFileReader fr = ParquetFileReader.open(localInput())) {
            final ParquetMetadata footer = fr.getFooter();
            final org.apache.parquet.schema.MessageType mt = footer.getFileMetaData().getSchema();
            // Use Avro schema from file metadata if available
            final String avroSchemaStr = footer.getFileMetaData().getKeyValueMetaData().get("parquet.avro.schema");
            if (avroSchemaStr != null)
                return new Schema.Parser().parse(avroSchemaStr);
            // Fallback: map Parquet MessageType roughly to Avro (strings for unsupported)
            return parquetToAvroFallback(mt);
        } catch (Exception ignore) {
        }
        return null;
    }

    private Schema parquetToAvroFallback(final org.apache.parquet.schema.MessageType mt) {
        // Best-effort: treat primitive fields as their closest Avro types; others as
        // string.
        final List<Field> fields = new ArrayList<>();
        for (org.apache.parquet.schema.Type t : mt.getFields()) {
            final String name = t.getName();
            Schema fSchema = Schema.create(Type.STRING);
            if (t.isPrimitive()) {
                fSchema = switch (t.asPrimitiveType().getPrimitiveTypeName()) {
                    case INT32 -> Schema.create(Type.INT);
                    case INT64 -> Schema.create(Type.LONG);
                    case DOUBLE -> Schema.create(Type.DOUBLE);
                    case FLOAT -> Schema.create(Type.FLOAT);
                    case BINARY -> Schema.create(Type.BYTES);
                    case BOOLEAN -> Schema.create(Type.INT);
                    default -> Schema.create(Type.STRING);
                }; // map to int(0/1)
            }
            fields.add(new Field(name, nullable(fSchema), null, (Object) null));
        }
        final Schema rec = Schema.createRecord(safeName(Meta.name(file.getName())), null, getClass().getPackageName(),
                false);
        rec.setFields(fields);
        return rec;
    }

    private Schema buildAvroSchema(final Meta meta) {
        final List<Field> fields = new ArrayList<>();
        for (final Column c : meta.columns()) {
            fields.add(new Field(c.name(), nullable(avroType(c)), null, (Object) null));
        }
        final Schema rec = Schema.createRecord(safeName(meta.name()), null, getClass().getPackageName(), false);
        rec.setFields(fields);
        return rec;
    }

    private static Schema nullable(final Schema s) {
        final List<Schema> u = new ArrayList<>(2);
        u.add(Schema.create(Type.NULL));
        u.add(s);
        return Schema.createUnion(u);
    }

    private static String safeName(final String n) {
        String s = n;
        if (s.endsWith(Meta.META_NAME_SUFFIX))
            s = s.substring(0, s.length() - Meta.META_NAME_SUFFIX.length());
        return s.replaceAll("[^A-Za-z0-9_]", "_");
    }

    private Schema avroType(final Column c) {
        switch (c.type()) {
            case Column.TYPE_INT:
            case Column.TYPE_INT8:
            case Column.TYPE_UINT8:
            case Column.TYPE_INT16:
            case Column.TYPE_UINT16:
                return Schema.create(Type.INT);
            case Column.TYPE_INT64:
            case Column.TYPE_UINT:
                return Schema.create(Type.LONG);
            case Column.TYPE_DOUBLE:
                return Schema.create(Type.DOUBLE);
            case Column.TYPE_FLOAT:
                return Schema.create(Type.FLOAT);
            case Column.TYPE_BYTES:
                return Schema.create(Type.BYTES);
            case Column.TYPE_DECIMAL:
            case Column.TYPE_UUID:
            case Column.TYPE_IPV6:
            case Column.TYPE_DATE:
            case Column.TYPE_TIME:
            case Column.TYPE_STRING:
            default:
                return Schema.create(Type.STRING);
        }
    }

    private GenericRecord rowToRecord(final Meta meta, final Row row) {
        final GenericRecord rec = new GenericData.Record(ensureAvroSchema(meta));
        final Column[] cols = meta.columns();
        for (int i = 0; i < cols.length; i++) {
            final Column c = cols[i];
            final Object v = row.get(i);
            rec.put(c.name(), toAvroValue(c, v));
        }
        return rec;
    }

    private Object toAvroValue(final Column c, final Object v) {
        if (v == null)
            return null;
        switch (c.type()) {
            case Column.TYPE_INT:
            case Column.TYPE_INT8:
            case Column.TYPE_UINT8:
            case Column.TYPE_INT16:
            case Column.TYPE_UINT16:
                return ((Number) Row.cast(v, c.type(), c.precision())).intValue();
            case Column.TYPE_INT64:
            case Column.TYPE_UINT:
                return ((Number) Row.cast(v, c.type(), c.precision())).longValue();
            case Column.TYPE_DOUBLE:
                return ((Number) Row.cast(v, c.type(), c.precision())).doubleValue();
            case Column.TYPE_FLOAT:
                return ((Number) Row.cast(v, c.type(), c.precision())).floatValue();
            case Column.TYPE_BYTES: {
                final Object casted = Row.cast(v, c.type(), c.precision());
                if (casted instanceof byte[])
                    return ByteBuffer.wrap((byte[]) casted);
                try {
                    return ByteBuffer.wrap(IO.Hex.decode(Row.string(casted, c.type())));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            case Column.TYPE_DECIMAL:
            case Column.TYPE_UUID:
            case Column.TYPE_IPV6:
            case Column.TYPE_DATE:
            case Column.TYPE_TIME:
            case Column.TYPE_STRING:
            default:
                return Row.string(Row.cast(v, c.type(), c.precision()), c.type());
        }
    }

    private Row recordToRow(final Meta meta, final GenericRecord rec) {
        final Column[] cols = meta.columns();
        final Object[] a = new Object[cols.length];
        for (int i = 0; i < cols.length; i++) {
            final Column c = cols[i];
            final Object v = rec.get(c.name());
            a[i] = fromAvroValue(c, v);
        }
        return Row.create(meta, a);
    }

    private Object fromAvroValue(final Column c, final Object v) {
        if (v == null)
            return null;
        switch (c.type()) {
            case Column.TYPE_BYTES:
                if (v instanceof ByteBuffer bb) {
                    final byte[] b = new byte[bb.remaining()];
                    bb.get(b);
                    return b;
                }
                if (v instanceof byte[])
                    return (byte[]) v;
                return null;
            case Column.TYPE_INT:
            case Column.TYPE_INT8:
            case Column.TYPE_UINT8:
            case Column.TYPE_INT16:
            case Column.TYPE_UINT16:
            case Column.TYPE_INT64:
            case Column.TYPE_UINT:
            case Column.TYPE_DOUBLE:
            case Column.TYPE_FLOAT:
                // Let Row.cast handle numeric conversions from String/Number
                break;
            case Column.TYPE_DECIMAL:
            case Column.TYPE_UUID:
            case Column.TYPE_IPV6:
            case Column.TYPE_DATE:
            case Column.TYPE_TIME:
            case Column.TYPE_STRING:
            default:
                // Avro may return Utf8 - use toString()
                return v.toString();
        }
        return (v instanceof CharSequence) ? v.toString() : v;
    }

    private Column[] columnsFromAvro(final Schema s) {
        final List<Column> cols = new ArrayList<>();
        for (final Field f : s.getFields()) {
            final Schema t = unwrapNullable(f.schema());
            final short type = toLiteType(t.getType());
            final short defaultBytes = (type == Column.TYPE_STRING || type == Column.TYPE_BYTES) ? Short.MAX_VALUE
                    : (short) -1;
            final short bytes = Column.bytes(type, defaultBytes, (short) 0);
            cols.add(new Column(f.name(), type, bytes, (short) 0, false, null, null));
        }
        return cols.toArray(Column[]::new);
    }

    private static Schema unwrapNullable(final Schema s) {
        if (s.getType() == Type.UNION) {
            for (Schema e : s.getTypes()) {
                if (e.getType() != Type.NULL)
                    return e;
            }
            return Schema.create(Type.NULL);
        }
        return s;
    }

    private short toLiteType(final Type avroType) {
        return switch (avroType) {
            case INT -> Column.TYPE_INT;
            case LONG -> Column.TYPE_INT64;
            case DOUBLE -> Column.TYPE_DOUBLE;
            case FLOAT -> Column.TYPE_FLOAT;
            case BYTES -> Column.TYPE_BYTES;
            case STRING -> Column.TYPE_STRING;
            case BOOLEAN -> Column.TYPE_INT;
            default -> Column.TYPE_STRING;
        }; // map to 0/1
    }
}

// ---------- Minimal NIO-backed Parquet IO (no Hadoop) ----------

final class NioInputFile implements InputFile {
    private final File file;

    NioInputFile(final File file) {
        this.file = file;
    }

    @Override
    public long getLength() throws IOException {
        return file.length();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new NioSeekableInputStream(file);
    }
}

final class NioSeekableInputStream extends SeekableInputStream {
    private final FileChannel ch;
    private long pos = 0L;

    NioSeekableInputStream(final File file) throws IOException {
        this.ch = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    }

    @Override
    public int read() throws IOException {
        final ByteBuffer one = ByteBuffer.allocate(1);
        int n = ch.read(one, pos);
        if (n <= 0)
            return -1;
        pos += n;
        one.flip();
        return one.get() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        final ByteBuffer bb = ByteBuffer.wrap(b, off, len);
        int n = ch.read(bb, pos);
        if (n > 0)
            pos += n;
        return n;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int n = ch.read(dst, pos);
        if (n > 0)
            pos += n;
        return n;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int off, int len) throws IOException {
        int read = 0;
        while (read < len) {
            int n = read(bytes, off + read, len - read);
            if (n < 0)
                throw new EOFException("Unexpected EOF");
            read += n;
        }
    }

    @Override
    public void readFully(ByteBuffer dst) throws IOException {
        while (dst.hasRemaining()) {
            int n = read(dst);
            if (n < 0)
                throw new EOFException("Unexpected EOF");
        }
    }

    @Override
    public long getPos() {
        return pos;
    }

    @Override
    public void seek(long newPos) {
        this.pos = newPos;
    }

    @Override
    public int available() throws IOException {
        long rem = ch.size() - pos;
        return (rem > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) rem;
    }

    @Override
    public void close() throws IOException {
        ch.close();
    }
}

final class NioOutputFile implements OutputFile {
    private final File file;

    NioOutputFile(final File file) {
        this.file = file;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return createOrOverwrite(blockSizeHint);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        if (file.getParentFile() != null) {
            final File parent = file.getParentFile();
            final java.nio.file.Path ppath = parent.toPath();
            if (Files.exists(ppath)) {
                if (Files.isSymbolicLink(ppath)) {
                    // 심볼릭 링크의 대상이 디렉토리인지 확인
                    java.nio.file.Path target = Files.readSymbolicLink(ppath);
                    java.nio.file.Path resolved = ppath.resolveSibling(target).normalize();
                    if (!(Files.exists(resolved) && Files.isDirectory(resolved))) {
                        throw new IOException(
                                "Parent is a symlink but its target is not a directory: " + parent + " -> " + target);
                    }
                } else if (!Files.isDirectory(ppath)) {
                    throw new IOException("Parent exists but is not a directory: " + parent);
                }
            } else {
                Files.createDirectories(ppath);
            }
        }
        final FileChannel ch = FileChannel.open(
                file.toPath(),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        return new NioPositionOutputStream(ch);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }
}

final class NioPositionOutputStream extends PositionOutputStream {
    private final FileChannel ch;
    private long pos = 0L;

    NioPositionOutputStream(final FileChannel ch) {
        this.ch = ch;
    }

    @Override
    public long getPos() {
        return pos;
    }

    @Override
    public void write(int b) throws IOException {
        final ByteBuffer bb = ByteBuffer.allocate(1);
        bb.put((byte) b);
        bb.flip();
        int n = ch.write(bb, pos);
        if (n > 0)
            pos += n;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        final ByteBuffer bb = ByteBuffer.wrap(b, off, len);
        int n = ch.write(bb, pos);
        if (n > 0)
            pos += n;
    }

    @Override
    public void close() throws IOException {
        ch.close();
    }
}
