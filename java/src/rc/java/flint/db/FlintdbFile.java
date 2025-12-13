package flint.db;

import java.io.File;
import java.io.IOException;


/**
 * Flintdb file implementation of GenericFile interface.
 */
final class FlintdbFile implements GenericFile {
    final Table table;

    FlintdbFile(final Table table) throws IOException {
        this.table = table;
    }

    static FlintdbFile open(final File file) throws IOException {
        return new FlintdbFile(Table.open(file, Table.OPEN_READ));
    }

    static FlintdbFile create(final File file, final Meta meta, final Logger logger) throws IOException {
        return new FlintdbFile(Table.open(file, meta, logger));
    }

    @Override
    public void close() throws Exception {
        table.close();
    }

    @Override
    public Meta meta() throws IOException {
        return table.meta();
    }

    @Override
    public long write(Row row) throws IOException {
        return table.apply(row);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Cursor<Row> find(Filter.Limit limit, Comparable<Row> filter) throws Exception {
        final Cursor<Long> cursor = table.find(Index.PRIMARY, Filter.ASCENDING, limit, new Comparable[] {
            Filter.ALL, filter
        });
        return adapt(cursor, table);
    }

    @Override
    public Cursor<Row> find() throws Exception {
        return find("");
    }

    @Override
    public Cursor<Row> find(String where) throws Exception {
        final Cursor<Long> cursor = table.find(where);
        return adapt(cursor, table);
    }

    static Cursor<Row> adapt(final Cursor<Long> cursor, final Table table) {
        return new Cursor<Row>() {
            @Override
            public Row next() {
                try {
                    long i = cursor.next();
                    if (i == -1) return null;
                        return table.read(i);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
            @Override
            public void close() throws Exception {
                cursor.close();
            }
        };
    }

    @Override
    public long fileSize() {
        return table.bytes();
    }
    

    @Override
    public long rows(boolean force) throws IOException {
        return table.rows();
    }
}
