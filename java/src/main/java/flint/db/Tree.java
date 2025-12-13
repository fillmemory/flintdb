package flint.db;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;

interface Tree extends Closeable {
    
    long count() throws IOException;
    long bytes() throws IOException;
    int height() throws IOException;
    Storage storage();

    void put(final long key) throws IOException;
    boolean delete(final long key) throws IOException;

    Cursor<Long> find(final int sort, final Comparable<Long> comparable) throws Exception;
    Long get(final Comparable<Long> key) throws Exception;

    /**
     * Create a new instance of B+Tree.
     *
     * @param file      B+Tree index file
     * @param mutable   turn on/off mutable storage
     * @param storage   Storage type (default, v2, memory)
     * @param increment Storage increment size
     * @param cacheSize Node cacheSize
     * @param sorter    Key sorter
     * @return a new instance of B+Tree
     * @throws IOException if an I/O error occurs
     */
    static Tree newInstance(final File file, //
			final boolean mutable, //
			final String storage, //
			final int increment, //
			final int cacheSize, //
			final Comparator<Long> sorter, //
            final WAL wal //
	) throws IOException {
        // if ("2".equals(System.getProperty("BPTREE.VERSION"))) 
        //     return new BPlusTreeV2( //
        //             file, //
        //             mutable, //
        //             storage, //
        //             increment, //
        //             cacheSize, //
        //             sorter //
        //     );
        try {
            return new BPlusTree( //
                    file, //
                    mutable, //
                    storage, //
                    increment, //
                    cacheSize, //
                    sorter, //
                    wal //
            );
        } catch(IOException e) {
            throw new IOException(String.format("Failed to create B+Tree for file: %s", file), e);
        }
    }
}
