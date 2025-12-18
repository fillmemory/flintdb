package flint.db;

import java.io.File;
import java.io.IOException;

/**
 * Write-Ahead Logging (WAL) interface for database operations.
 * 
 * <pre>
 * 1. BPlusTree, Table 의 구조를 변경을 최소화 하도록 WAL Layer만 추가
 * 2. MVCC는 미구현
 * 3. TODO: Memory Mode 지원 (안정성 낮음, 속도 빠름, Bulk Insert 용)
 * </pre>
 */
interface WAL extends AutoCloseable {
  
    /**
     * Starts a new transaction in the WAL.
     * @return the transaction ID
     */
    long begin();

    /**
     * Commits the transaction with the given ID.
     * @param id the transaction ID
     */
    void commit(long id);

    /**
     * Rolls back the transaction with the given ID.
     * @param id the transaction ID
     */
    void rollback(long id);


    /**
     * Recovers the database state from the WAL.
     * @throws IOException
     */
    void recover() throws IOException;

    /**
     * Creates a checkpoint and optionally truncates the WAL if all transactions are committed.
     * @throws IOException
     */
    void checkpoint() throws IOException;


    /**
     * No Transaction WAL implementation. (For in-memory databases or fast bulk, etc.)
     */
    static final WAL NONE = new WAL() {
        @Override
        public void close() {
        }

        @Override
        public long begin() {
            return 1; // Always return 1 for no-transaction WAL
        }

        @Override
        public void commit(long id) {
        }

        @Override
        public void rollback(long id) {
        }

        @Override
        public void recover() {
        }

        @Override
        public void checkpoint() {
        }
    };

    static WAL open(File file, Meta meta) throws IOException {
        return new WALImpl(file, meta);
    }

    static Storage wrap(WAL wal, Storage.Options options, Callback callback) throws IOException {
        if (wal == null) 
            throw new IllegalArgumentException("WAL instance cannot be null");

        if (wal instanceof WALImpl) {
            WALImpl walImpl = (WALImpl) wal;
            return walImpl.wrap(options, callback);
        }

        assert wal == WAL.NONE;
        return Storage.create(options);
    }

    /**
     * Callback interface for applying or rolling back WAL records.
     */
    static interface Callback {
        /**
         * Applies the changes recorded at the given offset.
         * @param offset
         * @throws IOException
         */
        void refresh(long offset) throws IOException;
    }

    // Operation types
    static final byte OP_BEGIN = 0x00;      // Transaction start
    static final byte OP_WRITE = 0x01;      // Write page
    static final byte OP_DELETE = 0x02;     // Delete page
    static final byte OP_UPDATE = 0x03;     // Update page
    static final byte OP_COMMIT = 0x10;     // Transaction commit
    static final byte OP_ROLLBACK = 0x11;   // Transaction rollback
    static final byte OP_CHECKPOINT = 0x20; // Checkpoint marker
}
