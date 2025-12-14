package flint.db;

public interface Transaction extends AutoCloseable {
    void commit();
    void rollback();
    long id();

    static Transaction noop() {
        return new Transaction() {
            @Override
            public void commit() {
                // no-op
            }

            @Override
            public void rollback() {
                // no-op
            }

            @Override
            public long id() {
                return 0L;
            }

            @Override
            public void close() {
            }
        };
    }
}
