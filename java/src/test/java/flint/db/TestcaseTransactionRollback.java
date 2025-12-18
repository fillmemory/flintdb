package flint.db;

import java.io.File;

/**
 * Validates that transactional rollback keeps index/table row counts consistent.
 *
 * This specifically guards against the classic bug where B+Tree count is stored
 * outside WAL scope (e.g., in a file header) and becomes inconsistent after rollback.
 */
public final class TestcaseTransactionRollback {

    static void TRUE(boolean expr, String exc) {
        if (expr) return;
        throw new RuntimeException(exc);
    }

    public static void main(String[] args) throws Exception {
        final File file = new File("temp/test_tx_rollback.flintdb");
        Table.drop(file);

        final Meta meta = new Meta(file.getName())
                .columns(new Column[] {
                        new Column.Builder("id", Column.TYPE_INT64).create(),
                })
                .indexes(new Index[] {
                        new Table.PrimaryKey(new String[] { "id" }),
                })
                .walMode(Meta.WAL_OPT_TRUNCATE);

        try (Table t = Table.open(file, meta, new Logger.NullLogger())) {
            TRUE(t.rows() == 0L, "Expected empty table");

            // 1) Insert and rollback
            try (Transaction tx = t.begin()) {
                Row r = Row.create(meta, new Object[] { 1L });
                t.apply(r, false, tx);
                TRUE(t.rows() == 1L, "Expected rows=1 inside transaction");
                tx.rollback();
            }
            TRUE(t.rows() == 0L, "Expected rows=0 after rollback");

            // 2) Insert and commit
            try (Transaction tx = t.begin()) {
                Row r = Row.create(meta, new Object[] { 2L });
                t.apply(r, false, tx);
                tx.commit();
            }
            TRUE(t.rows() == 1L, "Expected rows=1 after commit");
        }

        // 3) Reopen and verify persisted count
        try (Table t = Table.open(file, Meta.OPEN_RDONLY, new Logger.NullLogger())) {
            TRUE(t.rows() == 1L, "Expected rows=1 after reopen");
        }

        System.out.println("OK");
    }
}
