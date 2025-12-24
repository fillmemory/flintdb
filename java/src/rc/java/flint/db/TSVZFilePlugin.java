package flint.db;

import java.io.File;
import java.io.IOException;

/**
 * Plugin for ZIP-container TSV/CSV files (.tsvz, .csvz)
 */
public class TSVZFilePlugin implements GenericFilePlugin {

    @Override
    public int priority() {
        return 10; // higher than default TSV fallback
    }

    @Override
    public boolean supports(File file) {
        final String name = file.getName().toLowerCase();
        if (name.endsWith(".tsvz") || name.endsWith(".csvz")) return true;
        // also accept .zip that contains .tsv/.csv entries
        if (name.endsWith(".zip")) {
            try (java.util.zip.ZipFile zf = new java.util.zip.ZipFile(file)) {
                var en = zf.entries();
                while (en.hasMoreElements()) {
                    var e = en.nextElement();
                    String ename = e.getName().toLowerCase();
                    if (ename.endsWith(".tsv") || ename.endsWith(".csv") || ename.endsWith("data.tsv") || ename.endsWith("data.csv"))
                        return true;
                }
            } catch (IOException ignored) {
            }
        }
        return false;
    }

    @Override
    public GenericFile open(File file) throws IOException {
        return TSVZFile.open(file);
    }

    @Override
    public GenericFile create(File file, Column[] columns, Logger logger) throws IOException {
        return TSVZFile.create(file, columns, logger);
    }
}
