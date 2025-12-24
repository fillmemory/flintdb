package flint.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * ZIP-container TSV/CSV file handler. Internally extracts data entry to a
 * temporary TSV/CSV file and delegates to `TSVFile` for parsing.
 */
public final class TSVZFile implements GenericFile {

    private final File container;
    private final File tmp;
    private final TSVFile inner;
    private final boolean created;

    private TSVZFile(final File container, final File tmp, final TSVFile inner, final boolean created) {
        this.container = container;
        this.tmp = tmp;
        this.inner = inner;
        this.created = created;
    }

    static TSVZFile open(final File file) throws IOException {
        // extract data entry to temp file
        try (ZipFile zf = new ZipFile(file)) {
            ZipEntry target = null;
            // prefer data.tsv/data.csv, otherwise first .tsv/.csv
            Enumeration<? extends ZipEntry> en = zf.entries();
            while (en.hasMoreElements()) {
                ZipEntry e = en.nextElement();
                String n = e.getName().toLowerCase();
                if (n.equals("data.tsv") || n.equals("data.csv")) {
                    target = e;
                    break;
                }
                if (target == null && (n.endsWith(".tsv") || n.endsWith(".csv")))
                    target = e;
            }

            if (target == null) throw new IOException("No data entry (.tsv/.csv) in zip container");

            String suffix = target.getName().toLowerCase().endsWith(".csv") ? ".csv" : ".tsv";
            Path tmpPath = Files.createTempFile("flintdb_tsvz_", suffix);
            try (InputStream in = zf.getInputStream(target); OutputStream out = new FileOutputStream(tmpPath.toFile())) {
                byte[] buf = new byte[8192];
                int r;
                while ((r = in.read(buf)) > 0) out.write(buf, 0, r);
            }

            TSVFile t = TSVFile.open(tmpPath.toFile());
            return new TSVZFile(file, tmpPath.toFile(), t, false);
        }
    }

    static TSVZFile create(final File file, final Column[] columns, final Logger logger) throws IOException {
        String name = file.getName().toLowerCase();
        String suffix = name.endsWith(".csvz") ? ".csv" : ".tsv";
        Path tmpPath = Files.createTempFile("flintdb_tsvz_create_", suffix);
        TSVFile t = TSVFile.create(tmpPath.toFile(), columns, logger);
        return new TSVZFile(file, tmpPath.toFile(), t, true);
    }

    @Override
    public Meta meta() throws IOException {
        return inner.meta();
    }

    @Override
    public long write(final Row row) throws IOException {
        return inner.write(row);
    }

    @Override
    public Cursor<Row> find(final Filter.Limit limit, final Comparable<Row> filter) throws Exception {
        return inner.find(limit, filter);
    }

    @Override
    public Cursor<Row> find() throws Exception {
        return inner.find();
    }

    @Override
    public Cursor<Row> find(final String where) throws Exception {
        return inner.find(where);
    }

    @Override
    public long fileSize() {
        return inner.fileSize();
    }

    @Override
    public long rows(final boolean force) throws IOException {
        return inner.rows(force);
    }

    @Override
    public void close() throws IOException {
        try {
            inner.close();
        } finally {
            // if this was created/modified, pack into zip
            if (created && tmp != null && tmp.exists()) {
                Path tmpZip = Files.createTempFile("flintdb_tsvz_out_", ".zip");
                try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(tmpZip.toFile()))) {
                    // data entry
                    String dataName = container.getName().toLowerCase().endsWith(".csvz") ? "data.csv" : "data.tsv";
                    ZipEntry dataEntry = new ZipEntry(dataName);
                    zos.putNextEntry(dataEntry);
                    try (InputStream in = new FileInputStream(tmp)) {
                        byte[] buf = new byte[8192];
                        int r;
                        while ((r = in.read(buf)) > 0) zos.write(buf, 0, r);
                    }
                    zos.closeEntry();

                    // meta entry
                    ZipEntry metaEntry = new ZipEntry(container.getName() + Meta.META_NAME_SUFFIX);
                    zos.putNextEntry(metaEntry);
                    String metaText = SQL.stringify(inner.meta());
                    zos.write(metaText.getBytes());
                    zos.closeEntry();
                }

                // move tmpZip to final container location atomically if possible
                Path dest = container.toPath();
                Files.move(tmpZip, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            }

            // cleanup tmp data file
            try {
                if (tmp != null && tmp.exists()) tmp.delete();
            } catch (Exception ignored) {}
        }
    }
}
