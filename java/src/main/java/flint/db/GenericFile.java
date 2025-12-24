/**
 * 
 */
package flint.db;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Interface for handling record files in various formats.
 */
public interface GenericFile extends AutoCloseable {
    
    /**
     * Plugin registry for file format handlers.
     * Plugins are loaded dynamically via ServiceLoader.
     */
    class PluginRegistry {
        private static final List<GenericFilePlugin> plugins = new ArrayList<>();
        private static volatile boolean initialized = false;
        
        static {
            loadPlugins();
        }
        
        private static synchronized void loadPlugins() {
            if (initialized) return;
            
            // Load plugins via ServiceLoader
            ServiceLoader<GenericFilePlugin> loader = ServiceLoader.load(GenericFilePlugin.class);
            for (GenericFilePlugin plugin : loader) {
                plugins.add(plugin);
            }

            // Ensure built-in TSVZ plugin is available even without ServiceLoader config
            try {
                plugins.add(new TSVZFilePlugin());
            } catch (Throwable ignored) {}
            
            // Sort by priority (descending)
            plugins.sort(Comparator.comparingInt(GenericFilePlugin::priority).reversed());
            
            initialized = true;
        }
        
        static GenericFilePlugin findPlugin(File file) {
            for (GenericFilePlugin plugin : plugins) {
                if (plugin.supports(file)) {
                    return plugin;
                }
            }
            return null;
        }
        
        static List<GenericFilePlugin> getPlugins() {
            return new ArrayList<>(plugins);
        }
    }

    /**
     * Get the metadata for this record file.
     * @return
     * @throws IOException
     */
    Meta meta() throws IOException;

    /**
     * Write a new row to the record file.
     * @param row The row to write.
     * @return The offset of the written row.
     * @throws IOException
     */
    long write(final Row row) throws IOException;

    /**
     * Find rows in the record file.
     * @param limit The maximum number of rows to return.
     * @param filter The filter to apply.
     * @return A cursor over the matching rows.
     * @throws Exception
     */
    Cursor<Row> find(final Filter.Limit limit, final Comparable<Row> filter) throws Exception;

    /**
     * Find all rows in the record file.
     * @return A cursor over all rows.
     * @throws Exception
     */
    Cursor<Row> find() throws Exception;

    /**
     * Find rows in the record file.
     * @param where The filter to apply.
     * @return A cursor over the matching rows.
     * @throws Exception
     */
    Cursor<Row> find(final String where) throws Exception;

    /**
     * Get the size of the record file.
     * @return The size of the file in bytes.
     */
    long fileSize();

    long rows(boolean force) throws IOException;

    /**
     * Check if the record file format is supported.
     * @param file The record file.
     * @return True if the format is supported, false otherwise.
     */
    static boolean supports(final File file) {
        var name = file.getName();
        return name.endsWith(".tsv")
            || name.endsWith(".csv") 
            || name.endsWith(".jsonl")
            || name.endsWith(".tsv.gz") 
            || name.endsWith(".csv.gz") 
            || name.endsWith(".tsvz")
            || name.endsWith(".csvz")
            // || name.endsWith(".jsonl.gz")
            // || name.endsWith(".parquet")
            || name.endsWith(".tbl")
            || name.endsWith(".tbl.gz")
            //
            || (name.endsWith(".gz") && canRead(file))
            // || name.endsWith(".zip") 
            || hasPluginSupport(file)
            ;
    }

    private static boolean hasPluginSupport(final File file) {
        GenericFilePlugin plugin = PluginRegistry.findPlugin(file);
        return plugin != null;
    }

    static boolean canRead(final File file) {
        return TSVFile.canRead(file);
    }

    /**
     * Open a record file from File.
     * @param file The record file.
     * @return The opened record file.
     * @throws IOException
     */
    static GenericFile open(final File file) throws IOException {
        // Try to find a plugin that supports this file
        GenericFilePlugin plugin = PluginRegistry.findPlugin(file);
        if (plugin != null) {
            return plugin.open(file);
        }
        
        // Fallback to TSVFile as default
        return TSVFile.open(file);
    }

    /**
     * Open a record file or JDBC connection from URI.
     * @param uri The URI (file:// for files, jdbc: for databases).
     * @return The opened record file or JDBC connection.
     * @throws IOException
     */
    static GenericFile open(final java.net.URI uri) throws IOException {
        if (uri == null) throw new IllegalArgumentException("uri cannot be null");
        
        final String scheme = uri.getScheme();
        if (scheme == null) {
            // No scheme, treat as file path
            return open(new File(uri.getPath()));
        }
        
        if (scheme.equals("jdbc")) {
            return JdbcTable.open(uri);
        }
        
        if (scheme.equals("file")) {
            return open(new File(uri.getPath()));
        }
        
        throw new IllegalArgumentException("Unsupported URI scheme: " + scheme);
    }

    /**
     * Open a record file or JDBC connection from string path/URI.
     * @param path The file path or URI string.
     * @return The opened record file or JDBC connection.
     * @throws IOException
     */
    static GenericFile open(final String path) throws IOException {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path cannot be null or empty");
        }
        
        if (path.startsWith("jdbc:")) {
            try {
                return open(new java.net.URI(path));
            } catch (Exception ex) {
                throw new IOException("Invalid JDBC URI: " + path, ex);
            }
        }
        
        return open(new File(path));
    }

    /**
     * Create a new record file.
     * @param file The record file.
     * @param columns The columns for the record file.
     * @return The created record file.
     * @throws IOException
     */
    public static GenericFile create(final File file, final Column[] columns) throws IOException {
        return create(file, columns, new Logger.NullLogger());
    }

    /**
     * Create a new record file.
     * @param file The record file.
     * @param columns The columns for the record file.
     * @param logger The logger to use.
     * @return The created record file.
     * @throws IOException
     */
    static GenericFile create(final File file, final Column[] columns, final Logger logger) throws IOException {
        // Try to find a plugin that supports this file
        GenericFilePlugin plugin = PluginRegistry.findPlugin(file);
        if (plugin != null) {
            return plugin.create(file, columns, logger);
        }
        
        // Fallback to TSVFile as default
        return TSVFile.create(file, columns, logger);
    }

    /**
     * Create a new record file.
     * @param file The record file.
     * @param meta The metadata for the record file.
     * @return The created record file.
     * @throws IOException
     */
    static GenericFile create(final File file, final Meta meta) throws IOException {
        return create(file, meta, new Logger.NullLogger());
    }

    /**
     * Create a new record file.
     * @param file The record file.
     * @param meta The metadata for the record file.
     * @param logger The logger to use.
     * @return The created record file.
     * @throws IOException
     */
    static GenericFile create(final File file, final Meta meta, final Logger logger) throws IOException {
        // Try to find a plugin that supports this file
        GenericFilePlugin plugin = PluginRegistry.findPlugin(file);
        if (plugin != null) {
            return plugin.create(file, meta, logger);
        }
        
        // Fallback to TSVFile as default
        return TSVFile.create(file, meta.columns(), logger);
    }

    /**
     * Drop a record file.
     * @param file The record file.
     * @throws IOException
     */
    public static void drop(File file) throws IOException {
        final String name = file.getName();
		final File meta = new File(file.getParentFile(), name.concat(Meta.META_NAME_SUFFIX));
		final File crc = new File(file.getParentFile(), ".".concat(name).concat(".crc")); // .filename.parquet.crc
		if (meta.exists()) meta.delete();
		if (crc.exists()) crc.delete();
        file.delete();
    }
}
