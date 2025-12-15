package flint.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * JDBC configuration manager for connection aliases
 * 
 * Reads JDBC connection settings from configuration file and resolves aliases.
 * 
 * Configuration file locations (in order of precedence):
 * 1. ./jdbc.properties (current directory)
 * 2. ~/.flintdb/jdbc.properties (user home)
 * 3. System property: -Dflintdb.jdbc.config=/path/to/jdbc.properties
 * 
 * Configuration format:
 * <pre>
 * # JDBC connection aliases
 * mydb = jdbc:mysql://localhost:3306/mydb?user=admin&password=secret&table={table}
 * testdb = jdbc:h2:mem:test?table={table}
 * prod = jdbc:postgresql://prod.server.com/production?table={table}
 * </pre>
 * 
 * Usage in SQL:
 * <pre>
 * SELECT * FROM @mydb:users
 * SELECT * FROM @testdb:customers WHERE age > 18
 * INSERT INTO output.tsv FROM @prod:orders
 * </pre>
 */
public final class JdbcConfig {
    private static final Map<String, String> aliases = new LinkedHashMap<>();
    private static boolean loaded = false;
    private static final Object LOCK = new Object();

    private JdbcConfig() {}

    /**
     * Load JDBC configuration from default locations
     */
    public static void load() {
        synchronized (LOCK) {
            if (loaded) return;
            
            // Try loading from various locations
            File configFile = findConfigFile();
            if (configFile != null && configFile.exists()) {
                try {
                    loadFromFile(configFile);
                    loaded = true;
                } catch (IOException e) {
                    System.err.println("Warning: Failed to load JDBC config from " + configFile + ": " + e.getMessage());
                }
            }
            
            loaded = true;
        }
    }

    /**
     * Find configuration file from standard locations
     */
    private static File findConfigFile() {
        // 1. System property
        String configPath = System.getProperty(Meta.PRODUCT_NAME_LC + ".jdbc.config");
        if (configPath != null && !configPath.isEmpty()) {
            File f = new File(configPath);
            if (f.exists()) return f;
        }

        // 2. Current directory
        File localConfig = new File("jdbc.properties");
        if (localConfig.exists()) {
            return localConfig;
        }

        // 3. User home directory
        String home = System.getProperty("user.home");
        if (home != null) {
            File homeConfig = new File(home, ".flintdb/jdbc.properties");
            if (homeConfig.exists()) {
                return homeConfig;
            }
        }

        return null;
    }

    /**
     * Load configuration from file
     */
    private static void loadFromFile(File file) throws IOException {
        Properties props = new Properties();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            props.load(reader);
        }

        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);
            if (value != null && !value.trim().isEmpty()) {
                aliases.put(key.trim(), value.trim());
            }
        }
    }

    /**
     * Resolve table reference to JDBC URI
     * 
     * @param tableRef Table reference (e.g., "@mydb:users" or "mydb.users" or "file.csv")
     * @return Resolved JDBC URI or original table reference if not an alias
     */
    public static String resolve(String tableRef) {
        if (tableRef == null || tableRef.isEmpty()) {
            return tableRef;
        }

        load();

        // Format: @alias:table or alias.table
        String alias = null;
        String table = null;

        if (tableRef.startsWith("@")) {
            // @mydb:users format
            int colonPos = tableRef.indexOf(':');
            if (colonPos > 1) {
                alias = tableRef.substring(1, colonPos);
                table = tableRef.substring(colonPos + 1);
            }
        } else if (tableRef.contains(".") && !tableRef.startsWith("jdbc:") && !tableRef.startsWith("/") && !tableRef.startsWith("./")) {
            // mydb.users format (but not file.csv, jdbc:..., or /path/file)
            int dotPos = tableRef.indexOf('.');
            String prefix = tableRef.substring(0, dotPos);
            
            // Check if this looks like an alias (not a file extension)
            if (aliases.containsKey(prefix)) {
                alias = prefix;
                table = tableRef.substring(dotPos + 1);
            }
        }

        if (alias != null && table != null) {
            String template = aliases.get(alias);
            if (template != null) {
                // Replace {table} placeholder
                return template.replace("{table}", table);
            }
        }

        return tableRef;
    }

    /**
     * Get all registered aliases
     */
    public static Map<String, String> getAliases() {
        load();
        return new LinkedHashMap<>(aliases);
    }

    /**
     * Register alias programmatically
     */
    public static void register(String alias, String jdbcTemplate) {
        if (alias == null || alias.isEmpty()) {
            throw new IllegalArgumentException("Alias cannot be null or empty");
        }
        if (jdbcTemplate == null || jdbcTemplate.isEmpty()) {
            throw new IllegalArgumentException("JDBC template cannot be null or empty");
        }
        
        synchronized (LOCK) {
            aliases.put(alias, jdbcTemplate);
        }
    }

    /**
     * Clear all aliases
     */
    public static void clear() {
        synchronized (LOCK) {
            aliases.clear();
            loaded = false;
        }
    }

    /**
     * Check if a table reference is a JDBC alias
     */
    public static boolean isAlias(String tableRef) {
        if (tableRef == null || tableRef.isEmpty()) {
            return false;
        }

        load();

        if (tableRef.startsWith("@")) {
            int colonPos = tableRef.indexOf(':');
            if (colonPos > 1) {
                String alias = tableRef.substring(1, colonPos);
                return aliases.containsKey(alias);
            }
        } else if (tableRef.contains(".") && !tableRef.startsWith("jdbc:") && !tableRef.startsWith("/") && !tableRef.startsWith("./")) {
            int dotPos = tableRef.indexOf('.');
            String prefix = tableRef.substring(0, dotPos);
            return aliases.containsKey(prefix);
        }

        return false;
    }
}
