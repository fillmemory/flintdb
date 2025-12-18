package flint.db;

import java.io.File;
import java.io.IOException;

/**
 * Plugin interface for handling different file formats.
 * Implementations should be registered via ServiceLoader.
 */
public interface GenericFilePlugin {
    
    /**
     * Get the priority of this plugin. Higher priority plugins are checked first.
     * Default implementations should return 0.
     * @return Priority value (higher = checked first)
     */
    default int priority() {
        return 0;
    }
    
    /**
     * Check if this plugin can handle the given file.
     * @param file The file to check
     * @return true if this plugin can handle the file
     */
    boolean supports(File file);
    
    /**
     * Open an existing file.
     * @param file The file to open
     * @return The opened GenericFile
     * @throws IOException if opening fails
     */
    GenericFile open(File file) throws IOException;
    
    /**
     * Create a new file with the given columns.
     * @param file The file to create
     * @param columns The columns for the file
     * @param logger The logger to use
     * @return The created GenericFile
     * @throws IOException if creation fails
     */
    GenericFile create(File file, Column[] columns, Logger logger) throws IOException;
    
    /**
     * Create a new file with the given metadata.
     * @param file The file to create
     * @param meta The metadata for the file
     * @param logger The logger to use
     * @return The created GenericFile
     * @throws IOException if creation fails
     */
    default GenericFile create(File file, Meta meta, Logger logger) throws IOException {
        return create(file, meta.columns(), logger);
    }
}
