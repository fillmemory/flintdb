package flint.db;

import java.io.File;
import java.io.IOException;

/**
 * Plugin for handling Union files.
 * Only available on platforms that support Union (not Android).
 */
public class UnionFilePlugin implements GenericFilePlugin {
    
    @Override
    public int priority() {
        return 10; // Higher priority than default
    }
    
    @Override
    public boolean supports(File file) {
        String name = file.getName();
        return name.endsWith(".union") || name.endsWith(Meta.TABLE_NAME_SUFFIX);
    }
    
    @Override
    public GenericFile open(File file) throws IOException {
        return Union.open(file);
    }
    
    @Override
    public GenericFile create(File file, Column[] columns, Logger logger) throws IOException {
        Meta meta = new Meta(file);
        meta.columns(columns);
        return create(file, meta, logger);
    }
    
    @Override
    public GenericFile create(File file, Meta meta, Logger logger) throws IOException {
        // Union only supports TableAdapter creation for .flintdb files
        if (file.getName().endsWith(Meta.TABLE_NAME_SUFFIX)) {
            return Union.TableAdapter.create(file, meta, logger);
        }
        // .union files are descriptors, not directly creatable via GenericFile
        throw new UnsupportedOperationException(
            "Cannot create .union descriptor files via GenericFile.create(). " +
            "Create a .union text file manually or use Union.TableAdapter for .flintdb files.");
    }
}
