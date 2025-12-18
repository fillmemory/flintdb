package flint.db;

import java.io.File;
import java.io.IOException;

/**
 * Plugin for handling Parquet files.
 * Only available when parquet-avro library is present.
 */
public class ParquetFilePlugin implements GenericFilePlugin {
    
    @Override
    public int priority() {
        return 10; // Higher priority than default
    }
    
    @Override
    public boolean supports(File file) {
        return file.getName().endsWith(".parquet");
    }
    
    @Override
    public GenericFile open(File file) throws IOException {
        return ParquetFile.open(file);
    }
    
    @Override
    public GenericFile create(File file, Column[] columns, Logger logger) throws IOException {
        return ParquetFile.create(file, columns, logger);
    }
}
