package flint.db;

import java.io.File;
import java.io.IOException;

/**
 * Default plugin for handling TSV/CSV files.
 * This plugin has the lowest priority and acts as a fallback.
 */
public class TSVFilePlugin implements GenericFilePlugin {
    
    @Override
    public int priority() {
        return 0; // Lowest priority - fallback handler
    }
    
    @Override
    public boolean supports(File file) {
        String name = file.getName();
        return name.endsWith(".tsv")
            || name.endsWith(".csv") 
            || name.endsWith(".tsv.gz") 
            || name.endsWith(".csv.gz") 
            || name.endsWith(".tbl")
            || name.endsWith(".tbl.gz")
            || (name.endsWith(".gz") && TSVFile.canRead(file));
    }
    
    @Override
    public GenericFile open(File file) throws IOException {
        return TSVFile.open(file);
    }
    
    @Override
    public GenericFile create(File file, Column[] columns, Logger logger) throws IOException {
        return TSVFile.create(file, columns, logger);
    }
}
