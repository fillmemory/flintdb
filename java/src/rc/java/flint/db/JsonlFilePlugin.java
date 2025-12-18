package flint.db;

import java.io.File;
import java.io.IOException;

/**
 * Plugin for handling JSONL files.
 */
public class JsonlFilePlugin implements GenericFilePlugin {
    
    @Override
    public int priority() {
        return 10; // Higher priority than default
    }
    
    @Override
    public boolean supports(File file) {
        String name = file.getName();
        return name.endsWith(".jsonl") || name.endsWith(".jsonl.gz");
    }
    
    @Override
    public GenericFile open(File file) throws IOException {
        return JsonlFile.open(file);
    }
    
    @Override
    public GenericFile create(File file, Column[] columns, Logger logger) throws IOException {
        return JsonlFile.create(file, columns, logger);
    }
}
