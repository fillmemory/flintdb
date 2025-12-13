package flint.db;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.Deflater;

/**
 * Write-Ahead Logging (WAL) implementation for database operations.
 * 
 * Table + Index modifications are first recorded in the WAL before being
 * applied
 * to the actual data files. This ensures data integrity and allows for recovery
 * in case
 * of crashes or failures.
 * 
 * 
 * <pre>
 *  * BPlusTree, Table 의 구조를 변경을 최소화 하도록 WAL Layer만 추가
 *  * Phase 1: Allow dirty reads (uncommitted changes visible to other transactions)
 *  *          - Simpler implementation, better performance
 *  *          - Crash recovery through WAL replay
 *  * Phase 2: MVCC support (future)
 * 
 *  * WAL MODE: TRUNCATE (default), LOG
 *  * - TRUNCATE: Auto-truncate after checkpoint (recommended for production)
 *  * - LOG: Keep all logs without truncation (WARNING: file grows indefinitely, use only for debugging/audit)
 *  * Compression is always enabled for better I/O performance and space efficiency
 *  * Batch logging for better performance
 * 
 *  * IMPLEMENTATION STATUS:
 *  * ✓ WAL logging with compression
 *  * ✓ Crash recovery with WAL replay
 *  * ✓ Auto-checkpoint and truncation
 *  * ✓ Read-your-own-writes within transaction
 *  * TODO: Async WAL writing for better performance
 * </pre>
 * 
 * <pre>
 * File Structure: (Byte Order: Little Endian)
 * [WAL Log File]
 * - WAL Header
 * - WAL Records 
 * 
 * [WAL Header]
 * - Magic Number (4B)
 * - Version (2B)
 * - Header Size (2B)
 * - Timestamp (8B) : Creation time
 * - Transaction ID (8B)
 * - Committed Offset (8B)
 * - Checkpoint Offset (8B)
 * - Total Count (4B)
 * - Processed Count (4B)
 * - Reserved (remaining bytes to fill header size)
 * 
 * [WAL Data File]
 * - Operation(1B) : OP_BEGIN(0x00), OP_WRITE(0x1), OP_DELETE(0x02), OP_UPDATE(0x03), OP_COMMIT(0x10), OP_ROLLBACK(0x11), OP_CHECKPOINT(0x20)
 * - Transaction ID(8B)
 * - Checksum(2B) : CRC16 of Page Data
 * - File Identifier (4B) : to identify which file the page belongs to
 * - Page Offset(8B)
 * - Flags(1B) : Bit 0: Compressed (0=no, 1=yes), Bit 1: Metadata-only (0=no, 1=yes)
 * - Page Size(4B) : Original size (before compression if compressed)
 * - Compressed Size(4B) : Only present if compressed flag is set
 * - Page Data(variable) : Empty if metadata-only, compressed if compression flag is set
 * </pre>
 */
final class WALImpl implements WAL {
    final WALLogStorage logger;
    final Map<File, WALStorage> storages = new HashMap<>();
    private final boolean autoTruncate;
    private long transactionCount = 0;
    private static final long CHECKPOINT_INTERVAL = Long.getLong("LITEDB_WAL_CHECKPOINT_INTERVAL", 10000L);

    WALImpl(File file, String mode) throws IOException {
        String m = mode != null ? mode.toUpperCase() : Meta.WAL_OPT_TRUNCATE;
        this.autoTruncate = Meta.WAL_OPT_TRUNCATE.equals(m);
        this.logger = new WALLogStorage(file, this.autoTruncate);
    }

    @Override
    public void close() throws Exception {
        // Sync WAL to disk before closing
        try (logger) {
            // Sync WAL to disk before closing
            logger.sync();
        }
        storages.entrySet().forEach(entry -> {
            WALStorage storage = entry.getValue();
            IO.close(storage);
        });
        storages.clear();
    }

    Storage wrap(final Storage.Options options, final Callback callback) throws IOException {
        if (storages.containsKey(options.file)) {
            return storages.get(options.file);
        }
        WALStorage storage = new WALStorage(options, storages.size(), logger, callback);
        storages.put(options.file, storage);
        return storage;
    }

    @Override
    public long begin() {
        final long id = logger.increaseAndGet();
        storages.entrySet().forEach(entry -> {
            WALStorage storage = entry.getValue();
            storage.transaction(id);
        });
        return id;
    }

    @Override
    public void commit(long id) {
        try {
            // Apply to origin storage first (all writes already done)
            for (WALStorage storage : storages.values()) {
                storage.commitTransaction(id);
            }
            
            // Then write COMMIT record to WAL (minimal logging)
            logger.commit(id);
            
            // Auto-checkpoint every N transactions for TRUNCATE mode
            transactionCount++;
            if (autoTruncate && transactionCount % CHECKPOINT_INTERVAL == 0) {
                logger.checkpoint();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to commit transaction " + id, e);
        }
    }

    @Override
    public void rollback(long id) {
        // Rollback all storages
        for (WALStorage storage : storages.values()) {
            storage.rollbackTransaction(id);
        }
        // Then log the rollback
        logger.rollback(id);
    }

    @Override
    public void recover() throws IOException {
        // Get committed records to replay
        Map<Integer, java.util.List<WALLogStorage.WALRecord>> recordsToReplay = logger.recover();
        
        // Replay records to each storage
        for (Map.Entry<Integer, java.util.List<WALLogStorage.WALRecord>> entry : recordsToReplay.entrySet()) {
            int fileId = entry.getKey();
            java.util.List<WALLogStorage.WALRecord> records = entry.getValue();
            
            // Find corresponding storage by identifier
            WALStorage storage = findStorageByIdentifier(fileId);
            if (storage == null) {
                System.err.println("WAL recovery: Storage not found for fileId=" + fileId);
                continue;
            }
            
            // Replay each record
            for (WALLogStorage.WALRecord record : records) {
                try {
                    storage.replay(record);
                } catch (Exception e) {
                    System.err.println("WAL recovery: Failed to replay record at position " + record.position + ": " + e.getMessage());
                }
            }
        }
    }
    
    private WALStorage findStorageByIdentifier(int identifier) {
        for (WALStorage storage : storages.values()) {
            if (storage.getIdentifier() == identifier) {
                return storage;
            }
        }
        return null;
    }

    @Override
    public void checkpoint() throws IOException {
        logger.checkpoint();
    }
}

/**
 * WAL Log Storage implementation for storing WAL records.
 */
final class WALLogStorage implements AutoCloseable {
    private static final int HEADER_SIZE = 4096; // Match filesystem block size for atomic writes
    private static final int TRUNCATE_TOLERANCE = 64; // Tolerance in bytes for checkpoint truncation

    private final FileChannel channel;
    private final IoBuffer HEADER;
    private final boolean autoTruncate;
    private long transactionId;
    private long committedOffset;
    private long checkpointOffset;
    private long currentPosition; // Track current write position
    private int totalCount;
    private int processedCount;
    private final DirectBufferPool bufferPool;
    
    // Batch logging
    private final IoBuffer batchBuffer;
    private int batchCount = 0;
    private static final int BATCH_SIZE = Integer.getInteger("LITEDB_WAL_BATCH_SIZE", 10000); // Flush after N log records
    private static final int COMPRESSION_THRESHOLD = Integer.getInteger("LITEDB_WAL_COMPRESSION_THRESHOLD", 8192); // Compress if data > N bytes
    private static final byte FLAG_COMPRESSED = 0x01;
    private static final byte FLAG_METADATA_ONLY = 0x02;


    WALLogStorage(File file, boolean autoTruncate) throws IOException {
        this.autoTruncate = autoTruncate;
        final Set<java.nio.file.OpenOption> opt = new java.util.HashSet<>();
        opt.add(java.nio.file.StandardOpenOption.READ);
        opt.add(java.nio.file.StandardOpenOption.WRITE);
        opt.add((file.exists() ? java.nio.file.StandardOpenOption.CREATE : java.nio.file.StandardOpenOption.CREATE_NEW));

        this.channel = (FileChannel) Files.newByteChannel(Paths.get(file.toURI()), opt);

        // Larger buffer pool for better performance
        this.bufferPool = new DirectBufferPool(8192, 256);
        
        // Initialize batch buffer (4MB for better compression)
        this.batchBuffer = bufferPool.borrowBuffer(4 * 1024 * 1024);

        if (channel.size() == 0) {
            this.HEADER = map(0, HEADER_SIZE);
            IoBuffer h = HEADER.slice();
            // Initialize header
            h.putInt(0x57414C21); // Magic Number 'WAL!'
            h.putShort((short) 1); // Version 1
            h.putShort((short) HEADER_SIZE); // Header Size
            h.putLong(System.currentTimeMillis()); // Timestamp
            // Other header fields initialized to zero by default
            this.currentPosition = HEADER_SIZE;
        } else {
            this.HEADER = map(0, HEADER_SIZE);
            IoBuffer h = HEADER.slice();
            // Load existing header
            int magic = h.getInt();
            if (magic != 0x57414C21) 
                throw new IOException("Invalid WAL file: incorrect magic number");
            short version = h.getShort();
            if (version != 1)
                throw new IOException("Unsupported WAL version: " + version);
            short headerSize = h.getShort();
            if (headerSize != HEADER_SIZE) 
                throw new IOException("Unsupported WAL header size: " + headerSize);
            h.getLong(); // Skip timestamp
            this.transactionId = h.getLong();
            this.committedOffset = h.getLong();
            this.checkpointOffset = h.getLong();  
            this.totalCount = h.getInt();
            this.processedCount = h.getInt();
            this.currentPosition = channel.size();
        }
    }

    @Override
    public void close() throws Exception {
        try (channel) {
            flushBatch(); // Flush any remaining batched records
            if (batchBuffer != null) {
                bufferPool.returnBuffer(batchBuffer);
            }
        } // Flush any remaining batched records
    }

    private IoBuffer map(long position, long size) throws IOException {
        return IoBuffer.wrap(channel.map(FileChannel.MapMode.READ_WRITE, position, size));
    }

    void flush() throws IOException {
        IoBuffer h = HEADER.slice();
        h.position(16); // Skip to transactionId
        h.putLong(transactionId);
        h.putLong(committedOffset);
        h.putLong(checkpointOffset);
        h.putInt(totalCount);
        h.putInt(processedCount);
    }
    
    private void flushBatch() throws IOException {
        if (batchCount == 0) {
            return;
        }
        
        synchronized (channel) {
            batchBuffer.flip();
            channel.position(currentPosition);
            while (batchBuffer.hasRemaining()) {
                channel.write(batchBuffer.unwrap());
            }
            currentPosition = channel.position();
            batchBuffer.clear();
            batchCount = 0;
            
            // Sync after batch flush for durability
            channel.force(false);
        }
    }

    void log(byte operation, long transactionId, int fileId, long pageOffset, IoBuffer pageData) throws IOException {
        log(operation, transactionId, fileId, pageOffset, pageData, false);
    }

    void log(byte operation, long transactionId, int fileId, long pageOffset, IoBuffer pageData, boolean metadataOnly) throws IOException {
        byte flags = 0;
        byte[] compressedData = null;
        int originalSize = 0;
        int dataSize = 0;
        
        if (metadataOnly) {
            flags |= FLAG_METADATA_ONLY;
        } else if (pageData != null && pageData.remaining() > 0) {
            originalSize = pageData.remaining();
            
            // Always try compression for data larger than threshold
            if (originalSize > COMPRESSION_THRESHOLD) {
                compressedData = compressData(pageData);
                if (compressedData.length < originalSize * 0.9) { // Only use if compression saves >10%
                    flags |= FLAG_COMPRESSED;
                    dataSize = compressedData.length;
                } else {
                    // Compression didn't help, use original
                    compressedData = null;
                    dataSize = originalSize;
                }
            } else {
                dataSize = originalSize;
            }
        }
        
        // Calculate record size
        int recordSize = 1 + 8 + 2 + 4 + 8 + 1 + 4; // Basic fields + flags
        if ((flags & FLAG_COMPRESSED) != 0) {
            recordSize += 4 + dataSize; // Compressed size + compressed data
        } else if ((flags & FLAG_METADATA_ONLY) == 0 && dataSize > 0) {
            recordSize += dataSize; // Original data
        }
        
        // If batch buffer doesn't have enough space, flush it
        if (batchBuffer.remaining() < recordSize) {
            flushBatch();
        }
        
        // Write to batch buffer
        short checksum = 0; // Placeholder for checksum calculation
        batchBuffer.put(operation);
        batchBuffer.putLong(transactionId);
        batchBuffer.putShort(checksum);
        batchBuffer.putInt(fileId);
        batchBuffer.putLong(pageOffset);
        batchBuffer.put(flags);
        batchBuffer.putInt(originalSize);
        
        if ((flags & FLAG_COMPRESSED) != 0) {
            // Write compressed data
            batchBuffer.putInt(dataSize);
            batchBuffer.put(compressedData);
        } else if ((flags & FLAG_METADATA_ONLY) == 0 && pageData != null && dataSize > 0) {
            // Write original data
            IoBuffer slice = pageData.slice();
            batchBuffer.put(slice);
        }
        
        batchCount++;
        
        // Flush batch if we've accumulated enough records
        if (batchCount >= BATCH_SIZE) {
            flushBatch();
        }
    }
    
    private byte[] compressData(IoBuffer data) throws IOException {
        IoBuffer slice = data.slice();
        byte[] input = new byte[slice.remaining()];
        slice.get(input);
        
        // Use Deflater with nowrap=true to skip GZIP header/trailer for smaller size
        Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION, true);
        try {
            deflater.setInput(input);
            deflater.finish();
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream(input.length);
            byte[] buffer = new byte[8192];
            while (!deflater.finished()) {
                int count = deflater.deflate(buffer);
                baos.write(buffer, 0, count);
            }
            return baos.toByteArray();
        } finally {
            deflater.end();
        }
    }

    long increaseAndGet() {
        return ++transactionId;
    }

    void commit(long id) {
        try {
            // Write commit record to WAL
            log(WAL.OP_COMMIT, id, 0, 0, null);
            committedOffset = currentPosition;
            totalCount++;
            // Batch will auto-flush and sync when full (BATCH_SIZE)
        } catch (IOException e) {
            throw new RuntimeException("Failed to commit transaction " + id, e);
        }
    }

    void rollback(long id) {
        try {
            // Write rollback record to WAL
            log(WAL.OP_ROLLBACK, id, 0, 0, null);
            totalCount++;
            // Don't flush - let OS buffer for performance
        } catch (IOException e) {
            throw new RuntimeException("Failed to rollback transaction " + id, e);
        }
    }

    Map<Integer, java.util.List<WALRecord>> recover() throws IOException {
        // Replay WAL records from last checkpoint
        if (channel.size() <= HEADER_SIZE) {
            return new HashMap<>(); // No records to replay
        }
        
        long position = Math.max(HEADER_SIZE, checkpointOffset);
        channel.position(position);
        
        // Read and collect WAL records
        Map<Long, Boolean> transactionStatus = new HashMap<>(); // txId -> committed
        Map<Long, java.util.List<WALRecord>> allRecords = new HashMap<>(); // txId -> records
        
        while (position < channel.size()) {
            try {
                WALRecord record = readRecord(position);
                if (record == null) break; // End of valid records
                
                position += record.size;
                
                switch (record.operation) {
                    case WAL.OP_COMMIT:
                        transactionStatus.put(record.transactionId, true);
                        break;
                    case WAL.OP_ROLLBACK:
                        transactionStatus.put(record.transactionId, false);
                        break;
                    case WAL.OP_CHECKPOINT:
                        // Clear old transactions before checkpoint
                        transactionStatus.clear();
                        allRecords.clear();
                        break;
                    case WAL.OP_WRITE:
                    case WAL.OP_UPDATE:
                    case WAL.OP_DELETE:
                        // Collect all operations
                        allRecords.computeIfAbsent(record.transactionId, k -> new java.util.ArrayList<>())
                            .add(record);
                        break;
                }
            } catch (Exception e) {
                // Corrupt record, stop recovery
                System.err.println("WAL recovery stopped at position " + position + ": " + e.getMessage());
                break;
            }
        }
        
        // Group committed records by file identifier
        Map<Integer, java.util.List<WALRecord>> recordsToReplay = new HashMap<>();
        for (Map.Entry<Long, java.util.List<WALRecord>> entry : allRecords.entrySet()) {
            Long txId = entry.getKey();
            // Only replay committed transactions
            if (Boolean.TRUE.equals(transactionStatus.get(txId))) {
                for (WALRecord record : entry.getValue()) {
                    recordsToReplay.computeIfAbsent(record.fileId, k -> new java.util.ArrayList<>())
                        .add(record);
                }
            }
        }
        
        processedCount = totalCount;
        flush();
        
        return recordsToReplay;
    }
    
    private WALRecord readRecord(long position) throws IOException {
        if (position + 28 > channel.size()) { // Minimum record size
            return null;
        }
        
        IoBuffer buffer = bufferPool.borrowBuffer(1024);
        try {
            channel.position(position);
            int read = channel.read(buffer.unwrap());
            if (read < 28) return null;
            
            buffer.flip();
            
            WALRecord record = new WALRecord();
            record.position = position;
            record.operation = buffer.get();
            record.transactionId = buffer.getLong();
            record.checksum = buffer.getShort();
            record.fileId = buffer.getInt();
            record.pageOffset = buffer.getLong();
            record.flags = buffer.get();
            record.originalSize = buffer.getInt();
            
            int dataSize = record.originalSize;
            if ((record.flags & FLAG_COMPRESSED) != 0) {
                if (buffer.remaining() < 4) return null;
                dataSize = buffer.getInt();
            }
            
            record.size = 28 + ((record.flags & FLAG_COMPRESSED) != 0 ? 4 : 0) + dataSize;
            
            // Read data if needed
            if ((record.flags & FLAG_METADATA_ONLY) == 0 && dataSize > 0) {
                if (buffer.remaining() < dataSize) {
                    // Need larger buffer
                    bufferPool.returnBuffer(buffer);
                    buffer = bufferPool.borrowBuffer(record.size);
                    channel.position(position);
                    channel.read(buffer.unwrap());
                    buffer.flip();
                    buffer.position(28 + ((record.flags & FLAG_COMPRESSED) != 0 ? 4 : 0));
                }
                
                record.data = new byte[dataSize];
                buffer.get(record.data);
            }
            
            return record;
        } finally {
            bufferPool.returnBuffer(buffer);
        }
    }
    
    static class WALRecord {
        long position;
        int size;
        byte operation;
        long transactionId;
        @SuppressWarnings("unused")
        short checksum;
        int fileId;
        long pageOffset;
        byte flags;
        int originalSize;
        byte[] data;
    }

    void sync() throws IOException {
        // Force flush batch, header and sync to disk
        flushBatch();
        flush();
        channel.force(false);
    }

    void checkpoint() throws IOException {
        // Flush any pending batch before checkpoint
        flushBatch();
        
        // Write checkpoint marker
        log(WAL.OP_CHECKPOINT, transactionId, 0, 0, null);
        flushBatch(); // Flush checkpoint record immediately
        
        checkpointOffset = currentPosition;
        totalCount++;
        flush();
        
        // If auto-truncate is enabled and checkpoint is at the end, truncate WAL
        if (autoTruncate && checkpointOffset >= currentPosition - TRUNCATE_TOLERANCE) {
            // Reset WAL to initial state
            channel.truncate(HEADER_SIZE);
            checkpointOffset = HEADER_SIZE;
            committedOffset = HEADER_SIZE;
            currentPosition = HEADER_SIZE;
            totalCount = 0;
            processedCount = 0;
            flush();
        }
    }
}

/**
 * WAL Data Storage implementation for storing actual data changes.
 */
final class WALStorage implements Storage {
    private final Storage origin;
    private final WALLogStorage logger;
    private final int identifier; // file identifier
    private long transaction = -1; // current transaction id
    private final WAL.Callback callback; // cache synchronization callback
    private final Map<Long, Map<Long, IoBuffer>> pendingWrites = new HashMap<>(); // txId -> (index -> data)

    WALStorage(final Options options, final int identifier, final WALLogStorage logger, final WAL.Callback callback) throws IOException {
        this.origin = Storage.create(options);
        this.logger = logger;
        this.identifier = identifier;
        this.callback = callback;
    }

    int getIdentifier() {
        return identifier;
    }
    
    void replay(WALLogStorage.WALRecord record) throws IOException {
        // Replay WAL record to origin storage
        switch (record.operation) {
            case WAL.OP_UPDATE -> {
                if (record.data != null) {
                    IoBuffer buffer = IoBuffer.wrap(java.nio.ByteBuffer.wrap(record.data));
                    origin.write(record.pageOffset, buffer);
                }
            }
            case WAL.OP_DELETE -> {
                origin.delete(record.pageOffset);
                if (callback != null) {
                    callback.refresh(record.pageOffset);
                }
            }
            case WAL.OP_WRITE -> {
            }
        }
        // WRITE operations with metadata-only flag don't need replay
        // (already written to origin during transaction)
            }

    @Override
    public void transaction(long id) {
        this.transaction = id;
    }

    void commitTransaction(long id) throws IOException {
        if (this.transaction != id) {
            return; // Not our transaction
        }
        
        // Apply pending writes to origin storage (no WAL logging for UPDATEs)
        // Recovery strategy: Origin has all committed data, WAL only tracks COMMIT markers
        Map<Long, IoBuffer> writes = pendingWrites.remove(id);
        if (writes != null) {
            for (Map.Entry<Long, IoBuffer> entry : writes.entrySet()) {
                origin.write(entry.getKey(), entry.getValue());
            }
        }
        
        this.transaction = -1;
    }

    void rollbackTransaction(long id) {
        if (this.transaction != id) {
            return; // Not our transaction
        }
        
        // Discard pending writes without applying to origin
        pendingWrites.remove(id);
        
        this.transaction = -1;
    }

    @Override
    public void close() throws IOException {
        origin.close();
    }

    @Override
    public long write(IoBuffer bb) throws IOException {
        // BPlusTree.java  -> storage.write(IoBuffer.wrap(EMPTY_BYTES)); => for obtaining index
        // TableImpl.java  -> storage.write(raw);
        // Write to origin storage immediately
        IoBuffer data = bb.slice();
        final long index = origin.write(data);
        
        // Log metadata for recovery (in case of rollback, we can't undo origin write)
        // This is minimal overhead - just metadata, no data duplication
        logger.log(WAL.OP_WRITE, transaction, identifier, index, null, true);

        return index;
    }

    @Override
    public void write(long index, IoBuffer bb) throws IOException {
        IoBuffer data = bb.slice();
        if (transaction > 0) {
            // Log actual data to WAL for crash recovery
            logger.log(WAL.OP_UPDATE, transaction, identifier, index, data.slice(), false);
            
            // Store in pending writes for commit
            pendingWrites.computeIfAbsent(transaction, k -> new HashMap<>())
                .put(index, data.slice());
        } else {
            // No transaction, write directly to origin
            origin.write(index, data);
        }
    }

    @Override
    public IoBuffer read(long index) throws IOException {
        // Check pending writes first (read-your-own-writes)
        if (transaction > 0) {
            Map<Long, IoBuffer> writes = pendingWrites.get(transaction);
            if (writes != null && writes.containsKey(index)) {
                return writes.get(index).slice();
            }
        }
        return origin.read(index);
    }

    @Override
    public boolean delete(long index) throws IOException {
        if (transaction > 0) {
            // Log the delete operation to WAL before deleting
            logger.log(WAL.OP_DELETE, transaction, identifier, index, null);
        }
        // Delete from origin storage
        boolean result = origin.delete(index);
        // Invoke callback to synchronize cache
        if (result && callback != null) {
            callback.refresh(index);
        }
        return result;
    }

    @Override
    public InputStream readAsStream(long index) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'readAsStream'");
    }

    @Override
    public long writeAsStream(InputStream stream) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'writeAsStream'");
    }

    @Override
    public void writeAsStream(long index, InputStream stream) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'writeAsStream'");
    }

    @Override
    public long count() throws IOException {
        return origin.count();
    }

    @Override
    public long bytes() {
        return origin.bytes();
    }

    @Override
    public IoBuffer head(int size) throws IOException {
        return origin.head(size);
    }

    @Override
    public IoBuffer head(int offset, int size) throws IOException {
        return origin.head(offset, size);
    }

    @Override
    public void lock() throws IOException {
        origin.lock();
    }

    @Override
    public short version() {
        return origin.version();
    }

    @Override
    public void status(PrintStream out) throws IOException {
        origin.status(out);
    }

    @Override
    public boolean readOnly() {
        return origin.readOnly();
    }
}