package flint.db;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
 *  * ✓ Optional metadata-only WAL for UPDATE/DELETE (WAL_PAGE_DATA=0)
 * 
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
    private final long CHECKPOINT_INTERVAL;

    WALImpl(File file, Meta meta) throws IOException {
        String m = meta.walMode() != null ? meta.walMode().toUpperCase() : Meta.WAL_OPT_TRUNCATE;
        this.autoTruncate = Meta.WAL_OPT_TRUNCATE.equals(m);
        this.CHECKPOINT_INTERVAL = meta.walCheckpointInterval() > 0 ? meta.walCheckpointInterval() : Long.getLong("FLINTDB_WAL_CHECKPOINT_INTERVAL", 10000L);
         // Initialize WAL log storage
        this.logger = new WALLogStorage(
            file,
            this.autoTruncate,
            meta.walBatchSize(),
            meta.walCompressionThreshold(),
            meta.walPageData()
        );
    }

    @Override
    public void close() throws Exception {
        // Make WAL header offsets durable and, in TRUNCATE mode, avoid leaving a large WAL
        // that will be fully scanned on next open.
        try (logger) {
            if (autoTruncate) {
                logger.checkpoint();
            }
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
            
            // Auto-checkpoint every N transactions for TRUNCATE mode (match C version behavior)
            transactionCount++;
            if (autoTruncate && transactionCount >= CHECKPOINT_INTERVAL) {
                logger.checkpoint();
                transactionCount = 0;
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
    private final int BATCH_SIZE; // = Integer.getInteger("FLINTDB_WAL_BATCH_SIZE", 10000); // Flush after N log records
    private final int COMPRESSION_THRESHOLD; // = Integer.getInteger("FLINTDB_WAL_COMPRESSION_THRESHOLD", 8192); // Compress if data > N bytes
    private final int DIRECT_WRITE_THRESHOLD; // Direct-write if record size >= N bytes
    private final boolean logPageData;
    private static final byte FLAG_COMPRESSED = 0x01;
    private static final byte FLAG_METADATA_ONLY = 0x02;

    private static long writeFully(FileChannel ch, ByteBuffer src, long position) throws IOException {
        while (src.hasRemaining()) {
            int written = ch.write(src, position);
            if (written <= 0) {
                throw new IOException("Failed to write WAL record: wrote=" + written);
            }
            position += written;
        }
        return position;
    }

    private static ByteBuffer threadLocalHeaderBuffer() {
        // Small reusable header buffer; ByteOrder must always be LITTLE_ENDIAN.
        return HEADER_BUFFER.get();
    }

    private static final ThreadLocal<ByteBuffer> HEADER_BUFFER = ThreadLocal.withInitial(() ->
            ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN)
    );


    WALLogStorage(File file, boolean autoTruncate, int walBatchSize, int walCompressionThreshold, int walPageData) throws IOException {
        this.autoTruncate = autoTruncate;
        this.BATCH_SIZE = walBatchSize > 0 ? walBatchSize : Integer.getInteger("FLINTDB_WAL_BATCH_SIZE", 10000);
        this.COMPRESSION_THRESHOLD = walCompressionThreshold > 0 ? walCompressionThreshold : Integer.getInteger("FLINTDB_WAL_COMPRESSION_THRESHOLD", 8192);
        final Set<java.nio.file.OpenOption> opt = new java.util.HashSet<>();
        opt.add(java.nio.file.StandardOpenOption.READ);
        opt.add(java.nio.file.StandardOpenOption.WRITE);
        opt.add((file.exists() ? java.nio.file.StandardOpenOption.CREATE : java.nio.file.StandardOpenOption.CREATE_NEW));

        this.channel = (FileChannel) Files.newByteChannel(Paths.get(file.toURI()), opt);

        // Larger buffer pool for better performance
        this.bufferPool = new DirectBufferPool(8192, 256);
        
        // Initialize batch buffer (4MB for better compression)
        this.batchBuffer = bufferPool.borrowBuffer(4 * 1024 * 1024);
        int directWrite = Integer.getInteger("FLINTDB_WAL_DIRECT_WRITE_THRESHOLD", 64 * 1024);
        if (directWrite < 0) directWrite = 0;
        // Clamp to batch buffer capacity to avoid pathological settings.
        this.DIRECT_WRITE_THRESHOLD = Math.min(directWrite, batchBuffer.capacity());

        // WAL page image logging (C's wal_page_data equivalent)
        // - 1: include payload for UPDATE/DELETE (enables replay)
        // - 0: metadata-only for UPDATE/DELETE (smaller WAL; replay can't restore updates)
        int pageData = Integer.getInteger("FLINTDB_WAL_PAGE_DATA", walPageData);
        this.logPageData = pageData != 0;

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

    boolean logPageData() {
        return logPageData;
    }

    @Override
    public void close() throws Exception {
        try (channel) {
            // Make close() durable when WALLogStorage is used directly.
            sync();
            if (batchBuffer != null) {
                bufferPool.returnBuffer(batchBuffer);
            }
        }
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

        // HEADER is memory-mapped; FileChannel.force() does not reliably flush dirty mapped pages.
        // Force the mapped header so offsets survive process exit/crash.
        if (HEADER.isMapped()) {
            ((java.nio.MappedByteBuffer) HEADER.unwrap()).force();
        }
    }
    
    private void flushBatch() throws IOException {
        if (batchCount == 0) {
            return;
        }
        
        synchronized (channel) {
            batchBuffer.flip();
            ByteBuffer src = batchBuffer.unwrap();
            currentPosition = writeFully(channel, src, currentPosition);
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

        // For large records, bypass the batch buffer to avoid extra memcpy into the batchBuffer.
        // Use offset-based writes so we don't change the channel's current position.
        if (DIRECT_WRITE_THRESHOLD > 0 && recordSize >= DIRECT_WRITE_THRESHOLD) {
            // Keep WAL ordering: flush pending batched records first.
            flushBatch();

            synchronized (channel) {
                ByteBuffer header = threadLocalHeaderBuffer();
                header.clear();

                short checksum = 0; // Placeholder for checksum calculation
                header.put(operation);
                header.putLong(transactionId);
                header.putShort(checksum);
                header.putInt(fileId);
                header.putLong(pageOffset);
                header.put(flags);
                header.putInt(originalSize);
                if ((flags & FLAG_COMPRESSED) != 0) {
                    header.putInt(dataSize);
                }
                header.flip();

                long pos = currentPosition;
                pos = writeFully(channel, header, pos);

                if ((flags & FLAG_COMPRESSED) != 0) {
                    ByteBuffer payload = ByteBuffer.wrap(compressedData);
                    pos = writeFully(channel, payload, pos);
                } else if ((flags & FLAG_METADATA_ONLY) == 0 && pageData != null && dataSize > 0) {
                    IoBuffer slice = pageData.slice();
                    ByteBuffer payload = slice.unwrap();
                    pos = writeFully(channel, payload, pos);
                }

                currentPosition = pos;
            }
            return;
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

        final String traceProp = System.getProperty("FLINTDB_WAL_RECOVERY_TRACE");
        final boolean trace = traceProp != null && ("1".equals(traceProp)
                || "true".equalsIgnoreCase(traceProp)
                || "yes".equalsIgnoreCase(traceProp)
                || "on".equalsIgnoreCase(traceProp));
        final long startTimeNs = trace ? System.nanoTime() : 0L;

        // Only scan up to the last known committed position.
        // This prevents hang-like behavior when the file has extra padded/zeroed bytes
        // beyond the last valid commit (e.g., preallocation, OS extension, or tail garbage).
        final long fileEnd = channel.size();
        final long scanEnd;
        if (committedOffset > 0) {
            scanEnd = Math.min(fileEnd, Math.max(HEADER_SIZE, committedOffset));
        } else {
            scanEnd = fileEnd;
        }

        long position = Math.max(HEADER_SIZE, checkpointOffset);
        if (position >= scanEnd) {
            if (trace) {
                long elapsedMs = (System.nanoTime() - startTimeNs) / 1_000_000L;
                System.err.println("WAL recovery trace: no-scan" +
                        " start=" + position +
                        " scanEnd=" + scanEnd +
                        " fileEnd=" + fileEnd +
                        " committedOffset=" + committedOffset +
                        " checkpointOffset=" + checkpointOffset +
                        " elapsedMs=" + elapsedMs);
            }
            return new HashMap<>();
        }
        channel.position(position);
        
        // Read and collect WAL records
        Map<Long, Boolean> transactionStatus = new HashMap<>(); // txId -> committed
        Map<Long, java.util.List<WALRecord>> allRecords = new HashMap<>(); // txId -> records

        long scannedBytes = 0L;
        long recordCount = 0L;
        long commitCount = 0L;
        long rollbackCount = 0L;
        long checkpointCount = 0L;
        
        while (position < scanEnd) {
            try {
                WALRecord record = readRecord(position);
                if (record == null) break; // End of valid records
                
                position += record.size;
                scannedBytes += record.size;
                recordCount++;
                
                switch (record.operation) {
                    case WAL.OP_COMMIT:
                        transactionStatus.put(record.transactionId, true);
                        commitCount++;
                        break;
                    case WAL.OP_ROLLBACK:
                        transactionStatus.put(record.transactionId, false);
                        rollbackCount++;
                        break;
                    case WAL.OP_CHECKPOINT:
                        // Clear old transactions before checkpoint
                        transactionStatus.clear();
                        allRecords.clear();
                        checkpointCount++;
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
        long replayCandidateCount = 0L;
        for (Map.Entry<Long, java.util.List<WALRecord>> entry : allRecords.entrySet()) {
            Long txId = entry.getKey();
            // Only replay committed transactions
            if (Boolean.TRUE.equals(transactionStatus.get(txId))) {
                for (WALRecord record : entry.getValue()) {
                    recordsToReplay.computeIfAbsent(record.fileId, k -> new java.util.ArrayList<>())
                        .add(record);
                    replayCandidateCount++;
                }
            }
        }
        
        processedCount = totalCount;
        flush();

        if (trace) {
            long elapsedMs = (System.nanoTime() - startTimeNs) / 1_000_000L;
            System.err.println("WAL recovery trace:" +
                    " start=" + Math.max(HEADER_SIZE, checkpointOffset) +
                    " scanEnd=" + scanEnd +
                    " fileEnd=" + fileEnd +
                    " committedOffset=" + committedOffset +
                    " checkpointOffset=" + checkpointOffset +
                    " scannedBytes=" + scannedBytes +
                    " records=" + recordCount +
                    " commits=" + commitCount +
                    " rollbacks=" + rollbackCount +
                    " checkpoints=" + checkpointCount +
                    " replayCandidates=" + replayCandidateCount +
                    " files=" + recordsToReplay.size() +
                    " elapsedMs=" + elapsedMs);
        }
        
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
    
    // Dirty page cache for uncommitted UPDATE/DELETE operations
    // Maps: page offset -> dirty page data (null for DELETE)
    private final Map<Long, IoBuffer> dirtyPages = new HashMap<>();
    private final Map<Long, Boolean> deletedPages = new HashMap<>(); // track deleted pages

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
                // UPDATE: replay data to origin
                if (record.data != null) {
                    IoBuffer buffer = IoBuffer.wrap(java.nio.ByteBuffer.wrap(record.data));
                    origin.write(record.pageOffset, buffer);
                }
            }
            case WAL.OP_DELETE -> {
                // DELETE: replay delete to origin
                origin.delete(record.pageOffset);
                if (callback != null) {
                    callback.refresh(record.pageOffset);
                }
            }
            case WAL.OP_WRITE -> {
                // WRITE (INSERT): already written to origin during transaction
                // Metadata-only record, no replay needed
            }
        }
    }

    @Override
    public void transaction(long id) {
        this.transaction = id;
    }

    void commitTransaction(long id) throws IOException {
        if (this.transaction != id) {
            return; // Not our transaction
        }

        // Commit ordering matters for crash-safety without recovery:
        // write all non-root pages first, then update root pointer (commonly index 0) last.
        // This ensures a crash mid-commit yields only orphan pages, not a broken tree.
        final long rootIndex = 0L;

        // Flush dirty pages to origin storage (excluding rootIndex)
        for (Map.Entry<Long, IoBuffer> entry : dirtyPages.entrySet()) {
            long index = entry.getKey();
            if (index == rootIndex) continue;
            IoBuffer data = entry.getValue();
            data.position(0); // Reset position before write
            origin.write(index, data);
        }

        // Apply deletes to origin storage (excluding rootIndex)
        for (Long index : deletedPages.keySet()) {
            if (index == rootIndex) continue;
            boolean deleted = origin.delete(index);
            if (deleted && callback != null) {
                callback.refresh(index);
            }
        }

        // Apply rootIndex update last (if present)
        IoBuffer rootUpdate = dirtyPages.get(rootIndex);
        if (rootUpdate != null) {
            rootUpdate.position(0);
            origin.write(rootIndex, rootUpdate);
        }

        // Apply rootIndex delete last (very uncommon; keep ordering consistent)
        if (deletedPages.containsKey(rootIndex)) {
            boolean deleted = origin.delete(rootIndex);
            if (deleted && callback != null) {
                callback.refresh(rootIndex);
            }
        }
        
        // Clear caches after commit
        dirtyPages.clear();
        deletedPages.clear();
        this.transaction = -1;
    }

    void rollbackTransaction(long id) {
        if (this.transaction != id) {
            return; // Not our transaction
        }
        
        // Discard dirty pages and deleted pages (don't apply to origin)
        dirtyPages.clear();
        deletedPages.clear();
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
        if (transaction > 0) {
            logger.log(WAL.OP_WRITE, transaction, identifier, index, null, true);
        }

        return index;
    }

    @Override
    public void write(long index, IoBuffer bb) throws IOException {
        if (transaction > 0) {
            // UPDATE within transaction: don't write to origin immediately
            // Store in dirty page cache and log to WAL
            IoBuffer data = bb.slice();
            // Make one immutable copy and reuse it for both WAL logging and dirty-page cache.
            byte[] copy = new byte[data.remaining()];
            data.get(copy);
            IoBuffer cached = IoBuffer.wrap(copy);

            if (logger.logPageData()) {
                logger.log(WAL.OP_UPDATE, transaction, identifier, index, cached, false);
            } else {
                // metadata-only mode: keep dirty page for read-your-own-writes and commit,
                // but do not write page image into WAL.
                logger.log(WAL.OP_UPDATE, transaction, identifier, index, null, true);
            }
            dirtyPages.put(index, cached);
        } else {
            // No transaction: direct write to origin
            IoBuffer data = bb.slice();
            origin.write(index, data);
        }
    }

    @Override
    public IoBuffer read(long index) throws IOException {
        // Check if page is deleted in current transaction
        if (deletedPages.containsKey(index) && transaction > 0) {
            throw new IOException("Page " + index + " has been deleted in current transaction");
        }
        
        // Check dirty page cache first (read-your-own-writes)
        IoBuffer dirtyPage = dirtyPages.get(index);
        if (dirtyPage != null) {
            // Return a copy to avoid external modification
            IoBuffer copy = dirtyPage.duplicate();
            copy.position(0);
            return copy;
        }
        
        // Not in dirty cache, read from origin storage
        return origin.read(index);
    }

    @Override
    public boolean delete(long index) throws IOException {
        if (transaction > 0) {
            // DELETE within transaction: don't delete from origin immediately
            // Mark as deleted in cache and log to WAL
            if (logger.logPageData()) {
                logger.log(WAL.OP_DELETE, transaction, identifier, index, null);
            } else {
                logger.log(WAL.OP_DELETE, transaction, identifier, index, null, true);
            }
            deletedPages.put(index, true);
            dirtyPages.remove(index); // Remove from dirty cache if exists
            return true;
        } else {
            // No transaction: direct delete from origin
            boolean result = origin.delete(index);
            if (result && callback != null) {
                callback.refresh(index);
            }
            return result;
        }
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