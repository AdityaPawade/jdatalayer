package com.adtsw.jdatalayer.rocksdb;

import com.adtsw.jcommons.utils.JsonUtil;
import com.adtsw.jdatalayer.core.client.AbstractDBClient;
import com.adtsw.jdatalayer.core.model.StorageFormat;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j

public class RocksDBClient extends AbstractDBClient {

    private final Map<String, RocksDB> namespaces;
    private final Map<String, ReentrantReadWriteLock> locks;
    private final TypeReference<TreeMap<String, Object>> mapTypeReference = new TypeReference<>() {};
    private final ReadOptions readOptions;
    private final Filter bloomFilter;
    private final LRUCache blockCache;
    private final LRUCache blockCacheCompressed;
    private final Statistics stats;
    
    public RocksDBClient(String baseStorageLocation, String namespace,
                         int blockCacheCapacityKB, int blockCacheCompressedCapacityKB, 
                         int rateBytesPerSecond, int maxWriteBuffers, int writeBufferSizeKB, int maxTotalWalSizeKB,
                         CompressionType compressionType, CompactionStyle compactionStyle, int maxAllowedSpaceUsageKB,
                         int maxBackgroundJobs) {
        
        RocksDB.loadLibrary();
        final Options options = new Options();

        this.readOptions = new ReadOptions().setFillCache(false);
        this.stats = new Statistics();
        this.bloomFilter = new BloomFilter(10);
        this.blockCache = new LRUCache(blockCacheCapacityKB * SizeUnit.KB, 6);
        this.blockCacheCompressed = new LRUCache(blockCacheCompressedCapacityKB * SizeUnit.KB, 10);
        setBasicOptions(
            options, this.stats,
            maxWriteBuffers, writeBufferSizeKB, maxTotalWalSizeKB,
            compressionType, compactionStyle, maxBackgroundJobs
        );
        setLSMOptions(options);
        setMemTableOptions(options);
        setTableFormatOptions(options, bloomFilter, blockCache, blockCacheCompressed);
        setFileManagerOptions(maxAllowedSpaceUsageKB, options);

        final RateLimiter rateLimiter = new RateLimiter(rateBytesPerSecond,10000, 10);
        options.setRateLimiter(rateLimiter);

        this.namespaces = new HashMap<>();
        this.locks = new HashMap<>();
        try {
            File baseDir = new File(baseStorageLocation + "/" + namespace);
            RocksDB defaultNamespace = RocksDB.open(options, baseDir.getAbsolutePath());
            this.namespaces.put(namespace, defaultNamespace);
            this.locks.put(namespace, new ReentrantReadWriteLock());
        } catch (RocksDBException e) {
            log.error("Error initializng RocksDB. Exception: '{}', message: '{}'", e.getCause(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void setTableFormatOptions(Options options, 
                                       Filter bloomFilter, LRUCache blockCache, LRUCache blockCacheCompressed) {
    
        options.setTableFormatConfig(new BlockBasedTableConfig()
            .setBlockCache(blockCache)
            .setFilterPolicy(bloomFilter)
            .setBlockSizeDeviation(5)
            .setBlockRestartInterval(10)
            .setCacheIndexAndFilterBlocks(true)
            .setBlockCacheCompressed(blockCacheCompressed));
    }

    private void setMemTableOptions(Options options) {
        options.setMemTableConfig(
            new SkipListMemTableConfig());
    }

    /*
    --rocksdb.write-buffer-size
    The amount of data to build up in each in-memory buffer (backed by a log file) before closing the buffer and queuing it to be flushed into standard storage. Default: 64MiB. Larger values may improve performance, especially for bulk loads.
    
    --rocksdb.max-write-buffer-number
    The maximum number of write buffers that built up in memory. If this number is reached before the buffers can be flushed, writes will be slowed or stalled. Default: 2.
    
    --rocksdb.min-write-buffer-number-to-merge
    Minimum number of write buffers that will be merged together when flushing to normal storage. Default: 1.
    
    --rocksdb.max-total-wal-size
    Maximum total size of WAL files that, when reached, will force a flush of all column families whose data is backed by the oldest WAL files. Setting this to a low value will trigger regular flushing of column family data from memtables, so that WAL files can be moved to the archive. Setting this to a high value will avoid regular flushing but may prevent WAL files from being moved to the archive and being removed.
    
    --rocksdb.delayed-write-rate (Hidden)
    Limited write rate to DB (in bytes per second) if we are writing to the last in-memory buffer allowed and we allow more than 3 buffers. Default: 16MiB/s.
     */
    private void setBasicOptions(Options options, Statistics stats,
                                 int maxWriteBuffers, int writeBufferSizeKB, int maxTotalWalSizeKB,
                                 CompressionType compressionType, CompactionStyle compactionStyle,
                                 int maxBackgroundJobs) {
        options.setCreateIfMissing(true)
            .setStatistics(stats)
            .setWriteBufferSize(writeBufferSizeKB * SizeUnit.KB)
            .setMaxWriteBufferNumber(maxWriteBuffers)
            .setMaxTotalWalSize(maxTotalWalSizeKB * SizeUnit.KB)
            .setMaxBackgroundJobs(maxBackgroundJobs)
            .setCompressionType(compressionType)
            .setCompactionStyle(compactionStyle);
    }

    /*
    --rocksdb.num-levels
    The number of levels for the database in the LSM tree. Default: 7.
    
    --rocksdb.num-uncompressed-levels
    The number of levels that do not use compression. The default value is 2. Levels above this number will use Snappy compression to reduce the disk space requirements for storing data in these levels.
    
    --rocksdb.dynamic-level-bytes
    If true, the amount of data in each level of the LSM tree is determined dynamically so as to minimize the space amplification; otherwise, the level sizes are fixed. The dynamic sizing allows RocksDB to maintain a well-structured LSM tree regardless of total data size. Default: true.
    
    --rocksdb.max-bytes-for-level-base
    The maximum total data size in bytes in level-1 of the LSM tree. Only effective if --rocksdb.dynamic-level-bytes is false. Default: 256MiB.
    
    --rocksdb.max-bytes-for-level-multiplier
    The maximum total data size in bytes for level L of the LSM tree can be calculated as max-bytes-for-level-base * (max-bytes-for-level-multiplier ^ (L-1)). Only effective if --rocksdb.dynamic-level-bytes is false. Default: 10.
    
    --rocksdb.level0-compaction-trigger
    Compaction of level-0 to level-1 is triggered when this many files exist in level-0. Setting this to a higher number may help bulk writes at the expense of slowing down reads. Default: 2.
    
    --rocksdb.level0-slowdown-trigger
    When this many files accumulate in level-0, writes will be slowed down to --rocksdb.delayed-write-rate to allow compaction to catch up. Default: 20.
    
    --rocksdb.level0-stop-trigger
    When this many files accumulate in level-0, writes will be stopped to allow compaction to catch up. Default: 36.
     */
    private void setLSMOptions(Options options) {
        options.setNumLevels(7)
            .setLevel0FileNumCompactionTrigger(2)
            .setLevel0SlowdownWritesTrigger(2);
    }

    private void setFileManagerOptions(long maxAllowedSpaceUsageKB, Options options) {
        try {
            SstFileManager sstFileManager = new SstFileManager(Env.getDefault());
            sstFileManager.setMaxAllowedSpaceUsage(maxAllowedSpaceUsageKB * SizeUnit.KB);
            options.setSstFileManager(sstFileManager);
        } catch (RocksDBException e) {
            log.error("Error setting file manager options. Cause: '{}', message: '{}'", e.getCause(), e.getMessage());
        }
    }

    public void saveEntity(String namespace, String set, String entityId, Map<String, Object> fields,
                           StorageFormat storageFormat) {

        String payload = JsonUtil.write(fields);
        payload = encode(storageFormat, payload);
        try {
            locks.get(namespace).writeLock().lock();
            namespaces.get(namespace).put(
                getKey(set, entityId).getBytes(StandardCharsets.UTF_8),
                payload.getBytes(StandardCharsets.UTF_8)
            );
            locks.get(namespace).writeLock().unlock();
        } catch (RocksDBException e) {
            log.error("Error saving entry. Cause: '{}', message: '{}'", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void saveEntities(String namespace, String set, Map<String, Map<String, Object>> entities,
                             StorageFormat storageFormat) {

        entities.forEach((String entityId, Map<String, Object> fields) -> {
            saveEntity(namespace, set, entityId, fields, storageFormat);
        });
    }

    public Map<String, Object> loadEntity(String namespace, String set, String entityId,
                                          StorageFormat storageFormat) {

        String storedPayload = null;
        try {
            locks.get(namespace).readLock().lock();
            byte[] storedBytes = namespaces.get(namespace).get(
                this.readOptions, getKey(set, entityId).getBytes(StandardCharsets.UTF_8)
            );
            locks.get(namespace).readLock().unlock();
            storedPayload = storedBytes == null ? null : new String(storedBytes, StandardCharsets.UTF_8);
        } catch (RocksDBException e) {
            log.error("Error loading entry. Cause: '{}', message: '{}'", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        storedPayload = storedPayload == null ? null : decode(storageFormat, storedPayload);
        return storedPayload == null ? null : JsonUtil.read(storedPayload, mapTypeReference);
    }

    private String getKey(String set, String entityId) {
        return set + "$$" + entityId;
    }

    public void shutdown() {

        namespaces.forEach((namespace, db) -> {

            logger.info("closing RocksDB database " + namespace);
            try {
                locks.get(namespace).writeLock().lock();
                db.syncWal();
                db.close();
                locks.get(namespace).writeLock().unlock();
            } catch (RocksDBException e) {
                logger.warn("Exception closing RocksDB database " + namespace, e);
            }
        });
    }
}