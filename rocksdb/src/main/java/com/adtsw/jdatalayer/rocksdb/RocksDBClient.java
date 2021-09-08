package com.adtsw.jdatalayer.rocksdb;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.adtsw.jcommons.utils.JsonUtil;
import com.adtsw.jdatalayer.core.client.AbstractDBClient;
import com.adtsw.jdatalayer.core.client.DBStats;
import com.adtsw.jdatalayer.core.model.StorageFormat;
import com.fasterxml.jackson.core.type.TypeReference;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Env;
import org.rocksdb.Filter;
import org.rocksdb.LRUCache;
import org.rocksdb.MemoryUsageType;
import org.rocksdb.MemoryUtil;
import org.rocksdb.Options;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.SstFileManager;
import org.rocksdb.Statistics;
import org.rocksdb.util.SizeUnit;

import lombok.extern.slf4j.Slf4j;

@Slf4j

public class RocksDBClient extends AbstractDBClient {

    private final Map<String, RocksDB> namespaces;
    private final Map<String, ReentrantReadWriteLock> locks;
    private final Map<String, ReadOptions> readOptions;
    private final Map<String, Filter> bloomFilters;
    private final Map<String, LRUCache> blockCaches;
    private final Map<String, LRUCache> compressedBlockCaches;
    private final Map<String, LRUCache> rowCaches;
    private final Map<String, Statistics> stats;
    private final TypeReference<TreeMap<String, Object>> mapTypeReference = new TypeReference<>() {};
    
    public RocksDBClient(String baseStorageLocation, String namespace,
                         int blockCacheCapacityKB, int blockCacheCompressedCapacityKB,
                         int rowCacheCapacityKB, int rateBytesPerSecond, 
                         int maxWriteBuffers, int writeBufferSizeKB, int maxTotalWalSizeKB,
                         CompressionType compressionType, CompactionStyle compactionStyle, int maxAllowedSpaceUsageKB,
                         int maxBackgroundJobs) {
        
        RocksDB.loadLibrary();

        this.namespaces = new HashMap<>();
        this.locks = new HashMap<>();
        this.readOptions = new HashMap<>();
        this.bloomFilters = new HashMap<>();
        this.blockCaches = new HashMap<>();
        this.compressedBlockCaches = new HashMap<>();
        this.rowCaches = new HashMap<>();
        this.stats = new HashMap<>();

        final Options options = new Options();

        ReadOptions namespaceReadOptions = new ReadOptions();
        namespaceReadOptions.setFillCache(false);
        Statistics namespaceStats = new Statistics();
        BloomFilter namespaceBloomFilter = new BloomFilter(10);
        LRUCache namespaceBlockCache = new LRUCache(blockCacheCapacityKB * SizeUnit.KB, 10);
        LRUCache namespaceBlockCacheCompressed = new LRUCache(blockCacheCompressedCapacityKB * SizeUnit.KB, 10);
        LRUCache namespaceRowCache = new LRUCache(rowCacheCapacityKB * SizeUnit.KB, 10);
        
        this.readOptions.put(namespace, namespaceReadOptions);
        this.stats.put(namespace, namespaceStats);
        this.bloomFilters.put(namespace, namespaceBloomFilter);
        this.blockCaches.put(namespace, namespaceBlockCache);
        this.compressedBlockCaches.put(namespace, namespaceBlockCacheCompressed);
        this.rowCaches.put(namespace, namespaceRowCache);

        setBasicOptions(
            options, namespaceStats,
            maxWriteBuffers, writeBufferSizeKB, maxTotalWalSizeKB,
            compressionType, compactionStyle, maxBackgroundJobs
        );
        setLSMOptions(options);
        setMemTableOptions(options);
        setTableFormatOptions(options, namespaceBloomFilter, namespaceBlockCache, namespaceBlockCacheCompressed);
        setFileManagerOptions(maxAllowedSpaceUsageKB, options);
        setRateLimitOptions(rateBytesPerSecond, options);
        setRowCacheOptions(options, namespaceRowCache);

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

    /**
     * TableFormatConfig is used to config the internal Table format of a RocksDB.
     * To make a RocksDB to use a specific Table format, its associated
     * TableFormatConfig should be properly set and passed into Options via
     * Options.setTableFormatConfig() and open the db using that Options.
     */
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

    private void setRowCacheOptions(Options options, Cache rowCache) {
        options.setRowCache(rowCache);
    }

    /**
     * MemTableConfig is used to config the internal mem-table of a RocksDB. It is
     * required for each memtable to have one such sub-class to allow Java
     * developers to use it.
     *
     * To make a RocksDB to use a specific MemTable format, its associated
     * MemTableConfig should be properly set and passed into Options via
     * Options.setMemTableFactory() and open the db using that Options.
     */
    private void setMemTableOptions(Options options) {
        options.setMemTableConfig(
            new SkipListMemTableConfig());
    }

    /*
     * --rocksdb.write-buffer-size The amount of data to build up in each in-memory
     * buffer (backed by a log file) before closing the buffer and queuing it to be
     * flushed into standard storage. Default: 64MiB. Larger values may improve
     * performance, especially for bulk loads.
     * 
     * --rocksdb.max-write-buffer-number The maximum number of write buffers that
     * built up in memory. If this number is reached before the buffers can be
     * flushed, writes will be slowed or stalled. Default: 2.
     * 
     * --rocksdb.min-write-buffer-number-to-merge Minimum number of write buffers
     * that will be merged together when flushing to normal storage. Default: 1.
     * 
     * --rocksdb.max-total-wal-size Maximum total size of WAL files that, when
     * reached, will force a flush of all column families whose data is backed by
     * the oldest WAL files. Setting this to a low value will trigger regular
     * flushing of column family data from memtables, so that WAL files can be moved
     * to the archive. Setting this to a high value will avoid regular flushing but
     * may prevent WAL files from being moved to the archive and being removed.
     * 
     * --rocksdb.delayed-write-rate (Hidden) Limited write rate to DB (in bytes per
     * second) if we are writing to the last in-memory buffer allowed and we allow
     * more than 3 buffers. Default: 16MiB/s.
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
     * --rocksdb.num-levels The number of levels for the database in the LSM tree.
     * Default: 7.
     * 
     * --rocksdb.num-uncompressed-levels The number of levels that do not use
     * compression. The default value is 2. Levels above this number will use Snappy
     * compression to reduce the disk space requirements for storing data in these
     * levels.
     * 
     * --rocksdb.dynamic-level-bytes If true, the amount of data in each level of
     * the LSM tree is determined dynamically so as to minimize the space
     * amplification; otherwise, the level sizes are fixed. The dynamic sizing
     * allows RocksDB to maintain a well-structured LSM tree regardless of total
     * data size. Default: true.
     * 
     * --rocksdb.max-bytes-for-level-base The maximum total data size in bytes in
     * level-1 of the LSM tree. Only effective if --rocksdb.dynamic-level-bytes is
     * false. Default: 256MiB.
     * 
     * --rocksdb.max-bytes-for-level-multiplier The maximum total data size in bytes
     * for level L of the LSM tree can be calculated as max-bytes-for-level-base *
     * (max-bytes-for-level-multiplier ^ (L-1)). Only effective if
     * --rocksdb.dynamic-level-bytes is false. Default: 10.
     * 
     * --rocksdb.level0-compaction-trigger Compaction of level-0 to level-1 is
     * triggered when this many files exist in level-0. Setting this to a higher
     * number may help bulk writes at the expense of slowing down reads. Default: 2.
     * 
     * --rocksdb.level0-slowdown-trigger When this many files accumulate in level-0,
     * writes will be slowed down to --rocksdb.delayed-write-rate to allow
     * compaction to catch up. Default: 20.
     * 
     * --rocksdb.level0-stop-trigger When this many files accumulate in level-0,
     * writes will be stopped to allow compaction to catch up. Default: 36.
     */
    private void setLSMOptions(Options options) {
        options.setNumLevels(7)
            .setLevel0FileNumCompactionTrigger(2)
            .setLevel0SlowdownWritesTrigger(10)
            .setLevel0StopWritesTrigger(36);
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

    private void setRateLimitOptions(int rateBytesPerSecond, final Options options) {
        final RateLimiter rateLimiter = new RateLimiter(rateBytesPerSecond,10000, 10);
        options.setRateLimiter(rateLimiter);
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
                this.readOptions.get(namespace), getKey(set, entityId).getBytes(StandardCharsets.UTF_8)
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

    @Override
    public DBStats getStatistics() {

        DBStats dbStats = new DBStats();
        namespaces.forEach((namespace, db) -> {
            
            addDBMetrics(dbStats, namespace, db);

            LRUCache namespaceCache = this.blockCaches.get(namespace);
            addCacheMetrics(dbStats, namespace, db, "block_cache", namespaceCache);

            LRUCache namespaceCompressedCache = this.compressedBlockCaches.get(namespace);
            addCacheMetrics(dbStats, namespace, db, "compressed_block_cache", namespaceCompressedCache);

            LRUCache namespaceRowCache = this.rowCaches.get(namespace);
            addCacheMetrics(dbStats, namespace, db, "row_cache", namespaceRowCache);
        });
        return dbStats;
    }

    private void addDBMetrics(DBStats dbStats, String namespace, RocksDB db) {

        Map<MemoryUsageType, Long> memoryUsage = MemoryUtil.getApproximateMemoryUsageByType(
            Arrays.asList(db), new HashSet<Cache>()
        );

        Long kMemTableTotal = memoryUsage.get(MemoryUsageType.kMemTableTotal);
        Long kMemTableUnFlushed = memoryUsage.get(MemoryUsageType.kMemTableUnFlushed);
        Long kTableReadersTotal = memoryUsage.get(MemoryUsageType.kTableReadersTotal);
        // Long kNumUsageTypes = memoryUsage.get(MemoryUsageType.kNumUsageTypes);
        dbStats.add(namespace + "_kMemTableTotal", kMemTableTotal / 1024L);
        dbStats.add(namespace + "_kMemTableUnFlushed", kMemTableUnFlushed / 1024L);
        dbStats.add(namespace + "_kTableReadersTotal", kTableReadersTotal / 1024L);
        // dbStats.add(namespace + "_kNumUsageTypes", kNumUsageTypes);
    }

    private void addCacheMetrics(DBStats dbStats, String namespace, RocksDB db, String cacheName,
            LRUCache namespaceCache) {

        Set<Cache> namespaceCaches = new HashSet<Cache>();
        namespaceCaches.add(namespaceCache);
        Map<MemoryUsageType, Long> memoryUsage = MemoryUtil.getApproximateMemoryUsageByType(
            Arrays.asList(), namespaceCaches
        );

        Long kCacheTotal = memoryUsage.get(MemoryUsageType.kCacheTotal);
        dbStats.add(namespace + "_" + cacheName + "_kCacheTotal", kCacheTotal / 1024L);
        // dbStats.add(namespace + "_" + cacheName + "_memUsage", namespaceCache.getUsage() / 1024L);
        // dbStats.add(namespace + "_" + cacheName + "_pinnedMemUsage", namespaceCache.getPinnedUsage() / 1024L);
    }

    public void shutdown() {

        namespaces.forEach((namespace, db) -> {

            logger.info("closing RocksDB database " + namespace);
            try {
                locks.get(namespace).writeLock().lock();
                db.syncWal();
                this.readOptions.get(namespace).close();
                db.close();
                locks.get(namespace).writeLock().unlock();
            } catch (RocksDBException e) {
                logger.warn("Exception closing RocksDB database " + namespace, e);
            }
        });

    }
}