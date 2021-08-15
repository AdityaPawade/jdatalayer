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

@Slf4j

public class RocksDBClient extends AbstractDBClient {

    private final Map<String, RocksDB> namespaces;
    private final TypeReference<TreeMap<String, Object>> mapTypeReference = new TypeReference<>() {};
    private final ReadOptions readOptions;
    private final Filter bloomFilter;
    private final LRUCache blockCache;
    private final LRUCache blockCacheCompressed;
    private final Statistics stats;
    
    public RocksDBClient(String baseStorageLocation, String namespace,
                         int blockCacheCapacityKB, 
                         int blockCacheCompressedCapacityKB, 
                         int rateBytesPerSecond,
                         int writeBufferSizeKB) {
        
        RocksDB.loadLibrary();
        final Options options = new Options();

        this.readOptions = new ReadOptions().setFillCache(false);
        this.stats = new Statistics();
        this.bloomFilter = new BloomFilter(10);
        this.blockCache = new LRUCache(blockCacheCapacityKB * SizeUnit.KB, 6);
        this.blockCacheCompressed = new LRUCache(blockCacheCompressedCapacityKB * SizeUnit.KB, 10);
        setBasicOptions(options, this.stats, writeBufferSizeKB);
        setMemTableOptions(options);
        setTableFormatOptions(options, bloomFilter, blockCache, blockCacheCompressed);

        final RateLimiter rateLimiter = new RateLimiter(rateBytesPerSecond,10000, 10);
        options.setRateLimiter(rateLimiter);

        this.namespaces = new HashMap<>();
        try {
            File baseDir = new File(baseStorageLocation + "/" + namespace);
            RocksDB defaultNamespace = RocksDB.open(options, baseDir.getAbsolutePath());
            this.namespaces.put(namespace, defaultNamespace);
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
            new HashSkipListMemTableConfig()
                .setHeight(4)
                .setBranchingFactor(4)
                .setBucketCount(2000000));
    }

    private void setBasicOptions(Options options, Statistics stats, int writeBufferSizeKB) {
        CompressionType compressionType = CompressionType.SNAPPY_COMPRESSION;
        options.setCreateIfMissing(true)
            .setStatistics(stats)
            .setWriteBufferSize(writeBufferSizeKB * SizeUnit.KB)
            .setMaxWriteBufferNumber(3)
            .setMaxBackgroundJobs(10)
            .setCompressionType(compressionType)
            .setCompactionStyle(CompactionStyle.UNIVERSAL);
    }

    public void saveEntity(String namespace, String set, String entityId, Map<String, Object> fields,
                           StorageFormat storageFormat) {

        String payload = JsonUtil.write(fields);
        payload = encode(storageFormat, payload);
        try {
            namespaces.get(namespace).put(
                getKey(set, entityId).getBytes(StandardCharsets.UTF_8),
                payload.getBytes(StandardCharsets.UTF_8)
            );
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
            byte[] storedBytes = namespaces.get(namespace).get(
                this.readOptions, getKey(set, entityId).getBytes(StandardCharsets.UTF_8)
            );
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

        namespaces.forEach((s, db) -> {

            logger.info("closing RocksDB database " + db);
            db.close();
        });
    }
}