package com.adtsw.jdatalayer.rocksdb;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.adtsw.jcommons.utils.JsonUtil;
import com.adtsw.jdatalayer.core.model.StorageFormat;
import com.fasterxml.jackson.core.type.TypeReference;

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RocksDBKVClient extends RocksDBClient {


    private final Map<String, ReentrantReadWriteLock> locks;
    private final TypeReference<TreeMap<String, Object>> mapTypeReference = new TypeReference<>() {};
    
    public RocksDBKVClient(String baseStorageLocation, String namespace,
                         int blockCacheCapacityKB, int blockCacheCompressedCapacityKB,
                         int rowCacheCapacityKB, int rateBytesPerSecond, 
                         int maxWriteBuffers, int writeBufferSizeKB, int maxTotalWalSizeKB,
                         CompressionType compressionType, CompactionStyle compactionStyle, int maxAllowedSpaceUsageKB,
                         int maxBackgroundJobs, boolean fillReadCache, boolean disableWAL) {
        
        super(baseStorageLocation, namespace, blockCacheCapacityKB, blockCacheCompressedCapacityKB, rowCacheCapacityKB,
                rateBytesPerSecond, maxWriteBuffers, writeBufferSizeKB, maxTotalWalSizeKB, compressionType,
                compactionStyle, maxAllowedSpaceUsageKB, maxBackgroundJobs, fillReadCache, disableWAL);

        this.locks = new HashMap<>();
        this.locks.put(namespace, new ReentrantReadWriteLock());
    }

    public void saveEntity(String namespace, String set, String entityId, Map<String, Object> fields,
                           StorageFormat storageFormat) {

        String payload = JsonUtil.write(fields);
        payload = encode(storageFormat, payload);
        try {
            locks.get(namespace).writeLock().lock();
            RocksDB db = getDB(namespace);
            db.put(
                getWriteOptions(namespace),
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
            byte[] storedBytes = getDB(namespace).get(
                getReadOptions(namespace), getKey(set, entityId).getBytes(StandardCharsets.UTF_8)
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
}