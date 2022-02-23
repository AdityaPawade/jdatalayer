package com.adtsw.jdatalayer.rocksdb;

import static com.adtsw.jcommons.utils.EncoderUtil.decode;
import static com.adtsw.jcommons.utils.EncoderUtil.encode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.adtsw.jcommons.models.EncodingFormat;
import com.adtsw.jcommons.utils.JsonUtil;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

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

    @Override
    public void saveEntity(String namespace, String set, String entityId, Map<String, Object> fields,
                           EncodingFormat encodingFormat) {

        String payload = JsonUtil.write(fields);
        payload = encode(encodingFormat, payload);
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

    @Override
    public void saveEntities(String namespace, String set, Map<String, Map<String, Object>> entities,
                             EncodingFormat encodingFormat) {

        entities.forEach((String entityId, Map<String, Object> fields) -> {
            saveEntity(namespace, set, entityId, fields, encodingFormat);
        });
    }

    @Override
    public Map<String, Object> loadEntity(String namespace, String set, String entityId,
                                          EncodingFormat encodingFormat) {

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
        storedPayload = storedPayload == null ? null : decode(encodingFormat, storedPayload);
        return storedPayload == null ? null : JsonUtil.read(storedPayload, mapTypeReference);
    }
    
    @Override
    public void deleteEntity(String namespace, String set, String entityId) {

        try {
            locks.get(namespace).writeLock().lock();
            RocksDB db = getDB(namespace);
            db.delete(
                getWriteOptions(namespace),
                getKey(set, entityId).getBytes(StandardCharsets.UTF_8)
            );
            locks.get(namespace).writeLock().unlock();
        } catch (RocksDBException e) {
            log.error("Error saving entry. Cause: '{}', message: '{}'", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteEntities(String namespace, String set, List<String> entities) {

        entities.forEach((String entityId) -> {
            deleteEntity(namespace, set, entityId);
        });
    }

    @Override
    public List<String> getAllEntityIds(String namespace, String set) {

        List<String> keys = new ArrayList<>();
        try {
            locks.get(namespace).writeLock().lock();
            RocksIterator itr = getDB(namespace).newIterator(getReadOptions(namespace));
            itr.seekToFirst();
            while (itr.isValid()) {
                byte[] storedBytes = itr.key();
                Pair<String, String> setWithEntityId = getEntityIdDetails(new String(storedBytes, StandardCharsets.UTF_8));
                if(StringUtils.equals(setWithEntityId.getValue0(), set)) {
                    String entityId = setWithEntityId.getValue1();
                    keys.add(entityId);
                }
                itr.next();
            }
            locks.get(namespace).writeLock().unlock();
        } catch (Exception e) {
            log.error("Error getting all entries. Cause: '{}', message: '{}'", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        return keys;
    }

    private String getKey(String set, String entityId) {
        return set + "$$" + entityId;
    }

    private Pair<String, String> getEntityIdDetails(String storedKey) {
        String[] splits = storedKey.split("\\$\\$");
        return new Pair<>(splits[0], splits[1]);
    }
}