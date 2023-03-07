package com.adtsw.jdatalayer.rocksdb;

import static com.adtsw.jcommons.utils.EncoderUtil.decode;
import static com.adtsw.jcommons.utils.EncoderUtil.encode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.adtsw.jcommons.models.EncodingFormat;
import com.adtsw.jcommons.utils.JsonUtil;
import com.fasterxml.jackson.core.type.TypeReference;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RocksDBKVClient extends RocksDBClient {

    private final TypeReference<TreeMap<String, Object>> mapTypeReference = new TypeReference<>() {};
    
    public RocksDBKVClient(String baseStorageLocation, String namespace,
                         int blockCacheCapacityKB, int blockCacheCompressedCapacityKB,
                         int rowCacheCapacityKB, int rateBytesPerSecond, 
                         int maxWriteBuffers, int writeBufferSizeKB, int maxTotalWalSizeKB,
                         CompressionType compressionType, CompactionStyle compactionStyle, int maxAllowedSpaceUsageKB,
                         int maxBackgroundJobs, boolean fillReadCache, boolean disableWAL, boolean syncOnWrite) {
        
        super(baseStorageLocation, namespace, blockCacheCapacityKB, blockCacheCompressedCapacityKB, rowCacheCapacityKB,
                rateBytesPerSecond, maxWriteBuffers, writeBufferSizeKB, maxTotalWalSizeKB, compressionType,
                compactionStyle, maxAllowedSpaceUsageKB, maxBackgroundJobs, fillReadCache, disableWAL, syncOnWrite);
    }

    @Override
    public void put(String namespace, String set, String entityId, Map<String, Object> fields,
                    EncodingFormat encodingFormat) {

        String payload = JsonUtil.write(fields);
        payload = encode(encodingFormat, payload);
        try {
            WriteLock writeLock = getLock(namespace).writeLock();
            writeLock.lock();
            assertDBOpen(namespace);
            RocksDB db = getDB(namespace);
            db.put(
                getWriteOptions(namespace),
                getKey(set, entityId).getBytes(StandardCharsets.UTF_8),
                payload.getBytes(StandardCharsets.UTF_8)
            );
            writeLock.unlock();
        } catch (RocksDBException e) {
            log.error("Error saving entry. Cause: '{}', message: '{}'", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(String namespace, String set, Map<String, Map<String, Object>> entities,
                    EncodingFormat encodingFormat) {

        entities.forEach((String entityId, Map<String, Object> fields) -> {
            put(namespace, set, entityId, fields, encodingFormat);
        });
    }

    @Override
    public Map<String, Object> get(String namespace, String set, String entityId,
                    EncodingFormat encodingFormat) {

        String storedPayload = null;
        try {
            ReadLock readLock = getLock(namespace).readLock();
            readLock.lock();
            assertDBOpen(namespace);
            byte[] storedBytes = getDB(namespace).get(
                getReadOptions(namespace), getKey(set, entityId).getBytes(StandardCharsets.UTF_8)
            );
            readLock.unlock();
            storedPayload = storedBytes == null ? null : new String(storedBytes, StandardCharsets.UTF_8);
        } catch (RocksDBException e) {
            log.error("Error loading entry. Cause: '{}', message: '{}'", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
        storedPayload = storedPayload == null ? null : decode(encodingFormat, storedPayload);
        return storedPayload == null ? null : JsonUtil.read(storedPayload, mapTypeReference);
    }
    
    @Override
    public void delete(String namespace, String set, String entityId) {

        try {
            WriteLock writeLock = getLock(namespace).writeLock();
            writeLock.lock();
            assertDBOpen(namespace);
            RocksDB db = getDB(namespace);
            db.delete(
                getWriteOptions(namespace),
                getKey(set, entityId).getBytes(StandardCharsets.UTF_8)
            );
            writeLock.unlock();
        } catch (RocksDBException e) {
            log.error("Error saving entry. Cause: '{}', message: '{}'", e.getCause(), e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String namespace, String set, List<String> entities) {

        entities.forEach((String entityId) -> {
            delete(namespace, set, entityId);
        });
    }

    @Override
    public List<String> getIds(String namespace, String set) {

        List<String> keys = new ArrayList<>();
        try {
            ReadLock readLock = getLock(namespace).readLock();
            readLock.lock();
            assertDBOpen(namespace);
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
            readLock.unlock();
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
        if(splits.length > 1) {
            return new Pair<String,String>(
                splits[0], storedKey.substring(splits[0].length() + 2, storedKey.length())
            );
        }
        return new Pair<>(splits[0], splits[0]);
    }
}