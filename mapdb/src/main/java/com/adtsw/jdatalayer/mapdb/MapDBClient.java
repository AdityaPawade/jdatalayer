package com.adtsw.jdatalayer.mapdb;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.adtsw.jcommons.models.EncodingFormat;
import com.adtsw.jcommons.utils.JsonUtil;
import com.adtsw.jdatalayer.core.client.AbstractDBClient;
import com.adtsw.jdatalayer.core.client.DBStats;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import static com.adtsw.jcommons.utils.EncoderUtil.decode;
import static com.adtsw.jcommons.utils.EncoderUtil.encode;

public class MapDBClient extends AbstractDBClient {

    protected static Logger logger = LogManager.getLogger(MapDBClient.class);
    
    private final Map<String, DB> namespaces;
    private final Map<String, String> baseStorageLocations;
    private final TypeReference<TreeMap<String, Object>> mapTypeReference = new TypeReference<>() {};

    public MapDBClient(String baseStorageLocation, String namespace) {

        initDB(baseStorageLocation);

        this.namespaces = new HashMap<>();
        this.baseStorageLocations = new HashMap<>();
        DB defaultNamespace = DBMaker.fileDB(new File(getNamespaceStorageLocation(baseStorageLocation, namespace)))
            .fileMmapEnable().checksumHeaderBypass().make();
        this.namespaces.put(namespace, defaultNamespace);
        this.baseStorageLocations.put(namespace, baseStorageLocation);
    }

    private String getNamespaceStorageLocation(String baseStorageLocation, String namespace) {
        return baseStorageLocation + "/" + namespace;
    }

    private void initDB(String baseStorageLocation) {
    
        try {
            FileUtils.forceMkdir(new File(baseStorageLocation));
        } catch (IOException e) {
            throw new RuntimeException("Unable to create file at location " + baseStorageLocation, e);
        }
    }

    public void saveEntity(String namespace, String set, String entityId, Map<String, Object> fields,
                           EncodingFormat encodingFormat) {

        BTreeMap<String, String> table = namespaces.get(namespace)
            .treeMap(set, Serializer.STRING, Serializer.STRING)
            .createOrOpen();

        String payload = JsonUtil.write(fields);
        payload = encode(encodingFormat, payload);
        table.put(entityId, payload);
    }

    public void saveEntities(String namespace, String set, Map<String, Map<String, Object>> entities,
                             EncodingFormat encodingFormat) {
    
        BTreeMap<String, String> table = namespaces.get(namespace)
            .treeMap(set, Serializer.STRING, Serializer.STRING)
            .createOrOpen();

        entities.forEach((String entityId, Map<String, Object> fields) -> {
            String payload = JsonUtil.write(fields);
            payload = encode(encodingFormat, payload);
            table.put(entityId, payload);
        });
    }

    public Map<String, Object> loadEntity(String namespace, String set, String entityId,
                                          EncodingFormat encodingFormat) {

        BTreeMap<String, String> table = namespaces.get(namespace)
            .treeMap(set, Serializer.STRING, Serializer.STRING)
            .createOrOpen();

        String storedPayload = table.get(entityId);
        storedPayload = storedPayload == null ? null : decode(encodingFormat, storedPayload);
        return storedPayload == null ? null : JsonUtil.read(storedPayload, mapTypeReference);
    }

    @Override
    public DBStats getStatistics() {

        DBStats dbStats = new DBStats();
        return dbStats;
    }

    public void shutdown() {
        
        namespaces.forEach((s, db) -> {

            logger.info("closing MapDB database " + db);
            if(!db.isClosed()) {
                db.close();
            }
        });
    }

    public void clear() {

        this.baseStorageLocations.forEach((namespace, baseStorageLocation) -> {
            String namespaceStorageLocation = getNamespaceStorageLocation(baseStorageLocation, namespace);
            try {
                FileUtils.deleteDirectory(new File(namespaceStorageLocation));
            } catch (IOException e) {
                throw new RuntimeException("Unable to delete directory at " + namespaceStorageLocation, e);
            }
        });
    }
}
