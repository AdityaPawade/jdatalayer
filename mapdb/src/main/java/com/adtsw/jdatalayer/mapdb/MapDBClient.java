package com.adtsw.jdatalayer.mapdb;

import com.adtsw.jcommons.utils.JsonUtil;
import com.adtsw.jdatalayer.core.client.AbstractDBClient;
import com.adtsw.jdatalayer.core.model.StorageFormat;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class MapDBClient extends AbstractDBClient {

    protected static Logger logger = LogManager.getLogger(MapDBClient.class);
    
    private final Map<String, DB> namespaces;
    private final TypeReference<TreeMap<String, Object>> mapTypeReference = new TypeReference<>() {};

    public MapDBClient(String baseStorageLocation, String namespace) {
        
        this.namespaces = new HashMap<>();
        DB defaultNamespace = DBMaker.fileDB(new File(baseStorageLocation + "/" + namespace))
            .fileMmapEnable().checksumHeaderBypass().make();
        this.namespaces.put(namespace, defaultNamespace);
    }
    
    public void saveEntity(String namespace, String set, String entityId, Map<String, Object> fields,
                           StorageFormat storageFormat) {

        BTreeMap<String, String> table = namespaces.get(namespace)
            .treeMap(set, Serializer.STRING, Serializer.STRING)
            .createOrOpen();

        String payload = JsonUtil.write(fields);
        payload = encode(storageFormat, payload);
        table.put(entityId, payload);
    }

    public void saveEntities(String namespace, String set, Map<String, Map<String, Object>> entities, 
                             StorageFormat storageFormat) {
    
        BTreeMap<String, String> table = namespaces.get(namespace)
            .treeMap(set, Serializer.STRING, Serializer.STRING)
            .createOrOpen();

        entities.forEach((String entityId, Map<String, Object> fields) -> {
            String payload = JsonUtil.write(fields);
            payload = encode(storageFormat, payload);
            table.put(entityId, payload);
        });
    }

    public Map<String, Object> loadEntity(String namespace, String set, String entityId, 
                                          StorageFormat storageFormat) {

        BTreeMap<String, String> table = namespaces.get(namespace)
            .treeMap(set, Serializer.STRING, Serializer.STRING)
            .createOrOpen();

        String storedPayload = table.get(entityId);
        storedPayload = storedPayload == null ? null : decode(storageFormat, storedPayload);
        return storedPayload == null ? null : JsonUtil.read(storedPayload, mapTypeReference);
    }

    public void shutdown() {
        
        namespaces.forEach((s, db) -> {

            logger.info("closing MapDB database " + db);
            if(!db.isClosed()) {
                db.close();
            }
        });
    }
}
