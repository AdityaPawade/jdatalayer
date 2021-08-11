package com.adtsw.jdatalayer.client;

import com.adtsw.jdatalayer.model.StorageFormat;

import java.util.Map;

public interface IDBClient {

    void saveEntity(String namespace, String set, String entityId, Map<String, Object> fields,
                    StorageFormat storageFormat);
    
    void saveEntities(String namespace, String set, Map<String, Map<String, Object>> entities, 
                      StorageFormat storageFormat);

    Map<String, Object> loadEntity(String namespace, String set, String entityId, StorageFormat storageFormat);

    void shutdown();
}
