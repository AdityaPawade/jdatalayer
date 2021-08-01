package com.adtsw.jdatalayer.client;

import java.util.Map;

public interface IDBClient {

    void saveEntity(String namespace, String set, String entityId, Map<String, Object> fields);
    
    void saveEntities(String namespace, String set, Map<String, Map<String, Object>> entities);

    Map<String, Object> loadEntity(String namespace, String set, String entityId);

    void shutdown();
}
