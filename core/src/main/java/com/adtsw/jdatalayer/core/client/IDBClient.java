package com.adtsw.jdatalayer.core.client;

import java.util.List;
import java.util.Map;

import com.adtsw.jcommons.models.EncodingFormat;

public interface IDBClient {

    void saveEntity(String namespace, String set, String entityId, Map<String, Object> fields,
                    EncodingFormat encodingFormat);
    
    void saveEntities(String namespace, String set, Map<String, Map<String, Object>> entities,
                      EncodingFormat encodingFormat);

    Map<String, Object> loadEntity(String namespace, String set, String entityId, EncodingFormat encodingFormat);

    void deleteEntity(String namespace, String set, String entityId);

    void deleteEntities(String namespace, String set, List<String> entities);

    List<String> getAllEntityIds(String namespace, String set);

    DBStats getStatistics();
    
    void shutdown();
    
    void clear();
}
