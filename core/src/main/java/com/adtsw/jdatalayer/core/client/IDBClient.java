package com.adtsw.jdatalayer.core.client;

import java.util.List;
import java.util.Map;

import com.adtsw.jcommons.models.EncodingFormat;

public interface IDBClient {

    void put(String namespace, String set, String entityId, Map<String, Object> fields,
            EncodingFormat encodingFormat);
    
    void put(String namespace, String set, Map<String, Map<String, Object>> entities,
            EncodingFormat encodingFormat);

    Map<String, Object> get(String namespace, String set, String entityId, 
            EncodingFormat encodingFormat);

    void delete(String namespace, String set, String entityId);

    void delete(String namespace, String set, List<String> entities);

    List<String> getIds(String namespace, String set);

    List<String> getIds(String namespace);

    DBStats getStatistics();
    
    void shutdown();
    
    void clear();
}
