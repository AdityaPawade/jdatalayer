package com.adtsw.jdatalayer.core.accessobject;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.adtsw.jcommons.utils.JsonUtil;
import com.adtsw.jdatalayer.core.annotations.DBEntityConfiguration;
import com.adtsw.jdatalayer.core.annotations.EntityId;
import com.adtsw.jdatalayer.core.client.IDBClient;
import com.adtsw.jdatalayer.core.model.DBEntity;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class DBAccessObject {

    private final IDBClient dbClient;
    @Getter
    private final String namespace;
    private final TypeReference<TreeMap<String, Object>> mapTypeReference = new TypeReference<>() {};

    public void put(DBEntity entity) {

        try {
            
            DBEntityConfiguration configs = entity.getClass().getAnnotation(DBEntityConfiguration.class);
            String entityId = getId(entity);
            TreeMap<String, Object> mappedEntity = JsonUtil.convert(entity, mapTypeReference);

            dbClient.put(namespace, configs.setName(), entityId, mappedEntity, configs.encodingFormat());

        } catch (Exception e) {
            throw new RuntimeException("Exception while saving entity", e);
        }
    }

    public void put(List<? extends DBEntity> entities) {

        try {

            if(CollectionUtils.isNotEmpty(entities)) {

                DBEntity firstEntity = entities.get(0);
                DBEntityConfiguration configs = firstEntity.getClass().getAnnotation(DBEntityConfiguration.class);

                HashMap<String, Map<String, Object>> dbEntities = new HashMap<>();
                for (DBEntity entity : entities) {
                    String entityId = getId(entity);
                    TreeMap<String, Object> mappedEntity = JsonUtil.convert(entity, mapTypeReference);
                    dbEntities.put(entityId, mappedEntity);
                }

                dbClient.put(namespace, configs.setName(), dbEntities, configs.encodingFormat());
            }

        } catch (Exception e) {
            throw new RuntimeException("Exception while saving entity", e);
        }
    }
    
    public <T extends DBEntity> T get(String entityId, Class<T> clazz) {

        DBEntityConfiguration configs = clazz.getAnnotation(DBEntityConfiguration.class);
        Map<String, Object> savedEntity = dbClient.get(
            namespace, configs.setName(), entityId, configs.encodingFormat()
        );
        if(savedEntity == null) return null;
        return JsonUtil.convert(savedEntity, clazz);
    }

    public <T extends DBEntity> void delete(String entityId, Class<T> clazz) {

        DBEntityConfiguration configs = clazz.getAnnotation(DBEntityConfiguration.class);
        dbClient.delete(namespace, configs.setName(), entityId);
    }

    public <T extends DBEntity> void delete(List<String> entityIds, Class<T> clazz) {

        DBEntityConfiguration configs = clazz.getAnnotation(DBEntityConfiguration.class);
        dbClient.delete(namespace, configs.setName(), entityIds);
    }

    public <T extends DBEntity> List<String> getIds(Class<T> clazz) {
        DBEntityConfiguration configs = clazz.getAnnotation(DBEntityConfiguration.class);
        return dbClient.getIds(namespace, configs.setName());
    }

    private String getId(DBEntity entity) throws IllegalAccessException {

        List<Field> idFields = FieldUtils.getFieldsListWithAnnotation(entity.getClass(), EntityId.class);
        if(CollectionUtils.isNotEmpty(idFields)) {
            Field entityIdField = idFields.get(0);
            return String.valueOf(FieldUtils.readField(entityIdField, entity));
        } else {
            List<Method> idMethods = MethodUtils.getMethodsListWithAnnotation(entity.getClass(), EntityId.class);
            if(CollectionUtils.isNotEmpty(idMethods)) {
                Method entityIdMethod = idMethods.get(0);
                try {
                    return String.valueOf(MethodUtils.invokeMethod(entity, entityIdMethod.getName()));
                } catch (Exception e) {
                    throw new RuntimeException("Unable to invoke id method");
                } 
            }
        }
        throw new RuntimeException("Unable to find entity id field");
    }
    
    public void shutdown() {
        dbClient.shutdown();
    }
    
    public void clear() {
        dbClient.clear();
    }
}
