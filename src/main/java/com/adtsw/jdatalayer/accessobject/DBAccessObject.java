package com.adtsw.jdatalayer.accessobject;

import com.adtsw.jdatalayer.annotations.DBEntityConfiguration;
import com.adtsw.jdatalayer.annotations.EntityId;
import com.adtsw.jdatalayer.client.IDBClient;
import com.adtsw.jcommons.utils.JsonUtil;
import com.adtsw.jdatalayer.model.DBEntity;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.AllArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@AllArgsConstructor
public class DBAccessObject {

    private final IDBClient dbClient;
    private final String namespace;
    private final TypeReference<TreeMap<String, Object>> mapTypeReference = new TypeReference<>() {};

    public void saveEntity(DBEntity entity) {

        try {
            
            DBEntityConfiguration configs = entity.getClass().getAnnotation(DBEntityConfiguration.class);
            String entityId = getEntityId(entity);
            TreeMap<String, Object> mappedEntity = JsonUtil.convert(entity, mapTypeReference);

            dbClient.saveEntity(namespace, configs.setName(), entityId, mappedEntity, configs.storageFormat());

        } catch (Exception e) {
            throw new RuntimeException("Exception while saving entity", e);
        }
    }

    public void saveEntities(List<? extends DBEntity> entities) {

        try {

            if(CollectionUtils.isNotEmpty(entities)) {

                DBEntity firstEntity = entities.get(0);
                DBEntityConfiguration configs = firstEntity.getClass().getAnnotation(DBEntityConfiguration.class);

                HashMap<String, Map<String, Object>> dbEntities = new HashMap<>();
                for (DBEntity entity : entities) {
                    String entityId = getEntityId(entity);
                    TreeMap<String, Object> mappedEntity = JsonUtil.convert(entity, mapTypeReference);
                    dbEntities.put(entityId, mappedEntity);
                }

                dbClient.saveEntities(namespace, configs.setName(), dbEntities, configs.storageFormat());
            }

        } catch (Exception e) {
            throw new RuntimeException("Exception while saving entity", e);
        }
    }
    
    public <T extends DBEntity> T loadEntity(String entityId, Class<T> clazz) {

        DBEntityConfiguration configs = clazz.getAnnotation(DBEntityConfiguration.class);
        Map<String, Object> savedEntity = dbClient.loadEntity(
            namespace, configs.setName(), entityId, configs.storageFormat()
        );
        if(savedEntity == null) return null;
        return JsonUtil.convert(savedEntity, clazz);
    }

    private String getEntityId(DBEntity entity) throws IllegalAccessException {

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
}
