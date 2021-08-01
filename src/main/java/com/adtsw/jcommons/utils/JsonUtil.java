package com.adtsw.jcommons.utils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonUtil {

    protected static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.setVisibilityChecker(mapper
            .getSerializationConfig()
            .getDefaultVisibilityChecker()
            .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
            .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
            .withSetterVisibility(JsonAutoDetect.Visibility.NONE));
        mapper.findAndRegisterModules();
    }

    public static <T> T read(String content, TypeReference<T> valueTypeRef) {
        try {
            return mapper.readValue(content, valueTypeRef);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T read(String content, Class<T> valueType) {
        try {
            return mapper.readValue(content, valueType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T convert(Object value, TypeReference<T> valueTypeRef) {
        try {
            return mapper.convertValue(value, valueTypeRef);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T convert(Object value, Class<T> valueTypeRef) {
        try {
            return mapper.convertValue(value, valueTypeRef);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
