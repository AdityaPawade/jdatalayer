package com.adtsw.jdatalayer.core.client;

import java.util.Map;
import java.util.TreeMap;

import lombok.Getter;

public class DBStats {
    
    @Getter
    private final Map<String, Long> statistics;

    public DBStats() {
        this.statistics = new TreeMap<>();
    }
    
    public void add(String id, Long value) {
        statistics.put(id, value);
    }
}
