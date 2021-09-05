package com.adtsw.jdatalayer.rocksdb;

import java.util.Map;
import java.util.TreeMap;

import lombok.Getter;

public class RocksDBStats {
    
    @Getter
    private final Map<String, Long> statistics;

    public RocksDBStats() {
        this.statistics = new TreeMap<>();
    }
    
    public void add(String id, Long value) {
        statistics.put(id, value);
    }
}
