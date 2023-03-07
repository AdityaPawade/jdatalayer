package com.adtsw.jdatalayer.rocksdb.metrics;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

import com.adtsw.jcommons.metrics.prometheus.PrometheusStatsCollector;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;

public class RocksDBStatsCollector implements PrometheusStatsCollector {

    protected static Logger logger = LogManager.getLogger(RocksDBStatsCollector.class);

    private final RocksDB rocksDB;
    private final String namespace;
    private final Statistics rocksDBStats;
    private final List<ColumnFamilyHandle> columnFamilyHandles;
    private final Set<String> columnFamilyNames;
    private final Gauge gaugeProperties;
    private final Gauge gaugeStats;

    /**
     * Wrap RocksDB instance from Application
     * @param rocksDB RocksDB instance from Application
     * @param rocksDBStats RocksDB statistics instance from Application
     * @param listCFHandles RocksDB Column Families from Application
     */
    public RocksDBStatsCollector(RocksDB rocksDB, String namespace,
        Statistics rocksDBStats, List<ColumnFamilyHandle> listCFHandles, 
        CollectorRegistry registry) {
        
        this.rocksDB = rocksDB;
        this.namespace = namespace;
        this.rocksDBStats = rocksDBStats;
        this.columnFamilyHandles = listCFHandles;
        this.columnFamilyNames = new HashSet<>();

        this.gaugeProperties = Gauge.build()
            .name("rocksdb_" + this.namespace + "_props")
            .help("RocksDB " + this.namespace + " Column Family Properties")
            .labelNames("namespace", "cf", "prop")
            .create().register(registry);

        this.gaugeStats = Gauge.build()
            .name("rocksdb_" + this.namespace + "_stats")
            .help("RocksDB " + this.namespace + " Statistics")
            .labelNames("namespace", "ticker")
            .create().register(registry);

        updateColumnFamilyNames();
    }

    /**
     * Update list of Column Families when the application needs to update
     * @param updatedCFHandles new list of Column Families
     */
    public void setColumnFamilyHandles(List<ColumnFamilyHandle> updatedCFHandles) {
        logger.info("Updating CF Handles");
        if (updatedCFHandles == null) {
            logger.warn("New list of CF Handles is null");
            return;
        }
        synchronized (columnFamilyHandles) {
            columnFamilyHandles.clear();
            columnFamilyHandles.addAll(updatedCFHandles);
        }
        onCFHandleChange();
    }

    /**
     * Add a new Column Family from application
     * @param newCFHandle a new Column Family that is created from application
     */
    public void addColumnFamilyFHandles(ColumnFamilyHandle newCFHandle) {
        logger.info("Adding CF Handles");
        if (newCFHandle == null) {
            logger.warn("CF Handle to be added is null");
            return;
        }
        synchronized (columnFamilyHandles) {
            columnFamilyHandles.add(newCFHandle);
        }
        onCFHandleChange();
    }

    /**
     * Remove a dropped Column Family from application
     * @param cfHandle the Column Family that application has dropped it
     */
    public void removeColumnFamilyHandles(ColumnFamilyHandle cfHandle) {
        logger.info("Removing CF Handle");
        if (cfHandle == null) {
            logger.warn("CF Handle to be removed is null");
            return;
        }
        synchronized (columnFamilyHandles) {
            columnFamilyHandles.remove(cfHandle);
        }
        onCFHandleChange();
    }

    private synchronized void onCFHandleChange() {
        Set<String> existingCFNames = new HashSet<>();
        existingCFNames.addAll(columnFamilyNames);

        updateColumnFamilyNames();

        List<String> props = RocksDBProperties.SORTED_ROCKSDB_PROPERTIES;
        for (String cfName : existingCFNames) {
            if (!columnFamilyNames.contains(cfName)) { // CF is dropped
                for (String prop : props) {
                    gaugeProperties.remove(cfName, prop);
                }
            }
        }
    }

    private void updateColumnFamilyNames() {
        logger.info("Updating CF Names");
        synchronized (columnFamilyNames) {
            columnFamilyNames.clear();
            for (ColumnFamilyHandle cfHandle : columnFamilyHandles) {
                try {
                    String cfName = new String(cfHandle.getName());
                    columnFamilyNames.add(cfName);
                } catch (RocksDBException ex) {
                    logger.warn("Exception updating column family names : " + ex.getMessage());
                }
            }
        }
        logger.info("Updated CF Names: " + columnFamilyNames);
    }

    private void updateGaugeProps() {
        if(this.rocksDB != null) {
            try {
                List<String> props = RocksDBProperties.SORTED_ROCKSDB_PROPERTIES;
                for (String prop : props) {
                    updateAggregatedGuageProps(prop);
                    for (ColumnFamilyHandle cfHandle : columnFamilyHandles) {
                        updateCFGuageProps(prop, cfHandle);
                    }
                }
            } catch (Exception ex) {
                logger.warn("Exception updating gauge properties", ex);
            }
        }
    }

    private void updateAggregatedGuageProps(String prop) {
        try {
            long longProperty = rocksDB.getAggregatedLongProperty(prop);
            gaugeProperties.labels(this.namespace, "aggregated", prop).set(longProperty);
        } catch (RocksDBException ex) {
            logger.warn("Exception getting property value for " + prop, ex);
        }
    }

    private void updateCFGuageProps(String prop, ColumnFamilyHandle cfHandle) {
        try {
            String cfName = new String(cfHandle.getName());
            long value = rocksDB.getLongProperty(cfHandle, prop);
            gaugeProperties.labels(this.namespace, cfName, prop).set(value);
        } catch (RocksDBException ex) {
            logger.warn("Exception getting property value for " + prop, ex);
        }
    }

    private void updateGaugeStats() {
        if(rocksDBStats != null) {
            for (TickerType ticker : TickerType.values()) {
                try {
                    long value = rocksDBStats.getTickerCount(ticker);
                    gaugeStats.labels(this.namespace, ticker.toString()).set(value);
                } catch (Exception ex) {
                    logger.warn("Exception updating gauge stats: ticker=" + ticker, ex);
                }
            }
        }
    }

    @Override
    public void update() {
        updateGaugeProps();
        updateGaugeStats();
    }
}
