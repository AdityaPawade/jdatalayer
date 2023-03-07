package com.adtsw.jdatalayer.rocksdb.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksDBProperties {

    /**
     * Properties: https://github.com/facebook/rocksdb/blob/master/include/rocksdb/db.h#L956-L988
     */
    public static final List<String> ROCKSDB_PROPERTIES = Arrays.asList(
            "rocksdb.num-immutable-mem-table",
            "rocksdb.mem-table-flush-pending",
            "rocksdb.compaction-pending",
            "rocksdb.background-errors",
            "rocksdb.cur-size-active-mem-table",
            "rocksdb.cur-size-all-mem-tables",
            "rocksdb.size-all-mem-tables",
            "rocksdb.num-entries-active-mem-table",
            "rocksdb.num-entries-imm-mem-tables",
            "rocksdb.num-deletes-active-mem-table",
            "rocksdb.num-deletes-imm-mem-tables",
            "rocksdb.estimate-num-keys",
            "rocksdb.estimate-table-readers-mem",
            "rocksdb.is-file-deletions-enabled",
            "rocksdb.num-snapshots",
            "rocksdb.oldest-snapshot-time",
            "rocksdb.num-live-versions",
            "rocksdb.current-super-version-number",
            "rocksdb.estimate-live-data-size",
            "rocksdb.min-log-number-to-keep",
            "rocksdb.min-obsolete-sst-number-to-keep",
            "rocksdb.total-sst-files-size",
            "rocksdb.live-sst-files-size",
            "rocksdb.base-level",
            "rocksdb.estimate-pending-compaction-bytes",
            "rocksdb.num-running-compactions",
            "rocksdb.num-running-flushes",
            "rocksdb.actual-delayed-write-rate",
            "rocksdb.is-write-stopped",
//            "rocksdb.estimate-oldest-key-time",
            "rocksdb.block-cache-capacity",
            "rocksdb.block-cache-usage",
            "rocksdb.block-cache-pinned-usage"
    );

    public static final List<String> SORTED_ROCKSDB_PROPERTIES = new ArrayList<>(ROCKSDB_PROPERTIES);

    static {
        SORTED_ROCKSDB_PROPERTIES.sort(String::compareTo);
    }

}
