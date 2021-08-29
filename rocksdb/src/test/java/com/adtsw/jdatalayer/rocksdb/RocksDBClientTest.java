package com.adtsw.jdatalayer.rocksdb;

import com.adtsw.jdatalayer.core.accessobject.DBAccessObject;
import com.adtsw.jdatalayer.core.annotations.DBEntityConfiguration;
import com.adtsw.jdatalayer.core.annotations.EntityId;
import com.adtsw.jdatalayer.core.model.DBEntity;
import com.adtsw.jdatalayer.core.model.StorageFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;

import java.util.Arrays;
import java.util.List;

public class RocksDBClientTest {

    @Test
    public void testClient() {

        List<Order> orderItems = Arrays.asList(
            new Order("o1", "s1", "msg1"),
            new Order("o2", "s2", "msg2")
        );

        int blockCacheCapacityKB = 64;
        int blockCacheCompressedCapacityKB = 64;
        int rateBytesPerSecond = 10000000;
        int writeBufferSizeKB = 8;
        CompressionType compressionType = CompressionType.NO_COMPRESSION;
        CompactionStyle compactionStyle = CompactionStyle.LEVEL;
        int maxWriteBuffers = 3;
        int maxBackgroundJobs = 10;
        int maxAllowedSpaceUsageKB = 1;
        int maxTotalWalSizeKB = 1;

        RocksDBClient dbClient = new RocksDBClient(
            "/tmp", "rocksDBTest",
            blockCacheCapacityKB, blockCacheCompressedCapacityKB, rateBytesPerSecond, 
            maxWriteBuffers, writeBufferSizeKB, maxTotalWalSizeKB,
            compressionType, compactionStyle, maxAllowedSpaceUsageKB, maxBackgroundJobs
        );
        DBAccessObject dbo = new DBAccessObject(dbClient, "rocksDBTest");
        
        OrdersString ordersString = new OrdersString("u1", orderItems);
        dbo.saveEntity(ordersString);
        OrdersString storedOrdersString = dbo.loadEntity("u1", OrdersString.class);
        Assert.assertEquals(2, storedOrdersString.getOrderItems().size());

        OrdersGzip ordersGzip = new OrdersGzip("u1", orderItems);
        dbo.saveEntity(ordersGzip);
        OrdersGzip storedOrdersGzip = dbo.loadEntity("u1", OrdersGzip.class);
        Assert.assertEquals(2, storedOrdersGzip.getOrderItems().size());
        
        dbo.shutdown();
    }
}

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
class Order {

    @JsonProperty("oId")
    private String orderId;
    @JsonProperty("sts")
    private String status;
    @JsonProperty("stsMsg")
    private String statusMessage;
}

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@DBEntityConfiguration(setName = "ordersSTRING", storageFormat = StorageFormat.STRING)
class OrdersString implements DBEntity {

    @EntityId
    @JsonProperty("uId")
    public String userId;
    @JsonProperty("oItms")
    private List<Order> orderItems;
}

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@DBEntityConfiguration(setName = "ordersGZIP", storageFormat = StorageFormat.GZIP_WITH_BASE64)
class OrdersGzip implements DBEntity {

    @EntityId
    @JsonProperty("uId")
    public String userId;
    @JsonProperty("oItms")
    private List<Order> orderItems;
}
