package com.adtsw.jdatalayer.rocksdb;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;

import com.adtsw.jcommons.models.EncodingFormat;
import com.adtsw.jdatalayer.core.accessobject.DBAccessObject;
import com.adtsw.jdatalayer.core.annotations.DBEntityConfiguration;
import com.adtsw.jdatalayer.core.annotations.EntityId;
import com.adtsw.jdatalayer.core.client.DBStats;
import com.adtsw.jdatalayer.core.model.DBEntity;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

public class RocksDBClientTest {

    @Test
    public void testClient() {

        List<Order> orderItems = Arrays.asList(
            new Order("o1", "s1", "msg1"),
            new Order("o2", "s2", "msg2")
        );

        int blockCacheCapacityKB = 64;
        int blockCacheCompressedCapacityKB = 64;
        int rowCacheCapacityKB = 64;
        int rateBytesPerSecond = 10000000;
        int writeBufferSizeKB = 8;
        CompressionType compressionType = CompressionType.NO_COMPRESSION;
        CompactionStyle compactionStyle = CompactionStyle.LEVEL;
        int maxWriteBuffers = 3;
        int maxBackgroundJobs = 10;
        int maxAllowedSpaceUsageKB = 1;
        int maxTotalWalSizeKB = 1;

        RocksDBKVClient dbClient = new RocksDBKVClient(
            "/tmp", "rocksDBTest",
            blockCacheCapacityKB, blockCacheCompressedCapacityKB, rowCacheCapacityKB, rateBytesPerSecond, 
            maxWriteBuffers, writeBufferSizeKB, maxTotalWalSizeKB,
            compressionType, compactionStyle, maxAllowedSpaceUsageKB, maxBackgroundJobs,
            true, false, true
        );
        DBAccessObject dbo = new DBAccessObject(dbClient, "rocksDBTest");
        
        OrdersString ordersString = new OrdersString("u1", orderItems);
        dbo.put(ordersString);
        OrdersString orders2String = new OrdersString("u2", orderItems);
        dbo.put(orders2String);

        List<String> allEntityIds = dbo.getIds(OrdersString.class);
        Assert.assertEquals(2, allEntityIds.size());

        OrdersString storedOrdersString = dbo.get("u1", OrdersString.class);
        Assert.assertEquals(2, storedOrdersString.getOrderItems().size());
        dbo.delete("u1", OrdersString.class);
        OrdersString storedOrdersStringAfterDelete = dbo.get("u1", OrdersString.class);
        Assert.assertNull(storedOrdersStringAfterDelete);

        OrdersGzip ordersGzip = new OrdersGzip("u3", orderItems);
        dbo.put(ordersGzip);

        allEntityIds = dbo.getIds(OrdersString.class);
        Assert.assertEquals(1, allEntityIds.size());
        allEntityIds = dbo.getIds(OrdersGzip.class);
        Assert.assertEquals(1, allEntityIds.size());

        OrdersGzip storedOrdersGzip = dbo.get("u3", OrdersGzip.class);
        Assert.assertEquals(2, storedOrdersGzip.getOrderItems().size());
        dbo.delete("u3", OrdersGzip.class);
        OrdersGzip storedOrdersGzipAfterDelete = dbo.get("u3", OrdersGzip.class);
        Assert.assertNull(storedOrdersGzipAfterDelete);
        
        DBStats statistics = dbClient.getStatistics();
        statistics.getStatistics().forEach((key, value) -> {
            System.out.println(key + " -> " + value);
        });

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
@DBEntityConfiguration(setName = "ordersSTRING", encodingFormat = EncodingFormat.STRING)
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
@DBEntityConfiguration(setName = "ordersGZIP", encodingFormat = EncodingFormat.GZIP_WITH_BASE64)
class OrdersGzip implements DBEntity {

    @EntityId
    @JsonProperty("uId")
    public String userId;
    @JsonProperty("oItms")
    private List<Order> orderItems;
}
