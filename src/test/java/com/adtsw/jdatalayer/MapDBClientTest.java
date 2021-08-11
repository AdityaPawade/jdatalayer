package com.adtsw.jdatalayer;

import com.adtsw.jcommons.utils.EncryptionUtil;
import com.adtsw.jdatalayer.accessobject.DBAccessObject;
import com.adtsw.jdatalayer.annotations.DBEntityConfiguration;
import com.adtsw.jdatalayer.annotations.EntityId;
import com.adtsw.jdatalayer.client.MapDBClient;
import com.adtsw.jdatalayer.model.DBEntity;
import com.adtsw.jdatalayer.model.StorageFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MapDBClientTest {

    @Test
    public void testClient() {

        List<Order> orderItems = Arrays.asList(
            new Order("o1", "s1", "msg1"),
            new Order("o2", "s2", "msg2")
        );

        MapDBClient dbClient = new MapDBClient("/tmp", "test");
        DBAccessObject dbo = new DBAccessObject(dbClient, "test");
        
        OrdersString ordersString = new OrdersString("u1", orderItems);
        dbo.saveEntity(ordersString);
        OrdersString storedOrdersString = dbo.loadEntity("u1", OrdersString.class);
        Assert.assertEquals(2, storedOrdersString.getOrderItems().size());

        OrdersGzip ordersGzip = new OrdersGzip("u1", orderItems);
        dbo.saveEntity(ordersGzip);
        OrdersGzip storedOrdersGzip = dbo.loadEntity("u1", OrdersGzip.class);
        Assert.assertEquals(2, storedOrdersGzip.getOrderItems().size());
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
