package com.adtsw.jdatalayer.mapdb;

import com.adtsw.jcommons.models.EncodingFormat;
import com.adtsw.jdatalayer.core.accessobject.DBAccessObject;
import com.adtsw.jdatalayer.core.annotations.DBEntityConfiguration;
import com.adtsw.jdatalayer.core.annotations.EntityId;
import com.adtsw.jdatalayer.core.model.DBEntity;
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

        MapDBClient dbClient = new MapDBClient("/tmp", "mapDBTest");
        DBAccessObject dbo = new DBAccessObject(dbClient, "mapDBTest");
        
        OrdersString ordersString = new OrdersString("u1", orderItems);
        dbo.put(ordersString);
        OrdersString storedOrdersString = dbo.get("u1", OrdersString.class);
        Assert.assertEquals(2, storedOrdersString.getOrderItems().size());

        OrdersGzip ordersGzip = new OrdersGzip("u1", orderItems);
        dbo.put(ordersGzip);
        OrdersGzip storedOrdersGzip = dbo.get("u1", OrdersGzip.class);
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
