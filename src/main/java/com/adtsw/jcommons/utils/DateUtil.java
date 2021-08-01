package com.adtsw.jcommons.utils;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

public class DateUtil {
        
    private static final ZoneId kolkataZoneId = ZoneId.of("Asia/Kolkata");

    private static final DateTimeFormatterBuilder dateTimeFormatterBuilder = new DateTimeFormatterBuilder()
        .append(DateTimeFormatter.ofPattern("[yyyy-MM-dd'T'HH:mm:ss]" + "[yyyy-MM-dd' 'HH:mm:ss]"));

    private static final DateTimeFormatter dateTimeFormatter = dateTimeFormatterBuilder.toFormatter()
        .withZone(kolkataZoneId);

    public static OffsetDateTime getCurrentIndianDate() {

        return OffsetDateTime.now(kolkataZoneId);
    }
    
    public static long getCurrentIndianEpochSecond() {

        return getCurrentIndianDate().toEpochSecond();
    }
    
    public static String getDateTime(Long timestamp) {

        OffsetDateTime offsetDateTime = getOffsetDateTime(timestamp);
        return offsetDateTime.format(dateTimeFormatter);
    }

    public static OffsetDateTime getOffsetDateTime(Long timestamp) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestamp), kolkataZoneId);
    }

    public static ZonedDateTime getZonedDateTime(Long timestamp) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), kolkataZoneId);
    }

    public static Long getTimestamp(String dateTime) {

        Instant result = Instant.from(dateTimeFormatter.parse(dateTime));
        return result.getEpochSecond();
    }
}
