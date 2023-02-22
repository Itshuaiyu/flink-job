package com.sino.dbass.utils;

import javax.xml.crypto.Data;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateParseAndFormat {
    private static String formatPattern = "yyyy-MM-dd HH:mm:ss";
    public static LocalDateTime strParseToLocalDateTime(String dateDtr){
        LocalDateTime parse = null;
        try {
            parse = LocalDateTime.parse(dateDtr, DateTimeFormatter.ISO_DATE_TIME);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return parse;
    }

    //将 localDateTime 转化为 Date
    public static Date localDateTimeToDate(LocalDateTime localDateTime){
        if (null == localDateTime){
            return null;
        }
        Date date = null;
        try {
            ZoneId zoneId = ZoneId.systemDefault();
            ZonedDateTime zdt = localDateTime.atZone(zoneId);
            date = Date.from(zdt.toInstant());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date;
    }
}
