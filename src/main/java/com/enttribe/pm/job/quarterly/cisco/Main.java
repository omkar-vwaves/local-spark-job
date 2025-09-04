package com.enttribe.pm.job.quarterly.cisco;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.TimeZone;

import com.enttribe.commons.lang.DateUtils;

public class Main {

    public static void main(String[] args) throws ParseException {
        // String timestamp = getDateTime(1749920439L);
        // System.out.println("timestamp: " + timestamp);

        long timestamp = getTimeStampForRowKey("202407170600");
        System.out.println("timestamp: " + timestamp);
        // String op = getTimeForQuarterly(timestamp, 00);
        // System.out.println("Result: " + op);
    }

    private static long getTimeStampForRowKey(String processingTime) {
        long timestamp = System.currentTimeMillis();
        try {
            timestamp = com.enttribe.commons.lang.DateUtils.parse("yyMMddHHmm", processingTime + "00").getTime();
        } catch (Exception e) {
            System.out.println("Exception in Getting Timestamp : " + e.getMessage());
        }
        return timestamp;
    }

    private static String getTimeForQuarterly(String timeKey, Integer mins) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(DateUtils.addMinutes(formatter.parse(timeKey), -mins));
        int minute = calendar.get(Calendar.MINUTE);
        int roundedMinute = (minute / 15) * 15;
        if (minute % 15 != 0) {
            roundedMinute = ((minute / 15) + 1) * 15;
        }
        calendar.set(Calendar.MINUTE, roundedMinute);
        calendar.set(Calendar.SECOND, 0);
        return DateUtils.format("yyyyMMddHHmmss", calendar.getTime());
    }

    // private static String getDateTime(Long dateTime) {
    // Instant instant = Instant.ofEpochSecond(dateTime);
    // DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm")
    // .withZone(ZoneId.of("UTC"));
    // String formattedDate = formatter.format(instant);
    // return formattedDate;
    // }
    private static String getDateTime(Long dateTime) {
        // Convert epoch seconds to Instant
        Instant instant = Instant.ofEpochSecond(dateTime);

        // Convert to Calendar in UTC
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.setTimeInMillis(instant.toEpochMilli());

        int minute = calendar.get(Calendar.MINUTE);
        int second = calendar.get(Calendar.SECOND);

        // Calculate total seconds past the hour
        int totalSeconds = minute * 60 + second;

        // Round up to the next 15-minute slot (in seconds)
        int roundedSlotSeconds = ((totalSeconds + 899) / 900) * 900;

        // Update calendar
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.add(Calendar.SECOND, roundedSlotSeconds);

        // Format to yyyyMMddHHmm in UTC
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        return formatter.format(calendar.getTime());
    }

}
