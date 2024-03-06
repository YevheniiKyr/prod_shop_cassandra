package org.example;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class Utils {
    public static Instant stringDateToInstant(String date){

        String pattern = "EEE MMM dd HH:mm:ss z yyyy";

        // Create a formatter with Ukrainian locale and the specific pattern
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, Locale.forLanguageTag("uk"))
                .withZone(ZoneId.of("Europe/Kiev"));

        try {
            // Parse the string into ZonedDateTime
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(date, formatter);

            // Convert ZonedDateTime to Instant
            Instant instant = zonedDateTime.toInstant();
            return instant;
        } catch (Exception e) {
            System.err.println("Error parsing date: " + e.getMessage());
        }
        return null;
    }
}
