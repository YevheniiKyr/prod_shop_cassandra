package org.example;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

public class RandomDateGenerator {
    public static Date generate() {
        LocalDate startDate = LocalDate.of(2020, 1, 1); // Start date
        LocalDate currentDate = LocalDate.now(); // Current date
        long daysBetween = ChronoUnit.DAYS.between(startDate, currentDate);
        long randomDays = ThreadLocalRandom.current().nextLong(daysBetween + 1); // +1 to include the current day
        LocalDate randomDate = startDate.plusDays(randomDays);
        ZoneId defaultZoneId = ZoneId.systemDefault();
        Date date = Date.from(randomDate.atStartOfDay(defaultZoneId).toInstant());
        return date;
    }
}

