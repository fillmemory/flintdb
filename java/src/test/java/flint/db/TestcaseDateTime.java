package flint.db;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class TestcaseDateTime {
    
    public static void main(String[] args) {
        var s = "2023-10-01 13:40:39.1";
        System.out.println(date(s));
        System.out.println(format(date(s)));
        System.out.println(l2d(date(s)));
        System.out.println(d2l(l2d(date(s))));

        System.out.println(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    static String format(LocalDateTime dt) {
        return dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
    }

    static java.util.Date l2d(LocalDateTime dt) {
        return java.sql.Timestamp.valueOf(dt);
    }

    static LocalDateTime d2l(java.util.Date d) {
        // return new java.sql.Timestamp(d.getTime()).toLocalDateTime();
        return d.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

	/**
     * Parses a date-time string into a LocalDateTime object using various supported formats.
     * Supports formats: yyyy-MM-dd, yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm:ss.S, yyyy-MM-dd HH:mm:ss.SSS
     *
     * @param s the date-time string to parse
     * @return LocalDateTime object or null if format not recognized
     * @throws RuntimeException if parsing fails
     */
    static LocalDateTime date(final String s) {
        try {
            switch (s.length()) {
            case 10 -> {
                return LocalDateTime.parse(s + " 00:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                }
            case 19 -> {
                return LocalDateTime.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                }
            case 21 -> {
                return LocalDateTime.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"));
                }
            case 23 -> {
                return LocalDateTime.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                }
            }
            return null;
        } catch (DateTimeParseException ex) {
            throw new RuntimeException("dateTime(" + s + ") : " + ex.getMessage());
        }
    }
}
