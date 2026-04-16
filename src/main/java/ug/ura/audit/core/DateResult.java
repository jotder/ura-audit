package ug.ura.audit.core;

import java.time.LocalDate;

public record DateResult(LocalDate date, String dateSource) {
    public static DateResult fromFilename(LocalDate date) {
        return new DateResult(date, "filename");
    }
    public static final DateResult NO_MATCH = new DateResult(null, "mtime");
}
