package ug.ura.audit.core;

import org.yaml.snakeyaml.Yaml;
import java.io.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.*;
import java.util.regex.*;

public class DateExtractor {
    private final List<DatePatternEntry> patterns;

    public DateExtractor(List<DatePatternEntry> patterns) {
        this.patterns = patterns;
    }

    public static DateExtractor fromYaml(Path yamlPath) throws IOException {
        try (Reader reader = Files.newBufferedReader(yamlPath)) {
            return fromYaml(reader);
        }
    }

    @SuppressWarnings("unchecked")
    public static DateExtractor fromYaml(Reader reader) {
        Yaml yaml = new Yaml();
        Object raw = yaml.load(reader);
        if (!(raw instanceof List<?> list))
            throw new IllegalArgumentException("date_patterns.yaml must be a YAML list");

        List<DatePatternEntry> entries = new ArrayList<>();
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> map))
                throw new IllegalArgumentException("Each date pattern entry must be a YAML map");
            String name = (String) map.get("name");
            String regex = (String) map.get("pattern");
            int yearGroup = ((Number) map.get("year_group")).intValue();
            int monthGroup = ((Number) map.get("month_group")).intValue();
            int dayGroup = ((Number) map.get("day_group")).intValue();
            entries.add(new DatePatternEntry(name, Pattern.compile(regex), yearGroup, monthGroup, dayGroup));
        }
        return new DateExtractor(Collections.unmodifiableList(entries));
    }

    public DateResult extract(String fileName) {
        String normalized = stripGzSuffix(fileName);
        for (DatePatternEntry entry : patterns) {
            Matcher m = entry.pattern().matcher(normalized);
            if (m.find()) {
                try {
                    int year = Integer.parseInt(m.group(entry.yearGroup()));
                    int month = Integer.parseInt(m.group(entry.monthGroup()));
                    int day = Integer.parseInt(m.group(entry.dayGroup()));
                    LocalDate date = LocalDate.of(year, month, day);
                    return DateResult.fromFilename(date);
                } catch (java.time.DateTimeException e) {
                    continue; // Invalid date, try next pattern
                }
            }
        }
        return DateResult.NO_MATCH;
    }

    private static String stripGzSuffix(String name) {
        return name.endsWith(".gz") ? name.substring(0, name.length() - 3) : name;
    }
}
