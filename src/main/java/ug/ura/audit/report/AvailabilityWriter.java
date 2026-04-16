package ug.ura.audit.report;

import ug.ura.audit.scan.EnrichedEntry;
import java.io.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class AvailabilityWriter {
    private static final List<String> ORIGINS = List.of(
            "master_data1", "master_data2", "mobile_money", "resubmited", "unique_trnx");

    public static void write(Path outputPath, List<EnrichedEntry> entries,
                             String runId, LocalDate windowStart, LocalDate windowEnd,
                             List<String> sourceKeys) throws IOException {
        // Group by (sourceKey, date) — only classified entries with dates in window
        Map<String, Map<LocalDate, List<EnrichedEntry>>> grouped = entries.stream()
                .filter(e -> e.classification().matched())
                .filter(e -> e.date() != null)
                .filter(e -> !e.date().isBefore(windowStart) && !e.date().isAfter(windowEnd))
                .collect(Collectors.groupingBy(
                        EnrichedEntry::sourceKey,
                        Collectors.groupingBy(EnrichedEntry::date)));

        try (BufferedWriter w = Files.newBufferedWriter(outputPath)) {
            // Header
            List<String> cols = new ArrayList<>();
            cols.addAll(List.of("run_id", "source_key", "source_label", "date", "files_total", "bytes_total"));
            for (String o : ORIGINS) cols.add("files_" + o);
            for (String o : ORIGINS) cols.add("bytes_" + o);
            cols.addAll(List.of("has_archive", "date_confidence", "min_mtime", "max_mtime"));
            w.write(String.join(",", cols));
            w.newLine();

            for (String sourceKey : sourceKeys) {
                String label = entries.stream()
                        .filter(e -> e.sourceKey().equals(sourceKey))
                        .findFirst()
                        .map(EnrichedEntry::sourceLabel)
                        .orElse(sourceKey);

                LocalDate d = windowStart;
                while (!d.isAfter(windowEnd)) {
                    List<EnrichedEntry> bucket = grouped
                            .getOrDefault(sourceKey, Map.of())
                            .getOrDefault(d, List.of());

                    List<String> vals = new ArrayList<>();
                    vals.add(runId);
                    vals.add(sourceKey);
                    vals.add(csvEscape(label));
                    vals.add(d.toString());
                    vals.add(String.valueOf(bucket.size()));
                    vals.add(String.valueOf(bucket.stream().mapToLong(e -> e.file().size()).sum()));

                    for (String o : ORIGINS) {
                        vals.add(String.valueOf(bucket.stream().filter(e -> e.file().origin().contains(o)).count()));
                    }
                    for (String o : ORIGINS) {
                        vals.add(String.valueOf(bucket.stream().filter(e -> e.file().origin().contains(o)).mapToLong(e -> e.file().size()).sum()));
                    }

                    vals.add(String.valueOf(bucket.stream().anyMatch(e -> e.file().isArchiveEntry())));

                    // date_confidence
                    if (bucket.isEmpty()) {
                        vals.add("");
                    } else {
                        boolean allFilename = bucket.stream().allMatch(e -> "filename".equals(e.dateSource()));
                        boolean allMtime = bucket.stream().allMatch(e -> "mtime".equals(e.dateSource()));
                        vals.add(allFilename ? "filename" : allMtime ? "mtime" : "mixed");
                    }

                    vals.add(bucket.stream().map(e -> e.file().mtime()).min(Comparator.naturalOrder()).map(Object::toString).orElse(""));
                    vals.add(bucket.stream().map(e -> e.file().mtime()).max(Comparator.naturalOrder()).map(Object::toString).orElse(""));

                    w.write(String.join(",", vals));
                    w.newLine();
                    d = d.plusDays(1);
                }
            }
        }
    }

    private static String csvEscape(String val) {
        if (val.contains(",") || val.contains("\"") || val.contains("\n")) {
            return "\"" + val.replace("\"", "\"\"") + "\"";
        }
        return val;
    }
}
