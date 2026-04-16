package ug.ura.audit.report;

import ug.ura.audit.scan.EnrichedEntry;
import java.io.*;
import java.nio.file.*;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class ManifestWriter {
    private static final String HEADER = String.join("\t",
            "path", "origin", "size", "mtime", "archive_parent", "file_name",
            "source_key", "source_label", "confidence", "date", "date_source");

    public static void write(Path outputPath, List<EnrichedEntry> entries) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(outputPath)) {
            w.write(HEADER);
            w.newLine();
            for (EnrichedEntry e : entries) {
                w.write(String.join("\t",
                        e.file().path(),
                        e.file().origin(),
                        String.valueOf(e.file().size()),
                        DateTimeFormatter.ISO_INSTANT.format(e.file().mtime()),
                        e.file().archiveParent() != null ? e.file().archiveParent() : "",
                        e.file().fileName(),
                        e.sourceKey(),
                        e.sourceLabel(),
                        e.confidence(),
                        e.date() != null ? e.date().toString() : "",
                        e.dateSource()
                ));
                w.newLine();
            }
        }
    }

    public static List<String[]> read(Path manifestPath) throws IOException {
        List<String> lines = Files.readAllLines(manifestPath);
        return lines.stream()
                .skip(1)
                .map(line -> line.split("\t", -1))
                .toList();
    }
}
