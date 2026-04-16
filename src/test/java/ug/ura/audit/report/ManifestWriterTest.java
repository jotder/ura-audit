package ug.ura.audit.report;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ug.ura.audit.core.ClassifyResult;
import ug.ura.audit.core.DateResult;
import ug.ura.audit.scan.EnrichedEntry;
import ug.ura.audit.scan.FileEntry;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ManifestWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void round_trip_write_and_read() throws IOException {
        FileEntry file = new FileEntry(
                "/airtel_mobile_money/2023/txn_20230412.csv",
                "txn_20230412.csv",
                1024L,
                Instant.parse("2023-04-12T10:00:00Z"),
                "/airtel_mobile_money",
                null
        );
        ClassifyResult classification = new ClassifyResult("momo_transactions", "Mobile Money Transactions", "high");
        DateResult dateResult = DateResult.fromFilename(LocalDate.of(2023, 4, 12));
        EnrichedEntry entry = new EnrichedEntry(file, classification, dateResult);

        Path output = tempDir.resolve("manifest.tsv");
        ManifestWriter.write(output, List.of(entry));

        List<String[]> rows = ManifestWriter.read(output);
        assertEquals(1, rows.size());
        String[] row = rows.get(0);

        assertEquals("/airtel_mobile_money/2023/txn_20230412.csv", row[0]);
        assertEquals("txn_20230412.csv", row[5]);
        assertEquals("momo_transactions", row[6]);
        assertEquals("2023-04-12", row[9]);
        assertEquals("filename", row[10]);
    }

    @Test
    void unclassified_entry_has_empty_source_key() throws IOException {
        FileEntry file = new FileEntry(
                "/unknown/somefile.bin",
                "somefile.bin",
                512L,
                Instant.parse("2023-01-01T00:00:00Z"),
                "/unknown",
                null
        );
        EnrichedEntry entry = new EnrichedEntry(file, ClassifyResult.NO_MATCH, DateResult.NO_MATCH);

        Path output = tempDir.resolve("manifest_unclassified.tsv");
        ManifestWriter.write(output, List.of(entry));

        List<String[]> rows = ManifestWriter.read(output);
        assertEquals(1, rows.size());
        String[] row = rows.get(0);

        assertEquals("", row[6]);
        assertEquals("", row[9]);
    }
}
