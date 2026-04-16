package ug.ura.audit.report;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ug.ura.audit.core.ClassifyResult;
import ug.ura.audit.core.DateResult;
import ug.ura.audit.scan.EnrichedEntry;
import ug.ura.audit.scan.FileEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AvailabilityWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void produces_full_grid_with_gaps() throws IOException {
        LocalDate windowStart = LocalDate.of(2023, 4, 10);
        LocalDate windowEnd   = LocalDate.of(2023, 4, 12);

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

        Path output = tempDir.resolve("availability.csv");
        AvailabilityWriter.write(output, List.of(entry), "run-001", windowStart, windowEnd,
                List.of("momo_transactions"));

        List<String> lines = Files.readAllLines(output);

        // header + 3 days = 4 lines
        assertEquals(4, lines.size());

        // header starts with run_id,source_key
        assertTrue(lines.get(0).startsWith("run_id,source_key"),
                "Header should start with 'run_id,source_key' but was: " + lines.get(0));

        // 2023-04-10: zero files, zero bytes
        String line10 = lines.stream().filter(l -> l.contains("2023-04-10")).findFirst().orElseThrow();
        assertTrue(line10.contains(",0,0"),
                "Line for 2023-04-10 should contain ',0,0' but was: " + line10);

        // 2023-04-12: 1 file, 1024 bytes
        String line12 = lines.stream().filter(l -> l.contains("2023-04-12")).findFirst().orElseThrow();
        assertTrue(line12.contains(",1,1024"),
                "Line for 2023-04-12 should contain ',1,1024' but was: " + line12);
    }

    @Test
    void empty_entries_produces_all_zero_grid() throws IOException {
        LocalDate windowStart = LocalDate.of(2023, 4, 10);
        LocalDate windowEnd   = LocalDate.of(2023, 4, 11);

        Path output = tempDir.resolve("availability_empty.csv");
        AvailabilityWriter.write(output, List.of(), "run-002", windowStart, windowEnd,
                List.of("momo_transactions"));

        List<String> lines = Files.readAllLines(output);

        // header + 2 days = 3 lines
        assertEquals(3, lines.size());

        // both data lines contain ",0,0,"
        for (int i = 1; i <= 2; i++) {
            assertTrue(lines.get(i).contains(",0,0,"),
                    "Data line " + i + " should contain ',0,0,' but was: " + lines.get(i));
        }
    }
}
