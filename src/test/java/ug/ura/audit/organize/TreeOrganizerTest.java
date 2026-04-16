package ug.ura.audit.organize;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ug.ura.audit.core.ClassifyResult;
import ug.ura.audit.core.DateResult;
import ug.ura.audit.scan.EnrichedEntry;
import ug.ura.audit.scan.FileEntry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TreeOrganizerTest {

    @TempDir
    Path tempDir;

    private FileEntry fileEntry(Path path, String fileName, String archiveParent) {
        return new FileEntry(path.toString(), fileName, 0L, Instant.now(), "test", archiveParent);
    }

    @Test
    void copies_file_into_source_date_tree() throws Exception {
        // Create source file
        Path sourceFile = tempDir.resolve("txn_20230412.csv");
        Files.writeString(sourceFile, "momo data");

        FileEntry fe = fileEntry(sourceFile, "txn_20230412.csv", null);
        ClassifyResult cr = new ClassifyResult("momo_transactions", "MoMo Transactions", "high");
        DateResult dr = DateResult.fromFilename(LocalDate.of(2023, 4, 12));
        EnrichedEntry entry = new EnrichedEntry(fe, cr, dr);

        Path organizedRoot = tempDir.resolve("organized");
        TreeOrganizer organizer = new TreeOrganizer(organizedRoot);

        int count = organizer.organize(List.of(entry));

        assertEquals(1, count);
        Path expected = organizedRoot.resolve("momo_transactions/2023-04-12/txn_20230412.csv");
        assertTrue(Files.exists(expected), "File should exist at target path");
        assertEquals("momo data", Files.readString(expected));
    }

    @Test
    void skips_unclassified_entries() throws Exception {
        Path sourceFile = tempDir.resolve("unknown.csv");
        Files.writeString(sourceFile, "some data");

        FileEntry fe = fileEntry(sourceFile, "unknown.csv", null);
        EnrichedEntry entry = new EnrichedEntry(fe, ClassifyResult.NO_MATCH, DateResult.fromFilename(LocalDate.now()));

        Path organizedRoot = tempDir.resolve("organized");
        TreeOrganizer organizer = new TreeOrganizer(organizedRoot);

        int count = organizer.organize(List.of(entry));

        assertEquals(0, count);
        assertFalse(Files.exists(organizedRoot), "Organized dir should not be created");
    }

    @Test
    void skips_entries_without_date() throws Exception {
        Path sourceFile = tempDir.resolve("nodateFile.csv");
        Files.writeString(sourceFile, "some data");

        FileEntry fe = fileEntry(sourceFile, "nodateFile.csv", null);
        ClassifyResult cr = new ClassifyResult("momo_transactions", "MoMo Transactions", "high");
        EnrichedEntry entry = new EnrichedEntry(fe, cr, DateResult.NO_MATCH);

        Path organizedRoot = tempDir.resolve("organized");
        TreeOrganizer organizer = new TreeOrganizer(organizedRoot);

        int count = organizer.organize(List.of(entry));

        assertEquals(0, count);
    }

    @Test
    void duplicate_filename_gets_counter_suffix() throws Exception {
        // Create two source files with different paths but same filename
        Path sourceDir1 = tempDir.resolve("src1");
        Path sourceDir2 = tempDir.resolve("src2");
        Files.createDirectories(sourceDir1);
        Files.createDirectories(sourceDir2);

        Path sourceFile1 = sourceDir1.resolve("txn.csv");
        Path sourceFile2 = sourceDir2.resolve("txn.csv");
        Files.writeString(sourceFile1, "data1");
        Files.writeString(sourceFile2, "data2");

        ClassifyResult cr = new ClassifyResult("momo_transactions", "MoMo Transactions", "high");
        DateResult dr = DateResult.fromFilename(LocalDate.of(2023, 4, 12));

        FileEntry fe1 = fileEntry(sourceFile1, "txn.csv", null);
        FileEntry fe2 = fileEntry(sourceFile2, "txn.csv", null);

        EnrichedEntry entry1 = new EnrichedEntry(fe1, cr, dr);
        EnrichedEntry entry2 = new EnrichedEntry(fe2, cr, dr);

        Path organizedRoot = tempDir.resolve("organized");
        TreeOrganizer organizer = new TreeOrganizer(organizedRoot);

        int count = organizer.organize(List.of(entry1, entry2));

        assertEquals(2, count);

        Path targetDir = organizedRoot.resolve("momo_transactions/2023-04-12");
        assertTrue(Files.exists(targetDir.resolve("txn.csv")), "txn.csv should exist");
        assertTrue(Files.exists(targetDir.resolve("txn_1.csv")), "txn_1.csv should exist");
    }
}
