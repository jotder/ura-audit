package ug.ura.audit.scan;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class FileWalkerTest {

    private final FileWalker walker = new FileWalker();

    @Test
    void walks_nested_directories(@TempDir Path tempDir) throws IOException {
        Path dir2023 = Files.createDirectory(tempDir.resolve("2023"));
        Path dirBal = Files.createDirectory(tempDir.resolve("bal"));

        Files.writeString(dir2023.resolve("report.csv"), "data");
        Files.writeString(dir2023.resolve("summary.csv"), "data");
        Files.writeString(dirBal.resolve("balance.csv"), "data");

        List<FileEntry> entries = walker.walk(tempDir, "TEST_ORIGIN");

        assertThat(entries).hasSize(3);
        assertThat(entries).allMatch(e -> e.origin().equals("TEST_ORIGIN"));
        assertThat(entries).allMatch(e -> e.archiveParent() == null);

        Set<String> fileNames = entries.stream()
                .map(FileEntry::fileName)
                .collect(Collectors.toSet());
        assertThat(fileNames).containsExactlyInAnyOrder("report.csv", "summary.csv", "balance.csv");
    }

    @Test
    void empty_directory_returns_empty_list(@TempDir Path tempDir) throws IOException {
        List<FileEntry> entries = walker.walk(tempDir, "EMPTY_ORIGIN");

        assertThat(entries).isEmpty();
    }

    @Test
    void walkAll_combines_multiple_roots(@TempDir Path tempDir) throws IOException {
        Path root1 = Files.createDirectory(tempDir.resolve("root1"));
        Path root2 = Files.createDirectory(tempDir.resolve("root2"));

        Files.writeString(root1.resolve("file1.csv"), "data");
        Files.writeString(root2.resolve("file2.csv"), "data");

        List<FileWalker.RootConfig> roots = List.of(
                new FileWalker.RootConfig(root1, "ORIGIN_A"),
                new FileWalker.RootConfig(root2, "ORIGIN_B")
        );

        List<FileEntry> entries = walker.walkAll(roots);

        assertThat(entries).hasSize(2);
        Set<String> origins = entries.stream()
                .map(FileEntry::origin)
                .collect(Collectors.toSet());
        assertThat(origins).containsExactlyInAnyOrder("ORIGIN_A", "ORIGIN_B");
    }

    @Test
    void nonexistent_root_skipped_gracefully(@TempDir Path tempDir) throws IOException {
        Path nonexistent = tempDir.resolve("does_not_exist");

        List<FileWalker.RootConfig> roots = List.of(
                new FileWalker.RootConfig(nonexistent, "GHOST_ORIGIN")
        );

        List<FileEntry> entries = walker.walkAll(roots);

        assertThat(entries).isEmpty();
    }
}
