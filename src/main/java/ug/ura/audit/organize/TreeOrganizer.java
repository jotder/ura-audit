package ug.ura.audit.organize;

import ug.ura.audit.scan.EnrichedEntry;
import java.io.IOException;
import java.nio.file.*;
import java.util.List;

public class TreeOrganizer {
    private final Path dataRoot;

    public TreeOrganizer(Path dataRoot) {
        this.dataRoot = dataRoot;
    }

    /**
     * Copy classified, dated, non-archive files into <dataRoot>/<sourceKey>/<date>/filename.
     * Skips unclassified entries (empty sourceKey), entries without a date, and archive entries.
     * Returns number of files copied.
     */
    public int organize(List<EnrichedEntry> entries) throws IOException {
        int copied = 0;
        for (EnrichedEntry e : entries) {
            if (!e.classification().matched()) continue;
            if (e.date() == null) continue;
            if (e.file().isArchiveEntry()) continue;

            Path source = Path.of(e.file().path());
            if (!Files.isRegularFile(source)) {
                System.err.println("WARN: source file not found, skipping: " + source);
                continue;
            }

            Path targetDir = dataRoot
                    .resolve(e.sourceKey())
                    .resolve(e.date().toString()); // YYYY-MM-DD

            Files.createDirectories(targetDir);

            Path targetFile = targetDir.resolve(e.file().fileName());
            if (Files.exists(targetFile)) {
                String stem = stemOf(e.file().fileName());
                String ext = extOf(e.file().fileName());
                int counter = 1;
                do {
                    targetFile = targetDir.resolve(stem + "_" + counter + ext);
                    counter++;
                } while (Files.exists(targetFile));
            }

            Files.copy(source, targetFile, StandardCopyOption.COPY_ATTRIBUTES);
            copied++;
        }
        return copied;
    }

    private static String stemOf(String name) {
        int dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(0, dot) : name;
    }

    private static String extOf(String name) {
        int dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(dot) : "";
    }
}
