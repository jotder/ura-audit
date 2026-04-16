package ug.ura.audit.scan;

import java.time.Instant;

public record FileEntry(
        String path,
        String fileName,
        long size,
        Instant mtime,
        String origin,
        String archiveParent
) {
    public boolean isArchiveEntry() {
        return archiveParent != null;
    }
}
