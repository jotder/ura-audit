package ug.ura.audit.scan;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

public class FileWalker {

    public List<FileEntry> walk(Path root, String originTag) throws IOException {
        List<FileEntry> entries = new ArrayList<>();
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (attrs.isRegularFile()) {
                    entries.add(new FileEntry(
                            file.toString(),
                            file.getFileName().toString(),
                            attrs.size(),
                            attrs.lastModifiedTime().toInstant(),
                            originTag,
                            null
                    ));
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                System.err.println("WARN: cannot read " + file + ": " + exc.getMessage());
                return FileVisitResult.CONTINUE;
            }
        });
        return entries;
    }

    public List<FileEntry> walkAll(List<RootConfig> roots) throws IOException {
        List<FileEntry> all = new ArrayList<>();
        for (RootConfig rc : roots) {
            if (Files.isDirectory(rc.path())) {
                all.addAll(walk(rc.path(), rc.originTag()));
            } else {
                System.err.println("WARN: root does not exist or is not a directory: " + rc.path());
            }
        }
        return all;
    }

    public record RootConfig(Path path, String originTag) {}
}
