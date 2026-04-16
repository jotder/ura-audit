package ug.ura.audit.core;

import java.util.List;
import java.util.regex.Pattern;

public record CatalogEntry(
        String key,
        String label,
        List<Pattern> patterns,
        List<String> pathHints
) {
    public boolean matches(String fileName, String fullPath) {
        return matches(fileName, fullPath, "");
    }

    /**
     * Check if fileName matches patterns, subject to path hint constraints.
     * Path hints are checked against both the full file path and the origin tag,
     * so that origin-based routing works regardless of local vs remote path format.
     */
    public boolean matches(String fileName, String fullPath, String origin) {
        if (!pathHints.isEmpty()) {
            String normalizedPath = fullPath.replace('\\', '/');
            boolean pathOk = pathHints.stream().anyMatch(hint ->
                    normalizedPath.contains(hint) || origin.contains(hint));
            if (!pathOk) return false;
        }
        return patterns.stream().anyMatch(p -> p.matcher(fileName).find());
    }
}
