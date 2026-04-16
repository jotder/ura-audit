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
        if (!pathHints.isEmpty()) {
            boolean pathOk = pathHints.stream().anyMatch(fullPath::contains);
            if (!pathOk) return false;
        }
        return patterns.stream().anyMatch(p -> p.matcher(fileName).find());
    }
}
