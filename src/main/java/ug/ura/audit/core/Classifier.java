package ug.ura.audit.core;

import java.util.List;

public class Classifier {
    private final List<CatalogEntry> catalog;

    public Classifier(List<CatalogEntry> catalog) {
        this.catalog = catalog;
    }

    public ClassifyResult classify(String fileName, String fullPath) {
        String normalizedName = stripGzSuffix(fileName);
        CatalogEntry firstMatch = null;
        int matchCount = 0;
        for (CatalogEntry entry : catalog) {
            if (entry.matches(normalizedName, fullPath)) {
                if (firstMatch == null) firstMatch = entry;
                matchCount++;
            }
        }
        if (firstMatch == null) return ClassifyResult.NO_MATCH;
        String confidence = matchCount > 1 ? "ambiguous" : "high";
        return new ClassifyResult(firstMatch.key(), firstMatch.label(), confidence);
    }

    private static String stripGzSuffix(String name) {
        return name.endsWith(".gz") ? name.substring(0, name.length() - 3) : name;
    }
}
