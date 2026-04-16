package ug.ura.audit.core;

import org.yaml.snakeyaml.Yaml;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

public class CatalogLoader {

    public static List<CatalogEntry> load(Path yamlPath) throws IOException {
        try (Reader reader = Files.newBufferedReader(yamlPath)) {
            return load(reader);
        }
    }

    @SuppressWarnings("unchecked")
    public static List<CatalogEntry> load(Reader reader) {
        Yaml yaml = new Yaml();
        Object raw = yaml.load(reader);
        if (!(raw instanceof List<?> list)) {
            throw new IllegalArgumentException("catalog.yaml must be a YAML list");
        }
        List<CatalogEntry> entries = new ArrayList<>();
        for (Object item : list) {
            if (!(item instanceof Map)) {
                throw new IllegalArgumentException("Each catalog entry must be a YAML map");
            }
            Map<String, Object> map = (Map<String, Object>) item;
            String key = (String) map.get("key");
            String label = (String) map.get("label");
            List<String> patternStrings = (List<String>) map.getOrDefault("patterns", List.of());
            List<Pattern> compiled = patternStrings.stream().map(Pattern::compile).toList();
            List<String> pathHints = (List<String>) map.getOrDefault("path_hints", List.of());
            if (pathHints == null) pathHints = List.of();
            entries.add(new CatalogEntry(key, label, compiled, Collections.unmodifiableList(pathHints)));
        }
        return Collections.unmodifiableList(entries);
    }
}
