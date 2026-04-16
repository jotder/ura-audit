package ug.ura.audit.core;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class CatalogLoaderTest {

    private static final Path CATALOG_PATH = Path.of("config", "catalog.yaml");

    @Test
    void catalog_has_exactly_19_sources() throws Exception {
        List<CatalogEntry> catalog = CatalogLoader.load(CATALOG_PATH);
        assertThat(catalog).hasSize(19);
    }

    @Test
    @SuppressWarnings("unchecked")
    void every_source_has_at_least_3_test_cases() throws Exception {
        Yaml yaml = new Yaml();
        try (Reader reader = Files.newBufferedReader(CATALOG_PATH)) {
            List<Map<String, Object>> rawList = yaml.load(reader);
            for (Map<String, Object> entry : rawList) {
                String key = (String) entry.get("key");
                List<?> tests = (List<?>) entry.get("tests");
                assertThat(tests)
                        .as("Entry '%s' must have at least 3 test cases", key)
                        .isNotNull()
                        .hasSizeGreaterThanOrEqualTo(3);
            }
        }
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Collection<DynamicTest> all_embedded_test_cases() throws Exception {
        // Load catalog once for the classifier
        List<CatalogEntry> catalog = CatalogLoader.load(CATALOG_PATH);
        Classifier classifier = new Classifier(catalog);

        // Re-read raw YAML to get test cases (including the `tests:` field not in CatalogEntry)
        Yaml yaml = new Yaml();
        List<Map<String, Object>> rawList;
        try (Reader reader = Files.newBufferedReader(CATALOG_PATH)) {
            rawList = yaml.load(reader);
        }

        List<DynamicTest> tests = new ArrayList<>();
        for (Map<String, Object> entry : rawList) {
            String entryKey = (String) entry.get("key");
            List<Map<String, Object>> testCases = (List<Map<String, Object>>) entry.get("tests");
            if (testCases == null) continue;

            for (Map<String, Object> tc : testCases) {
                String name = (String) tc.get("name");
                String path = tc.containsKey("path") ? (String) tc.get("path") : "/dummy/" + name;
                String expect = (String) tc.get("expect");
                if (expect == null) expect = "";

                final String finalExpect = expect;
                final String displayName = entryKey + " | " + name + " @ " + path;

                tests.add(dynamicTest(displayName, () -> {
                    ClassifyResult result = classifier.classify(name, path);
                    if (finalExpect.isEmpty()) {
                        assertThat(result.matched())
                                .as("Expected no match for '%s' at '%s'", name, path)
                                .isFalse();
                    } else {
                        assertThat(result.sourceKey())
                                .as("Expected key '%s' for '%s' at '%s'", finalExpect, name, path)
                                .isEqualTo(finalExpect);
                    }
                }));
            }
        }
        return tests;
    }
}
