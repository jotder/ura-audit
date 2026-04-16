package ug.ura.audit.core;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

class DateExtractorTest {

    private static final Path PATTERNS_PATH = Path.of("config", "date_patterns.yaml");

    @TestFactory
    @SuppressWarnings("unchecked")
    Collection<DynamicTest> all_embedded_test_cases() throws Exception {
        DateExtractor extractor = DateExtractor.fromYaml(PATTERNS_PATH);

        Yaml yaml = new Yaml();
        List<Map<String, Object>> rawList;
        try (Reader reader = Files.newBufferedReader(PATTERNS_PATH)) {
            rawList = yaml.load(reader);
        }

        List<DynamicTest> tests = new ArrayList<>();
        for (Map<String, Object> entry : rawList) {
            String patternName = (String) entry.get("name");
            List<Map<String, Object>> testCases = (List<Map<String, Object>>) entry.get("tests");
            if (testCases == null) continue;

            for (Map<String, Object> tc : testCases) {
                String input = (String) tc.get("input");
                String expectedDateStr = (String) tc.get("date");

                final String displayName = patternName + " | " + input;

                tests.add(dynamicTest(displayName, () -> {
                    DateResult result = extractor.extract(input);
                    if (expectedDateStr == null || expectedDateStr.isEmpty()) {
                        assertThat(result.date())
                                .as("Expected no date for '%s'", input)
                                .isNull();
                    } else {
                        LocalDate expected = LocalDate.parse(expectedDateStr);
                        assertThat(result.date())
                                .as("Expected date '%s' for '%s'", expectedDateStr, input)
                                .isEqualTo(expected);
                        assertThat(result.dateSource()).isEqualTo("filename");
                    }
                }));
            }
        }
        return tests;
    }
}
