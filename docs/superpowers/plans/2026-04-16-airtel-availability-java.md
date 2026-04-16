# Airtel URA Data Availability (Java) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single-JAR tool that runs on the target data server (`192.168.30.24`), walks filesystem roots, classifies files by source, extracts dates from filenames, produces an availability report (CSV + XLSX), and organizes files into a clean `<data_root>/<source>/<date>/` directory tree for downstream ETL.

**Architecture:** A Java 24 application that runs on-target (where the data lives). Uses `java.nio.file.Files.walk()` to traverse filesystem roots, classifies files against a YAML pattern catalog (same `config/catalog.yaml` from the Python prototype), extracts business dates from filenames via regex, then: (1) writes a TSV manifest + availability CSV + summary XLSX, and (2) copies/symlinks files into an organized tree. Source-by-source development — each source is a vertical slice: catalog patterns → tests → walk → classify → organize → report.

**Tech Stack:** Java 24, Maven, SnakeYAML (YAML parsing), Apache POI (XLSX), JUnit 5 + AssertJ (testing). No SSH/paramiko — runs directly on-target.

**Spec reference:** `docs/superpowers/specs/2026-04-15-airtel-data-availability-design.md`

**Environment assumptions:**
- **Development host:** Windows 11, Java 25 (GraalVM CE), Maven 3.6.3.
- **Target host:** `192.168.30.24`, Java 24, user `gamma`.
- **Working directory:** `C:\sandbox\URA\AIRTEL`.
- **Compatibility:** Compile with `--release 24` so the JAR runs on target's Java 24.
- **Existing assets:** `config/catalog.yaml` (19 sources, 67 test cases) and `config/date_patterns.yaml` (4 patterns) from the Python prototype — reused as-is.

**Source-by-source strategy:** Core infrastructure is built first (Tasks 0–7). Then each source is a self-contained task that verifies patterns against real filenames, walks relevant roots, organizes files, and produces a per-source report. We start with **Mobile Money Transactions** (`momo_transactions`).

---

## File structure

```
C:\sandbox\URA\AIRTEL\
├── .gitignore                                   # out/, target/, *.class, .idea/
├── pom.xml                                      # Maven build with shade plugin for fat JAR
├── README.md                                    # How to build + deploy + run on target
├── config/
│   ├── catalog.yaml                             # 19 sources × regex patterns × embedded tests (existing)
│   └── date_patterns.yaml                       # Date regex catalog (existing)
├── docs/                                        # Existing specs + plans
├── src/main/java/ug/ura/audit/
│   ├── Main.java                                # CLI entry point (picocli-less, args parsing)
│   ├── core/
│   │   ├── CatalogEntry.java                    # Record: key, label, compiled patterns, path hints, tests
│   │   ├── CatalogLoader.java                   # Loads catalog.yaml → List<CatalogEntry>
│   │   ├── ClassifyResult.java                  # Record: sourceKey, confidence
│   │   ├── Classifier.java                      # Pattern + path-hint matching engine
│   │   ├── DateResult.java                      # Record: date (LocalDate|null), dateSource
│   │   ├── DatePatternEntry.java                # Record: name, compiled pattern, groups
│   │   └── DateExtractor.java                   # Loads date_patterns.yaml, extracts dates from filenames
│   ├── scan/
│   │   ├── FileEntry.java                       # Record: path, fileName, size, mtime, origin, archiveParent
│   │   ├── FileWalker.java                      # Walks roots via Files.walk(), emits Stream<FileEntry>
│   │   └── ArchiveEnumerator.java               # Lists tar/tar.gz entries as FileEntry (metadata only)
│   ├── organize/
│   │   └── TreeOrganizer.java                   # Copies/symlinks files into <root>/<source>/<date>/
│   └── report/
│       ├── ManifestWriter.java                  # Writes enriched manifest TSV
│       ├── AvailabilityWriter.java              # Writes source × date CSV grid
│       └── SummaryWriter.java                   # Writes summary.xlsx (overview + per-source sheets)
├── src/test/java/ug/ura/audit/
│   ├── core/
│   │   ├── CatalogLoaderTest.java               # Loads real catalog.yaml, parametrized from embedded tests
│   │   ├── ClassifierTest.java                  # Unit tests with inline catalog
│   │   ├── DateExtractorTest.java               # Parametrized from date_patterns.yaml test cases
│   │   └── DateExtractorUnitTest.java           # Edge cases: invalid dates, multi-match, no match
│   ├── scan/
│   │   ├── FileWalkerTest.java                  # Walks a temp directory fixture
│   │   └── ArchiveEnumeratorTest.java           # Lists entries from a test tarball
│   ├── organize/
│   │   └── TreeOrganizerTest.java               # Verifies copy into <root>/<source>/<date>/
│   └── report/
│       ├── ManifestWriterTest.java              # Round-trip: write → read → compare
│       ├── AvailabilityWriterTest.java          # Golden-file comparison
│       └── SummaryWriterTest.java               # Verify XLSX structure
└── src/test/resources/
    └── fixtures/
        ├── tiny_catalog.yaml                    # 2-entry catalog for unit tests
        ├── tiny_date_patterns.yaml              # 2-pattern config for unit tests
        └── tiny_manifest.golden.tsv             # Golden file for manifest round-trip
```

Each `src/main` file has one clear responsibility. Records are used for immutable data carriers. The `Classifier` and `DateExtractor` are the only stateful classes (they hold compiled patterns loaded from YAML).

---

## Task 0: Java project scaffold

**Files:**
- Create: `pom.xml`
- Modify: `.gitignore` (add Java/Maven entries)
- Remove: `src/airtel_availability/` (Python source), `tests/` (Python tests), `pyproject.toml`, `tests/conftest.py`

- [ ] **Step 1: Update `.gitignore` for Java/Maven**

Replace the Python `.gitignore` with:

```gitignore
# Run outputs — never committed
out/
tmp/

# Java
target/
*.class
*.jar
!src/

# IDE
.idea/
*.iml
.vscode/
.project
.classpath
.settings/

# OS
.DS_Store
Thumbs.db

# Secrets
.env
*.pem

# Claude
.claude/
```

- [ ] **Step 2: Create `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>ug.ura.audit</groupId>
    <artifactId>airtel-availability</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <name>Airtel Data Availability Audit</name>
    <description>
        Walks Airtel file-submission roots, classifies files by source,
        extracts dates, produces availability report and organized directory tree.
    </description>

    <properties>
        <maven.compiler.release>24</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <snakeyaml.version>2.4</snakeyaml.version>
        <poi.version>5.4.0</poi.version>
        <junit.version>5.12.2</junit.version>
        <assertj.version>3.27.3</assertj.version>
    </properties>

    <dependencies>
        <!-- YAML parsing -->
        <dependency>
            <groupId>org.yaml</groupId>
            <artifactId>snakeyaml</artifactId>
            <version>${snakeyaml.version}</version>
        </dependency>

        <!-- XLSX output -->
        <dependency>
            <groupId>org.apache.poi</groupId>
            <artifactId>poi-ooxml</artifactId>
            <version>${poi.version}</version>
        </dependency>

        <!-- Testing -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit.version}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <version>${assertj.version}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.14.0</version>
                <configuration>
                    <release>24</release>
                </configuration>
            </plugin>

            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.5.3</version>
            </plugin>

            <!-- Fat JAR for deployment to target -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.6.0</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals><goal>shade</goal></goals>
                        <configuration>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>ug.ura.audit.Main</mainClass>
                                </transformer>
                            </transformers>
                            <createDependencyReducedPom>false</createDependencyReducedPom>
                        </configuration>
                    </execution>
                </executions>
            </plugin>

            <!-- Copy config/ into JAR resources isn't needed — we read config from filesystem at runtime -->
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: Create placeholder Main.java**

Create `src/main/java/ug/ura/audit/Main.java`:

```java
package ug.ura.audit;

public class Main {
    public static void main(String[] args) {
        System.out.println("airtel-availability v1.0-SNAPSHOT");
    }
}
```

- [ ] **Step 4: Remove Python source files**

Remove the Python source and test directories. The YAML configs and docs stay.

```bash
rm -rf src/airtel_availability tests pyproject.toml
```

Verify `config/catalog.yaml` and `config/date_patterns.yaml` still exist.

- [ ] **Step 5: Verify Maven build**

```bash
mvn clean compile
```

Expected: `BUILD SUCCESS`. The Main class compiles with `--release 24`.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: pivot to Java 24 — Maven scaffold, remove Python source"
```

---

## Task 1: Catalog loader + classifier engine

**Files:**
- Create: `src/main/java/ug/ura/audit/core/CatalogEntry.java`
- Create: `src/main/java/ug/ura/audit/core/CatalogLoader.java`
- Create: `src/main/java/ug/ura/audit/core/ClassifyResult.java`
- Create: `src/main/java/ug/ura/audit/core/Classifier.java`
- Create: `src/test/java/ug/ura/audit/core/ClassifierTest.java`
- Create: `src/test/java/ug/ura/audit/core/CatalogLoaderTest.java`
- Create: `src/test/resources/fixtures/tiny_catalog.yaml`

### Records

- [ ] **Step 1: Create ClassifyResult record**

```java
package ug.ura.audit.core;

/**
 * Result of classifying a filename against the catalog.
 * sourceKey is empty string if no match. confidence is "high" or "ambiguous".
 */
public record ClassifyResult(String sourceKey, String sourceLabel, String confidence) {

    public static final ClassifyResult NO_MATCH = new ClassifyResult("", "", "none");

    public boolean matched() {
        return !sourceKey.isEmpty();
    }
}
```

- [ ] **Step 2: Create CatalogEntry record**

```java
package ug.ura.audit.core;

import java.util.List;
import java.util.regex.Pattern;

/**
 * One entry from catalog.yaml. Patterns are pre-compiled at construction time.
 * pathHints is empty list if no path restriction applies.
 */
public record CatalogEntry(
        String key,
        String label,
        List<Pattern> patterns,
        List<String> pathHints
) {
    /**
     * Returns true if at least one pattern matches the filename AND
     * (pathHints is empty OR the full path contains at least one hint).
     */
    public boolean matches(String fileName, String fullPath) {
        // Path hint check first — if hints exist, at least one must match
        if (!pathHints.isEmpty()) {
            boolean pathOk = pathHints.stream().anyMatch(fullPath::contains);
            if (!pathOk) return false;
        }
        // At least one pattern must match the filename
        return patterns.stream().anyMatch(p -> p.matcher(fileName).find());
    }
}
```

- [ ] **Step 3: Create CatalogLoader**

```java
package ug.ura.audit.core;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Loads catalog.yaml into a list of CatalogEntry records.
 * Compiles regex patterns eagerly so classification is fast.
 */
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
            if (!(item instanceof Map<?, ?> map)) {
                throw new IllegalArgumentException("Each catalog entry must be a YAML map");
            }
            String key = (String) map.get("key");
            String label = (String) map.get("label");

            List<String> patternStrings = (List<String>) map.getOrDefault("patterns", List.of());
            List<Pattern> compiled = patternStrings.stream()
                    .map(Pattern::compile)
                    .toList();

            List<String> pathHints = (List<String>) map.getOrDefault("path_hints", List.of());
            if (pathHints == null) pathHints = List.of();

            entries.add(new CatalogEntry(key, label, compiled, Collections.unmodifiableList(pathHints)));
        }
        return Collections.unmodifiableList(entries);
    }
}
```

- [ ] **Step 4: Create Classifier**

```java
package ug.ura.audit.core;

import java.util.List;

/**
 * Classifies a file against the catalog. First match wins.
 * If multiple entries match, first-listed wins with confidence "ambiguous".
 */
public class Classifier {

    private final List<CatalogEntry> catalog;

    public Classifier(List<CatalogEntry> catalog) {
        this.catalog = catalog;
    }

    /**
     * Classify a file by its basename and full path.
     * For plain files, strip .gz suffix before matching (e.g. "foo.csv.gz" → "foo.csv").
     */
    public ClassifyResult classify(String fileName, String fullPath) {
        String normalizedName = stripGzSuffix(fileName);

        CatalogEntry firstMatch = null;
        int matchCount = 0;

        for (CatalogEntry entry : catalog) {
            if (entry.matches(normalizedName, fullPath)) {
                if (firstMatch == null) {
                    firstMatch = entry;
                }
                matchCount++;
            }
        }

        if (firstMatch == null) {
            return ClassifyResult.NO_MATCH;
        }

        String confidence = matchCount > 1 ? "ambiguous" : "high";
        return new ClassifyResult(firstMatch.key(), firstMatch.label(), confidence);
    }

    private static String stripGzSuffix(String name) {
        return name.endsWith(".gz") ? name.substring(0, name.length() - 3) : name;
    }
}
```

### Tests

- [ ] **Step 5: Create tiny_catalog.yaml fixture**

Create `src/test/resources/fixtures/tiny_catalog.yaml`:

```yaml
- key: momo_transactions
  label: "Mobile Money Transactions"
  patterns:
    - '(?i)(^|[/_-])(txn|trans|transactions)([_.-]|$)'
  path_hints: ["/airtel_mobile_money"]
  tests:
    - {name: "txn_20230412.csv", path: "/airtel_mobile_money/2023/txn_20230412.csv", expect: momo_transactions}
    - {name: "txn_20230412.csv", path: "/master_data1/txn_20230412.csv", expect: ""}

- key: air_recharge
  label: "AIR Recharge"
  patterns:
    - '(?i)(^|[/_-])rch([_.-]|$)'
  path_hints: []
  tests:
    - {name: "cbs_cdr_rch_20230412.add", expect: air_recharge}
    - {name: "random_report.csv", expect: ""}
```

- [ ] **Step 6: Write ClassifierTest**

Create `src/test/java/ug/ura/audit/core/ClassifierTest.java`:

```java
package ug.ura.audit.core;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ClassifierTest {

    private static Classifier classifier;

    @BeforeAll
    static void loadCatalog() {
        Reader reader = new InputStreamReader(
                ClassifierTest.class.getResourceAsStream("/fixtures/tiny_catalog.yaml"));
        List<CatalogEntry> catalog = CatalogLoader.load(reader);
        classifier = new Classifier(catalog);
    }

    @Test
    void momo_txn_under_correct_path_matches() {
        ClassifyResult r = classifier.classify("txn_20230412.csv", "/airtel_mobile_money/2023/txn_20230412.csv");
        assertThat(r.sourceKey()).isEqualTo("momo_transactions");
        assertThat(r.confidence()).isEqualTo("high");
    }

    @Test
    void momo_txn_under_wrong_path_no_match() {
        ClassifyResult r = classifier.classify("txn_20230412.csv", "/master_data1/txn_20230412.csv");
        assertThat(r.matched()).isFalse();
    }

    @Test
    void recharge_matches_without_path_hint() {
        ClassifyResult r = classifier.classify("cbs_cdr_rch_20230412.add", "/master_data1/cbs_cdr_rch_20230412.add");
        assertThat(r.sourceKey()).isEqualTo("air_recharge");
    }

    @Test
    void no_match_returns_empty() {
        ClassifyResult r = classifier.classify("random_report.csv", "/some/path/random_report.csv");
        assertThat(r.matched()).isFalse();
        assertThat(r.sourceKey()).isEmpty();
    }

    @Test
    void gz_suffix_stripped_before_matching() {
        ClassifyResult r = classifier.classify("cbs_cdr_rch_20230412.add.gz", "/master_data1/cbs_cdr_rch_20230412.add.gz");
        assertThat(r.sourceKey()).isEqualTo("air_recharge");
    }
}
```

- [ ] **Step 7: Write CatalogLoaderTest — parametrized from real catalog.yaml**

Create `src/test/java/ug/ura/audit/core/CatalogLoaderTest.java`:

```java
package ug.ura.audit.core;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * Loads the real catalog.yaml, runs every embedded test case.
 * This is the Java equivalent of the Python test_catalog.py.
 */
class CatalogLoaderTest {

    private static final Path CATALOG_PATH = Path.of("config", "catalog.yaml");
    private static Classifier classifier;
    private static List<CatalogEntry> catalog;

    @BeforeAll
    static void loadRealCatalog() throws IOException {
        catalog = CatalogLoader.load(CATALOG_PATH);
        classifier = new Classifier(catalog);
    }

    @Test
    void catalog_has_exactly_19_sources() {
        assertThat(catalog).hasSize(19);
    }

    @Test
    void every_source_has_at_least_3_test_cases() throws IOException {
        // Re-read raw YAML to access test cases (CatalogEntry doesn't store them)
        Yaml yaml = new Yaml();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> raw = yaml.load(java.nio.file.Files.newBufferedReader(CATALOG_PATH));
        for (Map<String, Object> entry : raw) {
            String key = (String) entry.get("key");
            @SuppressWarnings("unchecked")
            List<?> tests = (List<?>) entry.getOrDefault("tests", List.of());
            assertThat(tests)
                    .as("source '%s' must have >= 3 test cases", key)
                    .hasSizeGreaterThanOrEqualTo(3);
        }
    }

    @TestFactory
    Collection<DynamicTest> all_embedded_test_cases() throws IOException {
        Yaml yaml = new Yaml();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> raw = yaml.load(java.nio.file.Files.newBufferedReader(CATALOG_PATH));

        List<DynamicTest> tests = new ArrayList<>();
        for (Map<String, Object> entry : raw) {
            String sourceKey = (String) entry.get("key");
            @SuppressWarnings("unchecked")
            List<Map<String, String>> cases = (List<Map<String, String>>) entry.getOrDefault("tests", List.of());

            for (Map<String, String> tc : cases) {
                String name = tc.get("name");
                String path = tc.getOrDefault("path", "/dummy/" + name);
                String expect = tc.getOrDefault("expect", "");

                String testName = sourceKey + ": " + name + " → " + (expect.isEmpty() ? "NO_MATCH" : expect);
                tests.add(dynamicTest(testName, () -> {
                    ClassifyResult result = classifier.classify(name, path);
                    if (expect.isEmpty()) {
                        assertThat(result.matched())
                                .as("'%s' should not match any source", name)
                                .isFalse();
                    } else {
                        assertThat(result.sourceKey())
                                .as("'%s' should classify as '%s'", name, expect)
                                .isEqualTo(expect);
                    }
                }));
            }
        }
        return tests;
    }
}
```

- [ ] **Step 8: Run tests**

```bash
mvn test
```

Expected: All tests pass — 5 ClassifierTest + 2 structural CatalogLoaderTest + ~67 dynamic catalog tests ≈ 74 tests.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat(core): catalog loader + classifier engine with YAML-driven tests"
```

---

## Task 2: Date extractor

**Files:**
- Create: `src/main/java/ug/ura/audit/core/DateResult.java`
- Create: `src/main/java/ug/ura/audit/core/DatePatternEntry.java`
- Create: `src/main/java/ug/ura/audit/core/DateExtractor.java`
- Create: `src/test/java/ug/ura/audit/core/DateExtractorTest.java`
- Create: `src/test/java/ug/ura/audit/core/DateExtractorUnitTest.java`

- [ ] **Step 1: Create DateResult record**

```java
package ug.ura.audit.core;

import java.time.LocalDate;

/**
 * Result of extracting a date from a filename.
 * date is null if no pattern matched (falls back to mtime at call site).
 * dateSource is "filename" or "mtime".
 */
public record DateResult(LocalDate date, String dateSource) {

    public static DateResult fromFilename(LocalDate date) {
        return new DateResult(date, "filename");
    }

    public static final DateResult NO_MATCH = new DateResult(null, "mtime");
}
```

- [ ] **Step 2: Create DatePatternEntry record**

```java
package ug.ura.audit.core;

import java.util.regex.Pattern;

/**
 * One entry from date_patterns.yaml. The pattern is pre-compiled.
 * yearGroup/monthGroup/dayGroup indicate which regex groups hold Y/M/D.
 */
public record DatePatternEntry(
        String name,
        Pattern pattern,
        int yearGroup,
        int monthGroup,
        int dayGroup
) {}
```

- [ ] **Step 3: Create DateExtractor**

```java
package ug.ura.audit.core;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts a business date from a filename using regex patterns loaded from YAML.
 * Tries patterns in order; first match wins.
 * Returns DateResult.NO_MATCH if no pattern matches (caller should fall back to mtime).
 */
public class DateExtractor {

    private final List<DatePatternEntry> patterns;

    public DateExtractor(List<DatePatternEntry> patterns) {
        this.patterns = patterns;
    }

    public static DateExtractor fromYaml(Path yamlPath) throws IOException {
        try (Reader reader = Files.newBufferedReader(yamlPath)) {
            return fromYaml(reader);
        }
    }

    @SuppressWarnings("unchecked")
    public static DateExtractor fromYaml(Reader reader) {
        Yaml yaml = new Yaml();
        Object raw = yaml.load(reader);
        if (!(raw instanceof List<?> list)) {
            throw new IllegalArgumentException("date_patterns.yaml must be a YAML list");
        }

        List<DatePatternEntry> entries = new ArrayList<>();
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> map)) {
                throw new IllegalArgumentException("Each date pattern entry must be a YAML map");
            }
            String name = (String) map.get("name");
            String regex = (String) map.get("pattern");
            int yearGroup = ((Number) map.get("year_group")).intValue();
            int monthGroup = ((Number) map.get("month_group")).intValue();
            int dayGroup = ((Number) map.get("day_group")).intValue();

            entries.add(new DatePatternEntry(name, Pattern.compile(regex), yearGroup, monthGroup, dayGroup));
        }
        return new DateExtractor(Collections.unmodifiableList(entries));
    }

    /**
     * Extract a date from a filename. Strips .gz suffix first.
     * Returns DateResult.NO_MATCH if no pattern matches.
     */
    public DateResult extract(String fileName) {
        String normalized = stripGzSuffix(fileName);

        for (DatePatternEntry entry : patterns) {
            Matcher m = entry.pattern().matcher(normalized);
            if (m.find()) {
                try {
                    int year = Integer.parseInt(m.group(entry.yearGroup()));
                    int month = Integer.parseInt(m.group(entry.monthGroup()));
                    int day = Integer.parseInt(m.group(entry.dayGroup()));
                    LocalDate date = LocalDate.of(year, month, day);
                    return DateResult.fromFilename(date);
                } catch (java.time.DateTimeException e) {
                    // Invalid calendar date (e.g. Feb 30) — skip this pattern, try next
                    continue;
                }
            }
        }
        return DateResult.NO_MATCH;
    }

    private static String stripGzSuffix(String name) {
        return name.endsWith(".gz") ? name.substring(0, name.length() - 3) : name;
    }
}
```

- [ ] **Step 4: Update date_patterns.yaml format**

The existing `date_patterns.yaml` is Python-oriented. We need to add `year_group`, `month_group`, `day_group` fields for Java. Read the current file and update it.

The updated `config/date_patterns.yaml` should be:

```yaml
# Date extraction patterns. Order matters: first match wins.
# Each entry has: name, pattern (Java regex), year_group/month_group/day_group (1-based capture group indices),
# and embedded test cases.

- name: ymd_compact
  pattern: '(?<![0-9])(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?![0-9])'
  year_group: 1
  month_group: 2
  day_group: 3
  tests:
    - {input: "txn_20230412.csv", date: "2023-04-12"}
    - {input: "CBS_CDR_RCH_20230101.add", date: "2023-01-01"}
    - {input: "no_date_here.csv", date: ""}

- name: ymd_dashed
  pattern: '(?<![0-9])(20\d{2})[-_](0[1-9]|1[0-2])[-_](0[1-9]|[12]\d|3[01])(?![0-9])'
  year_group: 1
  month_group: 2
  day_group: 3
  tests:
    - {input: "report_2023-04-12.csv", date: "2023-04-12"}
    - {input: "report_2023_04_12.csv", date: "2023-04-12"}

- name: ymd_19xx
  pattern: '(?<![0-9])(19\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?![0-9])'
  year_group: 1
  month_group: 2
  day_group: 3
  tests:
    - {input: "archive_19990101.csv", date: "1999-01-01"}

- name: dmy_dashed
  pattern: '(?<![0-9])(0[1-9]|[12]\d|3[01])[-_](0[1-9]|1[0-2])[-_](20\d{2})(?![0-9])'
  year_group: 3
  month_group: 2
  day_group: 1
  tests:
    - {input: "report_12-04-2023.csv", date: "2023-04-12"}
    - {input: "report_31-12-2023.csv", date: "2023-12-31"}
```

- [ ] **Step 5: Write DateExtractorTest — parametrized from YAML**

Create `src/test/java/ug/ura/audit/core/DateExtractorTest.java`:

```java
package ug.ura.audit.core;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
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
    private static DateExtractor extractor;

    @BeforeAll
    static void load() throws IOException {
        extractor = DateExtractor.fromYaml(PATTERNS_PATH);
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Collection<DynamicTest> all_embedded_test_cases() throws IOException {
        Yaml yaml = new Yaml();
        List<Map<String, Object>> raw = yaml.load(java.nio.file.Files.newBufferedReader(PATTERNS_PATH));

        List<DynamicTest> tests = new ArrayList<>();
        for (Map<String, Object> entry : raw) {
            String patternName = (String) entry.get("name");
            List<Map<String, String>> cases = (List<Map<String, String>>) entry.getOrDefault("tests", List.of());

            for (Map<String, String> tc : cases) {
                String input = tc.get("input");
                String expectDate = tc.getOrDefault("date", "");

                String testName = patternName + ": " + input + " → " + (expectDate.isEmpty() ? "NO_MATCH" : expectDate);
                tests.add(dynamicTest(testName, () -> {
                    DateResult result = extractor.extract(input);
                    if (expectDate.isEmpty()) {
                        assertThat(result.date())
                                .as("'%s' should not match any date", input)
                                .isNull();
                    } else {
                        assertThat(result.date())
                                .as("'%s' should extract date '%s'", input, expectDate)
                                .isEqualTo(LocalDate.parse(expectDate));
                        assertThat(result.dateSource()).isEqualTo("filename");
                    }
                }));
            }
        }
        return tests;
    }
}
```

- [ ] **Step 6: Write DateExtractorUnitTest — edge cases**

Create `src/test/java/ug/ura/audit/core/DateExtractorUnitTest.java`:

```java
package ug.ura.audit.core;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

class DateExtractorUnitTest {

    private static DateExtractor extractor;

    @BeforeAll
    static void load() throws IOException {
        extractor = DateExtractor.fromYaml(Path.of("config", "date_patterns.yaml"));
    }

    @Test
    void invalid_calendar_date_skipped() {
        // Feb 30 doesn't exist — should not match
        DateResult r = extractor.extract("report_20230230.csv");
        assertThat(r.date()).isNull();
    }

    @Test
    void gz_suffix_stripped() {
        DateResult r = extractor.extract("txn_20230412.csv.gz");
        assertThat(r.date()).isEqualTo(LocalDate.of(2023, 4, 12));
    }

    @Test
    void no_date_returns_mtime_source() {
        DateResult r = extractor.extract("random_file.csv");
        assertThat(r.dateSource()).isEqualTo("mtime");
        assertThat(r.date()).isNull();
    }

    @Test
    void multiple_dates_first_wins() {
        // "txn_20230412_20230413.csv" — first date (20230412) should win
        DateResult r = extractor.extract("txn_20230412_20230413.csv");
        assertThat(r.date()).isEqualTo(LocalDate.of(2023, 4, 12));
    }
}
```

- [ ] **Step 7: Run tests**

```bash
mvn test
```

Expected: All date extractor tests pass alongside the classifier tests from Task 1.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat(core): date extractor with YAML-driven patterns and tests"
```

---

## Task 3: File walker

**Files:**
- Create: `src/main/java/ug/ura/audit/scan/FileEntry.java`
- Create: `src/main/java/ug/ura/audit/scan/FileWalker.java`
- Create: `src/test/java/ug/ura/audit/scan/FileWalkerTest.java`

- [ ] **Step 1: Create FileEntry record**

```java
package ug.ura.audit.scan;

import java.time.Instant;

/**
 * Metadata for one file (or archive entry) discovered during a scan.
 * origin identifies which root the file was found under.
 * archiveParent is null for plain files; set for entries inside tarballs.
 */
public record FileEntry(
        String path,
        String fileName,
        long size,
        Instant mtime,
        String origin,
        String archiveParent
) {
    /** True if this entry came from inside an archive. */
    public boolean isArchiveEntry() {
        return archiveParent != null;
    }
}
```

- [ ] **Step 2: Create FileWalker**

```java
package ug.ura.audit.scan;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/**
 * Walks one or more filesystem roots and collects FileEntry records.
 * Follows the directory structure, collecting regular files only.
 * Each root is tagged as an "origin" in the resulting entries.
 */
public class FileWalker {

    /**
     * Walk a single root directory and return all regular files as FileEntry records.
     * @param root       the directory to walk
     * @param originTag  label for this root (e.g. "/airtel_mobile_money")
     * @return list of FileEntry for every regular file under root
     */
    public List<FileEntry> walk(Path root, String originTag) throws IOException {
        List<FileEntry> entries = new ArrayList<>();

        Files.walkFileTree(root, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
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
                return java.nio.file.FileVisitResult.CONTINUE;
            }

            @Override
            public java.nio.file.FileVisitResult visitFileFailed(Path file, IOException exc) {
                // Log and continue — permission denied, broken symlink, etc.
                System.err.println("WARN: cannot read " + file + ": " + exc.getMessage());
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });

        return entries;
    }

    /**
     * Walk multiple roots and return all files from all roots.
     * @param roots  map of root path → origin tag
     */
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

    /** Configuration for one scan root. */
    public record RootConfig(Path path, String originTag) {}
}
```

- [ ] **Step 3: Write FileWalkerTest**

Create `src/test/java/ug/ura/audit/scan/FileWalkerTest.java`:

```java
package ug.ura.audit.scan;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FileWalkerTest {

    @TempDir
    Path tempDir;

    @Test
    void walks_nested_directories() throws IOException {
        // Create: root/2023/txn_20230412.csv, root/2023/txn_20230413.csv, root/bal/bal_20230412.csv
        Path dir2023 = Files.createDirectories(tempDir.resolve("2023"));
        Files.writeString(dir2023.resolve("txn_20230412.csv"), "data");
        Files.writeString(dir2023.resolve("txn_20230413.csv"), "data");
        Path dirBal = Files.createDirectories(tempDir.resolve("bal"));
        Files.writeString(dirBal.resolve("bal_20230412.csv"), "data");

        FileWalker walker = new FileWalker();
        List<FileEntry> entries = walker.walk(tempDir, "/airtel_mobile_money");

        assertThat(entries).hasSize(3);
        assertThat(entries).allSatisfy(e -> {
            assertThat(e.origin()).isEqualTo("/airtel_mobile_money");
            assertThat(e.archiveParent()).isNull();
            assertThat(e.size()).isGreaterThan(0);
        });
        assertThat(entries).extracting(FileEntry::fileName)
                .containsExactlyInAnyOrder("txn_20230412.csv", "txn_20230413.csv", "bal_20230412.csv");
    }

    @Test
    void empty_directory_returns_empty_list() throws IOException {
        FileWalker walker = new FileWalker();
        List<FileEntry> entries = walker.walk(tempDir, "/empty");
        assertThat(entries).isEmpty();
    }

    @Test
    void walkAll_combines_multiple_roots() throws IOException {
        Path root1 = Files.createDirectories(tempDir.resolve("root1"));
        Path root2 = Files.createDirectories(tempDir.resolve("root2"));
        Files.writeString(root1.resolve("a.csv"), "data");
        Files.writeString(root2.resolve("b.csv"), "data");

        FileWalker walker = new FileWalker();
        List<FileEntry> entries = walker.walkAll(List.of(
                new FileWalker.RootConfig(root1, "origin1"),
                new FileWalker.RootConfig(root2, "origin2")
        ));

        assertThat(entries).hasSize(2);
        assertThat(entries).extracting(FileEntry::origin)
                .containsExactlyInAnyOrder("origin1", "origin2");
    }

    @Test
    void nonexistent_root_skipped_gracefully() throws IOException {
        FileWalker walker = new FileWalker();
        List<FileEntry> entries = walker.walkAll(List.of(
                new FileWalker.RootConfig(tempDir.resolve("nonexistent"), "ghost")
        ));
        assertThat(entries).isEmpty();
    }
}
```

- [ ] **Step 4: Run tests**

```bash
mvn test
```

Expected: All tests pass (classifier + date extractor + file walker).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(scan): file walker using java.nio.file.Files.walkFileTree"
```

---

## Task 4: Manifest writer + reader

**Files:**
- Create: `src/main/java/ug/ura/audit/report/ManifestWriter.java`
- Create: `src/test/java/ug/ura/audit/report/ManifestWriterTest.java`

The manifest is the enriched TSV: one row per file with classification and date extraction results appended.

- [ ] **Step 1: Create enriched file record**

Create `src/main/java/ug/ura/audit/scan/EnrichedEntry.java`:

```java
package ug.ura.audit.scan;

import ug.ura.audit.core.ClassifyResult;
import ug.ura.audit.core.DateResult;

import java.time.LocalDate;

/**
 * A FileEntry enriched with classification and date extraction results.
 * This is the row model for the manifest TSV.
 */
public record EnrichedEntry(
        FileEntry file,
        ClassifyResult classification,
        DateResult dateResult
) {
    public String sourceKey() { return classification.sourceKey(); }
    public String sourceLabel() { return classification.sourceLabel(); }
    public String confidence() { return classification.confidence(); }
    public LocalDate date() { return dateResult.date(); }
    public String dateSource() { return dateResult.dateSource(); }
}
```

- [ ] **Step 2: Create ManifestWriter**

```java
package ug.ura.audit.report;

import ug.ura.audit.scan.EnrichedEntry;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Writes enriched manifest as TSV.
 * Columns: path, origin, size, mtime, archive_parent, file_name, source_key, source_label,
 *          confidence, date, date_source
 */
public class ManifestWriter {

    private static final String HEADER = String.join("\t",
            "path", "origin", "size", "mtime", "archive_parent", "file_name",
            "source_key", "source_label", "confidence", "date", "date_source");

    private static final DateTimeFormatter ISO_INSTANT = DateTimeFormatter.ISO_INSTANT;

    public static void write(Path outputPath, List<EnrichedEntry> entries) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(outputPath)) {
            w.write(HEADER);
            w.newLine();
            for (EnrichedEntry e : entries) {
                w.write(String.join("\t",
                        e.file().path(),
                        e.file().origin(),
                        String.valueOf(e.file().size()),
                        ISO_INSTANT.format(e.file().mtime()),
                        e.file().archiveParent() != null ? e.file().archiveParent() : "",
                        e.file().fileName(),
                        e.sourceKey(),
                        e.sourceLabel(),
                        e.confidence(),
                        e.date() != null ? e.date().toString() : "",
                        e.dateSource()
                ));
                w.newLine();
            }
        }
    }

    /**
     * Read manifest TSV back into a list of string arrays (for verification/testing).
     * Skips header row.
     */
    public static List<String[]> read(Path manifestPath) throws IOException {
        List<String> lines = Files.readAllLines(manifestPath);
        return lines.stream()
                .skip(1) // skip header
                .map(line -> line.split("\t", -1))
                .toList();
    }
}
```

- [ ] **Step 3: Write ManifestWriterTest**

Create `src/test/java/ug/ura/audit/report/ManifestWriterTest.java`:

```java
package ug.ura.audit.report;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ug.ura.audit.core.ClassifyResult;
import ug.ura.audit.core.DateResult;
import ug.ura.audit.scan.EnrichedEntry;
import ug.ura.audit.scan.FileEntry;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ManifestWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void round_trip_write_and_read() throws IOException {
        FileEntry f = new FileEntry(
                "/airtel_mobile_money/2023/txn_20230412.csv",
                "txn_20230412.csv",
                1024,
                Instant.parse("2023-04-12T10:00:00Z"),
                "/airtel_mobile_money",
                null
        );
        EnrichedEntry e = new EnrichedEntry(
                f,
                new ClassifyResult("momo_transactions", "Mobile Money Transactions", "high"),
                DateResult.fromFilename(LocalDate.of(2023, 4, 12))
        );

        Path out = tempDir.resolve("manifest.tsv");
        ManifestWriter.write(out, List.of(e));

        List<String[]> rows = ManifestWriter.read(out);
        assertThat(rows).hasSize(1);
        String[] row = rows.get(0);
        assertThat(row[0]).isEqualTo("/airtel_mobile_money/2023/txn_20230412.csv");
        assertThat(row[5]).isEqualTo("txn_20230412.csv");
        assertThat(row[6]).isEqualTo("momo_transactions");
        assertThat(row[9]).isEqualTo("2023-04-12");
        assertThat(row[10]).isEqualTo("filename");
    }

    @Test
    void unclassified_entry_has_empty_source_key() throws IOException {
        FileEntry f = new FileEntry("/some/path/unknown.dat", "unknown.dat", 100,
                Instant.now(), "/some", null);
        EnrichedEntry e = new EnrichedEntry(f, ClassifyResult.NO_MATCH, DateResult.NO_MATCH);

        Path out = tempDir.resolve("manifest.tsv");
        ManifestWriter.write(out, List.of(e));

        List<String[]> rows = ManifestWriter.read(out);
        assertThat(rows.get(0)[6]).isEmpty(); // source_key
        assertThat(rows.get(0)[9]).isEmpty(); // date
    }
}
```

- [ ] **Step 4: Run tests**

```bash
mvn test
```

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(report): manifest TSV writer with round-trip test"
```

---

## Task 5: Tree organizer

**Files:**
- Create: `src/main/java/ug/ura/audit/organize/TreeOrganizer.java`
- Create: `src/test/java/ug/ura/audit/organize/TreeOrganizerTest.java`

This is the **new feature** not in the Python prototype. Copies classified files into `<dataRoot>/<sourceKey>/<date>/filename`.

- [ ] **Step 1: Create TreeOrganizer**

```java
package ug.ura.audit.organize;

import ug.ura.audit.scan.EnrichedEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * Organizes classified files into a clean directory tree:
 *   <dataRoot>/<sourceKey>/<date>/filename
 *
 * Only copies files that are classified (sourceKey non-empty) and have a date.
 * Skips archive entries (they can't be copied individually without extraction).
 * Preserves original file — this is a COPY, not a move.
 */
public class TreeOrganizer {

    private final Path dataRoot;

    public TreeOrganizer(Path dataRoot) {
        this.dataRoot = dataRoot;
    }

    /**
     * Organize all classified, dated, non-archive entries into the tree.
     * @return number of files successfully copied
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
            // If duplicate filename in same source/date, append origin hash to disambiguate
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
```

- [ ] **Step 2: Write TreeOrganizerTest**

Create `src/test/java/ug/ura/audit/organize/TreeOrganizerTest.java`:

```java
package ug.ura.audit.organize;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ug.ura.audit.core.ClassifyResult;
import ug.ura.audit.core.DateResult;
import ug.ura.audit.scan.EnrichedEntry;
import ug.ura.audit.scan.FileEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TreeOrganizerTest {

    @TempDir
    Path tempDir;

    private EnrichedEntry makeEntry(Path sourceFile, String sourceKey, String label, LocalDate date) {
        FileEntry f = new FileEntry(
                sourceFile.toString(),
                sourceFile.getFileName().toString(),
                1024,
                Instant.now(),
                "/airtel_mobile_money",
                null
        );
        return new EnrichedEntry(
                f,
                new ClassifyResult(sourceKey, label, "high"),
                DateResult.fromFilename(date)
        );
    }

    @Test
    void copies_file_into_source_date_tree() throws IOException {
        // Create source file
        Path sourceFile = Files.writeString(tempDir.resolve("txn_20230412.csv"), "momo data");
        Path dataRoot = tempDir.resolve("organized");

        EnrichedEntry e = makeEntry(sourceFile, "momo_transactions", "Mobile Money Transactions",
                LocalDate.of(2023, 4, 12));

        TreeOrganizer organizer = new TreeOrganizer(dataRoot);
        int count = organizer.organize(List.of(e));

        assertThat(count).isEqualTo(1);
        Path expected = dataRoot.resolve("momo_transactions/2023-04-12/txn_20230412.csv");
        assertThat(expected).exists();
        assertThat(Files.readString(expected)).isEqualTo("momo data");
    }

    @Test
    void skips_unclassified_entries() throws IOException {
        Path sourceFile = Files.writeString(tempDir.resolve("unknown.csv"), "data");
        Path dataRoot = tempDir.resolve("organized");

        FileEntry f = new FileEntry(sourceFile.toString(), "unknown.csv", 100, Instant.now(), "/some", null);
        EnrichedEntry e = new EnrichedEntry(f, ClassifyResult.NO_MATCH, DateResult.NO_MATCH);

        TreeOrganizer organizer = new TreeOrganizer(dataRoot);
        int count = organizer.organize(List.of(e));

        assertThat(count).isEqualTo(0);
        assertThat(dataRoot).doesNotExist();
    }

    @Test
    void skips_entries_without_date() throws IOException {
        Path sourceFile = Files.writeString(tempDir.resolve("txn_nodate.csv"), "data");
        Path dataRoot = tempDir.resolve("organized");

        FileEntry f = new FileEntry(sourceFile.toString(), "txn_nodate.csv", 100, Instant.now(), "/mm", null);
        EnrichedEntry e = new EnrichedEntry(f,
                new ClassifyResult("momo_transactions", "MoMo", "high"),
                DateResult.NO_MATCH);

        TreeOrganizer organizer = new TreeOrganizer(dataRoot);
        int count = organizer.organize(List.of(e));

        assertThat(count).isEqualTo(0);
    }

    @Test
    void duplicate_filename_gets_counter_suffix() throws IOException {
        Path src1 = Files.writeString(tempDir.resolve("txn_20230412_a.csv"), "data1");
        Path src2 = Files.writeString(tempDir.resolve("txn_20230412_b.csv"), "data2");
        Path dataRoot = tempDir.resolve("organized");

        // Both have the same fileName after renaming for this test
        LocalDate date = LocalDate.of(2023, 4, 12);
        FileEntry f1 = new FileEntry(src1.toString(), "txn.csv", 100, Instant.now(), "/mm", null);
        FileEntry f2 = new FileEntry(src2.toString(), "txn.csv", 100, Instant.now(), "/mm", null);
        var cr = new ClassifyResult("momo_transactions", "MoMo", "high");
        var dr = DateResult.fromFilename(date);

        TreeOrganizer organizer = new TreeOrganizer(dataRoot);
        int count = organizer.organize(List.of(
                new EnrichedEntry(f1, cr, dr),
                new EnrichedEntry(f2, cr, dr)
        ));

        assertThat(count).isEqualTo(2);
        Path dir = dataRoot.resolve("momo_transactions/2023-04-12");
        assertThat(dir.resolve("txn.csv")).exists();
        assertThat(dir.resolve("txn_1.csv")).exists();
    }
}
```

- [ ] **Step 3: Run tests**

```bash
mvn test
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(organize): tree organizer — copies files into <root>/<source>/<date>/"
```

---

## Task 6: Availability report writer

**Files:**
- Create: `src/main/java/ug/ura/audit/report/AvailabilityWriter.java`
- Create: `src/test/java/ug/ura/audit/report/AvailabilityWriterTest.java`

Produces the `source × date` CSV grid per spec §6.1.

- [ ] **Step 1: Create AvailabilityWriter**

```java
package ug.ura.audit.report;

import ug.ura.audit.scan.EnrichedEntry;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Writes availability.csv — one row per (source_key, date) in the audit window.
 * Full grid: dates with zero files are emitted so gaps are first-class rows.
 * Columns per spec §6.1: run_id, source_key, source_label, date, files_total, bytes_total,
 *   files per origin, bytes per origin, has_archive, date_confidence, min_mtime, max_mtime.
 */
public class AvailabilityWriter {

    private static final List<String> ORIGINS = List.of(
            "master_data1", "master_data2", "mobile_money", "resubmited", "unique_trnx");

    public static void write(Path outputPath, List<EnrichedEntry> entries,
                             String runId, LocalDate windowStart, LocalDate windowEnd,
                             List<String> sourceKeys) throws IOException {

        // Group entries by (sourceKey, date)
        Map<String, Map<LocalDate, List<EnrichedEntry>>> grouped = entries.stream()
                .filter(e -> e.classification().matched())
                .filter(e -> e.date() != null)
                .filter(e -> !e.date().isBefore(windowStart) && !e.date().isAfter(windowEnd))
                .collect(Collectors.groupingBy(
                        e -> e.sourceKey(),
                        Collectors.groupingBy(e -> e.date())
                ));

        try (BufferedWriter w = Files.newBufferedWriter(outputPath)) {
            // Header
            List<String> cols = new ArrayList<>();
            cols.addAll(List.of("run_id", "source_key", "source_label", "date", "files_total", "bytes_total"));
            for (String o : ORIGINS) {
                cols.add("files_" + o);
            }
            for (String o : ORIGINS) {
                cols.add("bytes_" + o);
            }
            cols.addAll(List.of("has_archive", "date_confidence", "min_mtime", "max_mtime"));
            w.write(String.join(",", cols));
            w.newLine();

            // One row per (source, date) — full grid
            for (String sourceKey : sourceKeys) {
                String label = lookupLabel(entries, sourceKey);
                LocalDate d = windowStart;
                while (!d.isAfter(windowEnd)) {
                    List<EnrichedEntry> bucket = grouped
                            .getOrDefault(sourceKey, Map.of())
                            .getOrDefault(d, List.of());

                    List<String> vals = new ArrayList<>();
                    vals.add(runId);
                    vals.add(sourceKey);
                    vals.add(csvEscape(label));
                    vals.add(d.toString());
                    vals.add(String.valueOf(bucket.size()));
                    vals.add(String.valueOf(bucket.stream().mapToLong(e -> e.file().size()).sum()));

                    for (String o : ORIGINS) {
                        long count = bucket.stream().filter(e -> originMatches(e, o)).count();
                        vals.add(String.valueOf(count));
                    }
                    for (String o : ORIGINS) {
                        long bytes = bucket.stream().filter(e -> originMatches(e, o))
                                .mapToLong(e -> e.file().size()).sum();
                        vals.add(String.valueOf(bytes));
                    }

                    boolean hasArchive = bucket.stream().anyMatch(e -> e.file().isArchiveEntry());
                    vals.add(String.valueOf(hasArchive));

                    String dateConf = computeDateConfidence(bucket);
                    vals.add(dateConf);

                    String minMtime = bucket.stream()
                            .map(e -> e.file().mtime()).min(Comparator.naturalOrder())
                            .map(Object::toString).orElse("");
                    String maxMtime = bucket.stream()
                            .map(e -> e.file().mtime()).max(Comparator.naturalOrder())
                            .map(Object::toString).orElse("");
                    vals.add(minMtime);
                    vals.add(maxMtime);

                    w.write(String.join(",", vals));
                    w.newLine();
                    d = d.plusDays(1);
                }
            }
        }
    }

    private static boolean originMatches(EnrichedEntry e, String originShort) {
        return e.file().origin().contains(originShort);
    }

    private static String lookupLabel(List<EnrichedEntry> entries, String sourceKey) {
        return entries.stream()
                .filter(e -> e.sourceKey().equals(sourceKey))
                .findFirst()
                .map(EnrichedEntry::sourceLabel)
                .orElse(sourceKey);
    }

    private static String computeDateConfidence(List<EnrichedEntry> bucket) {
        if (bucket.isEmpty()) return "";
        boolean allFilename = bucket.stream().allMatch(e -> "filename".equals(e.dateSource()));
        boolean allMtime = bucket.stream().allMatch(e -> "mtime".equals(e.dateSource()));
        if (allFilename) return "filename";
        if (allMtime) return "mtime";
        return "mixed";
    }

    private static String csvEscape(String val) {
        if (val.contains(",") || val.contains("\"") || val.contains("\n")) {
            return "\"" + val.replace("\"", "\"\"") + "\"";
        }
        return val;
    }
}
```

- [ ] **Step 2: Write AvailabilityWriterTest**

Create `src/test/java/ug/ura/audit/report/AvailabilityWriterTest.java`:

```java
package ug.ura.audit.report;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ug.ura.audit.core.ClassifyResult;
import ug.ura.audit.core.DateResult;
import ug.ura.audit.scan.EnrichedEntry;
import ug.ura.audit.scan.FileEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AvailabilityWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void produces_full_grid_with_gaps() throws IOException {
        LocalDate start = LocalDate.of(2023, 4, 10);
        LocalDate end = LocalDate.of(2023, 4, 12);

        // One file on 2023-04-12, none on 10th or 11th
        FileEntry f = new FileEntry("/airtel_mobile_money/txn_20230412.csv", "txn_20230412.csv",
                1024, Instant.parse("2023-04-12T10:00:00Z"), "/airtel_mobile_money", null);
        EnrichedEntry e = new EnrichedEntry(f,
                new ClassifyResult("momo_transactions", "Mobile Money Transactions", "high"),
                DateResult.fromFilename(LocalDate.of(2023, 4, 12)));

        Path out = tempDir.resolve("availability.csv");
        AvailabilityWriter.write(out, List.of(e), "RUN1", start, end, List.of("momo_transactions"));

        List<String> lines = Files.readAllLines(out);
        assertThat(lines).hasSize(4); // header + 3 days
        assertThat(lines.get(0)).startsWith("run_id,source_key");

        // 2023-04-10: zero files
        assertThat(lines.get(1)).contains("2023-04-10,0,0");
        // 2023-04-12: one file
        assertThat(lines.get(3)).contains("2023-04-12,1,1024");
    }

    @Test
    void empty_entries_produces_all_zero_grid() throws IOException {
        LocalDate start = LocalDate.of(2023, 4, 10);
        LocalDate end = LocalDate.of(2023, 4, 11);

        Path out = tempDir.resolve("availability.csv");
        AvailabilityWriter.write(out, List.of(), "RUN1", start, end, List.of("momo_transactions"));

        List<String> lines = Files.readAllLines(out);
        assertThat(lines).hasSize(3); // header + 2 days
        // Both days should have 0 files
        assertThat(lines.get(1)).contains(",0,0,");
        assertThat(lines.get(2)).contains(",0,0,");
    }
}
```

- [ ] **Step 3: Run tests**

```bash
mvn test
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat(report): availability CSV writer with full source×date grid"
```

---

## Task 7: CLI entry point + pipeline wiring

**Files:**
- Modify: `src/main/java/ug/ura/audit/Main.java`

Wire everything together: parse CLI args → load catalog → load date patterns → walk roots → classify + extract dates → write manifest → write availability CSV → organize tree.

- [ ] **Step 1: Implement Main.java**

```java
package ug.ura.audit;

import ug.ura.audit.core.*;
import ug.ura.audit.organize.TreeOrganizer;
import ug.ura.audit.report.AvailabilityWriter;
import ug.ura.audit.report.ManifestWriter;
import ug.ura.audit.scan.EnrichedEntry;
import ug.ura.audit.scan.FileEntry;
import ug.ura.audit.scan.FileWalker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException {
        // Parse arguments
        Config config = Config.parse(args);

        System.out.println("=== Airtel Data Availability Audit ===");
        System.out.println("Catalog:   " + config.catalogPath);
        System.out.println("Patterns:  " + config.datePatternsPath);
        System.out.println("Roots:     " + config.roots.size());
        System.out.println("Window:    " + config.windowStart + " → " + config.windowEnd);
        System.out.println("Output:    " + config.outputDir);
        if (config.organizeRoot != null) {
            System.out.println("Organize:  " + config.organizeRoot);
        }
        System.out.println();

        // Load catalog and patterns
        List<CatalogEntry> catalog = CatalogLoader.load(config.catalogPath);
        Classifier classifier = new Classifier(catalog);
        DateExtractor dateExtractor = DateExtractor.fromYaml(config.datePatternsPath);

        System.out.println("Loaded " + catalog.size() + " catalog entries");

        // Walk roots
        FileWalker walker = new FileWalker();
        List<FileEntry> files = walker.walkAll(config.roots);
        System.out.println("Found " + files.size() + " files");

        // Classify + extract dates
        List<EnrichedEntry> enriched = new ArrayList<>();
        for (FileEntry f : files) {
            ClassifyResult cr = classifier.classify(f.fileName(), f.path());
            DateResult dr = dateExtractor.extract(f.fileName());
            // If no date from filename, fall back to mtime
            if (dr.date() == null && f.mtime() != null) {
                LocalDate mtimeDate = f.mtime().atOffset(ZoneOffset.UTC).toLocalDate();
                dr = new DateResult(mtimeDate, "mtime");
            }
            enriched.add(new EnrichedEntry(f, cr, dr));
        }

        // Stats
        long classified = enriched.stream().filter(e -> e.classification().matched()).count();
        long unclassified = enriched.size() - classified;
        System.out.printf("Classified: %d  Unclassified: %d  (%.1f%%)%n",
                classified, unclassified,
                enriched.isEmpty() ? 0 : (classified * 100.0 / enriched.size()));

        // Filter to requested sources (if --source specified)
        List<EnrichedEntry> filtered = enriched;
        List<String> sourceKeys;
        if (!config.sourceFilter.isEmpty()) {
            filtered = enriched.stream()
                    .filter(e -> config.sourceFilter.contains(e.sourceKey()) || !e.classification().matched())
                    .toList();
            sourceKeys = config.sourceFilter;
        } else {
            sourceKeys = catalog.stream().map(CatalogEntry::key).toList();
        }

        // Create output directory
        String runId = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss'Z'")
                .format(Instant.now().atOffset(ZoneOffset.UTC));
        Path runDir = config.outputDir.resolve("run_" + runId);
        Files.createDirectories(runDir);

        // Write manifest
        Path manifestPath = runDir.resolve("manifest.tsv");
        ManifestWriter.write(manifestPath, enriched);
        System.out.println("Wrote manifest: " + manifestPath);

        // Write availability CSV
        Path availPath = runDir.resolve("availability.csv");
        AvailabilityWriter.write(availPath, filtered, runId, config.windowStart, config.windowEnd, sourceKeys);
        System.out.println("Wrote availability: " + availPath);

        // Organize tree (if requested)
        if (config.organizeRoot != null) {
            TreeOrganizer organizer = new TreeOrganizer(config.organizeRoot);
            int copied = organizer.organize(filtered);
            System.out.println("Organized " + copied + " files into " + config.organizeRoot);
        }

        System.out.println("\n=== Done ===");
    }

    /** Simple CLI argument parsing — no external dependency. */
    static class Config {
        Path catalogPath = Path.of("config/catalog.yaml");
        Path datePatternsPath = Path.of("config/date_patterns.yaml");
        List<FileWalker.RootConfig> roots = new ArrayList<>();
        LocalDate windowStart = LocalDate.now().minusYears(5);
        LocalDate windowEnd = LocalDate.now();
        Path outputDir = Path.of("out");
        Path organizeRoot = null;  // null means don't organize
        List<String> sourceFilter = List.of(); // empty means all sources

        static Config parse(String[] args) {
            Config c = new Config();
            int i = 0;
            while (i < args.length) {
                switch (args[i]) {
                    case "--catalog" -> c.catalogPath = Path.of(args[++i]);
                    case "--patterns" -> c.datePatternsPath = Path.of(args[++i]);
                    case "--root" -> {
                        // --root /path:tag
                        String[] parts = args[++i].split(":", 2);
                        c.roots.add(new FileWalker.RootConfig(Path.of(parts[0]),
                                parts.length > 1 ? parts[1] : parts[0]));
                    }
                    case "--window-start" -> c.windowStart = LocalDate.parse(args[++i]);
                    case "--window-end" -> c.windowEnd = LocalDate.parse(args[++i]);
                    case "--output" -> c.outputDir = Path.of(args[++i]);
                    case "--organize" -> c.organizeRoot = Path.of(args[++i]);
                    case "--source" -> {
                        List<String> sources = new ArrayList<>(c.sourceFilter);
                        sources.add(args[++i]);
                        c.sourceFilter = sources;
                    }
                    default -> {
                        System.err.println("Unknown argument: " + args[i]);
                        printUsage();
                        System.exit(1);
                    }
                }
                i++;
            }
            if (c.roots.isEmpty()) {
                System.err.println("ERROR: At least one --root is required");
                printUsage();
                System.exit(1);
            }
            return c;
        }

        static void printUsage() {
            System.err.println("""
                    Usage: java -jar airtel-availability.jar [options]
                    
                    Required:
                      --root <path>:<tag>    Filesystem root to scan (repeatable)
                    
                    Optional:
                      --catalog <path>       Path to catalog.yaml (default: config/catalog.yaml)
                      --patterns <path>      Path to date_patterns.yaml (default: config/date_patterns.yaml)
                      --window-start <date>  Audit window start (default: 5 years ago)
                      --window-end <date>    Audit window end (default: today)
                      --output <dir>         Output directory (default: out/)
                      --organize <dir>       Copy files into <dir>/<source>/<date>/ tree
                      --source <key>         Filter to this source only (repeatable)
                    
                    Example (MoMo Transactions only):
                      java -jar airtel-availability.jar \\
                        --root /airtel_mobile_money:/airtel_mobile_money \\
                        --source momo_transactions \\
                        --organize /data/organized \\
                        --output /data/reports
                    """);
        }
    }
}
```

- [ ] **Step 2: Verify build + package**

```bash
mvn clean package -DskipTests
```

Expected: `BUILD SUCCESS`, fat JAR at `target/airtel-availability-1.0-SNAPSHOT.jar`.

- [ ] **Step 3: Local smoke test with a temp fixture**

```bash
# Create a tiny local fixture
mkdir -p /tmp/airtel_test/momo/2023
echo "test data" > /tmp/airtel_test/momo/2023/txn_20230412.csv
echo "test data" > /tmp/airtel_test/momo/2023/txn_20230413.csv
echo "test data" > /tmp/airtel_test/momo/bal_20230412.csv

java -jar target/airtel-availability-1.0-SNAPSHOT.jar \
  --root /tmp/airtel_test/momo:/airtel_mobile_money \
  --source momo_transactions \
  --organize /tmp/airtel_test/organized \
  --output /tmp/airtel_test/out \
  --window-start 2023-04-01 \
  --window-end 2023-04-30
```

Expected output:
```
=== Airtel Data Availability Audit ===
Found 3 files
Classified: 2  Unclassified: 1
Wrote manifest: /tmp/airtel_test/out/run_.../manifest.tsv
Wrote availability: /tmp/airtel_test/out/run_.../availability.csv
Organized 2 files into /tmp/airtel_test/organized

=== Done ===
```

Verify:
```bash
ls /tmp/airtel_test/organized/momo_transactions/2023-04-12/
# Should contain: txn_20230412.csv
ls /tmp/airtel_test/organized/momo_transactions/2023-04-13/
# Should contain: txn_20230413.csv
```

- [ ] **Step 4: Run full test suite**

```bash
mvn test
```

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: CLI entry point wiring walk → classify → report → organize pipeline"
```

---

## Task 8: MoMo Transactions — remote recon + real data run

**This is the first source-by-source task. It requires SSH access to `192.168.30.24`.**

This task is **manual/interactive** — it cannot be fully automated because it depends on:
1. SSH connectivity to the target host
2. Actual file listings that may reveal new patterns needing catalog updates
3. Deploying the JAR to the target and running it there

- [ ] **Step 1: SSH recon — list MoMo directory structure**

```bash
ssh gamma@192.168.30.24 "find /airtel_mobile_money -maxdepth 3 -type d | head -50"
ssh gamma@192.168.30.24 "find /airtel_mobile_money -type f | head -100"
ssh gamma@192.168.30.24 "find /airtel_mobile_money -type f -name '*txn*' -o -name '*trans*' | head -50"
```

Review output. Note:
- What subdirectory structure exists (by year? by month? flat?)
- Actual filenames — do they match the catalog patterns?
- Any filename formats not covered by current `txn|trans|transactions` patterns

- [ ] **Step 2: Update catalog if needed**

If recon reveals patterns not in `catalog.yaml`, update the `momo_transactions` entry:
- Add new regex patterns
- Add test cases for the new filenames found
- Run `mvn test` to verify catalog tests still pass

- [ ] **Step 3: Build fat JAR**

```bash
mvn clean package
```

- [ ] **Step 4: Deploy to target**

```bash
scp target/airtel-availability-1.0-SNAPSHOT.jar gamma@192.168.30.24:/tmp/
scp -r config/ gamma@192.168.30.24:/tmp/airtel-audit-config/
```

- [ ] **Step 5: Run on target — MoMo Transactions only**

```bash
ssh gamma@192.168.30.24 "java -jar /tmp/airtel-availability-1.0-SNAPSHOT.jar \
  --root /airtel_mobile_money:/airtel_mobile_money \
  --catalog /tmp/airtel-audit-config/catalog.yaml \
  --patterns /tmp/airtel-audit-config/date_patterns.yaml \
  --source momo_transactions \
  --organize /tmp/airtel_organized \
  --output /tmp/airtel_reports \
  --window-start 2021-04-15 \
  --window-end 2026-04-15"
```

- [ ] **Step 6: Review results**

```bash
# Pull reports back to local
scp -r gamma@192.168.30.24:/tmp/airtel_reports/run_* out/

# Check classification rate
head -5 out/run_*/manifest.tsv

# Check availability grid
head -20 out/run_*/availability.csv

# Check organized tree
ssh gamma@192.168.30.24 "find /tmp/airtel_organized/momo_transactions -type d | head -30"
ssh gamma@192.168.30.24 "find /tmp/airtel_organized/momo_transactions -type f | wc -l"
```

Review:
- Classification rate — are most MoMo files classified?
- Unclassified files — do we need new patterns?
- Organized tree — does `<root>/momo_transactions/<date>/` look correct?
- Date coverage — any gaps in the 5-year window?

- [ ] **Step 7: Commit any catalog refinements**

```bash
git add config/catalog.yaml
git commit -m "refine(catalog): update momo_transactions patterns from real data recon"
```

---

## Adding more sources (template for future tasks)

Each subsequent source follows the same pattern as Task 8:

```markdown
## Task N: [Source Name] — recon + run

- [ ] Step 1: SSH recon — list files matching this source's expected roots/patterns
- [ ] Step 2: Update catalog.yaml if new patterns discovered, add test cases, run `mvn test`
- [ ] Step 3: Build + deploy JAR (if code changed)
- [ ] Step 4: Run on target with `--source <key>` and `--organize`
- [ ] Step 5: Review results (classification rate, gaps, organized tree)
- [ ] Step 6: Commit catalog refinements
```

**Source order (suggested, based on data organization):**
1. `momo_transactions` (Task 8) — `/airtel_mobile_money` root, "more better organized"
2. `momo_balance_snapshot` — same root, different pattern
3. `air_recharge` — `/master_data1`, `/master_data2`, `cbs_cdr_rch`
4. `air_subscription` — same roots, `cbs_cdr_sub`
5. `air_adjustment` — same roots, `cbs_cdr_adj`
6. `me2u_transfer` — same roots, `cbs_cdr_trf`
7. `sdp` — same roots, `cbs_cdr_sdp`
8. `vms_physical`, `vms_electronic` — `vou` patterns
9. `in_voice`, `in_sms`, `in_gprs` — CBS/OCS CDR patterns
10. `vas_other`, `msc`, `hlr` — misc
11. `dealer_management`, `bank_momo_float` — misc
12. `postpaid_billing`, `postpaid_payments` — postpaid

Each source task takes 15-30 minutes once the infrastructure is in place.

---

## Deferred items (add as needed after first source is running)

These are in the file structure but intentionally deferred until the first end-to-end source works:

- **ArchiveEnumerator** (spec §4.4) — enumerate tar/tar.gz entries as FileEntry. Add when a source's recon reveals significant tarball content. Task 3's FileWalker already handles plain files and .gz.
- **UnclassifiedWriter** (spec §6.3) — aggregate unmatched files by signature. Useful after running all sources; not needed per-source.
- **SummaryWriter** (spec §6.3) — summary.xlsx with overview + per-source heatmap sheets. Add after multiple sources have data.
- **Reconciliation asserts** (spec §7.4) — post-run sanity checks. Add once the pipeline is stable.

---

## Summary

| Task | What | Depends on |
|------|------|-----------|
| 0 | Java project scaffold | — |
| 1 | Catalog loader + classifier | 0 |
| 2 | Date extractor | 0 |
| 3 | File walker | 0 |
| 4 | Manifest writer | 1, 2, 3 |
| 5 | Tree organizer | 1, 2, 3 |
| 6 | Availability report writer | 4 |
| 7 | CLI entry point + pipeline | 1–6 |
| 8 | MoMo Transactions (real data) | 7 + SSH access |
| 9+ | Remaining 18 sources | 7 + SSH access |
