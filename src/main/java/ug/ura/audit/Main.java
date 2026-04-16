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
        Config config = Config.parse(args);

        System.out.println("=== Airtel Data Availability Audit ===");
        System.out.println("Catalog:   " + config.catalogPath);
        System.out.println("Patterns:  " + config.datePatternsPath);
        System.out.println("Roots:     " + config.roots.size());
        System.out.println("Window:    " + config.windowStart + " to " + config.windowEnd);
        System.out.println("Output:    " + config.outputDir);
        if (config.organizeRoot != null) {
            System.out.println("Organize:  " + config.organizeRoot);
        }
        if (!config.sourceFilter.isEmpty()) {
            System.out.println("Sources:   " + config.sourceFilter);
        }
        System.out.println();

        // Load catalog and patterns
        List<CatalogEntry> catalog = CatalogLoader.load(config.catalogPath);
        Classifier classifier = new Classifier(catalog);
        DateExtractor dateExtractor = DateExtractor.fromYaml(config.datePatternsPath);
        long activeCount = catalog.stream().filter(CatalogEntry::active).count();
        System.out.println("Loaded " + catalog.size() + " catalog entries (" + activeCount + " active)");

        // Walk roots
        FileWalker walker = new FileWalker();
        List<FileEntry> files = walker.walkAll(config.roots);
        System.out.println("Found " + files.size() + " files");

        // Classify + extract dates
        List<EnrichedEntry> enriched = new ArrayList<>();
        for (FileEntry f : files) {
            ClassifyResult cr = classifier.classify(f.fileName(), f.path(), f.origin());
            DateResult dr = dateExtractor.extract(f.fileName());
            if (dr.date() == null && f.mtime() != null) {
                LocalDate mtimeDate = f.mtime().atOffset(ZoneOffset.UTC).toLocalDate();
                dr = new DateResult(mtimeDate, "mtime");
            }
            enriched.add(new EnrichedEntry(f, cr, dr));
        }

        long classified = enriched.stream().filter(e -> e.classification().matched()).count();
        long unclassified = enriched.size() - classified;
        System.out.printf("Classified: %d  Unclassified: %d  (%.1f%%)%n",
                classified, unclassified,
                enriched.isEmpty() ? 0 : (classified * 100.0 / enriched.size()));

        // Filter to requested sources (explicit --source) or active sources (default)
        List<EnrichedEntry> filtered = enriched;
        List<String> sourceKeys;
        if (!config.sourceFilter.isEmpty()) {
            filtered = enriched.stream()
                    .filter(e -> config.sourceFilter.contains(e.sourceKey()) || !e.classification().matched())
                    .toList();
            sourceKeys = config.sourceFilter;
        } else {
            sourceKeys = catalog.stream().filter(CatalogEntry::active).map(CatalogEntry::key).toList();
            filtered = enriched.stream()
                    .filter(e -> sourceKeys.contains(e.sourceKey()) || !e.classification().matched())
                    .toList();
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

        // Organize tree
        if (config.organizeRoot != null) {
            TreeOrganizer organizer = new TreeOrganizer(config.organizeRoot);
            int copied = organizer.organize(filtered);
            System.out.println("Organized " + copied + " files into " + config.organizeRoot);
        }

        System.out.println("\n=== Done ===");
    }

    static class Config {
        Path catalogPath = Path.of("config/catalog.yaml");
        Path datePatternsPath = Path.of("config/date_patterns.yaml");
        List<FileWalker.RootConfig> roots = new ArrayList<>();
        LocalDate windowStart = LocalDate.now().minusYears(5);
        LocalDate windowEnd = LocalDate.now();
        Path outputDir = Path.of("out");
        Path organizeRoot = null;
        List<String> sourceFilter = List.of();

        static Config parse(String[] args) {
            Config c = new Config();
            int i = 0;
            while (i < args.length) {
                switch (args[i]) {
                    case "--catalog" -> c.catalogPath = Path.of(args[++i]);
                    case "--patterns" -> c.datePatternsPath = Path.of(args[++i]);
                    case "--root" -> {
                        String rootArg = args[++i];
                        // On Windows/Git-Bash the shell converts path1:tag into path1;tag (semicolon).
                        // On Unix the separator is colon, but we must skip Windows drive-letter colons.
                        String rootPath;
                        String rootTag;
                        int semi = rootArg.indexOf(';');
                        if (semi >= 0) {
                            // Git Bash converted "path:tag" → "path;tag"
                            rootPath = rootArg.substring(0, semi);
                            rootTag  = rootArg.substring(semi + 1);
                        } else {
                            // Unix: split on last colon, skipping any drive-letter colon (len==1 before colon)
                            int lastColon = rootArg.lastIndexOf(':');
                            if (lastColon > 1) {
                                rootPath = rootArg.substring(0, lastColon);
                                rootTag  = rootArg.substring(lastColon + 1);
                            } else {
                                rootPath = rootArg;
                                rootTag  = rootArg;
                            }
                        }
                        c.roots.add(new FileWalker.RootConfig(Path.of(rootPath), rootTag));
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
