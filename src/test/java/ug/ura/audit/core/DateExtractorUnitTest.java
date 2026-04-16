package ug.ura.audit.core;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

class DateExtractorUnitTest {

    private static DateExtractor extractor;

    @BeforeAll
    static void loadExtractor() throws Exception {
        extractor = DateExtractor.fromYaml(Path.of("config", "date_patterns.yaml"));
    }

    @Test
    void invalid_calendar_date_skipped() {
        // Feb 30 does not exist — should fall through all patterns and return NO_MATCH
        DateResult result = extractor.extract("report_20230230.csv");
        assertThat(result.date()).isNull();
    }

    @Test
    void gz_suffix_stripped() {
        DateResult result = extractor.extract("txn_20230412.csv.gz");
        assertThat(result.date()).isEqualTo(LocalDate.of(2023, 4, 12));
        assertThat(result.dateSource()).isEqualTo("filename");
    }

    @Test
    void no_date_returns_mtime_source() {
        DateResult result = extractor.extract("random_file.csv");
        assertThat(result.date()).isNull();
        assertThat(result.dateSource()).isEqualTo("mtime");
    }

    @Test
    void multiple_dates_first_wins() {
        // Two valid dates in the filename — the first one found should win
        DateResult result = extractor.extract("txn_20230412_20230413.csv");
        assertThat(result.date()).isEqualTo(LocalDate.of(2023, 4, 12));
    }
}
