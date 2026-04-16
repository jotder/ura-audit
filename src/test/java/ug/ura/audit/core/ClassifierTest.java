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
    static void setUp() throws Exception {
        try (Reader reader = new InputStreamReader(
                ClassifierTest.class.getResourceAsStream("/fixtures/tiny_catalog.yaml"))) {
            List<CatalogEntry> catalog = CatalogLoader.load(reader);
            classifier = new Classifier(catalog);
        }
    }

    @Test
    void momo_txn_under_correct_path_matches() {
        ClassifyResult result = classifier.classify(
                "txn_20230412.csv",
                "/airtel_mobile_money/2023/txn_20230412.csv");
        assertThat(result.sourceKey()).isEqualTo("momo_transactions");
        assertThat(result.confidence()).isEqualTo("high");
    }

    @Test
    void momo_txn_under_wrong_path_no_match() {
        ClassifyResult result = classifier.classify(
                "txn_20230412.csv",
                "/master_data1/txn_20230412.csv");
        assertThat(result.matched()).isFalse();
    }

    @Test
    void recharge_matches_without_path_hint() {
        ClassifyResult result = classifier.classify(
                "cbs_cdr_rch_20230412.add",
                "/some/arbitrary/path");
        assertThat(result.sourceKey()).isEqualTo("air_recharge");
    }

    @Test
    void no_match_returns_empty() {
        ClassifyResult result = classifier.classify(
                "random_report.csv",
                "/some/path");
        assertThat(result.matched()).isFalse();
        assertThat(result.sourceKey()).isEmpty();
    }

    @Test
    void gz_suffix_stripped_before_matching() {
        ClassifyResult result = classifier.classify(
                "cbs_cdr_rch_20230412.add.gz",
                "/some/path");
        assertThat(result.sourceKey()).isEqualTo("air_recharge");
    }
}
