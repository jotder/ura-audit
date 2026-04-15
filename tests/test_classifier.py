"""Layer 1 unit tests — classifier engine with a small inline catalog."""
import pytest
from airtel_availability.classifier import Classifier, CatalogEntry

INLINE_CATALOG = [
    CatalogEntry(
        key="air_recharge",
        label="AIR Recharge",
        patterns=[r"(?i)(^|[/_-])rch([_.-]|$)", r"(?i)cbs_cdr_rch_"],
        path_hints=[],
    ),
    CatalogEntry(
        key="momo_transactions",
        label="MoMo Transactions",
        patterns=[r"(?i)txn", r"(?i)transactions"],
        path_hints=["/airtel_mobile_money"],
    ),
    CatalogEntry(
        key="in_voice",
        label="IN Voice",
        patterns=[r"(?i)(^|[/_-])voi([_.-]|$)", r"(?i)cbs_cdr_voi_"],
        path_hints=[],
    ),
]


@pytest.fixture
def classifier():
    return Classifier(INLINE_CATALOG)


def test_exact_rch_match(classifier):
    r = classifier.classify("cbs_cdr_rch_20230412.add", "/master_data1/x/y/cbs_cdr_rch_20230412.add")
    assert r.source_key == "air_recharge"
    assert r.confidence == "high"


def test_path_hint_required_for_momo(classifier):
    # 'txn' pattern matches, but path hint requires /airtel_mobile_money
    r1 = classifier.classify("txn_20230412.csv", "/master_data1/x/txn_20230412.csv")
    assert r1.source_key == ""      # path hint not satisfied → no match
    r2 = classifier.classify("txn_20230412.csv", "/airtel_mobile_money/2023/txn_20230412.csv")
    assert r2.source_key == "momo_transactions"


def test_no_match_returns_empty(classifier):
    r = classifier.classify("random_file_xyz.csv", "/master_data1/random_file_xyz.csv")
    assert r.source_key == ""
    assert r.confidence == "unknown"


def test_ambiguous_first_wins(classifier):
    # Contrived: a filename containing both 'rch' and 'voi' tokens
    r = classifier.classify("rch_voi_bundle.csv", "/master_data1/rch_voi_bundle.csv")
    assert r.source_key == "air_recharge"        # first in catalog wins
    assert r.confidence == "ambiguous"


def test_case_insensitive(classifier):
    r = classifier.classify("CBS_CDR_RCH_20230412.ADD", "/master_data1/CBS_CDR_RCH_20230412.ADD")
    assert r.source_key == "air_recharge"
