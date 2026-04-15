"""Layer 1 tests — runs every `tests:` case embedded in the real catalog.yaml."""
from pathlib import Path

import pytest
import yaml

from airtel_availability.classifier import Classifier


CATALOG_PATH = Path("config/catalog.yaml")


def _load_cases():
    data = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    cases = []
    for entry in data:
        for case in entry.get("tests", []):
            cases.append(
                pytest.param(
                    case["name"],
                    case.get("path", f"/master_data1/synthetic/{case['name']}"),
                    case["expect"],
                    id=f"{entry['key']}::{case['name']}",
                )
            )
    return cases


@pytest.fixture(scope="module")
def classifier():
    return Classifier.from_yaml(CATALOG_PATH)


@pytest.mark.parametrize("name,path,expected_key", _load_cases())
def test_catalog_entry(classifier, name, path, expected_key):
    result = classifier.classify(name, path)
    assert result.source_key == expected_key, (
        f"expected {expected_key!r}, got {result.source_key!r} (confidence={result.confidence})"
    )


def test_at_least_nineteen_sources():
    data = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    assert len(data) == 19, f"spec requires 19 sources, catalog has {len(data)}"


def test_every_source_has_tests():
    data = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    for entry in data:
        assert entry.get("tests"), f"catalog entry {entry['key']} has no tests:"
        assert len(entry["tests"]) >= 3, (
            f"catalog entry {entry['key']} has {len(entry['tests'])} tests; require >=3"
        )
