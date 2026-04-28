"""Parser unit tests.

Run from the parser_unit_test/ directory:

    pytest

Or with verbose output:

    pytest -v
"""

import os
import sys

# Make parsers.py importable regardless of where pytest is invoked from.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parsers import parse_nc_sboe


def test_parse_nc_sboe_returns_officials():
    raw = open("tests/fixtures/nc_sboe_sample.json").read()
    results = parse_nc_sboe(raw)
    assert len(results) > 0
    assert all(r["county_fips"] for r in results)
    assert all(r["full_name"].strip() for r in results)
