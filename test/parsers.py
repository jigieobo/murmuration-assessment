"""Source-specific parsers for the elected officials data acquisition pipeline.

Each parser takes raw fetched content (a string) and returns a flat list of
dictionaries that match the validation contract described in the design
document:

    {
        "county_fips":   str,  # 5-digit FIPS, must exist in `counties`
        "full_name":     str,  # non-empty
        "office_title":  str,  # raw title as it appears in the source
        "party":         str,  # canonicalized party label
        "source_url":    str,
        "storage_path":  str,
    }
"""

import json
from typing import Dict, List, Optional


# Mapping from NC SBOE raw party codes to canonical party labels.
# NC publishes results using these short codes; we expand them so downstream
# entities (officials, office_holders) store consistent values across sources.
_PARTY_MAP = {
    "REP": "Republican",
    "DEM": "Democratic",
    "LIB": "Libertarian",
    "GRE": "Green",
    "UNA": "Unaffiliated",
}


def parse_nc_sboe(
    raw: str,
    source_url: str = "https://er.ncsbe.gov/...",
    storage_path: str = "raw/state_portals/nc_sboe/2026-04-27T14:00Z.json",
) -> List[Dict[str, Optional[str]]]:
    """Parse a North Carolina State Board of Elections JSON payload.

    Filters out non-elected entries (losing candidates) and reshapes each
    winner into the canonical flat dict expected by the reconciliation step.

    Args:
        raw: The JSON string fetched from the NC SBOE endpoint.
        source_url: The URL the payload was fetched from. Attached to every
            output record so reconciliation can trace it back.
        storage_path: The data lake path where this raw payload was written.
            Attached to every output record for the same reason.

    Returns:
        A list of flat dictionaries, one per elected official.
    """
    data = json.loads(raw)
    entries = data.get("results", [])

    officials = []
    for entry in entries:
        # Skip losing candidates - we only record elected winners
        if not entry.get("elected", False):
            continue

        candidate = entry.get("candidate", {})
        raw_party = candidate.get("party")
        canonical_party = _PARTY_MAP.get(raw_party, raw_party)

        officials.append({
            "county_fips":  entry.get("county_fips"),
            "full_name":    (candidate.get("name") or "").strip(),
            "office_title": entry.get("office"),
            "party":        canonical_party,
            "source_url":   source_url,
            "storage_path": storage_path,
        })

    return officials
