# Elected Officials Data Acquisition — Rhode Island MVP
**Murmuration Technical Analyst Assessment — Part 2**

A single-file Python pipeline that pulls Rhode Island (RI) municipal (muni) elected
officials from the RI Secretary of State (Tier 1), cross-check coverage
against the RI Open Data Portal (Tier 2), and writes a **SQLite database**
(`ri_officials.db`) that follows the Part 1 data model.


## How to run

```bash
pip install -r requirements.txt
python ri_officials_mvp.py self-test   # offline smoke + behavior demo
python ri_officials_mvp.py run         # end-to-end against live URLs
```

`run` produces **`ri_officials.db`** in the working directory. Inspect
with `sqlite3 ri_officials.db` or any DB browser.

---

## What the pipeline does

Five stages mirroring Section 3 of the Part 1 design doc:

```
fetch reference (Census API)  ──►  seed counties + municipalities
                                          │
fetch officials (RI SOS, per-muni loop)  ─► data lake + collection_log (hash, path)
                                          │
parse winners (RI SOS JSON shape)
                                          │
validate per-record  ─►  pass: reconcile (fuzzy)
                       └─ fail: needs_review queue
                                          │
post-load checks (gap rate + staleness)
                                          │
cross-check vs Tier 2 (data.ri.gov XLSX, 39-muni reference)
```

### Where it acquires data

Reference data (counties + munis) comes from the Census Bureau's 2020
PL94-171 endpoint, in a loop:

```python
#Python
CENSUS_COUSUB_URL = (
    f"https://api.census.gov/data/2020/dec/pl"
    f"?get=NAME,P1_001N&for=county%20subdivision:*&in=state:{STATE_FIPS}"
)

def seed_reference_from_census(conn, ...) -> dict[str, int]:
    counties = fetch_census_counties()
    for c in counties:
        conn.execute("INSERT OR IGNORE INTO counties (...) VALUES (...)", (...))
    munis = fetch_census_municipalities()
    for m in munis:
        conn.execute("INSERT OR IGNORE INTO municipalities (...) VALUES (...)", (...))
```

Officeholders come from the **RI Secretary of State Elections Division**,
which publishes one JSON file per municipality on S3. The pipeline runs
once and loops internally over every seeded muni:

```python
#Python
def fetch_ri_sos_all_munis(conn, source_id, base_url):
    """Loop over every seeded RI municipality and fetch its results JSON
    into the data lake. Per-muni URL = base_url/<slug>.json."""
    munis = [r[0] for r in conn.execute(
        "SELECT muni_name FROM municipalities WHERE state_fips = ?", (STATE_FIPS,))]
    for muni in munis:
        slug = _to_url_slug(muni)                  # "East Greenwich" -> "east_greenwich"
        url  = f"{base_url}/{slug}.json"
        path, log_id = fetch_to_data_lake(url, TIER1_SLUG, conn, source_id)
        # Each fetch hashes the bytes (SHA-256), writes to data/raw/ri_sos/<UTC>.json,
        # and inserts a collection_log row. Identical re-fetches are detected by
        # hash and skipped without re-parsing.
```

### Where it transforms

The parser pulls winners out of the real RI SOS contest shape — filters
non-municipal contests, ranks candidates by votes, drops write-ins,
strips party-code prefixes from displayed names:

```python
#Python
def parse_ri_sos_winners(path, *, source_url=None):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    election_date = data.get("election_date")          # carries to every record

    out = []
    for contest in data.get("contests", []):
        office_title, muni_name = _split_municipal_contest(contest["name"])
        if not office_title:        # state, federal, or ballot-question -> drop
            continue
        seats = int(contest.get("votes_allowed", "1"))
        ranked = sorted(
            ((int(c["votes"]), c) for c in contest["candidates"]
             if not _is_write_in(c.get("name"))),
            key=lambda t: -t[0],
        )
        for _votes, cand in ranked[:seats]:           # top-N winners
            out.append({
                "muni_name":     muni_name,
                "full_name":     _strip_party_prefix(cand["name"], cand["party_code"]),
                "office_title":  office_title,
                "party":         _canonical_party(cand["party_code"]),
                "election_date": election_date,
                ...
            })
    return out
```

The reconciler does the four moves from the design doc — validate,
fuzzy-map title to canonical office_type, fuzzy-match person within
muni, run the term-lifecycle decision:

```python
#Python
def reconcile_record(record, conn, *, source_id, term_start):
    ok, reason, muni_id = validate_record(record, conn)
    if not ok:
        write_to_needs_review(conn, record, reason, source_id)
        return "needs_review"

    # Fuzzy title match against (canonical, synonym) pairs in office_type_synonyms.
    ot_id, canonical, score = fuzzy_canonical_office_type(record["office_title"], conn)
    if ot_id is None:
        write_to_needs_review(conn, record,
            f"no canonical office_type (best score {score:.0f} < {FUZZY_TITLE_THRESHOLD})",
            source_id)
        return "needs_review"

    office_id   = _find_or_create_office(conn, muni_id=muni_id, ...)
    official_id = (fuzzy_match_official_within_muni(record["full_name"], muni_id, conn)
                   or _create_new_official(...))

    # Term lifecycle: single-seat closes-and-opens on person change;
    # multi-seat keeps a current row per (office, person).
    ...
```

### What it outputs

A **SQLite database** at `ri_officials.db` (override with `--db-path`)
with these tables:

```
states                counties              municipalities
office_types          office_type_synonyms  offices
officials             office_holders
sources               collection_log        needs_review
contact_info          addresses             (defined; empty in MVP)
```

Schema follows Part 1 with one documented deviation: a `municipalities`
table parallel to `counties`, and `offices.county_fips` XOR
`offices.muni_id` enforced by a CHECK constraint. Rhode Island has no elected
county government, so the operative unit of local government is the
city/town and that's where every `office_holder` row hangs from.

### Basic validation

Two layers, mirroring Section 4 of the design doc.

**Per-record contract** — runs between the parser and the reconciler.
Failed records persist in `needs_review` with the original payload +
reason + lake `storage_path`:

```python
#Python
REQUIRED_FIELDS = ("muni_name", "full_name", "office_title")

def validate_record(record, conn):
    for f in REQUIRED_FIELDS:
        if not record.get(f) or not str(record[f]).strip():
            return False, f"missing or empty: {f}", None
    row = conn.execute(
        "SELECT muni_id FROM municipalities "
        "WHERE state_fips = ? AND LOWER(muni_name) = LOWER(?)",
        (STATE_FIPS, record["muni_name"].strip()),
    ).fetchone()
    if not row:
        return False, f"unknown municipality: {record['muni_name']!r}", None
    return True, None, row[0]
```

**Post-load sanity checks** — run after every reconcile sweep. The
design's two named v1 checks: gap rate and staleness:

```python
#Python
def check_gap_rate_munis(conn):
    """Fraction of RI munis with zero current officeholders.
    >30% empty -> pipeline broken (parser silently dropped rows, etc.)."""
    total = conn.execute("SELECT COUNT(*) FROM municipalities WHERE state_fips=?",
                         (STATE_FIPS,)).fetchone()[0]
    with_holders = conn.execute("""
        SELECT COUNT(DISTINCT m.muni_id) FROM municipalities m
        JOIN offices o ON o.muni_id = m.muni_id
        JOIN office_holders oh ON oh.office_id = o.office_id AND oh.is_current=1
        WHERE m.state_fips = ?""", (STATE_FIPS,)).fetchone()[0]
    gap = (total - with_holders) / total
    return {"name": "gap_rate_munis", "passed": gap <= GAP_RATE_THRESHOLD,
            "metric": gap, ...}
```

**Tier 2 cross-check** — set-membership against `Gen24EX.xlsx` from
data.ri.gov (39 distinct munis after stripping precinct suffixes). If a
muni we loaded doesn't appear in the state's authoritative reference,
something's wrong with our Tier 1 ingest.

#### Sample `needs_review` entry

When a record fails per-record validation OR the fuzzy office-title
mapper can't find a canonical type with score ≥ 80, it lands here with
the original payload, the failure reason, and the lake `storage_path`
back to the artifact it came from. Example from a real run:

```
review_id      : 14
failure_reason : "no canonical office_type for 'Town Treasurer'
                  (best fuzzy score 51 < threshold 80)"
flagged_at     : 2026-04-30T13:42:09.117Z
source_id      : 1                                      # RI SOS, tier 1
storage_path   : data/raw/ri_sos/2026-04-30T13-41-58Z.json
raw_record     : {"muni_name":     "Scituate",
                  "full_name":     "Patrick A. Smith",
                  "office_title":  "Town Treasurer",
                  "party":         "Democratic",
                  "election_date": "November 05, 2024",
                  "source_url":    "https://rigov.s3.amazonaws.com/.../scituate.json",
                  "storage_path":  "data/raw/ri_sos/..."}
```

**Why it was flagged:** "Town Treasurer" is a real elected office in
several RI towns, but the bootstrap synonym list in
`OFFICE_TYPES_WITH_SYNONYMS` only seeds Mayor, Council, Clerk, School
Committee, Moderator, and Sergeant equivalents — there's no
`Local Treasurer` canonical type yet. The fuzzy matcher's best
`token_set_ratio` against any of the 41 seeded synonyms came in at 51,
well below the threshold. This is why it routes to `needs_review`, 
silently mapping a treasurer to the closest wrong canonical (`Local
Records Clerk`, say) would put a treasurer in a clerk-shaped row and
poison every downstream report.

---

## Sample queries

Both queries below ran against the database produced by `self-test`
(Providence + Newport + Middletown loaded from the inline fixture). On
a real run with all 39 munis fetched, the same queries return per-cycle
data for every county and town/city.

### Query 1 — everything we know about ONE COUNTY

Demonstrates that RI's `government_form='no_county_government'` flag is
visible in the data — every county-tier officeholder count is zero, and
local democracy lives in the muni rollup.

```sql
SELECT m.muni_name, m.muni_type, m.population,
       COUNT(DISTINCT o.office_id)        AS offices,
       COUNT(DISTINCT off.official_id)    AS officials,
       COUNT(DISTINCT CASE WHEN oh.is_current=1
                           THEN oh.holder_id END)  AS current_holders
FROM counties c
LEFT JOIN municipalities m  ON m.county_fips    = c.county_fips
LEFT JOIN offices        o  ON o.muni_id        = m.muni_id
LEFT JOIN office_holders oh ON oh.office_id     = o.office_id
LEFT JOIN officials     off ON off.official_id  = oh.official_id
WHERE c.county_fips = '44007'                 -- Providence County
GROUP BY m.muni_id
ORDER BY current_holders DESC, m.muni_name;
```

```
muni            type      pop     offices  officials  current
--------------- ------ -------    -------  ---------  -------
Providence      city   190934           2          4        4
Pawtucket       city    75604           0          0        0

county.government_form        = 'no_county_government'
county.has_elected_government = 0
```

### Query 2 — everything we know about ONE TOWN/CITY

Demonstrates cross-state title normalization — `local_title` keeps
"Mayor" and "City Council" verbatim while `canonical_office` would map a
"Town Council Member" or "County Commissioner" elsewhere to the same
`Local Legislative Member` row.

```sql
SELECT m.muni_name, m.muni_type, c.county_name AS in_county,
       ot.canonical_name AS canonical_office,
       o.local_title, o.district_or_seat AS seat,
       off.full_name AS holder, off.party,
       oh.term_start, oh.term_end, oh.is_current,
       oh.assumption_method AS via,
       s.source_name AS source, s.reliability_tier AS tier
FROM municipalities m
JOIN counties      c  ON c.county_fips     = m.county_fips
JOIN offices       o  ON o.muni_id         = m.muni_id
JOIN office_types  ot ON ot.office_type_id = o.office_type_id
JOIN office_holders oh ON oh.office_id     = o.office_id
JOIN officials    off ON off.official_id   = oh.official_id
JOIN sources       s  ON s.source_id       = oh.source_id
WHERE m.muni_name = 'Providence'
ORDER BY ot.canonical_name, oh.is_current DESC, off.full_name;
```

```
canonical_office          | local_title    | holder         | party       | term_start | current | tier
------------------------- | -------------- | -------------- | ----------- | ---------- | ------- | ----
Chief Local Executive     | Mayor          | Brett Smiley   | Democratic  | 2025-01-01 | YES     | 1
Local Legislative Member  | City Council   | First Council  | Democratic  | 2025-01-01 | YES     | 1
Local Legislative Member  | City Council   | Second Council | Democratic  | 2025-01-01 | YES     | 1
Local Legislative Member  | City Council   | Third Council  | Republican  | 2025-01-01 | YES     | 1
```

---

## Short written section

### Why Rhode Island

Rhode Island is the textbook case for the schema's `government_form='no_county_government'`
flag. Its 5 Census-defined counties (Bristol, Kent, Newport, Providence,
Washington) exist only as geographic and judicial-district designations
and there are no elected County Commissioners, no elected County
Executives, no elected Sheriffs (RI's "High Sheriffs" are appointed by
the Governor). Picking RI deliberately puts pressure on
the schema and forces the canonical `office_types` table to do the
cross-state normalization it was always designed to do: a "Town Council
Member" in Providence and a "County Commissioner" in Wake County, NC,
both map to the same canonical type.

### Sources used

**Tier 1 reference — U.S. Census Bureau (2020 PL94-171).** Counties and
municipalities are loop-fetched from `api.census.gov`, so the seed data
isn't hardcoded. RI's 5 counties come from the county endpoint; the 39
cities/towns come from the county-subdivision endpoint, which for RI
maps exactly to the operative local-government unit.

**Tier 1 officials — Rhode Island Secretary of State, Elections
Division.** Per-municipality JSON files served from Amazon S3 at
`https://rigov.s3.amazonaws.com/election/results/2024/general_election/<slug>.json`.
The script loops once over every seeded muni. Why Tier 1: it's the
authoritative source of certified election results, published by the
state itself.

**Tier 2 cross-check — RI Open Data Portal (`data.ri.gov`).** The
`Gen24EX.xlsx` summary export is bundled alongside the script. It
covers the same election from a different production pipeline at the
same source-of-truth tier as the SOS, so set-membership and (with more
time) per-(muni, contest) winner agreement are real cross-source
checks.

### What I'd do with another 4 hours

- **Richer Tier 2 cross-checks.** The XLSX's `Candidate_Breakout` sheet
  has ~21k rows of (precinct × contest × candidate × votes). For each
  loaded `office_holders` row, sum votes per (muni, contest, candidate)
  and assert the resulting top-`votes_allowed` matches what we loaded
  from Tier 1. Disagreements are the strongest production signal that
  the parser or reconciler is misbehaving.
- **Feedback Loop LLM `office_type_synonyms`.** Right now the
  synonym list is a hardcoded. The aligned v2 work (Section 5
  of the design doc) is a small tool that surfaces `needs_review`
  entries with the reconciler's best fuzzy guess + score, and on
  user's confirmation INSERTs new (canonical, synonym) rows. Every
  confirmation becomes a new `(raw_title, canonical_name)` training
  pair. Eventually, a classifier replaces the rules-based fuzzy match.
- **Term-lifecycle close-on-resignation via news monitoring.** The
  pipeline picks up mid-term changes only on the next scheduled run.
  News monitoring on a small set of feeds, routing "resigns" /
  "appointed" refers to a re-collection trigger for the affected
  jurisdiction would close that gap.
- **Real entity resolution.** The MVPs ' per-muni fuzzy-name match at
  threshold 0.92. With contextual features (same role,
  overlapping terms, same party) plus a human review queue for
  ambiguous matches, "Robert Smith" / "Bob Smith" wouldn't become two
  rows.
- **Parser robustness.** The parser is JSON-only. The reference
  implementation has a format-agnostic dispatcher (XLSX/CSV/JSON) with
  a header-alias registry. Turning that into a single file would
  protect against the format drift state SOS sites are known for.

### Where I'd be nervous in production

- **RI SOS URL stability.** State election sites move files between
  cycles. If the source URL changes, we would need to modify the data acquisition approach.
  works. 
- **Format drift on the SOS side.** The parser keys on a `(TOWN|CITY) OF`
  marker in contest names and a small set of party-code prefixes. A
  new cycle that renames "Town Clerk TOWN OF BRISTOL" to "TOWN CLERK -
  BRISTOL" would silently drop every clerk in the state.
- **Multi-seat misscounts on `votes_allowed`** The
  reconciler trusts `votes_allowed` to know how many winners to take.
  If the field is absent or wrong, a 5-seat Town Council loads only
  one row. The reference implies seat-count reference table would
  defuse this; the single-file MVP doesn't carry that yet.
- **Tier 2 reference quality.** If `Gen24EX.xlsx` goes stale or the
  portal moves the file, the cross-check silently downgrades to "0
  matched". Production should alert on a sudden spike in the unmatched
  rate.
- **Census API rate limits.** Without an API key, the Census API
  tolerates casual use but throttles aggressive automation. A
  production rollout to all 50 states would register a key.

---

## Schema deviations from Part 1

- Added `municipalities` table parallel to `counties` (state, name,
  type ∈ {city, town, …}, population, containing-county FK).
- Made `offices.county_fips` nullable, added `offices.muni_id`, with
  a `CHECK` constraint enforcing exactly one is set.

`contact_info` and `addresses` tables exist but are empty because Rhode Island SOS
election results don't carry contact data. Filling them needs a
secondary source per state (county directory scrape, RILA/NCACC
association rosters).

## Tests

```bash
pytest ri_officials_mvp.py     # one parser unit test
python ri_officials_mvp.py self-test    # end-to-end smoke + behavior demo
```

The unit test mirrors the `parse_nc_sboe_returns_officials` pattern
from Section 4 of the Part 1 design doc — same shape, same contract
assertions. The self-test uses Census fetch loop, fuzzy title
matcher, multi-seat top-N selection, party-prefix stripping, term
lifecycle close-and-open on single-seat, post-load sanity checks, and
Tier 2 cross-check against the bundled XLSX. Both run offline.

## AI usage note (per assignment policy)

I used Claude CoWork (Opus 4.7) as a coding partner. The AI hallucinated source links, but I manually verified the sources and had to modify the data acquisition phase in my Tier 2 source because the one the AI suggested didn't exist, so I used an Excel source instead of a URL. I directed the architectural decisions for the deliverable, and the AI helped turn those decisions into the code and structure.
