# Elected Officials Data Acquisition — Design & Planning Document
**Murmuration Technical Analyst Assessment | Part 1**

---

## Overview

Murmuration needs to build and maintain a dataset of all elected officials across U.S. county governments, including offices like County Commissioner, County Executive, County Clerk, Sheriff, and similar roles. This document describes a system for building and maintaining a dataset of elected officials across all U.S. county governments. The core challenge is that county government structures are not standardized nationally. For example, what's called a "County Commissioner" in one state may be a "County Supervisor" in another state. Some counties have an elected executive, and some don't, and source quality ranges from machine-readable APIs to PDFs on sites that haven't been updated in years. This design prioritizes correctness and credibility over completeness.

---

## 1. Data Model

### Design Philosophy

The database is organized around four core ideas: locations (states and counties), positions (like sheriff or county clerk), people holding those positions, and sources showing where the information came from. Rather than treating every county the same, the model separates a general office *type* from how that office is *instantiated* in a specific county — which is what allows it to handle the fact that governments are structured differently across the country. It also tracks relationships over time: when an official leaves office or a new one is elected, the system keeps that history rather than overwriting records. Every record is tied back to a source and to validation metadata, which makes the data easier to trust, audit, and update.

The schema handles three kinds of variability:

- **Structural variability** — not every county has the same offices (e.g., only some counties have an elected County Executive).
- **Temporal variability** — officials change due to elections, resignations, and mid-term appointments.
- **Source variability** — the same official may appear in multiple sources with slightly different name spellings or title formats.

The entity descriptions below call out which of these each entity is responsible for handling.

---

### Core Entities

#### `states`

Reference table of U.S. states. Mostly static — populated once and rarely touched.

- `state_fips` — 2-digit Census identifier; primary key.
- `state_abbr` — two-letter postal code (e.g., 'GA').
- `state_name` — full name.

#### `counties`

One row per county or county-equivalent.

- `county_fips` — 5-digit Census identifier; primary key. Stable across renames and unambiguous across states; FEC, Census, and most government datasets key on it.
- `state_fips` — link to the state.
- `county_name` — human-readable name (e.g., 'Fulton County').
- `county_type` — 'county', 'parish' (Louisiana), 'borough' (Alaska), or 'independent_city' (Virginia and a handful of others). Captures the few states that don't use the word *county* but have functionally equivalent units.
- `government_form` — 'standard', 'consolidated_city_county' (e.g., San Francisco, Indianapolis), or 'no_county_government' (Connecticut, Rhode Island, parts of Alaska). Tells the pipeline whether to expect typical county officeholders at all.
- `has_elected_government` — a boolean shortcut for the same idea, set to false for the handful of areas where county-level offices don't exist.
- `official_website` — the county's primary site, used as a starting point for collection.
- `population` — Census population, useful for downstream reporting and analyst-side filtering.
- `last_verified_at` — timestamp of the last successful verification.

> **Why FIPS codes?** They're the standard stable identifier used by Census, FEC, and most government data. They survive county name changes and are unambiguous across states.

> **Variability handled here:** *structural.* `county_type` and `government_form` capture the cases where state government doesn't follow the "50 standard counties per state" pattern.

#### `office_types`

A controlled vocabulary of canonical office types, normalized across states. This is the key entity for handling source-side variability — rather than storing raw title strings everywhere, we map them to canonical types.

- `office_type_id` — primary key.
- `canonical_name` — the standardized name we map raw titles to (e.g., 'County Commissioner / Supervisor', 'Sheriff', 'Register of Deeds').
- `description` — short explanation of what the role does, useful for analysts unfamiliar with regional naming conventions.
- `is_typically_board` — flag for offices that are normally multi-seat bodies (commissions, councils) versus single-seat offices (sheriff, clerk).

> **Variability handled here:** *source.* "County Commissioner" in Georgia, "County Supervisor" in California, and "Freeholder Director" in New Jersey all collapse to the same canonical type, which is what makes cross-state queries possible.

#### `offices`

An instantiated office within a specific county. Separates the *role* from the *person holding it*.

- `office_id` — primary key.
- `county_fips` — which county the office belongs to.
- `office_type_id` — which canonical type this office maps to.
- `local_title` — the raw title as it appears in that county (e.g., 'Freeholder Director'), preserved verbatim for traceability and for analysts who need the original wording.
- `district_or_seat` — the seat identifier within a multi-seat body (e.g., 'District 3', 'At-Large', 'Seat B'). NULL when the office is single-seat.
- `is_partisan` — whether the office is filled through a partisan election.
- `term_length_years` — typical term length, used for scheduling re-verification.
- `notes` — free text for unusual cases.

> **Why separate `offices` from `office_types`?** A county might have five Commissioner seats — each gets its own `offices` row (with different `district_or_seat` values), but they all share an `office_type_id`. This lets us answer questions like "how many commissioner seats exist in Georgia" cleanly, without string-matching on free-text titles.

> **Variability handled here:** *structural.* Multi-seat boards, single-seat offices, at-large versus district seats, and counties that don't have a given office at all use the same shape.

#### `officials`

The person.

- `official_id` — primary key.
- `full_name`, `first_name`, `last_name` — name fields, kept separately to support fuzzy matching and reconciliation across sources.
- `party` — 'Democratic', 'Republican', 'Nonpartisan', 'Independent', etc.
- `notes` — free text.

An `official` is a *person*, not a person-in-a-role. The same person holding two offices over their career (a county clerk who later becomes treasurer) is one `officials` row linked to two `office_holders` rows — which keeps the model honest about the difference between people and positions.

#### `office_holders` *(the join table — the heart of the model)*

Links an official to an office over a time period. This is the table that changes most often.

- `holder_id` — primary key.
- `official_id` — which person.
- `office_id` — which seat.
- `term_start` / `term_end` — when the term begins and ends. `term_end` may be a future date for a sitting official, or NULL if the end date is unknown.
- `is_current` — denormalized flag for "holds this seat right now." Looks redundant with `term_end`, but they serve different roles: `term_end` is the actual or scheduled end date, while `is_current` is what the pipeline marks based on present evidence (e.g., a directory listing that confirms the person is still serving). Keeping both makes "who holds this seat now" queries fast and unambiguous, especially when sources don't give us reliable dates.
- `assumption_method` — 'elected', 'appointed', 'interim', or 'acting'. Distinguishes a regularly elected official from someone filling a mid-term vacancy.
- `source_id` — which source this record came from.
- `collected_at` — when the pipeline first recorded this row.
- `last_verified_at` — when this row was most recently confirmed against a fresh source fetch.

> **Variability handled here:** *temporal.* By storing terms rather than overwriting officials, we preserve history. An election or resignation closes the old `office_holders` row and opens a new one — past officeholders aren't destroyed, and analysts can ask "who held this seat in 2022?" as easily as "who holds it now?"

#### `contact_info`

A contact point linked to either a specific person or a specific office.

- `contact_id` — primary key.
- `official_id` *or* `office_id` — exactly one of these is set, depending on whether the contact follows the person or the seat.
- `contact_type` — 'phone', 'fax', 'email', or 'website'.
- `location_label` — which physical location this contact serves (e.g., 'Washington Office', 'District Office'). NULL for things like a personal website that aren't tied to a place.
- `value` — the actual phone number, email, or URL.

A single official commonly has multiple contact rows. A U.S. Representative like Brian Jack, for instance, would have separate rows for the Washington Office phone, the Washington Office fax, the District Office phone, the District Office fax, and a campaign website — all linked to the same `official_id`, distinguished by `location_label` and `contact_type`. Pinning everything to a single phone or email column on `officials` would lose that.

#### `addresses`

Addresses for officials and offices are stored as discrete fields rather than one free-text blob.

- `address_id` — primary key.
- `official_id` *or* `office_id` — same pattern as `contact_info`.
- `location_label` — which location this address belongs to.
- `street`, `city`, `state`, `zip` — broken out so addresses can be geocoded, joined to other spatial datasets, and validated against Census or USPS lookups.

#### `sources`

Every record traces back to where it came from.

- `source_id` — primary key.
- `source_name` — human-readable name (e.g., 'Fulton County Official Website').
- `source_url` — the actual URL fetched.
- `source_type` — 'official_website', 'state_portal', 'ballotpedia', 'manual', 'api', etc.
- `reliability_tier` — 1 (highest) through 4 (lowest), corresponding to the tiers in Section 2.
- `last_fetched_at` — most recent fetch time.
- `notes` — free text for caveats (e.g., "site is JS-rendered; needs Playwright").

> **Variability handled here:** *source.* When two sources disagree on a name, party, or title, `reliability_tier` lets the pipeline (or an analyst) decide which one to trust.

#### `collection_log`

Audit trail of every collection run.

- `log_id` — primary key.
- `county_fips`, `source_id` — what was attempted.
- `run_at` — when the run happened.
- `status` — 'success', 'partial', 'failed', or 'needs_review'.
- `records_found` / `records_updated` — counts for that run.
- `error_message` — populated on failure.
- `raw_content_hash` — SHA-256 of the raw fetched content. This lets the pipeline cheaply detect whether a source actually changed since the last run, without re-parsing.

---

## 2. Data Sources

The source strategy is trust-based: pull from the most authoritative source available, fall back to secondary sources to fill gaps and cross-check, and mark the rest for manual review. Official government sources like state election authorities and county websites are closest to the data and treated as the strongest. Aggregators like Ballotpedia and OpenStates help where official sources are missing, stale, or unparseable. Each source is evaluated on five dimensions: how authoritative it is, how current it is, how complete its coverage is, how structured its output is, and how stable its format is over time. Because no national source covers every county well, the strategy explicitly accepts gaps and combines automation, fallbacks, and manual outreach (including public records requests) to keep the dataset accurate.

### Source Tiers

#### Tier 1 — Official Government Sources (most preferred)

**State election authorities and county official websites** (Secretaries of State, State Election Boards, county websites).

- Many states publish current elected officials as part of election certification records.
- Most current and authoritative source for any given county.
- Highest authority, but requires county-by-county collection.

**Census Bureau**

- County boundaries and FIPS codes.
- County-equivalent classifications (parishes, boroughs, independent cities).
- Updated decennially (every 10 years), with smaller boundary updates between.

> The Census Bureau is the foundational source for canonical county reference data. State election authorities and county websites are the primary sources for current officeholder data.

#### Tier 2 — Local Open Data Portals

**Socrata portals, state GIS/election APIs.**

- Structured supplemental data useful for validation and cross-reference. These are sites like Open Data Network. Coverage is uneven — strong in a handful of states, nonexistent in others.

#### Tier 3 — Reference Aggregators

**Ballotpedia and Wikipedia.**

- Broad coverage of elected officials with structured data.
- Not official, but actively maintained — Ballotpedia in particular has paid researchers.
- Ballotpedia has a paid API; free scraping is possible, but against ToS for commercial use, so licensing is worth exploring.
- Good for gap-filling and cross-validation; should never be the sole source for any official.

#### Tier 4 — Manual / Last Resort

For counties with no usable online presence, the only path to accurate data is human effort: emailing or calling county clerks for directory information, filing public records requests, and wrangling the resulting PDFs, spreadsheets, or paper printouts into the schema by hand. This work is slow and expensive, but for some counties it's the only way to get coverage at all.

---

## 3. Collection Architecture

The architecture is organized around a single idea: most of the work isn't fetching data, it's *deciding what changed since the last fetch and reconciling that against the existing data model*. Fetching, parsing, validating, and reconciling are kept as separable stages so that any one of them can be re-run independently against stored artifacts. This is important for fixing parser bugs, handling rate limits, and auditing contested records.

### High-level flow

```
Orchestrator (cron / Prefect)
        │
        ▼
  Fetch raw content
  → write to data lake (Amazon Lake Formation, Snowflake)
  → log path + SHA-256 hash to collection_log
        │
  [hash unchanged since last run?]
   YES → skip, update last_checked
   NO  ↓
  Parse & normalize
  → per-record validation
        │
   pass               fail
    ↓                   ↓
  Reconcile         needs_review
  (upsert)          queue
    ↓
  Data model
  (officials, offices,
   office_holders, …)
```

### Fetch and store raw

A scheduler triggers fetchers per source. Each raw payload (HTML, JSON, PDF) is written to a data lake, append-only, named by UTC timestamp:

```
raw/
  state_portals/nc_sboe/2026-04-27T14:00Z.json
  county_websites/13121/2026-04-27T09:15Z.html
```

The data lake is append-only because it acts like a permanent record of every snapshot the system has collected, rather than a file that keeps getting replaced. If raw data were overwritten, the system could lose evidence of what a source looked like last month, which would make auditing, debugging parser errors, or investigating data disputes much harder. This is why every parsed record carries a `storage_path` back to the snapshot it came from. Keeping every version lets the system compare changes over time, rerun parsers against old data if the extraction logic improves, and prove where a record came from at a specific point in time.

A SHA-256 hash of each raw payload is recorded in `collection_log.raw_content_hash`. If the next fetch produces an identical hash, the pipeline skips parsing and only refreshes timestamps. This keeps costs low across thousands of monitored sources, where most weeks nothing has changed.

### Parse and validate

Each fetched payload runs through a source-specific parser that extracts a flat list of records. Every record is checked against a validation contract before it's allowed into the reconciliation step. The contract requires a 5-digit FIPS code that exists in `counties`, a non-empty name, a non-empty raw office title, a parseable source URL, and a back-pointer to the raw artifact.

This contract sits *between* the parser and the data model. Parsers produce flat records (one office per row, free-text title, a single email or phone). The reconciliation step is what fans those flat records out into `officials`, `offices`, `office_holders`, `contact_info`, and `addresses`. Records that fail validation are written to a `needs_review` queue with the raw input attached.

### Reconcile — Keeping the data current

Reconciliation is where new fetches actually update the data model. Each validated record goes through four moves:

- **Canonical mapping.** The raw `local_title` is matched against `office_types`. Unrecognized titles go to review rather than being silently grouped under the wrong canonical type.

- **Person matching.** Fuzzy name matching against existing `officials`, scoped by county and contextual features (party, role overlap). Low-confidence matches are flagged, not auto-merged. Python offers several libraries for fuzzy string matching, like RapidFuzz and TheFuzz.

- **Term lifecycle.** When the person in a seat changes, the old `office_holders` row is closed (`is_current=FALSE`, `term_end` set) and a new one opens. This is what preserves history — past officeholders aren't destroyed, and "who held this seat in November 2014?" stays answerable.

- **Source-tier conflict resolution.** When two sources disagree (e.g., Ballotpedia and a county site disagree on party), the higher-tier source wins for the canonical value, and the disagreement is logged on the lower-tier record's `notes`. Conflicts are visible, not hidden.

The term lifecycle is worth making concrete, since it's the mechanism that actually keeps the data current as people leave and enter the office:

```
current = get_current_holder(office.id)
if current and current.official_id != new_official.id:
    # Different person in the seat → close the old row, open a new one
    close_office_holder(current, end_date=today())
    open_office_holder(office, new_official, source=source_id)
elif not current:
    open_office_holder(office, new_official, source=source_id)
else:
    # Same person, same seat → just refresh last_verified_at
    touch_office_holder(current, source=source_id)
```

### Update cadence

Three layers, each tuned to a different rate of change:

- **Post-election full sweep.** The highest-signal moment. Most turnover happens here, so every monitored source is re-fetched and fully reconciled.
- **Monthly hash-check.** For each monitored source, fetch and compare the SHA-256 against the last stored hash. Unchanged → skip parsing entirely. Changed → re-parse and reconcile. This catches mid-term changes (resignations, deaths, appointments) without paying parse cost on every run.
- **Event-driven re-collection.** A manual trigger when an analyst flags a known change (e.g., a news story about a sheriff's resignation) before the next scheduled run would pick it up.

---

## 4. Testing

Two levels of testing cover the most common failure modes, plus a third category worth flagging for later.

### Parser unit tests

Each parser is tested against saved raw HTML/JSON snapshots pulled from the data lake. No network calls, so tests stay reproducible even when the upstream site changes its markup. Each parser is asserted on the basics: at least one record returned, every record has a valid 5-digit FIPS, every record has a non-empty name, and optional fields like party are allowed to be missing without breaking. Edge cases get their own fixtures — a directory page with a missing party field, a state portal that returns an empty array, a PDF page where two offices share a row.

A small example. Given a fixture (`tests/fixtures/nc_sboe_sample.json`) like:

```json
{
  "election_date": "2024-11-05",
  "results": [
    {
      "county_fips": "37001",
      "county_name": "Alamance",
      "office": "Sheriff",
      "candidate": {"name": "Terry Johnson", "party": "REP"},
      "elected": true
    },
    {
      "county_fips": "37001",
      "county_name": "Alamance",
      "office": "Register of Deeds",
      "candidate": {"name": "Maria Lopez", "party": "DEM"},
      "elected": true
    }
  ]
}
```

The parser should produce a flat list of records — one per elected official, with party codes expanded and source/storage metadata attached:

```python
[
    {
        "county_fips":  "37001",
        "full_name":    "Terry Johnson",
        "office_title": "Sheriff",
        "party":        "Republican",
        "source_url":   "https://er.ncsbe.gov/...",
        "storage_path": "raw/state_portals/nc_sboe/2026-04-27T14:00Z.json",
    },
    {
        "county_fips":  "37001",
        "full_name":    "Maria Lopez",
        "office_title": "Register of Deeds",
        "party":        "Democratic",
        "source_url":   "https://er.ncsbe.gov/...",
        "storage_path": "raw/state_portals/nc_sboe/2026-04-27T14:00Z.json",
    },
]
```

The test itself should spot-check the basics:

```python
def test_parse_nc_sboe_returns_officials():
    raw = open("tests/fixtures/nc_sboe_sample.json").read()
    results = parse_nc_sboe(raw)
    assert len(results) > 0
    assert all(r["county_fips"] for r in results)
    assert all(r["full_name"].strip() for r in results)
```

The append-only data lake is what makes this cheap. Any newly discovered parser bug can be reproduced by pointing the parser at the snapshot from the day it broke, without re-fetching anything.

### Post-load sanity checks

These run after every collection sweep and catch systemic failures that single-parser tests would miss. Some examples include a parser silently returning an empty list, a reconciliation bug that closes too many `office_holders` rows, or a source that returns 200 OK with a "page not found" body.

The two checks I'd run in v1:

- **Gap rate.** Count counties whose offices have no current officeholder. The query joins `counties` → `offices` → `office_holders` (filtered by `is_current=TRUE`) and asks how many counties came back with zero current holders across all expected offices. If more than ~30% of counties are empty, the pipeline is broken, not just incomplete.

- **Staleness.** Count `office_holders` rows where `is_current=TRUE` and `last_verified_at` is older than a freshness threshold (e.g., 90 days). A handful is acceptable; a sudden spike points to a specific source going dark and is worth alerting on.

Both checks are written against the actual model — `is_current` and `last_verified_at` live on `office_holders`, not `officials`, because they describe a person-in-a-seat, not the person.

### Cross-entity consistency (would add with more time)

Worth flagging because the reconciliation logic has structural invariants that should never be violated. A few examples:

- No two `office_holders` rows with `is_current=TRUE` for the same single-seat office.
- Every `is_current=TRUE` row has a non-NULL `term_start`.
- Every `office_holders` row points to an `office` whose `county_fips` matches the source the record came from.

These would catch reconciliation bugs that the gap-rate test would miss. For example, the pipeline opens a duplicate current row instead of closing the old one.

---

## 5. Tradeoffs & What I'd Do With More Time

### What I Assumed / Cut

- **No entity resolution/deduplication.** If the same person appears in two sources with slightly different names ("Robert Smith" vs. "Bob Smith Jr."), they get two `officials` rows. Acceptable for v1, but needs fixing before any analytics use case that counts unique people.

- **No cross-source reconciliation logic.** When two sources disagree on a name or party, the pipeline currently takes the highest-tier source and flags the conflict in `notes`. There's no automated arbitration.

- **No event-driven ingestion.** The pipeline relies on scheduled hash-checks. A resignation announced today won't be picked up until the next monthly sweep (or until an analyst manually triggers a re-collection).

- **Validation is per-record, not cross-record.** A parser that returns the same person for every county would pass per-record validation; only the post-load sanity checks would catch it.

- **No analyst-facing observability surface.** Failure modes show up in `collection_log` and `needs_review`, but there's no dashboard summarizing freshness, gap rates, or per-state failure rates.

### With More Time

**Sources:**

- Map every state's Secretary of State/election authority and score them for machine-readability *before* writing a single scraper. This research phase pays for itself by routing effort to high-yield sources first.
- Pursue Ballotpedia API licensing for gap-filling. Evaluate whether it's worth it at scale.
- File public records requests for the handful of states with no usable online presence.
- Plan for bot defenses on government sites. As AI-driven scraping has scaled up, sites are deploying Cloudflare challenges, rate limits, CAPTCHA, and IP blocks more aggressively. Some Tier 1 sources that worked a year ago may not work today.

**Collection:**

- Build a real entity resolution layer: fuzzy name matching plus contextual features (same county, same role, overlapping terms) with a human review queue for ambiguous cases.
- Add news monitoring as an event-driven trigger for mid-term changes (resignations, deaths, appointments).
- Replace the rules-based canonical title matching with a fine-tuned classifier trained on the (raw_title → canonical_office_type) pairs we've already accumulated, including reviewer-confirmed mappings from the needs_review queue. High-confidence predictions stay automated; low-confidence ones go to review. Every confirmed mapping becomes new training data, so the model improves as coverage grows.
- Expand the fixture library to cover more failure modes: JS-heavy sites (Playwright), scanned PDFs, sites that return 200 OK with "page not found" in the body.

**Testing & observability:**

- Add per-state collection failure rate assertions that separate a single broken scraper from a systemic issue.
- Add cross-source reconciliation tests: when two sources disagree on a name or party, flag for review rather than silently picking one.
- Build an analyst-facing data quality dashboard surfacing staleness, gap rates, and validation error counts by state.

---

## AI Usage Note

I used Claude to help draft and structure this document and to pressure-test my implementation. I manually verified all source recommendations online and modified schema decisions based on my own knowledge of system design. I wrote the data model, unit testing, and tradeoffs & what I'd do with more time on my own.

