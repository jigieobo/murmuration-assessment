# Elected Officials Data Acquisition — Part 1

Murmuration Technical Analyst Assessment, Part 1.

## What's in this folder
- **`parsers.py`** — implementation of `parse_nc_sboe`, the source-specific
  parser referenced in Section 4 of the design document.
- **`tests/test_parsers.py`** — the parser unit test from the design document,
  in runnable form.
- **`tests/fixtures/nc_sboe_sample.json`** — input fixture the test loads.
- **`demo.py`** — small script that prints the parsed output so you can see
  what the parser actually produces.
- **`requirements.txt`** — the only dependency is `pytest`.

## Setup

From this directory:

```bash
pip install -r requirements.txt
```

## Run the tests

```bash
pytest -v 
```
or
```bash
python3 -m pytest 
```

A successful run looks like:

```
============================= test session starts ==============================
collected 1 item

tests/test_parsers.py::test_parse_nc_sboe_returns_officials PASSED      [100%]

============================= 1 passed in 0.04s ==============================
```

## Inspect what the parser produces

```bash
python demo.py
```

Expected output:

```
Parser returned 2 record(s):

[{'county_fips': '37001',
  'full_name': 'Terry Johnson',
  'office_title': 'Sheriff',
  'party': 'Republican',
  'source_url': 'https://er.ncsbe.gov/...',
  'storage_path': 'raw/state_portals/nc_sboe/2026-04-27T14:00Z.json'},
 {'county_fips': '37001',
  'full_name': 'Maria Lopez',
  'office_title': 'Register of Deeds',
  'party': 'Democratic',
  'source_url': 'https://er.ncsbe.gov/...',
  'storage_path': 'raw/state_portals/nc_sboe/2026-04-27T14:00Z.json'}]
```

The fixture has three entries; one has `"elected": false` and is filtered out
by the parser, which is why only two records come back.

## Layout

```
elected_officials_data_acquisition/
├── README.md
├── design_document.md
├── parsers.py
├── demo.py
├── requirements.txt
└── tests/
    ├── test_parsers.py
    └── fixtures/
        └── nc_sboe_sample.json
```

## Things to try

- Flip the `elected` flag on the third fixture entry from `false` to `true`
  and rerun `python demo.py` to see the filter behavior change.
- Add a record with a missing `party` field and rerun the test — it should
  still pass because the contract treats party as optional.
- Add a record with an empty `name` and rerun the test — it should fail on
  the `full_name.strip()` assertion, demonstrating what a real parser bug
  looks like in pytest output.
