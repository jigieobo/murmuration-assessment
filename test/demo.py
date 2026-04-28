"""Quick script to inspect what parse_nc_sboe actually produces.

Run from the parser_unit_test/ directory:

    python demo.py
"""

from pprint import pprint

from parsers import parse_nc_sboe


def main() -> None:
    raw = open("tests/fixtures/nc_sboe_sample.json").read()
    results = parse_nc_sboe(raw)

    print(f"Parser returned {len(results)} record(s):\n")
    pprint(results)


if __name__ == "__main__":
    main()
