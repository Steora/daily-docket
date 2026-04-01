"""CI/local entry point: `python scraper.py` (used by GitHub Actions)."""

from ext_v3 import main

if __name__ == "__main__":
    raise SystemExit(main())
