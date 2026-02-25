# BA Dedup2 - Canvas-to-DEC Matching Pipeline

## Communication Rules

- Be direct and honest. No filler phrases like "great question", "solid plan".
- If an idea is bad, say so and explain why.
- Lead with problems when an approach has them.

## Project Overview

**Purpose**: Match Canvas BA records to DEC BA records for Oil & Gas companies. Human reviewers use the output to approve/reject matches. No auto-merge — false merges have financial consequences.

**Primary blocking key**: SSN (not name/address).

## Active Files

```
BA_Dedup2/
├── canvas_to_dec_match.py   # Main matching engine (~1800 lines). All scoring, classification, API overrides, Excel output.
├── config_loader.py         # Config defaults + Snowflake BA_CONFIG/BA_LOOKUP override
├── snowflake_conn.py        # Snowflake browser-SSO connection helper
├── test_snowflake.py        # Connection test
├── test_name_compare.py     # Name comparison tests
└── load_dec_master.py       # Load DEC master data
```

## Architecture

Single-script pipeline (`canvas_to_dec_match.py`):
1. Load Canvas + DEC data from Snowflake
2. Load config from Snowflake BA_CONFIG (falls back to hardcoded defaults in config_loader.py)
3. SSN blocking → name comparison → address comparison → classification
4. API override pass (Claude) for ambiguous name scores
5. Google API override pass for ambiguous address scores
6. Write results to SQLite + Snowflake + Excel

## 4 Classification Buckets

| Bucket | Trigger |
|--------|---------|
| NEW BA AND NEW ADDRESS | No valid SSN, or SSN not in DEC |
| EXISTING BA ADD NEW ADDRESS | SSN match + name/addr scores within configured range |
| EXISTING BA AND EXISTING ADDRESS | SSN match + name/addr scores within configured range |
| NEEDS REVIEW | SSN match but scores don't fit any bucket range |

Classification uses `_classify()` with configurable per-bucket score ranges (BUCKET_RULES built from BA_CONFIG).

## Bucket Range Config (Snowflake BA_CONFIG)

Each bucket has 4 config keys: `{PREFIX}_MIN_NAME_SCORE`, `{PREFIX}_MAX_NAME_SCORE`, `{PREFIX}_MIN_ADDR_SCORE`, `{PREFIX}_MAX_ADDR_SCORE`.

| Bucket Prefix | Name Range Default | Addr Range Default |
|--------------|-------------------|-------------------|
| NEW_BA_NEW_ADDR | 0-0 | 0-0 |
| EXISTING_BA_NEW_ADDR | 100-100 | 0-0 |
| EXISTING_BA_EXISTING_ADDR | 100-100 | 100-100 |

## Key Functions in canvas_to_dec_match.py

- `name_compare()` — Fuzzy name matching with nickname resolution, business suffix stripping, industry term stripping, acronym detection
- `address_compare()` — Address similarity with PO Box normalization, street type/direction stripping, unit handling
- `_classify()` — Classify record into bucket based on configurable score ranges (BUCKET_RULES)
- `normalize_zip()` — Strip to digits, left-pad to 5
- `run_matching()` — Main entry point that orchestrates the full pipeline

## Dependencies

- `pandas`, `openpyxl` — Data manipulation and Excel output
- `thefuzz` — Fuzzy string matching
- `jellyfish` — Jaro-Winkler distance
- `snowflake-connector-python` — Snowflake access
- `anthropic` — Claude API for name disambiguation
- `googlemaps` — Google Places API for address validation
