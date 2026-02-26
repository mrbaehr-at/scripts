# Walmart — CSV to Airtable Upsert

## Background

Walmart needs to bulk-upsert Jira epic data from CSV exports into an Airtable base. The original script used [pyairtable](https://github.com/gtalarico/pyairtable), a popular Python wrapper for the Airtable REST API. However, Walmart has a hard security policy against third-party libraries that aren't on their approved list, and pyairtable is not approved.

## What we did

We analyzed pyairtable's internals and confirmed that the original script only used a single library feature: `Table.batch_upsert()`. Under the hood, that method makes a single PATCH (or PUT) request per batch to the Airtable REST API's native upsert endpoint (`performUpsert`).

We extracted and inlined that logic into a standalone `airtable_upsert()` function (~30 lines) that calls the Airtable API directly via `requests`. Everything else in the script — CSV parsing, column mapping, chunking, validation — was already custom code and didn't change.

### What was replaced

| pyairtable usage | Replaced with |
|---|---|
| `Api(api_key)` | `requests.Session()` with `Authorization: Bearer` header |
| `api.table(base_id, table_name)` | URL string: `https://api.airtable.com/v0/{base_id}/{table_name}` |
| `table.batch_upsert(records, key_fields, replace, typecast)` | Direct PATCH/PUT with `performUpsert.fieldsToMergeOn` in request body |
| pyairtable's retry logic (urllib3 `Retry`) | Simple retry loop on HTTP 429 using `Retry-After` header |

### What was kept as-is

- Column mapping loader (`mapping.csv` -> field ID dict)
- CSV row -> Airtable fields translator
- Chunked CSV reader (yields batches of N records)
- Key field validation
- All config variables and logging

## Dependencies

```
pip install requests
```

No other third-party libraries required. The script uses only `requests` + Python stdlib (`csv`, `os`, `sys`, `time`, `logging`, `pathlib`).

## Usage

1. Set environment variables (or edit defaults in the script):
   - `AIRTABLE_API_KEY` — Airtable personal access token
   - `AIRTABLE_BASE_ID` — target base ID
   - `AIRTABLE_TABLE_NAME` — target table name

2. Place your files:
   - `data.csv` — the CSV to import
   - `mapping.csv` — column name to Airtable field ID mapping

3. Run:
   ```
   python csv_to_airtable_upsert.py
   ```

## Files

- `csv_to_airtable_upsert.py` — the standalone upsert script
- `mapping.csv` — column mapping (CSV Column -> Airtable field ID) *(not committed — customer-specific)*
