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

## Duplicate handling

If the CSV contains multiple rows with the same upsert key (e.g. `IssueKey`), the **last row wins** — within each batch, later rows overwrite earlier ones with the same key. This prevents the Airtable API from rejecting a batch that contains the same record twice.

## Resume support

Progress is saved to `progress.json` after each successful batch. If the script crashes or is interrupted, re-running it will resume from the last successful batch. On successful completion, the progress file is automatically deleted.

## Logging

All output is written to both stdout and `upsert.log` (configurable via `LOG_FILE`). API error response bodies are logged before the script exits, so you can diagnose failures from the log file alone.

## Dependencies

```
pip install requests
```

No other third-party libraries required. The script uses only `requests` + Python stdlib (`csv`, `os`, `sys`, `time`, `logging`, `pathlib`).

## Usage

### 1. Install dependencies

```
pip install requests
```

### 2. Set environment variables

All configuration is via environment variables — nothing is hardcoded in the script.

| Variable | Required | Description |
|---|---|---|
| `AIRTABLE_API_KEY` | Yes | Airtable personal access token (PAT) with read/write scopes on the target base |
| `AIRTABLE_BASE_ID` | Yes | Target base ID (starts with `app`, e.g. `appXXXXXXXXXXXXX`) |
| `AIRTABLE_TABLE_NAME` | Yes | Target table name or ID (e.g. `Epics` or `tblXXXXXXXXXXXXX`) |
| `AIRTABLE_KEY_FIELD_IDS` | Yes | Comma-separated Airtable field ID(s) to merge on (see below) |

### 3. Understand the merge key

The merge key (`AIRTABLE_KEY_FIELD_IDS`) tells Airtable how to match incoming CSV rows to existing records. This should be the **unique identifier from your source data** — for example, a Jira Issue Key like `PROJ-1234` — **not** the Airtable record ID (`recXXXXXX`).

When upserting, Airtable will:
- **Update** the existing record if a record with that key value already exists
- **Create** a new record if no match is found

The value must be a **field ID** (starts with `fld`), not a field name. To find it: open the table in Airtable → click the field header → "Edit field" → the field ID is in the URL.

Example: if your CSV has an `IssueKey` column mapped to field `fldf7Xvt6tDmcgfLW`, set:
```
export AIRTABLE_KEY_FIELD_IDS=fldf7Xvt6tDmcgfLW
```

### 4. Prepare your files

- **`data.csv`** — the CSV to import. Can contain columns that aren't mapped; they'll be skipped.
- **`mapping.csv`** — maps CSV column names to Airtable field IDs. Format:

  ```
  CSV Column,Epics Table Field ID
  IssueKey,fldf7Xvt6tDmcgfLW
  EpicName,fldZnouG6p9i45J7L
  IssueStatus,fldj7kMjIoAsz0mVD
  ...
  ```

  The first column header must be `CSV Column`. The second column header must be `Epics Table Field ID`. Only mapped columns are sent to Airtable; unmapped columns are silently skipped.

### 5. Run

```
export AIRTABLE_API_KEY=patXXXXXX
export AIRTABLE_BASE_ID=appXXXXXX
export AIRTABLE_TABLE_NAME=Epics
export AIRTABLE_KEY_FIELD_IDS=fldXXXXXX

python csv_to_airtable_upsert.py
```

Output goes to both stdout and `upsert.log`. If the script is interrupted, just re-run it — it will resume from where it left off.

### Configuration options

These can be changed by editing the script directly:

| Variable | Default | Description |
|---|---|---|
| `CHUNK_SIZE` | `5` | Records per API request (max 10) |
| `TYPECAST` | `True` | Auto-coerce string values to field types (dates, selects, etc.) |
| `REPLACE` | `False` | `False` = patch (update only mapped fields). `True` = replace (blank unmapped fields) |
| `MAX_RETRIES` | `5` | Retry attempts on rate limit (HTTP 429) |
| `LOG_FILE` | `upsert.log` | Log file path |
| `PROGRESS_FILE` | `progress.json` | Resume checkpoint file |

## Files

- `csv_to_airtable_upsert.py` — the standalone upsert script
- `mapping.csv` — column mapping (CSV Column -> Airtable field ID) *(not committed — customer-specific)*
