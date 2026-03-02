"""
CSV → Airtable Upsert Workflow (with column mapping) — Standalone Edition
--------------------------------------------------------------------------
Uses a separate mapping CSV to translate CSV column names → Airtable field IDs
before upserting. The mapping CSV should have at minimum two columns:

    CSV Column          | Epics Table Field ID
    AcceptanceCriteria  | fldPRBp3TpIlymGb5
    Assignee            | fldS1imDEzzFWGxB6
    ...

The column headers for those two columns are configurable below via
MAPPING_CSV_COLUMN and MAPPING_FIELD_ID_COLUMN.

This script calls the Airtable REST API directly via `requests` and does not
depend on pyairtable or any other Airtable wrapper library.

Install dependencies:
    pip install requests

Usage:
    python csv_to_airtable_upsert_standalone.py
"""

import csv
import json
import os
import sys
import time
import logging
from pathlib import Path
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
BASE_ID          = os.getenv("AIRTABLE_BASE_ID", "")
TABLE_NAME       = os.getenv("AIRTABLE_TABLE_NAME", "")

# Path to the large CSV you want to import
CSV_FILE_PATH    = "data.csv"

# Path to the mapping CSV (CSV Column → Field ID)
MAPPING_CSV_PATH = "mapping.csv"

# Header names inside the mapping CSV
MAPPING_CSV_COLUMN      = "CSV Column"              # column containing the source CSV header name
MAPPING_FIELD_ID_COLUMN = "Epics Table Field ID"    # column containing the Airtable field ID

# The Airtable field ID(s) to use as the upsert merge key.
# This is the unique identifier from the SOURCE data (e.g. a Jira IssueKey),
# NOT the Airtable record ID. Must be field IDs (fldXXXXXX), not field names.
# Find the field ID in Airtable: click field header → "Edit field" → URL contains the ID.
KEY_FIELD_IDS = os.getenv("AIRTABLE_KEY_FIELD_IDS", "").split(",") if os.getenv("AIRTABLE_KEY_FIELD_IDS") else []

# Optional settings
TYPECAST   = True   # Coerce string values to correct field types (dates, checkboxes, etc.)
REPLACE    = False  # False = patch only; True = overwrite entire record (nulls unmapped fields)
CHUNK_SIZE = 5  # Rows per API request — max 10 per Airtable API limit

# Retry settings for rate limiting (HTTP 429)
MAX_RETRIES     = 5
DEFAULT_BACKOFF = 30  # seconds, used when Retry-After header is missing

# Airtable API base URL
AIRTABLE_API_URL = "https://api.airtable.com/v0"

# Progress file for resume support
PROGRESS_FILE = "progress.json"

# Log file
LOG_FILE = "upsert.log"

# ---------------------------------------------------------------------------
# Logging (configured in setup_logging(), called at start of main)
# ---------------------------------------------------------------------------
log = logging.getLogger(__name__)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_FILE),
        ],
    )


# ---------------------------------------------------------------------------
# Airtable API helpers (replaces pyairtable)
# ---------------------------------------------------------------------------

def airtable_upsert(session, base_id, table_name, records, key_fields,
                    replace=False, typecast=True):
    """
    Upsert records into an Airtable table via the REST API.

    Equivalent to pyairtable's Table.batch_upsert(), but for a single batch
    of up to 10 records (the Airtable API limit per request).

    Returns the parsed JSON response dict with keys:
        records, createdRecords, updatedRecords
    """
    url = f"{AIRTABLE_API_URL}/{base_id}/{table_name}"

    method = "PUT" if replace else "PATCH"

    payload = {
        "records": records,
        "performUpsert": {
            "fieldsToMergeOn": key_fields,
        },
        "typecast": typecast,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.request(method, url, json=payload, timeout=60)
        except requests.exceptions.Timeout:
            log.warning(
                "Request timed out (attempt %d/%d). Retrying in %ds...",
                attempt, MAX_RETRIES, DEFAULT_BACKOFF,
            )
            time.sleep(DEFAULT_BACKOFF)
            continue

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", DEFAULT_BACKOFF))
            log.warning(
                "Rate limited (429). Retry-After: %ds (attempt %d/%d)",
                retry_after, attempt, MAX_RETRIES,
            )
            time.sleep(retry_after)
            continue

        # Any other error — log details and raise
        log.error("API error %d: %s", resp.status_code, resp.text)
        resp.raise_for_status()

    raise RuntimeError(
        f"Airtable API rate limit exceeded after {MAX_RETRIES} retries"
    )


# ---------------------------------------------------------------------------
# Helpers (unchanged from original)
# ---------------------------------------------------------------------------

def load_mapping(mapping_path: str) -> dict[str, str]:
    """
    Parse the mapping CSV and return a dict of:
        { csv_column_name: airtable_field_id }

    Rows where either value is blank are skipped (i.e. unmapped columns).
    """
    mapping = {}
    with open(mapping_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        if MAPPING_CSV_COLUMN not in (reader.fieldnames or []):
            raise ValueError(
                f"Mapping CSV is missing expected column '{MAPPING_CSV_COLUMN}'.\n"
                f"Found columns: {reader.fieldnames}"
            )
        if MAPPING_FIELD_ID_COLUMN not in (reader.fieldnames or []):
            raise ValueError(
                f"Mapping CSV is missing expected column '{MAPPING_FIELD_ID_COLUMN}'.\n"
                f"Found columns: {reader.fieldnames}"
            )

        for row in reader:
            csv_col  = row[MAPPING_CSV_COLUMN].strip()
            field_id = row[MAPPING_FIELD_ID_COLUMN].strip()
            if csv_col and field_id:
                mapping[csv_col] = field_id

    return mapping


def apply_mapping(row: dict, col_to_field_id: dict[str, str]) -> dict:
    """
    Translate a raw CSV row (keyed by column name) into an Airtable fields dict
    (keyed by field ID). Columns not present in the mapping are dropped.
    Empty string values are dropped to avoid blanking existing field values.
    """
    fields = {}
    for csv_col, value in row.items():
        field_id = col_to_field_id.get(csv_col)
        if field_id and value != "":
            fields[field_id] = value
    return fields


def csv_chunks(filepath: str, col_to_field_id: dict[str, str], chunk_size: int):
    """
    Generator that yields lists of Airtable-ready record dicts, chunk_size at a time.
    Each record dict has the shape: {"fields": {field_id: value, ...}}
    """
    if chunk_size > 10:
        log.warning(
            "CHUNK_SIZE %d exceeds Airtable's 10-record batch limit; clamping to 10.",
            chunk_size,
        )
        chunk_size = 10

    with open(filepath, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        if reader.fieldnames is None:
            raise ValueError("Records CSV appears to be empty or has no header row.")

        # Warn about any CSV columns that have no mapping
        unmapped = [c for c in reader.fieldnames if c not in col_to_field_id]
        if unmapped:
            log.warning(
                "%d CSV column(s) have no mapping and will be skipped: %s",
                len(unmapped), unmapped,
            )

        chunk = {}  # keyed by merge key value to deduplicate within a chunk
        for row in reader:
            fields = apply_mapping(row, col_to_field_id)
            if not fields:
                continue

            # Deduplicate by merge key within the chunk
            merge_vals = tuple(fields.get(k, "") for k in KEY_FIELD_IDS)
            chunk[merge_vals] = {"fields": fields}

            if len(chunk) == chunk_size:
                yield list(chunk.values())
                chunk = {}

        if chunk:
            yield list(chunk.values())


def load_progress() -> dict:
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"last_chunk": 0, "total_created": 0, "total_updated": 0, "total_records": 0}


def save_progress(chunk_num: int, total_created: int, total_updated: int, total_records: int):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({
            "last_chunk": chunk_num,
            "total_created": total_created,
            "total_updated": total_updated,
            "total_records": total_records,
        }, f)


def validate_key_fields(col_to_field_id: dict[str, str]) -> None:
    """
    Confirm every KEY_FIELD_ID appears as a value in the mapping,
    so we don't silently upsert on a field that was never populated.
    """
    mapped_ids = set(col_to_field_id.values())
    missing = [fid for fid in KEY_FIELD_IDS if fid not in mapped_ids]
    if missing:
        raise ValueError(
            f"KEY_FIELD_IDS {missing} are not present in the column mapping.\n"
            "The key field must be mapped to a CSV column so it is included in every record."
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    setup_logging()

    # Validate required configuration
    missing = []
    if not AIRTABLE_API_KEY:
        missing.append("AIRTABLE_API_KEY")
    if not BASE_ID:
        missing.append("AIRTABLE_BASE_ID")
    if not TABLE_NAME:
        missing.append("AIRTABLE_TABLE_NAME")
    if not KEY_FIELD_IDS or KEY_FIELD_IDS == [""]:
        missing.append("AIRTABLE_KEY_FIELD_IDS")
    if missing:
        log.error("Missing required env var(s): %s", ", ".join(missing))
        sys.exit(1)

    # Validate file paths up front
    for path, label in [(CSV_FILE_PATH, "Records CSV"), (MAPPING_CSV_PATH, "Mapping CSV")]:
        if not Path(path).exists():
            log.error("%s not found: %s", label, path)
            sys.exit(1)

    # Load the column → field ID mapping
    log.info("Loading column mapping from: %s", MAPPING_CSV_PATH)
    col_to_field_id = load_mapping(MAPPING_CSV_PATH)
    log.info("Loaded %d column mappings", len(col_to_field_id))

    # Sanity-check key fields are in the mapping
    validate_key_fields(col_to_field_id)

    # Set up a requests session with auth header
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    })

    log.info("Connected → base: %s  table: %s", BASE_ID, TABLE_NAME)
    log.info("Upsert key field ID(s): %s", KEY_FIELD_IDS)

    progress = load_progress()
    start_after = progress["last_chunk"]
    total_created = progress["total_created"]
    total_updated = progress["total_updated"]
    total_records = progress["total_records"]

    if start_after > 0:
        log.info("Resuming from chunk %d (%d records already processed)",
                 start_after + 1, total_records)

    chunk_num = 0

    for chunk in csv_chunks(CSV_FILE_PATH, col_to_field_id, CHUNK_SIZE):
        chunk_num += 1

        if chunk_num <= start_after:
            continue

        log.info("Chunk %d — sending %d records...", chunk_num, len(chunk))

        result = airtable_upsert(
            session=session,
            base_id=BASE_ID,
            table_name=TABLE_NAME,
            records=chunk,
            key_fields=KEY_FIELD_IDS,
            replace=REPLACE,
            typecast=TYPECAST,
        )

        created = len(result.get("createdRecords", []))
        updated = len(result.get("updatedRecords", []))
        total_created += created
        total_updated += updated
        total_records += len(chunk)

        save_progress(chunk_num, total_created, total_updated, total_records)
        log.info("  created: %d  updated: %d", created, updated)

    # Clean up progress file on successful completion
    if Path(PROGRESS_FILE).exists():
        os.remove(PROGRESS_FILE)

    log.info("=" * 55)
    log.info("Done!")
    log.info("  Total processed : %d", total_records)
    log.info("  Created         : %d", total_created)
    log.info("  Updated         : %d", total_updated)


if __name__ == "__main__":
    main()
