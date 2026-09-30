#!/usr/bin/env python3
"""Load the Yocale appointments CSV feed into daily_appointments.

Replaces scraper.py. Yocale retired the Kibana site that scraper.py logged
into (last successful run 2026-09-13), and now publishes the same data as a
CSV behind HTTP Basic Auth, refreshed on their side every 30 minutes.

Two differences from the Kibana feed, both deliberate:

  * StartDateTime in the CSV is STORE-LOCAL (Pacific), while Kibana's
    startDateTime was UTC. Verified against rows the scraper had already
    written: booking 10819750 is "2026-08-30 18:00:00+00" in the table and
    "2026-08-30T10:10:00" in the CSV — the same 10:10 AM appointment. So the
    CSV value is localized to America/Los_Angeles, never treated as UTC.
    Getting this wrong shifts every appointment by 7-8 hours.

  * The CSV has no isGoogleBooking field. It has "Booking interface Type
    Label" (RWG / Widget / Calendar) instead, and RWG is Reserve with Google.
    The rows above carried "Widget" and were stored as is_google_booking
    false, so RWG => true reproduces the old column.

The feed is the full booking history (2023-04 onward, including future-dated
bookings), not a rolling window, so the 15-day window and the "appointments
in the future are invisible" problem that dogged the scraper are both gone.

Environment:
  YOCALE_CSV_URL              feed URL (default: the PCJL feed below)
  YOCALE_CSV_USER             Basic Auth username (PCJL: jl-pcjl-reports)
  YOCALE_CSV_PASSWORD         Basic Auth password
  SUPABASE_URL                target project
  SUPABASE_SERVICE_ROLE_KEY   service_role: this loader writes, and `anon`
                              holds SELECT only on daily_appointments (the
                              Yocale wallboard's read). service_role is also
                              BYPASSRLS, so the table's read-only anon policy
                              never applies to it. NB that policy was inert
                              until RLS was enabled on 2026-09-29 -- see
                              supabase/sql/turbo_enable_rls_on_inert_policy_tables.sql
  DRY_RUN=1                   fetch and summarize, write nothing
"""

import csv
import io
import logging
import os
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_URL = "https://jiffylube.yocale.com/reports/pcjl/appointments-daily.csv"
DEFAULT_USER = "jl-pcjl-reports"
KEYCHAIN_SERVICE = "yocale-csv-feed"
PACIFIC = ZoneInfo("America/Los_Angeles")
UPSERT_CHUNK = 500
FETCH_ATTEMPTS = 4
FETCH_BACKOFF_SECONDS = 20

# CSV header -> our column. The arrow characters are Yocale's, not a typo.
COLUMNS = {
    "id": "Id",
    "start": "StartDateTime",
    "created": "UtcCreatedDateTime",
    "location_id": "ProviderLocation - ProviderLocationId → ID",
    "location_name": "ProviderLocation - ProviderLocationId → Name",
    "business_id": "BusinessId",
    "business_name": "Business - BusinessId → BusinessName",
    "offering": "OfferingName",
    "status": "Status label",
    "interface": "Booking interface Type Label",
    "fullname": "UserProfile - UserId → Fullname",
    "email": "UserProfile - UserId → Email",
}


def keychain_password(account):
    """Read the feed password from the macOS Keychain, for local runs.

    Lets a developer run this without putting the password in a shell command
    (where it lands in history). CI sets YOCALE_CSV_PASSWORD instead and never
    reaches this. Returns None anywhere the Keychain is not available.
    """
    try:
        import subprocess
        result = subprocess.run(
            ["security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-a", account, "-w"],
            capture_output=True, text=True, timeout=10,
        )
        return result.stdout.strip() or None if result.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def fetch_with_retries(url, auth):
    """GET the feed, retrying the failures that are Yocale's cache, not ours.

    Yocale regenerates the file every 30 minutes, and a request that lands
    while it is being rebuilt can hang and then be dropped: the scheduled runs
    at 2026-09-29 06:27 and 13:35 both waited ~47s and died on
    RemoteDisconnected, while a run that got a cached copy answered in 1.1s.
    The job fires at the top of the hour, which is exactly when a 30-minute
    cache is most likely to be stale, so this will recur.

    Connection drops, timeouts and 5xx are retried. A 401/403 is not — those
    are settled facts about the credential or their rules, and hammering them
    helps nobody.
    """
    last_error = None
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            response = requests.get(url, auth=auth, timeout=(15, 180))
            if response.status_code >= 500:
                last_error = f"HTTP {response.status_code}"
                logger.warning("Attempt %d/%d: %s", attempt, FETCH_ATTEMPTS, last_error)
            else:
                return response
        except (requests.ConnectionError, requests.Timeout) as error:
            last_error = f"{type(error).__name__}: {error}"
            logger.warning("Attempt %d/%d: %s", attempt, FETCH_ATTEMPTS, last_error)

        if attempt < FETCH_ATTEMPTS:
            delay = FETCH_BACKOFF_SECONDS * attempt
            logger.info("Retrying in %ds", delay)
            time.sleep(delay)

    raise SystemExit(
        f"Feed unreachable after {FETCH_ATTEMPTS} attempts ({last_error}). "
        "If this persists across runs, the feed itself is down — check whether "
        "the URL serves from a browser before assuming it is our side."
    )


def fetch_csv():
    url = os.environ.get("YOCALE_CSV_URL") or DEFAULT_URL
    user = os.environ.get("YOCALE_CSV_USER") or DEFAULT_USER
    password = os.environ.get("YOCALE_CSV_PASSWORD") or keychain_password(user)
    if not user or not password:
        raise SystemExit(
            "No credentials. Set YOCALE_CSV_PASSWORD, or store it locally with:\n"
            f"  security add-generic-password -s {KEYCHAIN_SERVICE} -a {user} -w"
        )

    logger.info("Fetching %s", url)
    response = fetch_with_retries(url, (user, password))

    # Both of these were seen while the feed was being set up, so the messages
    # say what each one actually meant. 401: the login was rejected — note the
    # username is jl-pcjl-reports, not the PCJLjl-pcjl-reports that Yocale's
    # handover email ran together. 403: the login was accepted but the server
    # refused anyway; during setup that was an nginx rule denying every .csv
    # under /reports/, which is Yocale's to fix.
    if response.status_code == 401:
        raise SystemExit("401 from the feed: username or password rejected.")
    if response.status_code == 403:
        raise SystemExit(
            "403 from the feed: the login was accepted but the server refused "
            "the file. Ask Yocale to check the /reports/ rules and that the "
            "CSV exists and is readable."
        )
    response.raise_for_status()

    # Decode explicitly: the response carries no charset, so requests would
    # fall back to Latin-1 and mangle the "→" in the header names. utf-8-sig
    # also strips the byte-order mark, which would otherwise make the first
    # column "﻿Id" instead of "Id".
    rows = list(csv.DictReader(io.StringIO(response.content.decode("utf-8-sig"))))
    logger.info("Fetched %d rows", len(rows))
    if not rows:
        raise SystemExit("Feed returned no rows; refusing to continue.")

    missing = [c for c in COLUMNS.values() if c not in rows[0]]
    if missing:
        raise SystemExit(f"Feed is missing expected columns: {missing}")
    return rows


def split_name(fullname):
    parts = (fullname or "").strip().split()
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return parts[0], " ".join(parts[1:])


def transform(rows):
    """Map feed rows to daily_appointments records."""
    get = lambda row, key: (row.get(COLUMNS[key]) or "").strip() or None
    extracted_at = datetime.now(timezone.utc)
    data_date = datetime.now(PACIFIC).date()

    records, skipped = [], 0
    for row in rows:
        booking_id = get(row, "id")
        if not booking_id or not booking_id.isdigit():
            skipped += 1
            continue

        start_raw = get(row, "start")
        appointment_dt = None
        if start_raw:
            try:
                appointment_dt = datetime.fromisoformat(start_raw).replace(tzinfo=PACIFIC)
            except ValueError:
                logger.warning("Booking %s: unparseable StartDateTime %r", booking_id, start_raw)

        first_name, last_name = split_name(get(row, "fullname"))

        records.append({
            "booking_id": booking_id,
            "time_column": get(row, "created"),
            "start_date_time": start_raw,
            "appointment_datetime": appointment_dt.isoformat() if appointment_dt else None,
            "appointment_date": appointment_dt.date().isoformat() if appointment_dt else None,
            "appointment_time": appointment_dt.strftime("%H:%M") if appointment_dt else None,
            "appointment_time_12h": appointment_dt.strftime("%I:%M %p") if appointment_dt else None,
            "location_id": get(row, "location_id"),
            "location_name": get(row, "location_name"),
            "location_business_id": get(row, "business_id"),
            "location_business_name": get(row, "business_name"),
            "offering_name": get(row, "offering"),
            "booking_status_label": get(row, "status"),
            "is_google_booking": "true" if get(row, "interface") == "RWG" else "false",
            "customer_name": get(row, "fullname"),
            "client_first_name": first_name,
            "client_last_name": last_name,
            "client_email": get(row, "email"),
            "extracted_at": extracted_at.isoformat(),
            "data_date": data_date.isoformat(),
        })

    if skipped:
        logger.info("Skipped %d rows without a numeric booking id", skipped)
    return records


def summarize(records):
    by_location = {}
    for record in records:
        key = (record["location_business_id"], record["location_id"], record["location_name"])
        by_location[key] = by_location.get(key, 0) + 1
    logger.info("Locations in this feed:")
    for (business, location, name), count in sorted(by_location.items()):
        logger.info("  business=%s location=%s %s: %d rows", business, location, name, count)

    dates = [r["appointment_date"] for r in records if r["appointment_date"]]
    if dates:
        logger.info("Appointment dates %s -> %s", min(dates), max(dates))


def save(records):
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise SystemExit("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    client = create_client(url, key)
    written = 0
    for start in range(0, len(records), UPSERT_CHUNK):
        chunk = records[start:start + UPSERT_CHUNK]
        client.table("daily_appointments").upsert(chunk, on_conflict="booking_id").execute()
        written += len(chunk)
        logger.info("Upserted %d/%d", written, len(records))
    return written


def main():
    records = transform(fetch_csv())
    summarize(records)

    if os.environ.get("DRY_RUN") == "1":
        logger.info("DRY RUN - %d records prepared, nothing written", len(records))
        return 0

    written = save(records)
    logger.info("Done: %d records written to daily_appointments", written)
    return 0


if __name__ == "__main__":
    sys.exit(main())
