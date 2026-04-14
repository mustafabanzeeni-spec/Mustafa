from __future__ import annotations

import sqlite3
import re
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from flask import Flask, redirect, render_template, request, send_file, url_for


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "attendance.db"
LAST_LINKED_SYNC_AT_RIDER: float = 0.0
LAST_LINKED_SYNC_AT_EMPLOYEE: float = 0.0
LAST_LINKED_SYNC_AT_EMPLOYEE_ATTENDANCE: float = 0.0
LAST_LINKED_SYNC_AT_DRIVER_ATTENDANCE: float = 0.0
LINKED_SYNC_INTERVAL_SECONDS = 30

app = Flask(__name__)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS shifts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    shift_start TEXT NOT NULL,
    shift_end TEXT NOT NULL,
    FOREIGN KEY (employee_id) REFERENCES employees(id),
    UNIQUE(employee_id, date)
);

CREATE TABLE IF NOT EXISTS attendance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    check_in TEXT NOT NULL,
    check_out TEXT NOT NULL,
    FOREIGN KEY (employee_id) REFERENCES employees(id),
    UNIQUE(employee_id, date)
);

CREATE TABLE IF NOT EXISTS drivers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS rider_shifts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    shift_window TEXT NOT NULL,
    zone_code TEXT NOT NULL,
    area_name TEXT NOT NULL,
    driver_name TEXT NOT NULL,
    assignment_type TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS driver_attendance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    check_in TEXT NOT NULL,
    check_out TEXT NOT NULL,
    FOREIGN KEY (driver_id) REFERENCES drivers(id),
    UNIQUE(driver_id, date)
);

CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

SHIFT_ALIASES = {
    "employee_name": {"employeename", "employee", "name", "staffname", "fullname"},
    "date": {"date", "workdate", "day", "shiftdate"},
    "shift_start": {"shiftstart", "start", "starttime", "shiftin", "timein"},
    "shift_end": {"shiftend", "end", "endtime", "shiftout", "timeout"},
}

ATTENDANCE_ALIASES = {
    "employee_name": {"employeename", "employee", "name", "staffname", "fullname"},
    "date": {"date", "workdate", "day", "attendancedate"},
    "check_in": {"checkin", "in", "intime", "timein", "clockin"},
    "check_out": {"checkout", "out", "outtime", "timeout", "clockout"},
}

RIDER_SHIFT_ALIASES = {
    "date": {"date", "day", "workdate", "shiftdate"},
    "shift_window": {"shiftwindow", "shift", "shifttime", "window", "column2"},
    "zone_code": {"zonecode", "zonecode", "routecode", "code", "truckcode", "column3"},
    "area_name": {"areaname", "area", "location", "zone", "column4"},
    "driver_name": {"drivername", "ridername", "name", "column5"},
    "assignment_type": {"assignmenttype", "type", "status", "statusyes", "column7"},
}

DRIVER_ATTENDANCE_ALIASES = {
    "driver_name": {"drivername", "driver", "ridername", "name"},
    "date": {"date", "workdate", "day", "attendancedate"},
    "check_in": {"checkin", "in", "intime", "timein", "clockin"},
    "check_out": {"checkout", "out", "outtime", "timeout", "clockout"},
}


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.executescript(SCHEMA_SQL)


def get_setting(key: str, default: str = "") -> str:
    with get_connection() as conn:
        row = conn.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
    if not row:
        return default
    return str(row["value"])


def set_setting(key: str, value: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO app_settings(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        conn.commit()


def parse_time(value: str) -> datetime:
    cleaned = str(value).strip()
    for fmt in ("%H:%M", "%H:%M:%S", "%I:%M %p", "%I:%M:%S %p"):
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported time format: {value}")


def format_time_value(value: object) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, str):
        value = value.strip()
        for fmt in ("%H:%M", "%H:%M:%S", "%I:%M %p", "%I:%M:%S %p"):
            try:
                return datetime.strptime(value, fmt).strftime("%H:%M")
            except ValueError:
                continue
        try:
            return pd.to_datetime(value).strftime("%H:%M")
        except Exception:
            return value
    if isinstance(value, datetime):
        return value.strftime("%H:%M")
    if hasattr(value, "hour") and hasattr(value, "minute"):
        return f"{value.hour:02d}:{value.minute:02d}"
    return str(value)


def format_date_value(value: object) -> str:
    if pd.isna(value):
        return ""
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d")
    except Exception:
        return str(value)


def load_uploaded_table(upload) -> pd.DataFrame:
    filename = (upload.filename or "").lower()
    raw = upload.read()
    if not raw:
        return pd.DataFrame()
    data = BytesIO(raw)
    if filename.endswith(".csv"):
        df = pd.read_csv(data)
    elif filename.endswith(".xlsx") or filename.endswith(".xls"):
        df = pd.read_excel(data)
    else:
        return pd.DataFrame()
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    df.columns = [str(col).strip() for col in df.columns]
    if all(col.lower().startswith("unnamed:") or not col for col in df.columns):
        first_row = df.iloc[0].fillna("").astype(str).str.strip()
        df = df.iloc[1:].copy()
        df.columns = first_row.tolist()
    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")
    df.columns = [str(col).strip() for col in df.columns]
    return df


def normalize_column_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower().strip())


def map_columns(df: pd.DataFrame, aliases: dict[str, set[str]]) -> pd.DataFrame:
    normalized_to_original = {normalize_column_name(col): col for col in df.columns}
    rename_map: dict[str, str] = {}
    for target, allowed in aliases.items():
        for candidate in allowed:
            match = normalized_to_original.get(candidate)
            if match:
                rename_map[match] = target
                break
    return df.rename(columns=rename_map)


def required_columns_feedback(df: pd.DataFrame, aliases: dict[str, set[str]]) -> tuple[pd.DataFrame, list[str], list[str]]:
    mapped = map_columns(df, aliases)
    missing = [col for col in aliases if col not in mapped.columns]
    available = [str(col) for col in mapped.columns]
    return mapped, missing, available


def get_or_create_employee_id(conn: sqlite3.Connection, employee_name: str) -> int:
    normalized_name = employee_name.strip()
    conn.execute("INSERT OR IGNORE INTO employees(name) VALUES (?)", (normalized_name,))
    row = conn.execute("SELECT id FROM employees WHERE name = ?", (normalized_name,)).fetchone()
    return row["id"]


def get_or_create_driver_id(conn: sqlite3.Connection, driver_name: str) -> int:
    normalized_name = driver_name.strip()
    conn.execute("INSERT OR IGNORE INTO drivers(name) VALUES (?)", (normalized_name,))
    row = conn.execute("SELECT id FROM drivers WHERE name = ?", (normalized_name,)).fetchone()
    return row["id"]


def import_shifts_dataframe(df: pd.DataFrame) -> int:
    df = map_columns(df, SHIFT_ALIASES)
    required = {"employee_name", "date", "shift_start", "shift_end"}
    if df.empty or not required.issubset(set(df.columns)):
        return 0

    imported = 0
    with get_connection() as conn:
        for _, row in df.iterrows():
            employee_name = str(row.get("employee_name", "")).strip()
            if not employee_name:
                continue
            date = format_date_value(row.get("date"))
            shift_start = format_time_value(row.get("shift_start"))
            shift_end = format_time_value(row.get("shift_end"))
            if not (date and shift_start and shift_end):
                continue
            employee_id = get_or_create_employee_id(conn, employee_name)
            conn.execute(
                """
                INSERT INTO shifts(employee_id, date, shift_start, shift_end)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(employee_id, date)
                DO UPDATE SET shift_start = excluded.shift_start, shift_end = excluded.shift_end
                """,
                (employee_id, date, shift_start, shift_end),
            )
            imported += 1
        conn.commit()
    return imported


def replace_employee_shifts_from_dataframe(df: pd.DataFrame) -> int:
    df = map_columns(df, SHIFT_ALIASES)
    required = {"employee_name", "shift_start", "shift_end"}
    if df.empty or not required.issubset(set(df.columns)):
        return 0

    inserted = 0
    # If sheet has no date column, treat shifts as baseline defaults
    # so they apply to all attendance dates until changed.
    default_date = "1900-01-01"
    with get_connection() as conn:
        conn.execute("DELETE FROM shifts")
        for _, row in df.iterrows():
            employee_name = str(row.get("employee_name", "")).strip()
            if not employee_name:
                continue
            raw_date = format_date_value(row.get("date")) if "date" in df.columns else ""
            date = raw_date or default_date
            shift_start = format_time_value(row.get("shift_start"))
            shift_end = format_time_value(row.get("shift_end"))
            if not (date and shift_start and shift_end):
                continue
            employee_id = get_or_create_employee_id(conn, employee_name)
            conn.execute(
                """
                INSERT INTO shifts(employee_id, date, shift_start, shift_end)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(employee_id, date)
                DO UPDATE SET shift_start = excluded.shift_start, shift_end = excluded.shift_end
                """,
                (employee_id, date, shift_start, shift_end),
            )
            inserted += 1
        conn.commit()
    return inserted


def import_attendance_dataframe(df: pd.DataFrame) -> int:
    df = map_columns(df, ATTENDANCE_ALIASES)
    required = {"employee_name", "date", "check_in", "check_out"}
    if df.empty or not required.issubset(set(df.columns)):
        return 0

    imported = 0
    with get_connection() as conn:
        for _, row in df.iterrows():
            employee_name = str(row.get("employee_name", "")).strip()
            if not employee_name:
                continue
            date = format_date_value(row.get("date"))
            check_in = format_time_value(row.get("check_in"))
            check_out = format_time_value(row.get("check_out"))
            if not (date and check_in and check_out):
                continue
            employee_id = get_or_create_employee_id(conn, employee_name)
            conn.execute(
                """
                INSERT INTO attendance(employee_id, date, check_in, check_out)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(employee_id, date)
                DO UPDATE SET check_in = excluded.check_in, check_out = excluded.check_out
                """,
                (employee_id, date, check_in, check_out),
            )
            imported += 1
        conn.commit()
    return imported


def replace_employee_attendance_from_dataframe(df: pd.DataFrame) -> int:
    df = map_columns(df, ATTENDANCE_ALIASES)
    required = {"employee_name", "date", "check_in", "check_out"}
    if df.empty or not required.issubset(set(df.columns)):
        return 0

    inserted = 0
    with get_connection() as conn:
        conn.execute("DELETE FROM attendance")
        for _, row in df.iterrows():
            employee_name = str(row.get("employee_name", "")).strip()
            if not employee_name:
                continue
            date = format_date_value(row.get("date"))
            check_in = format_time_value(row.get("check_in"))
            check_out = format_time_value(row.get("check_out"))
            if not (date and check_in and check_out):
                continue
            employee_id = get_or_create_employee_id(conn, employee_name)
            conn.execute(
                """
                INSERT INTO attendance(employee_id, date, check_in, check_out)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(employee_id, date)
                DO UPDATE SET check_in = excluded.check_in, check_out = excluded.check_out
                """,
                (employee_id, date, check_in, check_out),
            )
            inserted += 1
        conn.commit()
    return inserted


def import_rider_shifts_dataframe(df: pd.DataFrame) -> int:
    df = map_columns(df, RIDER_SHIFT_ALIASES)
    required = {
        "date",
        "shift_window",
        "zone_code",
        "area_name",
        "driver_name",
        "assignment_type",
    }
    if df.empty or not required.issubset(set(df.columns)):
        return 0

    imported = 0
    with get_connection() as conn:
        for _, row in df.iterrows():
            date = format_date_value(row.get("date"))
            shift_window = str(row.get("shift_window", "")).strip()
            zone_code = str(row.get("zone_code", "")).strip()
            area_name = str(row.get("area_name", "")).strip()
            driver_name = str(row.get("driver_name", "")).strip()
            assignment_type = str(row.get("assignment_type", "")).strip()
            if not all([date, shift_window, zone_code, area_name, driver_name, assignment_type]):
                continue
            conn.execute(
                """
                INSERT INTO rider_shifts(date, shift_window, zone_code, area_name, driver_name, assignment_type)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (date, shift_window, zone_code, area_name, driver_name, assignment_type),
            )
            conn.execute("INSERT OR IGNORE INTO drivers(name) VALUES (?)", (driver_name,))
            imported += 1
        conn.commit()
    return imported


def replace_rider_shifts_from_dataframe(df: pd.DataFrame) -> int:
    df = map_columns(df, RIDER_SHIFT_ALIASES)
    required = {
        "date",
        "shift_window",
        "zone_code",
        "area_name",
        "driver_name",
        "assignment_type",
    }
    if df.empty or not required.issubset(set(df.columns)):
        return 0

    inserted = 0
    with get_connection() as conn:
        conn.execute("DELETE FROM rider_shifts")
        for _, row in df.iterrows():
            date = format_date_value(row.get("date"))
            shift_window = str(row.get("shift_window", "")).strip()
            zone_code = str(row.get("zone_code", "")).strip()
            area_name = str(row.get("area_name", "")).strip()
            driver_name = str(row.get("driver_name", "")).strip()
            assignment_type = str(row.get("assignment_type", "")).strip()
            if not all([date, shift_window, zone_code, area_name, driver_name, assignment_type]):
                continue
            conn.execute(
                """
                INSERT INTO rider_shifts(date, shift_window, zone_code, area_name, driver_name, assignment_type)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (date, shift_window, zone_code, area_name, driver_name, assignment_type),
            )
            conn.execute("INSERT OR IGNORE INTO drivers(name) VALUES (?)", (driver_name,))
            inserted += 1
        conn.commit()
    return inserted


def google_sheet_to_csv_url(url: str, sheet_name: str | None = None) -> str:
    cleaned = url.strip()
    if sheet_name:
        match = re.search(r"/d/([a-zA-Z0-9-_]+)", cleaned)
        if match:
            sheet_id = match.group(1)
            return (
                f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?"
                f"tqx=out:csv&sheet={quote_plus(sheet_name)}"
            )
    if "output=csv" in cleaned or "/export?" in cleaned:
        return cleaned
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", cleaned)
    if not match:
        return cleaned
    sheet_id = match.group(1)
    gid_match = re.search(r"gid=([0-9]+)", cleaned)
    gid = gid_match.group(1) if gid_match else "0"
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def load_google_sheet_dataframe(url: str, tab_candidates: list[str]) -> pd.DataFrame:
    last_exc: Exception | None = None
    for tab in tab_candidates:
        try:
            csv_url = google_sheet_to_csv_url(url, sheet_name=tab)
            df = pd.read_csv(csv_url)
            df.columns = [str(col).strip() for col in df.columns]
            if not df.empty and len(df.columns) > 0:
                return df
        except Exception as exc:
            last_exc = exc
            continue
    if last_exc:
        raise last_exc
    raise ValueError("Unable to read Google Sheet tabs")


def sync_linked_rider_shifts(force: bool = False) -> tuple[bool, str]:
    global LAST_LINKED_SYNC_AT_RIDER

    linked_url = get_setting("linked_rider_sheet_url", "").strip()
    if not linked_url:
        set_setting("linked_rider_sync_status", "idle")
        return False, "Google Sheet URL is not set"

    now_ts = datetime.now().timestamp()
    if not force and (now_ts - LAST_LINKED_SYNC_AT_RIDER) < LINKED_SYNC_INTERVAL_SECONDS:
        return False, "Auto-sync waiting for next interval"

    try:
        csv_url = google_sheet_to_csv_url(linked_url)
        df = pd.read_csv(csv_url)
        df.columns = [str(col).strip() for col in df.columns]
        imported = replace_rider_shifts_from_dataframe(df)
        LAST_LINKED_SYNC_AT_RIDER = now_ts
        set_setting("linked_rider_last_sync", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        set_setting("linked_rider_sync_status", "ok")
        return True, f"Google Sheet synced: {imported} rider shift rows"
    except Exception as exc:
        set_setting("linked_rider_sync_status", "error")
        return False, f"Google Sheet sync failed: {exc}"


def sync_linked_employee_shifts(force: bool = False) -> tuple[bool, str]:
    global LAST_LINKED_SYNC_AT_EMPLOYEE

    linked_url = get_setting("linked_employee_sheet_url", "").strip()
    if not linked_url:
        set_setting("linked_employee_sync_status", "idle")
        return False, "Employee Google Sheet URL is not set"
    sheet_tab = get_setting("linked_employee_sheet_tab", "Employee Shift")

    now_ts = datetime.now().timestamp()
    if not force and (now_ts - LAST_LINKED_SYNC_AT_EMPLOYEE) < LINKED_SYNC_INTERVAL_SECONDS:
        return False, "Auto-sync waiting for next interval"

    try:
        csv_url = google_sheet_to_csv_url(linked_url, sheet_name=sheet_tab)
        df = pd.read_csv(csv_url)
        df.columns = [str(col).strip() for col in df.columns]
        imported = replace_employee_shifts_from_dataframe(df)
        LAST_LINKED_SYNC_AT_EMPLOYEE = now_ts
        set_setting("linked_employee_last_sync", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        set_setting("linked_employee_sync_status", "ok")
        return True, f"Employee sheet synced: {imported} shift rows"
    except Exception as exc:
        set_setting("linked_employee_sync_status", "error")
        return False, f"Employee Google Sheet sync failed: {exc}"


def sync_linked_employee_attendance(force: bool = False) -> tuple[bool, str]:
    global LAST_LINKED_SYNC_AT_EMPLOYEE_ATTENDANCE

    linked_url = (
        get_setting("linked_employee_attendance_sheet_url", "").strip()
        or get_setting("linked_employee_sheet_url", "").strip()
        or get_setting("linked_rider_sheet_url", "").strip()
    )
    if not linked_url:
        set_setting("linked_employee_attendance_sync_status", "idle")
        return False, "Employee attendance sheet URL is not set"
    sheet_tab = get_setting("linked_employee_attendance_sheet_tab", "Employee attendance")

    now_ts = datetime.now().timestamp()
    if not force and (now_ts - LAST_LINKED_SYNC_AT_EMPLOYEE_ATTENDANCE) < LINKED_SYNC_INTERVAL_SECONDS:
        return False, "Auto-sync waiting for next interval"

    try:
        df = load_google_sheet_dataframe(
            linked_url,
            [sheet_tab, "Employee attendance", "Employee Attendance", "eMPLOYEE aTEENDANCE"],
        )
        imported = replace_employee_attendance_from_dataframe(df)
        LAST_LINKED_SYNC_AT_EMPLOYEE_ATTENDANCE = now_ts
        set_setting("linked_employee_attendance_last_sync", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        set_setting("linked_employee_attendance_sync_status", "ok")
        return True, f"Employee attendance synced: {imported} rows"
    except Exception as exc:
        set_setting("linked_employee_attendance_sync_status", "error")
        return False, f"Employee attendance sync failed: {exc}"


def sync_linked_driver_attendance(force: bool = False) -> tuple[bool, str]:
    global LAST_LINKED_SYNC_AT_DRIVER_ATTENDANCE

    linked_url = (
        get_setting("linked_driver_attendance_sheet_url", "").strip()
        or get_setting("linked_rider_sheet_url", "").strip()
    )
    if not linked_url:
        set_setting("linked_driver_attendance_sync_status", "idle")
        return False, "Driver attendance sheet URL is not set"
    sheet_tab = get_setting("linked_driver_attendance_sheet_tab", "Driver Ateendance")

    now_ts = datetime.now().timestamp()
    if not force and (now_ts - LAST_LINKED_SYNC_AT_DRIVER_ATTENDANCE) < LINKED_SYNC_INTERVAL_SECONDS:
        return False, "Auto-sync waiting for next interval"

    try:
        df = load_google_sheet_dataframe(
            linked_url,
            [sheet_tab, "Driver Ateendance", "Driver Attendance", "Driver aTTENDANCE"],
        )
        imported = replace_driver_attendance_from_dataframe(df)
        LAST_LINKED_SYNC_AT_DRIVER_ATTENDANCE = now_ts
        set_setting("linked_driver_attendance_last_sync", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        set_setting("linked_driver_attendance_sync_status", "ok")
        return True, f"Driver attendance synced: {imported} rows"
    except Exception as exc:
        set_setting("linked_driver_attendance_sync_status", "error")
        return False, f"Driver attendance sync failed: {exc}"


def import_driver_attendance_dataframe(df: pd.DataFrame) -> int:
    df = map_columns(df, DRIVER_ATTENDANCE_ALIASES)
    required = {"driver_name", "date", "check_in", "check_out"}
    if df.empty or not required.issubset(set(df.columns)):
        return 0

    imported = 0
    with get_connection() as conn:
        for _, row in df.iterrows():
            driver_name = str(row.get("driver_name", "")).strip()
            if not driver_name:
                continue
            date = format_date_value(row.get("date"))
            check_in = format_time_value(row.get("check_in"))
            check_out = format_time_value(row.get("check_out"))
            if not (date and check_in and check_out):
                continue
            driver_id = get_or_create_driver_id(conn, driver_name)
            conn.execute(
                """
                INSERT INTO driver_attendance(driver_id, date, check_in, check_out)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(driver_id, date)
                DO UPDATE SET check_in = excluded.check_in, check_out = excluded.check_out
                """,
                (driver_id, date, check_in, check_out),
            )
            imported += 1
        conn.commit()
    return imported


def replace_driver_attendance_from_dataframe(df: pd.DataFrame) -> int:
    df = map_columns(df, DRIVER_ATTENDANCE_ALIASES)
    required = {"driver_name", "date", "check_in", "check_out"}
    if df.empty or not required.issubset(set(df.columns)):
        return 0

    inserted = 0
    with get_connection() as conn:
        conn.execute("DELETE FROM driver_attendance")
        for _, row in df.iterrows():
            driver_name = str(row.get("driver_name", "")).strip()
            if not driver_name:
                continue
            date = format_date_value(row.get("date"))
            check_in = format_time_value(row.get("check_in"))
            check_out = format_time_value(row.get("check_out"))
            if not (date and check_in and check_out):
                continue
            driver_id = get_or_create_driver_id(conn, driver_name)
            conn.execute(
                """
                INSERT INTO driver_attendance(driver_id, date, check_in, check_out)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(driver_id, date)
                DO UPDATE SET check_in = excluded.check_in, check_out = excluded.check_out
                """,
                (driver_id, date, check_in, check_out),
            )
            inserted += 1
        conn.commit()
    return inserted


def calculate_hours(check_in: str, check_out: str) -> float:
    in_dt = parse_time(check_in)
    out_dt = parse_time(check_out)
    if out_dt < in_dt:
        out_dt += timedelta(days=1)
    return round((out_dt - in_dt).total_seconds() / 3600, 2)


def split_shift_window(shift_window: str) -> tuple[str, str]:
    if not shift_window:
        return "-", "-"
    raw = str(shift_window).strip().replace("\n", " ")
    raw = re.sub(r"[\u064B-\u065F]", "", raw)

    time_matches = re.findall(r"\d{1,2}:\d{2}\s*[AaPp][Mm]", raw)
    if len(time_matches) >= 2:
        start = format_time_value(time_matches[0]).strip()
        end = format_time_value(time_matches[1]).strip()
    else:
        parts = re.split(r"\s*-\s*|\s*–\s*", raw, maxsplit=1)
        if len(parts) != 2:
            return "-", "-"
        start = format_time_value(parts[0]).strip()
        end = format_time_value(parts[1]).strip()

    if not start or not end:
        return "-", "-"
    return start, end


def normalize_person_name(name: str) -> str:
    return re.sub(r"\s+", "", str(name).strip().lower())


def choose_shift_window_for_driver(
    driver_name: str,
    attendance_date: str,
    rider_shift_rows: list[sqlite3.Row],
) -> str | None:
    driver_norm = normalize_person_name(driver_name)
    date_candidates: list[tuple[str, str]] = []
    fallback_candidates: list[tuple[str, str]] = []

    for row in rider_shift_rows:
        shift_driver = row["driver_name"]
        shift_norm = normalize_person_name(shift_driver)
        same_or_partial = (
            driver_norm == shift_norm
            or driver_norm in shift_norm
            or shift_norm in driver_norm
        )
        if not same_or_partial:
            continue

        row_date = str(row["date"])
        row_shift = row["shift_window"]
        if row_date <= attendance_date:
            date_candidates.append((row_date, row_shift))
        fallback_candidates.append((row_date, row_shift))

    if date_candidates:
        date_candidates.sort(key=lambda x: x[0], reverse=True)
        return date_candidates[0][1]
    if fallback_candidates:
        fallback_candidates.sort(key=lambda x: x[0], reverse=True)
        return fallback_candidates[0][1]
    return None


def report_rows(
    grace_minutes: int = 5,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict]:
    filters: list[str] = []
    params: list[str] = []
    if start_date:
        filters.append("a.date >= ?")
        params.append(start_date)
    if end_date:
        filters.append("a.date <= ?")
        params.append(end_date)
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT
                e.name AS employee_name,
                a.date AS date,
                a.check_in AS check_in,
                a.check_out AS check_out,
                COALESCE(
                    s.shift_start,
                    (
                        SELECT s2.shift_start
                        FROM shifts s2
                        WHERE s2.employee_id = a.employee_id
                          AND s2.date <= a.date
                        ORDER BY s2.date DESC
                        LIMIT 1
                    ),
                    (
                        SELECT s3.shift_start
                        FROM shifts s3
                        WHERE s3.employee_id = a.employee_id
                        ORDER BY s3.date DESC
                        LIMIT 1
                    )
                ) AS shift_start,
                COALESCE(
                    s.shift_end,
                    (
                        SELECT s2.shift_end
                        FROM shifts s2
                        WHERE s2.employee_id = a.employee_id
                          AND s2.date <= a.date
                        ORDER BY s2.date DESC
                        LIMIT 1
                    ),
                    (
                        SELECT s3.shift_end
                        FROM shifts s3
                        WHERE s3.employee_id = a.employee_id
                        ORDER BY s3.date DESC
                        LIMIT 1
                    )
                ) AS shift_end
            FROM attendance a
            JOIN employees e ON e.id = a.employee_id
            LEFT JOIN shifts s ON s.employee_id = a.employee_id AND s.date = a.date
            {where_clause}
            ORDER BY a.date DESC, e.name ASC
            """,
            params,
        ).fetchall()

    output: list[dict] = []
    grace = timedelta(minutes=grace_minutes)

    for row in rows:
        worked_hours = calculate_hours(row["check_in"], row["check_out"])
        status = "No shift assigned"
        overtime_hours = 0.0

        if row["shift_start"]:
            check_in_dt = parse_time(row["check_in"])
            shift_start_dt = parse_time(row["shift_start"])
            status = "Late" if check_in_dt > (shift_start_dt + grace) else "On time"

            try:
                shift_hours = calculate_hours(row["shift_start"], row["shift_end"])
                overtime_hours = round(max(0.0, worked_hours - shift_hours), 2)
            except Exception:
                overtime_hours = 0.0

        output.append(
            {
                "employee_name": row["employee_name"],
                "date": row["date"],
                "check_in": row["check_in"],
                "check_out": row["check_out"],
                "shift_start": row["shift_start"] or "-",
                "shift_end": row["shift_end"] or "-",
                "worked_hours": worked_hours,
                "overtime_hours": overtime_hours,
                "status": status,
            }
        )

    return output


def resolve_report_range(
    period: str,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str | None, str | None]:
    today = datetime.today().date()
    if start_date and end_date:
        return start_date, end_date

    if period == "day":
        return today.isoformat(), today.isoformat()

    if period == "week":
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=6)
        return start.isoformat(), end.isoformat()

    if period == "month":
        month_start = today.replace(day=1)
        if month_start.month == 12:
            next_month_start = month_start.replace(year=month_start.year + 1, month=1, day=1)
        else:
            next_month_start = month_start.replace(month=month_start.month + 1, day=1)
        month_end = next_month_start - timedelta(days=1)
        return month_start.isoformat(), month_end.isoformat()

    if start_date:
        return start_date, end_date or start_date
    if end_date:
        return start_date or end_date, end_date

    return None, None


@app.route("/")
def home():
    return redirect(url_for("shifts"))


@app.before_request
def auto_sync_linked_rider_shifts() -> None:
    # Passive auto-sync: whenever user uses the app, update linked sheets if changed.
    sync_linked_employee_shifts(force=False)
    sync_linked_rider_shifts(force=False)
    sync_linked_employee_attendance(force=False)
    sync_linked_driver_attendance(force=False)


@app.route("/employees", methods=["GET", "POST"])
def employees():
    return redirect(url_for("shifts"))


@app.post("/employees/delete/<int:employee_id>")
def delete_employee(employee_id: int):
    with get_connection() as conn:
        conn.execute("DELETE FROM attendance WHERE employee_id = ?", (employee_id,))
        conn.execute("DELETE FROM shifts WHERE employee_id = ?", (employee_id,))
        conn.execute("DELETE FROM employees WHERE id = ?", (employee_id,))
        conn.commit()
    return redirect(url_for("shifts"))


@app.route("/shifts", methods=["GET", "POST"])
def shifts():
    with get_connection() as conn:
        employee_rows = conn.execute("SELECT id, name FROM employees ORDER BY name").fetchall()

    if request.method == "POST":
        action = request.form.get("action", "single")
        if action == "set_linked_sheet":
            linked_url = request.form.get("linked_sheet_url", "").strip()
            sheet_tab = request.form.get("linked_sheet_tab", "Employee Shift").strip() or "Employee Shift"
            if linked_url:
                set_setting("linked_employee_sheet_url", linked_url)
                set_setting("linked_employee_sheet_tab", sheet_tab)
                set_setting("linked_employee_sync_status", "idle")
                return redirect(url_for("shifts", msg="Employee Google Sheet URL saved"))
            return redirect(url_for("shifts", msg="Please enter a valid Google Sheet URL"))

        if action == "sync_linked":
            _, message = sync_linked_employee_shifts(force=True)
            return redirect(url_for("shifts", msg=message))

        if action == "clear":
            with get_connection() as conn:
                conn.execute("DELETE FROM shifts")
                conn.commit()
            return redirect(url_for("shifts", msg="All shifts cleared"))

        if action == "upload":
            upload = request.files.get("shift_file")
            if upload and upload.filename:
                raw_df = load_uploaded_table(upload)
                if raw_df.empty:
                    return redirect(url_for("shifts", msg="File is empty or unsupported"))
                checked_df, missing, available = required_columns_feedback(raw_df, SHIFT_ALIASES)
                if missing:
                    return redirect(
                        url_for(
                            "shifts",
                            msg=f"Missing columns: {', '.join(missing)} | Found: {', '.join(available)}",
                        )
                    )
                imported_count = import_shifts_dataframe(checked_df)
                return redirect(url_for("shifts", msg=f"Imported {imported_count} shift rows"))
            return redirect(url_for("shifts", msg="No file selected"))

        employee_id = request.form.get("employee_id")
        date = request.form.get("date")
        shift_start = request.form.get("shift_start")
        shift_end = request.form.get("shift_end")

        if employee_id and date and shift_start and shift_end:
            with get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO shifts(employee_id, date, shift_start, shift_end)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(employee_id, date)
                    DO UPDATE SET shift_start = excluded.shift_start, shift_end = excluded.shift_end
                    """,
                    (employee_id, date, shift_start, shift_end),
                )
                conn.commit()
        return redirect(url_for("shifts", msg="Shift saved"))

    with get_connection() as conn:
        shift_rows = conn.execute(
            """
            SELECT s.date, s.shift_start, s.shift_end, e.name AS employee_name
            FROM shifts s
            JOIN employees e ON e.id = s.employee_id
            ORDER BY s.date DESC, e.name ASC
            """
        ).fetchall()

    return render_template(
        "shifts.html",
        employees=employee_rows,
        shifts=shift_rows,
        msg=request.args.get("msg", ""),
        linked_sheet_url=get_setting("linked_employee_sheet_url", ""),
        linked_sheet_tab=get_setting("linked_employee_sheet_tab", "Employee Shift"),
        linked_last_sync=get_setting("linked_employee_last_sync", "Never"),
        linked_status=get_setting("linked_employee_sync_status", "idle"),
    )


@app.route("/attendance", methods=["GET", "POST"])
def attendance():
    with get_connection() as conn:
        employee_rows = conn.execute("SELECT id, name FROM employees ORDER BY name").fetchall()

    if request.method == "POST":
        action = request.form.get("action", "single")
        if action == "set_linked_sheet":
            linked_url = request.form.get("linked_sheet_url", "").strip()
            sheet_tab = request.form.get("linked_sheet_tab", "eMPLOYEE aTEENDANCE").strip() or "eMPLOYEE aTEENDANCE"
            if linked_url:
                set_setting("linked_employee_attendance_sheet_url", linked_url)
                set_setting("linked_employee_attendance_sheet_tab", sheet_tab)
                set_setting("linked_employee_attendance_sync_status", "idle")
                return redirect(url_for("attendance", msg="Employee attendance Google Sheet URL saved"))
            return redirect(url_for("attendance", msg="Please enter a valid Google Sheet URL"))

        if action == "sync_linked":
            _, message = sync_linked_employee_attendance(force=True)
            return redirect(url_for("attendance", msg=message))

        if action == "clear":
            with get_connection() as conn:
                conn.execute("DELETE FROM attendance")
                conn.commit()
            return redirect(url_for("attendance", msg="All attendance cleared"))

        if action == "upload":
            upload = request.files.get("attendance_file")
            if upload and upload.filename:
                raw_df = load_uploaded_table(upload)
                if raw_df.empty:
                    return redirect(url_for("attendance", msg="File is empty or unsupported"))
                checked_df, missing, available = required_columns_feedback(raw_df, ATTENDANCE_ALIASES)
                if missing:
                    return redirect(
                        url_for(
                            "attendance",
                            msg=f"Missing columns: {', '.join(missing)} | Found: {', '.join(available)}",
                        )
                    )
                imported_count = import_attendance_dataframe(checked_df)
                return redirect(url_for("attendance", msg=f"Imported {imported_count} attendance rows"))
            return redirect(url_for("attendance", msg="No file selected"))

        employee_id = request.form.get("employee_id")
        date = request.form.get("date")
        check_in = request.form.get("check_in")
        check_out = request.form.get("check_out")

        if employee_id and date and check_in and check_out:
            with get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO attendance(employee_id, date, check_in, check_out)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(employee_id, date)
                    DO UPDATE SET check_in = excluded.check_in, check_out = excluded.check_out
                    """,
                    (employee_id, date, check_in, check_out),
                )
                conn.commit()
        return redirect(url_for("attendance", msg="Attendance saved"))

    with get_connection() as conn:
        attendance_rows = conn.execute(
            """
            SELECT a.date, a.check_in, a.check_out, e.name AS employee_name
            FROM attendance a
            JOIN employees e ON e.id = a.employee_id
            ORDER BY a.date DESC, e.name ASC
            """
        ).fetchall()

    return render_template(
        "attendance.html",
        employees=employee_rows,
        attendance=attendance_rows,
        msg=request.args.get("msg", ""),
        linked_sheet_url=get_setting("linked_employee_attendance_sheet_url", get_setting("linked_employee_sheet_url", get_setting("linked_rider_sheet_url", ""))),
        linked_sheet_tab=get_setting("linked_employee_attendance_sheet_tab", "eMPLOYEE aTEENDANCE"),
        linked_last_sync=get_setting("linked_employee_attendance_last_sync", "Never"),
        linked_status=get_setting("linked_employee_attendance_sync_status", "idle"),
    )


@app.route("/riders-shifts", methods=["GET", "POST"])
def riders_shifts():
    if request.method == "POST":
        action = request.form.get("action", "single")
        if action == "set_linked_sheet":
            linked_url = request.form.get("linked_sheet_url", "").strip()
            if linked_url:
                set_setting("linked_rider_sheet_url", linked_url)
                set_setting("linked_rider_sync_status", "idle")
                return redirect(url_for("riders_shifts", msg="Google Sheet URL saved"))
            return redirect(url_for("riders_shifts", msg="Please enter a valid Google Sheet URL"))

        if action == "sync_linked":
            _, message = sync_linked_rider_shifts(force=True)
            return redirect(url_for("riders_shifts", msg=message))

        if action == "clear":
            with get_connection() as conn:
                conn.execute("DELETE FROM rider_shifts")
                conn.commit()
            return redirect(url_for("riders_shifts", msg="All rider shifts cleared"))

        if action == "upload":
            upload = request.files.get("rider_shift_file")
            if upload and upload.filename:
                raw_df = load_uploaded_table(upload)
                if raw_df.empty:
                    return redirect(url_for("riders_shifts", msg="File is empty or unsupported"))
                checked_df, missing, available = required_columns_feedback(raw_df, RIDER_SHIFT_ALIASES)
                if missing:
                    return redirect(
                        url_for(
                            "riders_shifts",
                            msg=f"Missing columns: {', '.join(missing)} | Found: {', '.join(available)}",
                        )
                    )
                imported_count = import_rider_shifts_dataframe(checked_df)
                return redirect(url_for("riders_shifts", msg=f"Imported {imported_count} rider shift rows"))
            return redirect(url_for("riders_shifts", msg="No file selected"))

        date = request.form.get("date", "")
        shift_window = request.form.get("shift_window", "")
        zone_code = request.form.get("zone_code", "")
        area_name = request.form.get("area_name", "")
        driver_name = request.form.get("driver_name", "")
        assignment_type = request.form.get("assignment_type", "")
        if all([date, shift_window, zone_code, area_name, driver_name, assignment_type]):
            with get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO rider_shifts(date, shift_window, zone_code, area_name, driver_name, assignment_type)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (date, shift_window, zone_code, area_name, driver_name, assignment_type),
                )
                conn.execute("INSERT OR IGNORE INTO drivers(name) VALUES (?)", (driver_name,))
                conn.commit()
            return redirect(url_for("riders_shifts", msg="Rider shift saved"))

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT date, shift_window, zone_code, area_name, driver_name, assignment_type
            FROM rider_shifts
            ORDER BY date DESC, driver_name ASC
            """
        ).fetchall()
    return render_template(
        "riders_shifts.html",
        rows=rows,
        msg=request.args.get("msg", ""),
        linked_sheet_url=get_setting("linked_rider_sheet_url", ""),
        linked_last_sync=get_setting("linked_rider_last_sync", "Never"),
        linked_status=get_setting("linked_rider_sync_status", "idle"),
    )


@app.route("/drivers-attendance", methods=["GET", "POST"])
def drivers_attendance():
    with get_connection() as conn:
        drivers = conn.execute("SELECT id, name FROM drivers ORDER BY name").fetchall()

    if request.method == "POST":
        action = request.form.get("action", "single")
        if action == "set_linked_sheet":
            linked_url = request.form.get("linked_sheet_url", "").strip()
            sheet_tab = request.form.get("linked_sheet_tab", "Driver aTTENDANCE").strip() or "Driver aTTENDANCE"
            if linked_url:
                set_setting("linked_driver_attendance_sheet_url", linked_url)
                set_setting("linked_driver_attendance_sheet_tab", sheet_tab)
                set_setting("linked_driver_attendance_sync_status", "idle")
                return redirect(url_for("drivers_attendance", msg="Driver attendance Google Sheet URL saved"))
            return redirect(url_for("drivers_attendance", msg="Please enter a valid Google Sheet URL"))

        if action == "sync_linked":
            _, message = sync_linked_driver_attendance(force=True)
            return redirect(url_for("drivers_attendance", msg=message))

        if action == "clear":
            with get_connection() as conn:
                conn.execute("DELETE FROM driver_attendance")
                conn.commit()
            return redirect(url_for("drivers_attendance", msg="All driver attendance cleared"))

        if action == "upload":
            upload = request.files.get("driver_attendance_file")
            if upload and upload.filename:
                raw_df = load_uploaded_table(upload)
                if raw_df.empty:
                    return redirect(url_for("drivers_attendance", msg="File is empty or unsupported"))
                checked_df, missing, available = required_columns_feedback(raw_df, DRIVER_ATTENDANCE_ALIASES)
                if missing:
                    return redirect(
                        url_for(
                            "drivers_attendance",
                            msg=f"Missing columns: {', '.join(missing)} | Found: {', '.join(available)}",
                        )
                    )
                imported_count = import_driver_attendance_dataframe(checked_df)
                return redirect(url_for("drivers_attendance", msg=f"Imported {imported_count} driver attendance rows"))
            return redirect(url_for("drivers_attendance", msg="No file selected"))

        driver_id = request.form.get("driver_id")
        date = request.form.get("date")
        check_in = request.form.get("check_in")
        check_out = request.form.get("check_out")
        if driver_id and date and check_in and check_out:
            with get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO driver_attendance(driver_id, date, check_in, check_out)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(driver_id, date)
                    DO UPDATE SET check_in = excluded.check_in, check_out = excluded.check_out
                    """,
                    (driver_id, date, check_in, check_out),
                )
                conn.commit()
            return redirect(url_for("drivers_attendance", msg="Driver attendance saved"))

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT da.date, da.check_in, da.check_out, d.name AS driver_name
            FROM driver_attendance da
            JOIN drivers d ON d.id = da.driver_id
            ORDER BY da.date DESC, d.name ASC
            """
        ).fetchall()
    return render_template(
        "drivers_attendance.html",
        drivers=drivers,
        attendance=rows,
        msg=request.args.get("msg", ""),
        linked_sheet_url=get_setting("linked_driver_attendance_sheet_url", get_setting("linked_rider_sheet_url", "")),
        linked_sheet_tab=get_setting("linked_driver_attendance_sheet_tab", "Driver aTTENDANCE"),
        linked_last_sync=get_setting("linked_driver_attendance_last_sync", "Never"),
        linked_status=get_setting("linked_driver_attendance_sync_status", "idle"),
    )


@app.route("/reports")
def reports():
    period = request.args.get("period", "all")
    start_date = request.args.get("start_date", "")
    end_date = request.args.get("end_date", "")

    resolved_start, resolved_end = resolve_report_range(period, start_date, end_date)
    rows = report_rows(grace_minutes=5, start_date=resolved_start, end_date=resolved_end)

    totals: dict[str, float] = {}
    late_count = 0
    on_time_count = 0
    for row in rows:
        totals[row["employee_name"]] = round(
            totals.get(row["employee_name"], 0.0) + row["worked_hours"], 2
        )
        if row["status"] == "Late":
            late_count += 1
        if row["status"] == "On time":
            on_time_count += 1

    summary = [{"employee_name": k, "total_worked_hours": v} for k, v in sorted(totals.items())]
    with get_connection() as conn:
        employee_count = conn.execute("SELECT COUNT(*) AS count FROM employees").fetchone()["count"]

    return render_template(
        "reports.html",
        rows=rows,
        summary=summary,
        filters={
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
        },
        export_query={
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
        },
        stats={
            "employee_count": employee_count,
            "attendance_count": len(rows),
            "late_count": late_count,
            "on_time_count": on_time_count,
        },
    )


@app.route("/driver-reports")
def driver_reports():
    period = request.args.get("period", "all")
    start_date = request.args.get("start_date", "")
    end_date = request.args.get("end_date", "")

    resolved_start, resolved_end = resolve_report_range(period, start_date, end_date)
    with get_connection() as conn:
        driver_rows = conn.execute(
            """
            SELECT
                da.date,
                da.check_in,
                da.check_out,
                d.name AS driver_name
            FROM driver_attendance da
            JOIN drivers d ON d.id = da.driver_id
            WHERE (? IS NULL OR da.date >= ?) AND (? IS NULL OR da.date <= ?)
            ORDER BY da.date DESC, d.name ASC
            """,
            (resolved_start, resolved_start, resolved_end, resolved_end),
        ).fetchall()
        rider_shift_rows = conn.execute(
            """
            SELECT date, driver_name, shift_window
            FROM rider_shifts
            ORDER BY date DESC
            """
        ).fetchall()
        driver_count = conn.execute(
            """
            SELECT COUNT(DISTINCT da.driver_id) AS count
            FROM driver_attendance da
            WHERE (? IS NULL OR da.date >= ?) AND (? IS NULL OR da.date <= ?)
            """,
            (resolved_start, resolved_start, resolved_end, resolved_end),
        ).fetchone()["count"]

    rows = []
    totals: dict[str, float] = {}
    overtime_totals: dict[str, float] = {}
    late_count = 0
    on_time_count = 0
    for row in driver_rows:
        hours = calculate_hours(row["check_in"], row["check_out"])
        matched_shift = choose_shift_window_for_driver(
            row["driver_name"], row["date"], rider_shift_rows
        )
        shift_start, shift_end = split_shift_window(matched_shift or "")
        status = "No shift assigned"
        overtime_hours = 0.0

        if shift_start != "-" and shift_end != "-":
            check_in_dt = parse_time(row["check_in"])
            shift_start_dt = parse_time(shift_start)
            status = "Late" if check_in_dt > (shift_start_dt + timedelta(minutes=5)) else "On time"
            shift_hours = calculate_hours(shift_start, shift_end)
            overtime_hours = round(max(0.0, hours - shift_hours), 2)
            if status == "Late":
                late_count += 1
            elif status == "On time":
                on_time_count += 1

        rows.append(
            {
                "driver_name": row["driver_name"],
                "date": row["date"],
                "check_in": row["check_in"],
                "check_out": row["check_out"],
                "worked_hours": hours,
                "shift_start": shift_start,
                "shift_end": shift_end,
                "overtime_hours": overtime_hours,
                "status": status,
            }
        )
        totals[row["driver_name"]] = round(totals.get(row["driver_name"], 0.0) + hours, 2)
        overtime_totals[row["driver_name"]] = round(
            overtime_totals.get(row["driver_name"], 0.0) + overtime_hours, 2
        )

    summary = [
        {
            "driver_name": k,
            "total_worked_hours": totals[k],
            "total_overtime_hours": overtime_totals.get(k, 0.0),
        }
        for k in sorted(totals.keys())
    ]

    return render_template(
        "driver_reports.html",
        rows=rows,
        summary=summary,
        filters={
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
        },
        export_query={
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
        },
        stats={
            "driver_count": driver_count,
            "attendance_count": len(rows),
            "late_count": late_count,
            "on_time_count": on_time_count,
        },
    )


@app.route("/reports/export")
def export_report():
    period = request.args.get("period", "all")
    start_date = request.args.get("start_date", "")
    end_date = request.args.get("end_date", "")
    resolved_start, resolved_end = resolve_report_range(
        period, start_date, end_date
    )

    rows = report_rows(grace_minutes=5, start_date=resolved_start, end_date=resolved_end)
    details_df = pd.DataFrame(rows)

    if details_df.empty:
        details_df = pd.DataFrame(
            columns=[
                "employee_name",
                "date",
                "check_in",
                "check_out",
                "shift_start",
                "shift_end",
                "worked_hours",
                "status",
            ]
        )

    summary_df = (
        details_df.groupby("employee_name", as_index=False)["worked_hours"].sum()
        if not details_df.empty
        else pd.DataFrame(columns=["employee_name", "worked_hours"])
    )
    summary_df = summary_df.rename(columns={"worked_hours": "total_worked_hours"})

    reports_dir = BASE_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    output_path = reports_dir / "attendance_report.xlsx"

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        details_df.to_excel(writer, sheet_name="Detailed", index=False)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

    return send_file(output_path, as_attachment=True)


@app.route("/driver-reports/export")
def export_driver_report():
    period = request.args.get("period", "all")
    start_date = request.args.get("start_date", "")
    end_date = request.args.get("end_date", "")
    resolved_start, resolved_end = resolve_report_range(period, start_date, end_date)

    with get_connection() as conn:
        driver_rows = conn.execute(
            """
            SELECT
                da.date,
                da.check_in,
                da.check_out,
                d.name AS driver_name
            FROM driver_attendance da
            JOIN drivers d ON d.id = da.driver_id
            WHERE (? IS NULL OR da.date >= ?) AND (? IS NULL OR da.date <= ?)
            ORDER BY da.date DESC, d.name ASC
            """,
            (resolved_start, resolved_start, resolved_end, resolved_end),
        ).fetchall()
        rider_shift_rows = conn.execute(
            """
            SELECT date, driver_name, shift_window
            FROM rider_shifts
            ORDER BY date DESC
            """
        ).fetchall()

    rows = []
    for row in driver_rows:
        hours = calculate_hours(row["check_in"], row["check_out"])
        matched_shift = choose_shift_window_for_driver(
            row["driver_name"], row["date"], rider_shift_rows
        )
        shift_start, shift_end = split_shift_window(matched_shift or "")
        status = "No shift assigned"
        overtime_hours = 0.0
        if shift_start != "-" and shift_end != "-":
            check_in_dt = parse_time(row["check_in"])
            shift_start_dt = parse_time(shift_start)
            status = "Late" if check_in_dt > (shift_start_dt + timedelta(minutes=5)) else "On time"
            shift_hours = calculate_hours(shift_start, shift_end)
            overtime_hours = round(max(0.0, hours - shift_hours), 2)

        rows.append(
            {
                "driver_name": row["driver_name"],
                "date": row["date"],
                "check_in": row["check_in"],
                "check_out": row["check_out"],
                "shift_start": shift_start,
                "shift_end": shift_end,
                "worked_hours": hours,
                "overtime_hours": overtime_hours,
                "status": status,
            }
        )

    details_df = pd.DataFrame(rows)
    if details_df.empty:
        details_df = pd.DataFrame(
            columns=[
                "driver_name",
                "date",
                "check_in",
                "check_out",
                "shift_start",
                "shift_end",
                "worked_hours",
                "overtime_hours",
                "status",
            ]
        )

    summary_df = (
        details_df.groupby("driver_name", as_index=False)[["worked_hours", "overtime_hours"]].sum()
        if not details_df.empty
        else pd.DataFrame(columns=["driver_name", "worked_hours", "overtime_hours"])
    )
    summary_df = summary_df.rename(
        columns={"worked_hours": "total_worked_hours", "overtime_hours": "total_overtime_hours"}
    )

    reports_dir = BASE_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    output_path = reports_dir / "driver_attendance_report.xlsx"

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        details_df.to_excel(writer, sheet_name="Detailed", index=False)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

    return send_file(output_path, as_attachment=True)


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)