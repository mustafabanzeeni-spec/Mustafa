from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path

import pandas as pd


REQUIRED_ATTENDANCE_COLUMNS = {"employee_name", "date", "check_in", "check_out"}
REQUIRED_SHIFT_COLUMNS = {"employee_name", "date", "shift_start", "shift_end"}


def load_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported file type: {path.suffix}. Use CSV or Excel.")


def validate_columns(df: pd.DataFrame, required: set[str], table_name: str) -> None:
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{table_name} is missing required columns: {', '.join(sorted(missing))}"
        )


def to_datetime(date_series: pd.Series, time_series: pd.Series) -> pd.Series:
    return pd.to_datetime(
        date_series.astype(str) + " " + time_series.astype(str),
        errors="coerce",
    )


def build_report(
    attendance_df: pd.DataFrame, shifts_df: pd.DataFrame, grace_minutes: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    validate_columns(attendance_df, REQUIRED_ATTENDANCE_COLUMNS, "Attendance file")
    validate_columns(shifts_df, REQUIRED_SHIFT_COLUMNS, "Shifts file")

    attendance_df = attendance_df.copy()
    shifts_df = shifts_df.copy()

    attendance_df["date"] = pd.to_datetime(attendance_df["date"], errors="coerce").dt.date
    shifts_df["date"] = pd.to_datetime(shifts_df["date"], errors="coerce").dt.date

    merged = attendance_df.merge(
        shifts_df,
        on=["employee_name", "date"],
        how="left",
    )

    merged["check_in_dt"] = to_datetime(merged["date"], merged["check_in"])
    merged["check_out_dt"] = to_datetime(merged["date"], merged["check_out"])
    merged["shift_start_dt"] = to_datetime(merged["date"], merged["shift_start"])
    merged["shift_end_dt"] = to_datetime(merged["date"], merged["shift_end"])

    overnight_mask = merged["shift_end_dt"] < merged["shift_start_dt"]
    merged.loc[overnight_mask, "shift_end_dt"] = (
        merged.loc[overnight_mask, "shift_end_dt"] + pd.Timedelta(days=1)
    )

    merged["worked_hours"] = (
        (merged["check_out_dt"] - merged["check_in_dt"]).dt.total_seconds() / 3600
    )
    merged.loc[merged["worked_hours"] < 0, "worked_hours"] = pd.NA

    grace_delta = timedelta(minutes=grace_minutes)
    merged["status"] = "No shift assigned"
    has_shift = merged["shift_start_dt"].notna()
    merged.loc[has_shift, "status"] = "On time"
    merged.loc[
        has_shift & (merged["check_in_dt"] > (merged["shift_start_dt"] + grace_delta)),
        "status",
    ] = "Late"

    detailed = merged[
        [
            "employee_name",
            "date",
            "check_in",
            "check_out",
            "shift_start",
            "shift_end",
            "worked_hours",
            "status",
        ]
    ].copy()
    detailed["worked_hours"] = detailed["worked_hours"].round(2)

    summary = (
        detailed.groupby("employee_name", dropna=False, as_index=False)["worked_hours"]
        .sum(min_count=1)
        .rename(columns={"worked_hours": "total_worked_hours"})
    )
    summary["total_worked_hours"] = summary["total_worked_hours"].round(2)

    return detailed, summary


def save_report(detailed: pd.DataFrame, summary: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        detailed.to_excel(writer, sheet_name="Detailed", index=False)
        summary.to_excel(writer, sheet_name="Summary", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate attendance report with late/on-time status."
    )
    parser.add_argument("--attendance", required=True, help="Path to attendance CSV/XLSX")
    parser.add_argument("--shifts", required=True, help="Path to shifts CSV/XLSX")
    parser.add_argument(
        "--grace-minutes",
        type=int,
        default=5,
        help="Allowed delay minutes before marking Late (default: 5)",
    )
    parser.add_argument(
        "--output",
        default="reports/attendance_report.xlsx",
        help="Output Excel file path (default: reports/attendance_report.xlsx)",
    )
    args = parser.parse_args()

    attendance_df = load_table(Path(args.attendance))
    shifts_df = load_table(Path(args.shifts))

    detailed, summary = build_report(attendance_df, shifts_df, args.grace_minutes)
    save_report(detailed, summary, Path(args.output))

    print(f"Report created: {Path(args.output).resolve()}")
    print("\nEmployee total worked hours:")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()