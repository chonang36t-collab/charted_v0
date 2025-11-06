"""Excel data loading utilities for Workforce/Shift Insight.

This module:
- Validates and cleans uploaded Excel files
- Supports dynamic column names (lowercase, underscores)
- Inserts records into SQLite via SQLAlchemy
"""

import pandas as pd
from flask import current_app
from sqlalchemy.exc import IntegrityError

from .. import db
from ..models import JobRecord  # You must define JobRecord in models.py

REQUIRED_COLUMNS = [
    "job_name",
    "shift_name",
    "full_name",
    "location",
    "site",
    "role",
    "month",
    "date",
    "day",
    "shift_start",
    "shift_end",
    "duration",
    "paid_hours",
    "hour_rate",
    "deductions",
    "additions",
    "total_pay",
    "client_hourly_rate",
    "client_net",
    "self_employed",
    "dns",
    "client",
    "job_status"
]


def parse_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and transform the input DataFrame.

    - Ensures required columns exist.
    - Coerces dtypes and calculates derived columns: sales, cost, profit, profit_margin.
    - Normalizes date to date type.
    - Drops rows with missing critical fields.
    """
    # Normalize incoming column names first (case/spacing tolerant)
    def _norm(col: str) -> str:
        return col.strip().lower().replace(" ", "_").replace("-", "_")

    df = df.rename(columns={c: _norm(str(c)) for c in df.columns})

    # Validate required columns after normalization
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    # Clean and convert date columns
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date", "full_name", "job_name"]).copy()

    # Coerce numerics
    numeric_cols = [
        "duration",
        "paid_hours",
        "hour_rate",
        "deductions",
        "additions",
        "total_pay",
        "client_hourly_rate",
        "client_net",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Add derived metric if needed
    if "total_pay" not in df.columns or df["total_pay"].sum() == 0:
        df["total_pay"] = df["paid_hours"] * df["hour_rate"]

    return df


def load_excel_to_db(file_like) -> dict:
    """Read an Excel file and insert into the database.

    Returns:
        dict: {inserted, duplicates, skipped}
    """
    try:
        raw = pd.read_excel(file_like, engine="openpyxl")
    except Exception as e:
        raise ValueError("Failed to read Excel file. Ensure it's a valid .xlsx file.") from e

    df = parse_and_clean(raw)

    inserted = 0
    duplicates = 0
    skipped = 0

    # Deduplicate within incoming file (by date + full_name + job_name)
    df = df.drop_duplicates(subset=["date", "full_name", "job_name"]).reset_index(drop=True)

    for _, row in df.iterrows():
        record = JobRecord(
            job_name=row["job_name"],
            shift_name=row["shift_name"],
            full_name=row["full_name"],
            location=row["location"],
            site=row["site"],
            role=row["role"],
            month=row["month"],
            date=row["date"],
            day=row["day"],
            shift_start=str(row["shift_start"]),
            shift_end=str(row["shift_end"]),
            duration=float(row["duration"]),
            paid_hours=float(row["paid_hours"]),
            hour_rate=float(row["hour_rate"]),
            deductions=float(row["deductions"]),
            additions=float(row["additions"]),
            total_pay=float(row["total_pay"]),
            client_hourly_rate=float(row["client_hourly_rate"]),
            client_net=float(row["client_net"]),
            self_employed=bool(row["self_employed"]) if not pd.isna(row["self_employed"]) else False,
            dns=str(row["dns"]),
            client=str(row["client"]),
            job_status=str(row["job_status"]),
        )

        db.session.add(record)
        try:
            db.session.commit()
            inserted += 1
        except IntegrityError:
            db.session.rollback()
            duplicates += 1
        except Exception as e:
            db.session.rollback()
            skipped += 1
            current_app.logger.error(f"Failed to insert row: {e}")

    return {"inserted": inserted, "duplicates": duplicates, "skipped": skipped}
