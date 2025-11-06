"""Analysis utilities for JobRecord-based Workforce/Shift insights.

All functions accept a pandas DataFrame of job records with canonical columns
from JobRecord and return JSON-serializable dicts suitable for Plotly.
"""
from __future__ import annotations

import math
from typing import Dict

import pandas as pd
import numpy as np


def compute_kpis(df: pd.DataFrame) -> Dict:
    if df.empty:
        return {"total_pay": 0.0, "paid_hours": 0.0, "avg_hour_rate": 0.0, "client_net": 0.0}
    total_pay = float(df.get("total_pay", 0).sum())
    paid_hours = float(df.get("paid_hours", 0).sum())
    client_net = float(df.get("client_net", 0).sum())
    # Weighted average hour rate by paid_hours when available
    if paid_hours:
        avg_rate = float((df.get("hour_rate", 0) * df.get("paid_hours", 0)).sum() / paid_hours)
    else:
        avg_rate = float(df.get("hour_rate", 0).mean() or 0.0)
    return {
        "total_pay": round(total_pay, 2),
        "paid_hours": round(paid_hours, 2),
        "avg_hour_rate": round(avg_rate, 2),
        "client_net": round(client_net, 2),
    }


def timeseries_by(df: pd.DataFrame, freq: str = "D") -> Dict:
    if df.empty:
        return {"x": [], "total_pay": [], "client_net": [], "paid_hours": []}
    ts = df.copy()
    ts["date"] = pd.to_datetime(ts["date"])  # ensure datetime
    grouped = (
        ts.set_index("date")
        .resample(freq)
        .agg({"total_pay": "sum", "client_net": "sum", "paid_hours": "sum"})
        .fillna(0)
    )
    grouped.index = grouped.index.strftime("%Y-%m-%d")
    return {
        "x": grouped.index.tolist(),
        "total_pay": grouped["total_pay"].round(2).tolist(),
        "client_net": grouped["client_net"].round(2).tolist(),
        "paid_hours": grouped["paid_hours"].round(2).tolist(),
    }


def top_n_clients(df: pd.DataFrame, n: int = 10) -> Dict:
    if df.empty:
        return {"labels": [], "values": []}
    g = df.groupby("client")["total_pay"].sum().sort_values(ascending=False).head(n)
    return {"labels": g.index.tolist(), "values": g.round(2).tolist()}


def top_n_locations(df: pd.DataFrame, n: int = 10) -> Dict:
    if df.empty:
        return {"labels": [], "values": []}
    g = df.groupby("location")["total_pay"].sum().sort_values(ascending=False).head(n)
    return {"labels": g.index.tolist(), "values": g.round(2).tolist()}


def hours_distribution(df: pd.DataFrame, bins: int = 20) -> Dict:
    if df.empty:
        return {"bins": [], "counts": []}
    series = df.get("paid_hours", pd.Series([], dtype=float)).fillna(0.0)
    counts, bin_edges = np.histogram(series, bins=bins)
    mids = ((bin_edges[:-1] + bin_edges[1:]) / 2).tolist()
    return {"bins": mids, "counts": counts.tolist()}


def summary_stats(df: pd.DataFrame) -> Dict:
    if df.empty:
        return {
            "total_pay": {"mean": 0.0, "median": 0.0, "std": 0.0},
            "paid_hours": {"mean": 0.0, "median": 0.0, "std": 0.0},
        }
    return {
        "total_pay": {
            "mean": round(float(df.get("total_pay", 0).mean() or 0.0), 2),
            "median": round(float(df.get("total_pay", 0).median() or 0.0), 2),
            "std": round(float(df.get("total_pay", 0).std() or 0.0), 2),
        },
        "paid_hours": {
            "mean": round(float(df.get("paid_hours", 0).mean() or 0.0), 2),
            "median": round(float(df.get("paid_hours", 0).median() or 0.0), 2),
            "std": round(float(df.get("paid_hours", 0).std() or 0.0), 2),
        },
    }
