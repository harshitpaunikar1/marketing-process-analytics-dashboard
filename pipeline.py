"""
Marketing analytics ETL pipeline and KPI computation for the analytics dashboard.
Ingests campaign, lead, and conversion data; computes funnel metrics and attribution.
"""
import json
import logging
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


SCHEMA = """
CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id TEXT PRIMARY KEY,
    name TEXT,
    channel TEXT,
    start_date TEXT,
    end_date TEXT,
    budget REAL,
    spend REAL
);

CREATE TABLE IF NOT EXISTS leads (
    lead_id TEXT PRIMARY KEY,
    campaign_id TEXT,
    created_at TEXT,
    stage TEXT,
    converted INTEGER DEFAULT 0,
    converted_at TEXT,
    deal_value REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id TEXT,
    event_type TEXT,
    occurred_at TEXT,
    metadata TEXT DEFAULT '{}'
);
"""


class MarketingDB:
    """SQLite storage for campaign, lead, and event data."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def insert_campaigns(self, records: List[Dict]) -> int:
        cur = self._conn.executemany(
            "INSERT OR REPLACE INTO campaigns VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(r["campaign_id"], r["name"], r["channel"], r.get("start_date", ""),
              r.get("end_date", ""), r.get("budget", 0), r.get("spend", 0))
             for r in records],
        )
        self._conn.commit()
        return cur.rowcount

    def insert_leads(self, records: List[Dict]) -> int:
        cur = self._conn.executemany(
            "INSERT OR REPLACE INTO leads VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(r["lead_id"], r["campaign_id"], r["created_at"], r.get("stage", "new"),
              int(r.get("converted", 0)), r.get("converted_at"), r.get("deal_value", 0))
             for r in records],
        )
        self._conn.commit()
        return cur.rowcount

    def insert_events(self, records: List[Dict]) -> int:
        cur = self._conn.executemany(
            "INSERT INTO events (lead_id, event_type, occurred_at, metadata) VALUES (?, ?, ?, ?)",
            [(r["lead_id"], r["event_type"], r["occurred_at"],
              json.dumps(r.get("metadata", {})))
             for r in records],
        )
        self._conn.commit()
        return cur.rowcount

    def query_df(self, sql: str, params: tuple = ()) -> pd.DataFrame:
        return pd.read_sql_query(sql, self._conn, params=params)


class MarketingKPIEngine:
    """Computes marketing KPIs from data in the marketing DB."""

    def __init__(self, db: MarketingDB):
        self.db = db

    def channel_performance(self) -> pd.DataFrame:
        sql = """
        SELECT c.channel,
               COUNT(DISTINCT c.campaign_id) AS campaigns,
               SUM(c.spend) AS total_spend,
               COUNT(DISTINCT l.lead_id) AS total_leads,
               SUM(l.converted) AS conversions,
               SUM(l.deal_value) AS pipeline_value
        FROM campaigns c
        LEFT JOIN leads l ON c.campaign_id = l.campaign_id
        GROUP BY c.channel
        """
        df = self.db.query_df(sql)
        df["cpl"] = (df["total_spend"] / df["total_leads"].replace(0, np.nan)).round(2)
        df["cac"] = (df["total_spend"] / df["conversions"].replace(0, np.nan)).round(2)
        df["cvr_pct"] = (df["conversions"] / df["total_leads"].replace(0, np.nan) * 100).round(2)
        df["roas"] = (df["pipeline_value"] / df["total_spend"].replace(0, np.nan)).round(2)
        return df.fillna(0)

    def funnel_metrics(self) -> pd.DataFrame:
        stages = ["new", "mql", "sql", "opportunity", "won"]
        records = []
        for stage in stages:
            count = self.db.query_df(
                "SELECT COUNT(*) AS cnt FROM leads WHERE stage = ?", (stage,)
            )["cnt"][0]
            records.append({"stage": stage, "count": int(count)})
        df = pd.DataFrame(records)
        df["drop_off_pct"] = 0.0
        for i in range(1, len(df)):
            prev = df.loc[i - 1, "count"]
            curr = df.loc[i, "count"]
            df.loc[i, "drop_off_pct"] = round((1 - curr / prev) * 100, 1) if prev > 0 else 0.0
        return df

    def cohort_conversion(self, period: str = "month") -> pd.DataFrame:
        sql = """
        SELECT strftime('%Y-%m', created_at) AS cohort,
               COUNT(*) AS leads,
               SUM(converted) AS converted,
               AVG(deal_value) AS avg_deal_value
        FROM leads
        GROUP BY cohort
        ORDER BY cohort
        """
        df = self.db.query_df(sql)
        df["conversion_rate_pct"] = (df["converted"] / df["leads"].replace(0, np.nan) * 100).round(2)
        return df.fillna(0)

    def campaign_roi(self) -> pd.DataFrame:
        sql = """
        SELECT c.campaign_id, c.name, c.channel, c.spend,
               COUNT(l.lead_id) AS leads,
               SUM(l.converted) AS won,
               SUM(l.deal_value) AS revenue
        FROM campaigns c
        LEFT JOIN leads l ON c.campaign_id = l.campaign_id
        GROUP BY c.campaign_id
        """
        df = self.db.query_df(sql)
        df["roi_pct"] = ((df["revenue"] - df["spend"]) / df["spend"].replace(0, np.nan) * 100).round(1)
        return df.fillna(0).sort_values("roi_pct", ascending=False)

    def dashboard_snapshot(self) -> Dict:
        ch = self.channel_performance()
        funnel = self.funnel_metrics()
        return {
            "total_spend": round(float(ch["total_spend"].sum()), 2),
            "total_leads": int(ch["total_leads"].sum()),
            "total_conversions": int(ch["conversions"].sum()),
            "blended_cpl": round(float(ch["total_spend"].sum() / max(ch["total_leads"].sum(), 1)), 2),
            "blended_cac": round(float(ch["total_spend"].sum() / max(ch["conversions"].sum(), 1)), 2),
            "pipeline_value": round(float(ch["pipeline_value"].sum()), 2),
            "funnel_stages": funnel.to_dict(orient="records"),
        }


if __name__ == "__main__":
    np.random.seed(42)
    db = MarketingDB(db_path=":memory:")
    channels = ["paid_search", "organic", "social", "email", "referral"]
    campaigns = [
        {"campaign_id": f"C{i:03d}", "name": f"Campaign {i}", "channel": channels[i % 5],
         "start_date": "2024-01-01", "end_date": "2024-12-31",
         "budget": np.random.uniform(5000, 50000),
         "spend": np.random.uniform(4000, 48000)}
        for i in range(10)
    ]
    db.insert_campaigns(campaigns)

    stages = ["new", "mql", "sql", "opportunity", "won", "lost"]
    leads = []
    for i in range(500):
        stage = np.random.choice(stages, p=[0.25, 0.25, 0.2, 0.15, 0.1, 0.05])
        converted = 1 if stage == "won" else 0
        leads.append({
            "lead_id": f"L{i:04d}",
            "campaign_id": f"C{np.random.randint(0, 10):03d}",
            "created_at": f"2024-{np.random.randint(1,13):02d}-01",
            "stage": stage,
            "converted": converted,
            "converted_at": "2024-06-15" if converted else None,
            "deal_value": float(np.random.lognormal(9, 0.5)) if converted else 0.0,
        })
    db.insert_leads(leads)

    engine = MarketingKPIEngine(db)
    print("Channel performance:")
    print(engine.channel_performance()[["channel", "total_spend", "total_leads",
                                         "conversions", "cpl", "cac", "roas"]].to_string(index=False))

    print("\nFunnel metrics:")
    print(engine.funnel_metrics().to_string(index=False))

    print("\nDashboard snapshot:")
    print(json.dumps(engine.dashboard_snapshot(), indent=2))
