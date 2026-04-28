"""
RFM segmentation engine for marketing analytics.
Computes Recency, Frequency, and Monetary scores and assigns customer segments.
"""
import warnings
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


RFM_SEGMENTS = {
    (5, 5): "Champions",
    (4, 5): "Champions",
    (5, 4): "Loyal Customers",
    (4, 4): "Loyal Customers",
    (3, 5): "Potential Loyalist",
    (3, 4): "Potential Loyalist",
    (5, 3): "Recent Customers",
    (4, 3): "Promising",
    (5, 2): "Promising",
    (3, 3): "Needs Attention",
    (4, 2): "Needs Attention",
    (2, 3): "About to Sleep",
    (2, 2): "About to Sleep",
    (1, 2): "At Risk",
    (2, 5): "Can't Lose Them",
    (1, 5): "Can't Lose Them",
    (1, 4): "At Risk",
    (1, 3): "At Risk",
    (2, 4): "Can't Lose Them",
    (1, 1): "Lost",
    (2, 1): "Hibernating",
    (3, 2): "Hibernating",
    (3, 1): "Hibernating",
    (4, 1): "Hibernating",
    (5, 1): "Promising",
}


class RFMAnalyzer:
    """
    Computes RFM scores from transaction data.
    Supports quintile-based or custom threshold scoring.
    """

    def __init__(self, reference_date: Optional[datetime] = None, n_quantiles: int = 5):
        self.reference_date = reference_date or datetime.utcnow()
        self.n_quantiles = n_quantiles
        self._rfm_df: Optional[pd.DataFrame] = None

    def compute(self, transactions: pd.DataFrame,
                customer_col: str = "customer_id",
                date_col: str = "transaction_date",
                amount_col: str = "amount") -> pd.DataFrame:
        """
        Compute per-customer recency (days), frequency (count), monetary (sum).
        """
        df = transactions.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col, customer_col])

        snapshot = pd.Timestamp(self.reference_date)
        rfm = df.groupby(customer_col).agg(
            recency=(date_col, lambda x: (snapshot - x.max()).days),
            frequency=(date_col, "count"),
            monetary=(amount_col, "sum"),
        ).reset_index()
        rfm["monetary"] = rfm["monetary"].clip(lower=0)
        self._rfm_df = rfm
        return rfm

    def score(self, rfm: pd.DataFrame) -> pd.DataFrame:
        """
        Assign 1-5 quintile scores.
        Recency: lower days -> higher score.
        Frequency and Monetary: higher -> higher score.
        """
        rfm = rfm.copy()
        labels = list(range(1, self.n_quantiles + 1))
        rfm["r_score"] = pd.qcut(rfm["recency"], q=self.n_quantiles,
                                  labels=labels[::-1], duplicates="drop").astype(int)
        rfm["f_score"] = pd.qcut(rfm["frequency"].rank(method="first"), q=self.n_quantiles,
                                  labels=labels, duplicates="drop").astype(int)
        rfm["m_score"] = pd.qcut(rfm["monetary"].rank(method="first"), q=self.n_quantiles,
                                  labels=labels, duplicates="drop").astype(int)
        rfm["rfm_score"] = rfm["r_score"] * 100 + rfm["f_score"] * 10 + rfm["m_score"]
        rfm["fm_avg"] = ((rfm["f_score"] + rfm["m_score"]) / 2).round().astype(int)
        rfm["segment"] = rfm.apply(
            lambda row: RFM_SEGMENTS.get((row["r_score"], row["fm_avg"]), "Other"), axis=1
        )
        return rfm

    def segment_summary(self, scored_rfm: pd.DataFrame) -> pd.DataFrame:
        """Aggregate customer count, avg monetary, and avg recency by segment."""
        return (
            scored_rfm.groupby("segment")
            .agg(
                customer_count=("segment", "count"),
                avg_monetary=("monetary", "mean"),
                avg_recency_days=("recency", "mean"),
                avg_frequency=("frequency", "mean"),
            )
            .round(2)
            .sort_values("customer_count", ascending=False)
            .reset_index()
        )

    def high_value_customers(self, scored_rfm: pd.DataFrame,
                              segments: Optional[List[str]] = None) -> pd.DataFrame:
        if segments is None:
            segments = ["Champions", "Loyal Customers", "Can't Lose Them"]
        return scored_rfm[scored_rfm["segment"].isin(segments)].copy()

    def churn_risk_customers(self, scored_rfm: pd.DataFrame) -> pd.DataFrame:
        risk_segments = ["At Risk", "About to Sleep", "Hibernating", "Lost"]
        return scored_rfm[scored_rfm["segment"].isin(risk_segments)].copy()

    def ltv_estimate(self, scored_rfm: pd.DataFrame,
                     avg_order_value: Optional[float] = None,
                     purchase_freq_monthly: Optional[float] = None,
                     lifespan_months: float = 24.0) -> pd.DataFrame:
        """
        Estimate simple LTV per customer: monetary * frequency * lifespan_months / observation_months.
        """
        rfm = scored_rfm.copy()
        if avg_order_value and purchase_freq_monthly:
            rfm["ltv"] = avg_order_value * purchase_freq_monthly * lifespan_months
        else:
            observation_months = max(1, rfm["recency"].max() / 30)
            rfm["ltv"] = (rfm["monetary"] / observation_months * lifespan_months).round(2)
        return rfm


if __name__ == "__main__":
    np.random.seed(42)
    n_customers = 500
    n_transactions = 5000
    customer_ids = [f"C{i:04d}" for i in range(1, n_customers + 1)]
    dates = pd.date_range("2023-01-01", "2024-12-31", freq="h")

    transactions = pd.DataFrame({
        "customer_id": np.random.choice(customer_ids, n_transactions),
        "transaction_date": np.random.choice(dates, n_transactions),
        "amount": np.abs(np.random.lognormal(4.5, 1.0, n_transactions)).round(2),
    })

    analyzer = RFMAnalyzer(reference_date=datetime(2025, 1, 1))
    rfm_raw = analyzer.compute(transactions)
    rfm_scored = analyzer.score(rfm_raw)

    print(f"Total customers analyzed: {len(rfm_scored)}")
    print("\nSample scored customers:")
    print(rfm_scored[["customer_id", "recency", "frequency", "monetary",
                        "r_score", "f_score", "m_score", "segment"]].head(10).to_string(index=False))

    print("\nSegment summary:")
    print(analyzer.segment_summary(rfm_scored).to_string(index=False))

    hvcs = analyzer.high_value_customers(rfm_scored)
    print(f"\nHigh-value customers: {len(hvcs)}")
    at_risk = analyzer.churn_risk_customers(rfm_scored)
    print(f"Churn-risk customers: {len(at_risk)}")
