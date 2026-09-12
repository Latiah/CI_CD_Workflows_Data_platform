"""Cleaning and transformation of raw sales CSVs.

Pure functions only — no Airflow and no database imports — so the rules can be
unit tested in milliseconds and reused from anywhere.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pandas as pd

RAW_COLUMNS = [
    "order_id",
    "order_ts",
    "customer_id",
    "customer_name",
    "region",
    "country",
    "channel",
    "category",
    "product",
    "quantity",
    "unit_price",
    "discount_pct",
]

CLEAN_COLUMNS = [
    "order_id",
    "order_ts",
    "order_date",
    "customer_id",
    "customer_name",
    "region",
    "country",
    "channel",
    "category",
    "product",
    "quantity",
    "unit_price",
    "discount_pct",
    "gross_amount",
    "net_amount",
    "source_file",
]

REQUIRED_TEXT_FIELDS = ["order_id", "customer_id", "region", "category", "product", "channel"]

# Channel spellings seen in the wild, mapped onto the canonical set.
CHANNEL_ALIASES = {
    "web": "web",
    "website": "web",
    "online": "web",
    "mobile_app": "mobile_app",
    "mobile": "mobile_app",
    "app": "mobile_app",
    "retail_store": "retail_store",
    "retail": "retail_store",
    "store": "retail_store",
    "partner": "partner",
    "phone": "phone",
    "call_center": "phone",
}


@dataclass
class TransformResult:
    clean: pd.DataFrame
    rejects: list[dict] = field(default_factory=list)
    rows_raw: int = 0

    @property
    def rows_loaded(self) -> int:
        return len(self.clean)

    @property
    def rows_rejected(self) -> int:
        return len(self.rejects)

    def summary(self) -> dict:
        return {
            "rows_raw": self.rows_raw,
            "rows_loaded": self.rows_loaded,
            "rows_rejected": self.rows_rejected,
            "accept_rate_pct": round(100 * self.rows_loaded / self.rows_raw, 2) if self.rows_raw else 0.0,
        }


class SchemaError(ValueError):
    """Raised when a file does not look like a sales extract at all."""


def read_csv(payload: bytes | str) -> pd.DataFrame:
    """Parse raw CSV bytes, keeping every value as text for explicit coercion."""
    import io

    if isinstance(payload, bytes):
        payload = payload.decode("utf-8-sig")
    frame = pd.read_csv(io.StringIO(payload), dtype=str, keep_default_na=False, na_values=[""])

    missing = [column for column in RAW_COLUMNS if column not in frame.columns]
    if missing:
        raise SchemaError(f"CSV is missing required columns: {', '.join(missing)}")
    return frame[RAW_COLUMNS]


def _reject(
    frame: pd.DataFrame, mask: pd.Series, reason: str, sink: list[dict], source_file: str
) -> pd.DataFrame:
    """Move rows matching `mask` into the reject sink and return what survives."""
    if not mask.any():
        return frame
    for record in frame.loc[mask, RAW_COLUMNS].to_dict(orient="records"):
        sink.append(
            {
                "source_file": source_file,
                "reason": reason,
                "raw_record": json.dumps({k: (None if pd.isna(v) else str(v)) for k, v in record.items()}),
            }
        )
    return frame.loc[~mask].copy()


def transform(frame: pd.DataFrame, source_file: str) -> TransformResult:
    """Apply every cleaning rule, returning clean rows plus an audit of rejects."""
    rows_raw = len(frame)
    rejects: list[dict] = []
    df = frame.copy()

    # 1. Normalise text: trim padding, collapse casing where it is not meaningful.
    for column in [
        "order_id",
        "customer_id",
        "customer_name",
        "region",
        "country",
        "channel",
        "category",
        "product",
    ]:
        df[column] = df[column].astype("string").str.strip()
    df["region"] = df["region"].str.title()
    df["country"] = df["country"].str.title()
    df["category"] = df["category"].str.title()
    df["channel"] = df["channel"].str.lower().map(CHANNEL_ALIASES).astype("string")

    # 2. Mandatory fields must be present.
    for column in REQUIRED_TEXT_FIELDS:
        blank = df[column].isna() | (df[column].astype("string").str.len() == 0)
        df = _reject(df, blank, f"missing_{column}", rejects, source_file)

    # 3. Timestamps: parse to UTC, drop anything unparseable.
    df["order_ts"] = pd.to_datetime(df["order_ts"], errors="coerce", utc=True, format="mixed")
    df = _reject(df, df["order_ts"].isna(), "invalid_order_ts", rejects, source_file)

    # 4. Numerics: coerce, then bound-check.
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0.0)

    df = _reject(
        df, df["quantity"].isna() | (df["quantity"] <= 0), "non_positive_quantity", rejects, source_file
    )
    df = _reject(
        df, df["unit_price"].isna() | (df["unit_price"] < 0), "invalid_unit_price", rejects, source_file
    )
    df = _reject(
        df,
        (df["discount_pct"] < 0) | (df["discount_pct"] >= 1),
        "discount_out_of_range",
        rejects,
        source_file,
    )

    # 5. Duplicate order ids within a batch: keep the first, audit the rest.
    duplicates = df.duplicated(subset=["order_id"], keep="first")
    df = _reject(df, duplicates, "duplicate_order_id", rejects, source_file)

    if df.empty:
        return TransformResult(clean=pd.DataFrame(columns=CLEAN_COLUMNS), rejects=rejects, rows_raw=rows_raw)

    # 6. Derived business columns.
    df["quantity"] = df["quantity"].astype(int)
    df["order_date"] = df["order_ts"].dt.date
    df["gross_amount"] = (df["quantity"] * df["unit_price"]).round(2)
    df["net_amount"] = (df["gross_amount"] * (1 - df["discount_pct"])).round(2)
    df["discount_pct"] = df["discount_pct"].round(4)
    df["source_file"] = source_file

    return TransformResult(clean=df[CLEAN_COLUMNS].reset_index(drop=True), rejects=rejects, rows_raw=rows_raw)


def transform_bytes(payload: bytes | str, source_file: str) -> TransformResult:
    """Convenience wrapper: raw CSV bytes in, TransformResult out."""
    return transform(read_csv(payload), source_file)
