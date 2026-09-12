"""Unit tests for the cleaning rules. No Docker, no database — fast enough to
run on every commit."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from data_generator.generate import generate_rows, to_csv
from pipeline.transform import SchemaError, read_csv, transform_bytes

HEADER = (
    "order_id,order_ts,customer_id,customer_name,region,country,channel,"
    "category,product,quantity,unit_price,discount_pct\n"
)


def csv_of(*rows: str) -> str:
    return HEADER + "\n".join(rows) + "\n"


GOOD_ROW = "A1,2024-05-01T10:00:00+00:00,CUST-1,Ada,Europe,Germany,web,Electronics,4K Monitor,2,100.00,0.10"


def test_clean_row_loads_with_correct_derived_amounts():
    result = transform_bytes(csv_of(GOOD_ROW), "sales/test.csv")

    assert result.rows_raw == 1
    assert result.rows_loaded == 1
    assert result.rows_rejected == 0

    row = result.clean.iloc[0]
    assert row["gross_amount"] == pytest.approx(200.00)
    assert row["net_amount"] == pytest.approx(180.00)
    assert str(row["order_date"]) == "2024-05-01"
    assert row["source_file"] == "sales/test.csv"


def test_whitespace_and_casing_are_normalised():
    row = "A2,2024-05-01T10:00:00+00:00,CUST-1,Ada,  europe  ,germany,WEB,electronics,Backpack,1,50.00,0.00"
    clean = transform_bytes(csv_of(row), "f.csv").clean.iloc[0]

    assert clean["region"] == "Europe"
    assert clean["country"] == "Germany"
    assert clean["category"] == "Electronics"
    assert clean["channel"] == "web"


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (
            "B1,2024-05-01T10:00:00+00:00,CUST-1,Ada,,Germany,web,Electronics,Hub,1,10.00,0.00",
            "missing_region",
        ),
        (
            "B2,,CUST-1,Ada,Europe,Germany,web,Electronics,Hub,1,10.00,0.00",
            "invalid_order_ts",
        ),
        (
            "B3,2024-05-01T10:00:00+00:00,CUST-1,Ada,Europe,Germany,web,Electronics,Hub,-3,10.00,0.00",
            "non_positive_quantity",
        ),
        (
            "B4,2024-05-01T10:00:00+00:00,CUST-1,Ada,Europe,Germany,web,Electronics,Hub,1,n/a,0.00",
            "invalid_unit_price",
        ),
        (
            "B5,2024-05-01T10:00:00+00:00,CUST-1,Ada,Europe,Germany,web,Electronics,Hub,1,10.00,1.50",
            "discount_out_of_range",
        ),
    ],
)
def test_bad_rows_are_rejected_with_a_reason(row, reason):
    result = transform_bytes(csv_of(row), "f.csv")

    assert result.rows_loaded == 0
    assert result.rows_rejected == 1
    assert result.rejects[0]["reason"] == reason
    # The original record is preserved for auditing.
    assert json.loads(result.rejects[0]["raw_record"])["order_id"].startswith("B")


def test_duplicate_order_ids_keep_the_first_occurrence():
    result = transform_bytes(csv_of(GOOD_ROW, GOOD_ROW), "f.csv")

    assert result.rows_loaded == 1
    assert [r["reason"] for r in result.rejects] == ["duplicate_order_id"]


def test_good_and_bad_rows_are_separated_in_one_batch():
    bad = "C1,2024-05-01T10:00:00+00:00,CUST-9,Bob,Europe,Germany,web,Home,Lamp,0,20.00,0.00"
    result = transform_bytes(csv_of(GOOD_ROW, bad), "f.csv")

    assert result.summary() == {
        "rows_raw": 2,
        "rows_loaded": 1,
        "rows_rejected": 1,
        "accept_rate_pct": 50.0,
    }


def test_missing_columns_raise_a_schema_error():
    with pytest.raises(SchemaError, match="missing required columns"):
        read_csv("order_id,order_ts\nA1,2024-05-01\n")


def test_generator_output_is_accepted_by_the_transform():
    """The generated feed must survive its own pipeline, defects included."""
    rows = generate_rows(500, seed=42)
    result = transform_bytes(to_csv(rows), "sales/generated.csv")

    assert result.rows_raw == 500
    assert result.rows_loaded > 400, "the deliberate defect rate should stay small"
    assert result.rows_loaded + result.rows_rejected == 500
    assert (result.clean["net_amount"] <= result.clean["gross_amount"]).all()
    assert result.clean["quantity"].min() >= 1
    assert not result.clean["order_id"].duplicated().any()


def test_empty_file_yields_no_rows_and_no_crash():
    result = transform_bytes(HEADER, "sales/empty.csv")

    assert result.rows_raw == 0
    assert result.rows_loaded == 0
    assert isinstance(result.clean, pd.DataFrame)
