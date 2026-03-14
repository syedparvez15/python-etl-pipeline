"""
test_transformer.py
-------------------
Unit tests for the DataTransformer class.
"""

import pytest
import pandas as pd
from src.transformer import DataTransformer

BASE_CONFIG = {
    "required_columns": ["id", "name", "amount"],
    "date_columns": ["created_at"],
    "numeric_columns": ["amount"],
}


def make_df():
    return pd.DataFrame({
        "id": [1, 2, 3, 3],
        "name": ["  Alice ", "Bob", "Charlie", "Charlie"],
        "amount": ["100", "-50", "200", "200"],
        "created_at": ["2024-01-01", "2024-01-02", "not-a-date", "not-a-date"],
    })


def test_clean_strips_whitespace():
    transformer = DataTransformer(BASE_CONFIG)
    df = make_df()
    result = transformer.clean(df)
    assert result["name"].iloc[0] == "Alice"


def test_deduplicate_removes_duplicates():
    transformer = DataTransformer(BASE_CONFIG)
    df = make_df()
    df = transformer.clean(df)
    result = transformer.deduplicate(df)
    assert len(result) == 3


def test_cast_numeric():
    transformer = DataTransformer(BASE_CONFIG)
    df = make_df()
    df = transformer.clean(df)
    result = transformer.cast_types(df)
    assert pd.api.types.is_numeric_dtype(result["amount"])


def test_cast_invalid_date_becomes_nat():
    transformer = DataTransformer(BASE_CONFIG)
    df = make_df()
    df = transformer.clean(df)
    result = transformer.cast_types(df)
    assert pd.isnull(result["created_at"].iloc[2])


def test_validate_missing_required_column_raises():
    transformer = DataTransformer(BASE_CONFIG)
    df = pd.DataFrame({"id": [1], "name": ["Alice"]})  # missing 'amount'
    with pytest.raises(ValueError, match="amount"):
        transformer.validate(df)


def test_business_rules_flags_negatives():
    transformer = DataTransformer(BASE_CONFIG)
    df = make_df()
    df = transformer.clean(df)
    df = transformer.cast_types(df)
    result = transformer.apply_business_rules(df)
    assert "amount_flag" in result.columns
    assert result["amount_flag"].iloc[1] is True
