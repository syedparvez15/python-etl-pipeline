"""
transformer.py
--------------
Handles all data transformation logic including cleaning, validation,
type casting, deduplication, and business rule application.
"""

import logging
import pandas as pd
from typing import List, Optional

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Applies transformation rules to extracted DataFrames.
    Supports cleaning, validation, enrichment, and business rule enforcement.
    """

    def __init__(self, config: dict):
        self.config = config
        self.required_columns = config.get("required_columns", [])
        self.date_columns = config.get("date_columns", [])
        self.numeric_columns = config.get("numeric_columns", [])

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply standard cleaning operations:
        - Strip whitespace from string columns
        - Normalize column names to snake_case
        - Drop fully empty rows
        """
        logger.info("Applying data cleaning...")

        # Normalize column names
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(r"[\s\-]+", "_", regex=True)
        )

        # Strip leading/trailing whitespace from string columns
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

        # Drop rows where all values are null
        before = len(df)
        df.dropna(how="all", inplace=True)
        dropped = before - len(df)
        if dropped:
            logger.warning(f"Dropped {dropped} fully empty rows.")

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate the DataFrame against required column definitions.
        Logs warnings for missing values in required fields.
        """
        logger.info("Validating required columns...")

        for col in self.required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column missing from dataset: '{col}'")
            null_count = df[col].isnull().sum()
            if null_count > 0:
                logger.warning(f"Column '{col}' has {null_count} null values.")

        return df

    def cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cast columns to their correct data types based on config.
        Handles date parsing and numeric conversion with error handling.
        """
        logger.info("Casting column types...")

        for col in self.date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                logger.info(f"Cast '{col}' to datetime.")

        for col in self.numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                logger.info(f"Cast '{col}' to numeric.")

        return df

    def deduplicate(self, df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Remove duplicate rows from the DataFrame.

        Args:
            df: Input DataFrame
            subset: Columns to consider for deduplication (default: all columns)

        Returns:
            Deduplicated DataFrame
        """
        before = len(df)
        df.drop_duplicates(subset=subset, keep="first", inplace=True)
        removed = before - len(df)
        if removed:
            logger.info(f"Removed {removed} duplicate rows.")
        return df

    def apply_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply domain-specific business rules.
        Extend this method with project-specific transformations.
        """
        logger.info("Applying business rules...")

        # Example: flag records with negative numeric values
        for col in self.numeric_columns:
            if col in df.columns:
                negatives = (df[col] < 0).sum()
                if negatives:
                    logger.warning(f"{negatives} negative values found in '{col}'. Flagging.")
                    df[f"{col}_flag"] = df[col] < 0

        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run the full transformation pipeline in sequence.
        """
        df = self.clean(df)
        df = self.validate(df)
        df = self.cast_types(df)
        df = self.deduplicate(df)
        df = self.apply_business_rules(df)
        logger.info(f"Transformation complete. Output shape: {df.shape}")
        return df
