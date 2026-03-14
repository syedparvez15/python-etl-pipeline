"""
extractor.py
------------
Handles data extraction from source systems (SQL databases, CSV files, APIs).
Supports multiple source types with configurable connection settings.
"""

import logging
import pandas as pd
import sqlalchemy
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class DataExtractor:
    """
    Extracts data from various source types:
    - SQL databases (via SQLAlchemy)
    - CSV / flat files
    - REST APIs (JSON responses)
    """

    def __init__(self, config: dict):
        self.config = config
        self.source_type = config.get("source_type", "sql")

    def extract_from_sql(self, query: str, connection_string: str) -> pd.DataFrame:
        """
        Extract data from a SQL database using the provided query.

        Args:
            query: SQL SELECT query to execute
            connection_string: SQLAlchemy-compatible connection string

        Returns:
            DataFrame containing the query results
        """
        try:
            engine = sqlalchemy.create_engine(connection_string)
            logger.info(f"Connecting to database and executing query...")
            df = pd.read_sql(query, engine)
            logger.info(f"Extracted {len(df)} rows from SQL source.")
            return df
        except Exception as e:
            logger.error(f"SQL extraction failed: {e}")
            raise

    def extract_from_csv(self, file_path: str, delimiter: str = ",") -> pd.DataFrame:
        """
        Extract data from a CSV or delimited flat file.

        Args:
            file_path: Path to the source CSV file
            delimiter: Column delimiter (default: comma)

        Returns:
            DataFrame containing the file contents
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")

        try:
            df = pd.read_csv(path, delimiter=delimiter)
            logger.info(f"Extracted {len(df)} rows from CSV: {file_path}")
            return df
        except Exception as e:
            logger.error(f"CSV extraction failed: {e}")
            raise

    def extract_from_api(self, url: str, headers: Optional[dict] = None) -> pd.DataFrame:
        """
        Extract data from a REST API endpoint returning JSON.

        Args:
            url: API endpoint URL
            headers: Optional request headers (e.g. auth tokens)

        Returns:
            DataFrame constructed from JSON response
        """
        import requests

        try:
            response = requests.get(url, headers=headers or {}, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Handle both list responses and nested data keys
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # Try common wrapper keys
                for key in ["data", "results", "records", "items"]:
                    if key in data:
                        df = pd.DataFrame(data[key])
                        break
                else:
                    df = pd.DataFrame([data])

            logger.info(f"Extracted {len(df)} records from API: {url}")
            return df
        except Exception as e:
            logger.error(f"API extraction failed: {e}")
            raise
