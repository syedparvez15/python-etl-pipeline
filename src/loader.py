"""
loader.py
---------
Handles loading transformed data into target destinations:
- SQL databases (append, replace, upsert)
- CSV output files
- Azure Blob Storage
"""

import logging
import pandas as pd
import sqlalchemy
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Loads transformed DataFrames into target destinations.
    Supports SQL databases, flat files, and Azure Blob Storage.
    """

    def __init__(self, config: dict):
        self.config = config

    def load_to_sql(
        self,
        df: pd.DataFrame,
        table_name: str,
        connection_string: str,
        if_exists: Literal["append", "replace", "fail"] = "append",
        chunksize: int = 1000,
    ) -> None:
        """
        Load DataFrame into a SQL database table.

        Args:
            df: Transformed DataFrame to load
            table_name: Target table name
            connection_string: SQLAlchemy connection string
            if_exists: Behaviour if table exists — 'append', 'replace', or 'fail'
            chunksize: Number of rows per insert batch
        """
        try:
            engine = sqlalchemy.create_engine(connection_string)
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
            )
            logger.info(
                f"Loaded {len(df)} rows into table '{table_name}' "
                f"(mode: {if_exists})."
            )
        except Exception as e:
            logger.error(f"SQL load failed: {e}")
            raise

    def load_to_csv(self, df: pd.DataFrame, output_path: str) -> None:
        """
        Write DataFrame to a CSV file.

        Args:
            df: Transformed DataFrame
            output_path: Destination file path
        """
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        try:
            df.to_csv(path, index=False)
            logger.info(f"Exported {len(df)} rows to CSV: {output_path}")
        except Exception as e:
            logger.error(f"CSV export failed: {e}")
            raise

    def load_to_azure_blob(
        self,
        df: pd.DataFrame,
        container_name: str,
        blob_name: str,
        connection_string: str,
    ) -> None:
        """
        Upload DataFrame as a CSV to Azure Blob Storage.

        Args:
            df: Transformed DataFrame
            container_name: Azure Blob container name
            blob_name: Target blob file name (e.g. 'output/data.csv')
            connection_string: Azure Storage connection string
        """
        try:
            from azure.storage.blob import BlobServiceClient

            csv_data = df.to_csv(index=False).encode("utf-8")
            blob_service = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service.get_blob_client(
                container=container_name, blob=blob_name
            )
            blob_client.upload_blob(csv_data, overwrite=True)
            logger.info(
                f"Uploaded {len(df)} rows to Azure Blob: "
                f"{container_name}/{blob_name}"
            )
        except ImportError:
            logger.error("azure-storage-blob package not installed.")
            raise
        except Exception as e:
            logger.error(f"Azure Blob upload failed: {e}")
            raise
