"""
pipeline.py
-----------
Main ETL pipeline orchestrator.
Coordinates extraction, transformation, and loading with logging and error handling.
"""

import logging
import time
from datetime import datetime
from src.extractor import DataExtractor
from src.transformer import DataTransformer
from src.loader import DataLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
    ],
)

logger = logging.getLogger(__name__)


def run_pipeline(config: dict) -> dict:
    """
    Execute the full ETL pipeline.

    Args:
        config: Pipeline configuration dictionary

    Returns:
        Summary dict with row counts and execution time
    """
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("ETL Pipeline started.")
    logger.info("=" * 60)

    summary = {
        "status": "success",
        "rows_extracted": 0,
        "rows_loaded": 0,
        "duration_seconds": 0,
        "errors": [],
    }

    try:
        # --- EXTRACT ---
        extractor = DataExtractor(config["source"])
        source_type = config["source"].get("source_type", "sql")

        if source_type == "sql":
            df = extractor.extract_from_sql(
                query=config["source"]["query"],
                connection_string=config["source"]["connection_string"],
            )
        elif source_type == "csv":
            df = extractor.extract_from_csv(
                file_path=config["source"]["file_path"],
                delimiter=config["source"].get("delimiter", ","),
            )
        elif source_type == "api":
            df = extractor.extract_from_api(
                url=config["source"]["url"],
                headers=config["source"].get("headers"),
            )
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        summary["rows_extracted"] = len(df)
        logger.info(f"Extraction complete: {len(df)} rows.")

        # --- TRANSFORM ---
        transformer = DataTransformer(config["transform"])
        df = transformer.transform(df)
        logger.info(f"Transformation complete: {len(df)} rows after cleaning.")

        # --- LOAD ---
        loader = DataLoader(config["target"])
        target_type = config["target"].get("target_type", "sql")

        if target_type == "sql":
            loader.load_to_sql(
                df=df,
                table_name=config["target"]["table_name"],
                connection_string=config["target"]["connection_string"],
                if_exists=config["target"].get("if_exists", "append"),
            )
        elif target_type == "csv":
            loader.load_to_csv(df=df, output_path=config["target"]["output_path"])
        elif target_type == "azure_blob":
            loader.load_to_azure_blob(
                df=df,
                container_name=config["target"]["container_name"],
                blob_name=config["target"]["blob_name"],
                connection_string=config["target"]["connection_string"],
            )

        summary["rows_loaded"] = len(df)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        summary["status"] = "failed"
        summary["errors"].append(str(e))

    finally:
        elapsed = round(time.time() - start_time, 2)
        summary["duration_seconds"] = elapsed
        logger.info(f"Pipeline finished in {elapsed}s — status: {summary['status']}")
        logger.info("=" * 60)

    return summary


if __name__ == "__main__":
    # Example config — replace with your actual settings or load from config.yaml
    import yaml

    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    result = run_pipeline(config)
    print(result)
