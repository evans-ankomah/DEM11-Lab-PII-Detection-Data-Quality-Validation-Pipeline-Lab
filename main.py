"""
Main entry point for PII Detection & Data Quality Validation Pipeline.
"""

import logging
import sys
from pathlib import Path
import yaml

from src.pipeline import DataPipeline


def setup_logging(log_dir: str = "logs", log_level: str = "DEBUG"):
    """Configure logging to file and console."""
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / "pipeline.log"
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    
    # File handler (DEBUG and above)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler (INFO and above)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    logging.info(f"Logging configured: {log_file}")


def load_config(config_file: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        logging.info(f"Configuration loaded from {config_file}")
        return config
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        return {}


def main():
    """Main pipeline execution."""
    # Setup logging
    setup_logging(log_dir="logs", log_level="DEBUG")
    
    logger = logging.getLogger(__name__)
    logger.info("Starting PII Detection & Data Quality Validation Pipeline")
    
    # Load configuration
    config = load_config("config/config.yaml")
    
    input_csv = config.get('paths', {}).get('input_csv', 'data/customers_raw.csv')
    output_dir = config.get('paths', {}).get('output_dir', 'output')
    
    # Verify input file exists
    if not Path(input_csv).exists():
        logger.error(f"Input file not found: {input_csv}")
        return False
    
    # Create and execute pipeline
    pipeline = DataPipeline(input_csv, output_dir)
    success = pipeline.execute()
    
    if success:
        logger.info("Pipeline execution completed successfully!")
        logger.info("Output files saved to: %s", output_dir)
        return True
    else:
        logger.error("Pipeline execution failed!")
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logging.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
