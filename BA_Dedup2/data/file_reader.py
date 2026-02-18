"""
File reader for Excel and CSV files.
Handles encoding detection and type inference.
"""
import pandas as pd
from pathlib import Path
from typing import Optional
from utils.logger import get_logger

logger = get_logger(__name__)


class FileReader:
    """
    File reader for Excel and CSV files.
    Provides robust reading with encoding detection and error handling.
    """

    def read_csv(self, file_path: str, encoding: Optional[str] = None) -> pd.DataFrame:
        """
        Read CSV file into a DataFrame.

        Args:
            file_path: Path to CSV file
            encoding: Optional encoding (auto-detected if None)

        Returns:
            DataFrame with CSV contents
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        try:
            # Try to read with specified or default encoding
            if encoding:
                df = pd.read_csv(file_path, encoding=encoding)
            else:
                # Try common encodings
                encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
                df = None

                for enc in encodings:
                    try:
                        df = pd.read_csv(file_path, encoding=enc)
                        logger.info(f"Successfully read CSV with encoding: {enc}")
                        break
                    except (UnicodeDecodeError, UnicodeError):
                        continue

                if df is None:
                    # Fallback: read with error handling
                    df = pd.read_csv(file_path, encoding='utf-8', encoding_errors='replace')
                    logger.warning("Read CSV with error replacement")

            logger.info(f"Read CSV: {file_path} ({len(df)} rows, {len(df.columns)} columns)")
            return df

        except Exception as e:
            logger.error(f"Failed to read CSV file {file_path}: {e}")
            raise

    def read_excel(self, file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """
        Read Excel file into a DataFrame.

        Args:
            file_path: Path to Excel file
            sheet_name: Optional sheet name (reads first sheet if None)

        Returns:
            DataFrame with Excel contents
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"Excel file not found: {file_path}")

        try:
            # Read Excel file
            if sheet_name:
                df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
                logger.info(f"Read Excel sheet: {sheet_name}")
            else:
                df = pd.read_excel(file_path, engine='openpyxl')
                logger.info(f"Read first Excel sheet")

            logger.info(f"Read Excel: {file_path} ({len(df)} rows, {len(df.columns)} columns)")
            return df

        except Exception as e:
            logger.error(f"Failed to read Excel file {file_path}: {e}")
            raise

    def list_excel_sheets(self, file_path: str) -> list:
        """
        List all sheet names in an Excel file.

        Args:
            file_path: Path to Excel file

        Returns:
            List of sheet names
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"Excel file not found: {file_path}")

        try:
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            sheet_names = excel_file.sheet_names

            logger.info(f"Excel file contains {len(sheet_names)} sheets: {sheet_names}")
            return sheet_names

        except Exception as e:
            logger.error(f"Failed to list Excel sheets: {e}")
            raise

    def detect_file_type(self, file_path: str) -> str:
        """
        Detect file type based on extension.

        Args:
            file_path: Path to file

        Returns:
            File type ('csv' or 'excel')
        """
        file_path = Path(file_path)
        suffix = file_path.suffix.lower()

        if suffix in ['.xlsx', '.xls']:
            return 'excel'
        elif suffix == '.csv':
            return 'csv'
        else:
            raise ValueError(f"Unsupported file type: {suffix}")
