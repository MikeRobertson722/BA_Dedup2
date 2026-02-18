"""
Ingestion Skills - Reusable data reading and field mapping functions.
Extracted from IngestionAgent for standalone use.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Optional
from data.file_reader import FileReader
from data.db_connector import DatabaseConnector
from utils.logger import get_logger

logger = get_logger(__name__)


def read_csv_file(file_path: str, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Read data from CSV file.

    Args:
        file_path: Path to CSV file
        encoding: File encoding (default: utf-8)

    Returns:
        DataFrame with CSV data

    Example:
        df = read_csv_file('input/sample_data.csv')
    """
    logger.info(f"Reading CSV file: {file_path}")
    file_reader = FileReader()
    df = file_reader.read_csv(file_path, encoding=encoding)
    logger.info(f"Read {len(df)} records from CSV")
    return df


def read_excel_file(file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """
    Read data from Excel file.

    Args:
        file_path: Path to Excel file
        sheet_name: Optional sheet name (default: first sheet)

    Returns:
        DataFrame with Excel data

    Example:
        df = read_excel_file('input/business_associates.xlsx', sheet_name='Sheet1')
    """
    logger.info(f"Reading Excel file: {file_path}")
    file_reader = FileReader()
    df = file_reader.read_excel(file_path, sheet_name=sheet_name)
    logger.info(f"Read {len(df)} records from Excel")
    return df


def read_database_table(table_name: str, db_path: Optional[str] = None) -> pd.DataFrame:
    """
    Read data from database table.

    Args:
        table_name: Name of database table
        db_path: Optional database path (default: from settings)

    Returns:
        DataFrame with table data

    Example:
        df = read_database_table('ba_source_records')
    """
    logger.info(f"Reading database table: {table_name}")
    db_connector = DatabaseConnector(db_path=db_path)
    df = db_connector.read_table(table_name)
    logger.info(f"Read {len(df)} records from database")
    return df


def apply_field_mappings(df: pd.DataFrame, field_map: Dict[str, str]) -> pd.DataFrame:
    """
    Apply field name mappings to rename columns.

    Args:
        df: Input DataFrame
        field_map: Dictionary mapping old column names to new names
                  Example: {'Business Name': 'name', 'Street': 'address'}

    Returns:
        DataFrame with renamed columns

    Example:
        field_map = {'Business Name': 'name', 'Contact Phone': 'phone'}
        df = apply_field_mappings(df, field_map)
    """
    rename_dict = {}
    for old_name, new_name in field_map.items():
        if old_name in df.columns:
            rename_dict[old_name] = new_name
            logger.debug(f"Mapping field: {old_name} -> {new_name}")

    if rename_dict:
        df = df.rename(columns=rename_dict)
        logger.info(f"Applied {len(rename_dict)} field mappings")
    else:
        logger.debug("No field mappings to apply")

    return df


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to lowercase with underscores.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with normalized column names

    Example:
        # Before: ['Business Name', 'Contact Person', 'ZIP Code']
        # After:  ['business_name', 'contact_person', 'zip_code']
        df = normalize_column_names(df)
    """
    original_columns = list(df.columns)
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
    normalized_columns = list(df.columns)

    logger.debug(f"Normalized {len(df.columns)} column names")

    # Log any columns that changed
    changed = [(orig, norm) for orig, norm in zip(original_columns, normalized_columns) if orig != norm]
    if changed:
        for orig, norm in changed[:5]:  # Log first 5 changes
            logger.debug(f"  {orig} -> {norm}")

    return df


def ingest_data(
    source_type: str,
    source_path: Optional[str] = None,
    table_name: Optional[str] = None,
    field_map: Optional[Dict[str, str]] = None,
    normalize_columns: bool = True
) -> pd.DataFrame:
    """
    Unified data ingestion function supporting multiple source types.

    Args:
        source_type: Type of source ('csv', 'excel', 'database')
        source_path: Path to file (for csv/excel)
        table_name: Table name (for database)
        field_map: Optional field name mappings
        normalize_columns: Whether to normalize column names (default: True)

    Returns:
        DataFrame with ingested data

    Example:
        # Read from CSV with field mappings
        df = ingest_data(
            source_type='csv',
            source_path='input/sample_data.csv',
            field_map={'Business Name': 'name'},
            normalize_columns=True
        )

        # Read from database
        df = ingest_data(
            source_type='database',
            table_name='ba_source_records'
        )
    """
    logger.info(f"Ingesting data from {source_type}")

    # Read based on source type
    if source_type == 'csv':
        if not source_path:
            raise ValueError("source_path required for CSV ingestion")
        df = read_csv_file(source_path)

    elif source_type == 'excel':
        if not source_path:
            raise ValueError("source_path required for Excel ingestion")
        df = read_excel_file(source_path)

    elif source_type == 'database':
        if not table_name:
            raise ValueError("table_name required for database ingestion")
        df = read_database_table(table_name)

    else:
        raise ValueError(f"Unsupported source type: {source_type}")

    # Apply field mappings if provided
    if field_map:
        df = apply_field_mappings(df, field_map)

    # Normalize column names if requested
    if normalize_columns:
        df = normalize_column_names(df)

    logger.info(f"Ingestion complete: {len(df)} records with {len(df.columns)} columns")

    return df
