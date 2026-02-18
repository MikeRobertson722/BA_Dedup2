"""
Ingestion Agent - Reads data from various sources (Excel, CSV, Database).
Normalizes column names and returns a standardized DataFrame.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Any
from agents.base_agent import BaseAgent
from data.file_reader import FileReader
from data.db_connector import DatabaseConnector
from config import settings


class IngestionAgent(BaseAgent):
    """
    Agent responsible for reading data from source (file or database).
    Supports Excel (.xlsx), CSV, and database tables.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize ingestion agent.

        Args:
            config: Configuration dictionary with keys:
                - source_type: 'excel', 'csv', or 'database'
                - source_path: Path to file (for excel/csv)
                - table_name: Table name (for database)
                - field_map: Optional field name mappings
        """
        super().__init__('ingestion', config)
        self.source_type = self.config.get('source_type', settings.INPUT_TYPE)
        self.source_path = self.config.get('source_path', settings.INPUT_PATH)
        self.table_name = self.config.get('table_name', settings.INPUT_TABLE)
        self.field_map = self.config.get('field_map', settings.FIELD_MAP)

    def execute(self, data: pd.DataFrame = None) -> pd.DataFrame:
        """
        Read data from the configured source.

        Args:
            data: Ignored for ingestion agent (reads from external source)

        Returns:
            DataFrame with ingested data
        """
        self.logger.info(f"Ingesting data from {self.source_type}: {self.source_path or self.table_name}")

        # Read based on source type
        if self.source_type in ['csv', 'excel']:
            df = self._read_file()
        elif self.source_type == 'database':
            df = self._read_database()
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

        # Apply field mappings if configured
        if self.field_map:
            df = self._apply_field_map(df)

        # Normalize column names (lowercase, underscore)
        df.columns = df.columns.str.lower().str.replace(' ', '_')

        self.logger.info(f"Ingested {len(df)} records with {len(df.columns)} columns")
        self.logger.debug(f"Columns: {list(df.columns)}")

        return df

    def _read_file(self) -> pd.DataFrame:
        """Read data from file (CSV or Excel)."""
        file_reader = FileReader()

        if self.source_type == 'csv':
            return file_reader.read_csv(self.source_path)
        elif self.source_type == 'excel':
            return file_reader.read_excel(self.source_path)
        else:
            raise ValueError(f"Unsupported file type: {self.source_type}")

    def _read_database(self) -> pd.DataFrame:
        """Read data from database table."""
        db_connector = DatabaseConnector()
        return db_connector.read_table(self.table_name)

    def _apply_field_map(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply field name mappings to rename columns.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with renamed columns
        """
        rename_dict = {}
        for old_name, new_name in self.field_map.items():
            if old_name in df.columns:
                rename_dict[old_name] = new_name
                self.logger.debug(f"Mapping field: {old_name} -> {new_name}")

        if rename_dict:
            df = df.rename(columns=rename_dict)
            self.logger.info(f"Applied {len(rename_dict)} field mappings")

        return df

    def validate(self, result: pd.DataFrame) -> bool:
        """
        Validate ingestion results.

        Args:
            result: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        if not super().validate(result):
            return False

        if len(result) == 0:
            self.logger.warning("No records ingested")
            return False

        return True
