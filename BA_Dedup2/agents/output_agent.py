"""
Output Agent - Writes deduplicated records to destination (database or file).
Also writes audit trail for traceability.
"""
import pandas as pd
from typing import Dict, Any, Optional
from agents.base_agent import BaseAgent
from data.table_writer import TableWriter
from data.file_reader import FileReader
from config import settings


class OutputAgent(BaseAgent):
    """
    Agent responsible for writing deduplicated results to destination.
    Supports database tables and file exports (CSV, Excel).
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize output agent.

        Args:
            config: Configuration dictionary with keys:
                - output_type: 'database', 'csv', or 'excel'
                - output_table: Table name for database output
                - output_path: File path for file output
                - write_audit: Whether to write audit trail
        """
        super().__init__('output', config)
        self.output_type = self.config.get('output_type', 'database')
        self.output_table = self.config.get('output_table', settings.OUTPUT_TABLE)
        self.output_audit_table = self.config.get('output_audit_table', settings.OUTPUT_AUDIT_TABLE)
        self.output_path = self.config.get('output_path', None)
        self.write_audit = self.config.get('write_audit', True)
        self.merge_audit = self.config.get('merge_audit', None)

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Write deduplicated data to destination.

        Args:
            data: Deduplicated DataFrame to write

        Returns:
            Same DataFrame (passthrough)
        """
        self.logger.info(f"Writing {len(data)} deduplicated records to {self.output_type}")

        # Clean metadata columns before writing
        df_clean = self._clean_metadata_columns(data)

        # Write to destination
        if self.output_type == 'database':
            self._write_to_database(df_clean)
        elif self.output_type in ['csv', 'excel']:
            self._write_to_file(df_clean)
        else:
            raise ValueError(f"Unsupported output type: {self.output_type}")

        # Write audit trail if enabled
        if self.write_audit and self.merge_audit is not None:
            self._write_audit_trail()

        self.logger.info("Output complete")

        return data

    def _clean_metadata_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove internal metadata columns before writing.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with metadata columns removed
        """
        metadata_cols = [col for col in df.columns if col.startswith('_')]
        normalized_cols = [col for col in df.columns if col.endswith('_normalized')]

        cols_to_drop = metadata_cols + normalized_cols

        if cols_to_drop:
            df_clean = df.drop(columns=cols_to_drop)
            self.logger.debug(f"Removed {len(cols_to_drop)} metadata columns")
            return df_clean
        else:
            # No columns to drop, return as-is (no copy needed for read-only output)
            return df

    def _write_to_database(self, df: pd.DataFrame):
        """Write data to database table."""
        writer = TableWriter()

        # Write main deduplicated table
        writer.write_table(
            df,
            table_name=self.output_table,
            if_exists='replace'  # Replace existing table
        )

        self.logger.info(f"Wrote {len(df)} records to table: {self.output_table}")

    def _write_to_file(self, df: pd.DataFrame):
        """Write data to CSV or Excel file."""
        if not self.output_path:
            raise ValueError("output_path must be specified for file output")

        if self.output_type == 'csv':
            df.to_csv(self.output_path, index=False)
            self.logger.info(f"Wrote {len(df)} records to CSV: {self.output_path}")

        elif self.output_type == 'excel':
            df.to_excel(self.output_path, index=False, engine='openpyxl')
            self.logger.info(f"Wrote {len(df)} records to Excel: {self.output_path}")

    def _write_audit_trail(self):
        """Write merge audit trail for traceability."""
        if not isinstance(self.merge_audit, pd.DataFrame):
            # Convert to DataFrame if it's a list
            if isinstance(self.merge_audit, list):
                audit_df = pd.DataFrame(self.merge_audit)
            else:
                self.logger.warning("Merge audit is not in expected format")
                return
        else:
            audit_df = self.merge_audit

        if audit_df.empty:
            self.logger.info("No audit records to write")
            return

        if self.output_type == 'database':
            writer = TableWriter()
            writer.write_table(
                audit_df,
                table_name=self.output_audit_table,
                if_exists='replace'
            )
            self.logger.info(f"Wrote {len(audit_df)} audit records to table: {self.output_audit_table}")

        elif self.output_type in ['csv', 'excel']:
            # Write audit to a separate file
            if self.output_path:
                from pathlib import Path
                output_path = Path(self.output_path)
                audit_path = output_path.parent / f"{output_path.stem}_audit{output_path.suffix}"

                if self.output_type == 'csv':
                    audit_df.to_csv(audit_path, index=False)
                else:
                    audit_df.to_excel(audit_path, index=False, engine='openpyxl')

                self.logger.info(f"Wrote audit trail to: {audit_path}")

    def validate(self, result: pd.DataFrame) -> bool:
        """
        Validate output results.

        Args:
            result: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        if not super().validate(result):
            return False

        # Check that data was actually written
        # (In a real implementation, would verify file/table exists)

        return True
