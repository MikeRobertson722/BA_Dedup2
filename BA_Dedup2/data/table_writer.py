"""
Table writer for writing DataFrames to database tables.
Handles table creation, batch writing, and error handling.
"""
import pandas as pd
from typing import Optional, Literal
from sqlalchemy import inspect
from config.db_config import get_db_connection
from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class TableWriter:
    """
    Table writer for persisting DataFrames to database tables.
    Supports batch writing and table creation.
    """

    def __init__(self):
        """Initialize table writer."""
        self.db = get_db_connection()

    def write_table(self,
                   df: pd.DataFrame,
                   table_name: str,
                   if_exists: Literal['fail', 'replace', 'append'] = 'append',
                   batch_size: Optional[int] = None) -> int:
        """
        Write DataFrame to database table.

        Args:
            df: DataFrame to write
            table_name: Name of destination table
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            batch_size: Number of rows per batch (uses settings.BATCH_SIZE if None)

        Returns:
            Number of rows written
        """
        if df.empty:
            logger.warning(f"Attempted to write empty DataFrame to {table_name}")
            return 0

        batch_size = batch_size or settings.BATCH_SIZE

        try:
            logger.info(f"Writing {len(df)} records to table: {table_name}")

            # Ensure table exists (for append mode)
            if if_exists == 'append' and not self._table_exists(table_name):
                logger.info(f"Table {table_name} does not exist, creating...")
                if_exists = 'replace'  # Create table on first write

            # Write to database
            df.to_sql(
                name=table_name,
                con=self.db.get_engine(),
                if_exists=if_exists,
                index=False,
                chunksize=batch_size
            )

            logger.info(f"Successfully wrote {len(df)} records to {table_name}")
            return len(df)

        except Exception as e:
            logger.error(f"Failed to write to table {table_name}: {e}")
            raise

    def write_batch(self,
                   df: pd.DataFrame,
                   table_name: str,
                   batch_size: Optional[int] = None):
        """
        Write DataFrame in batches with progress logging.

        Args:
            df: DataFrame to write
            table_name: Name of destination table
            batch_size: Number of rows per batch
        """
        batch_size = batch_size or settings.BATCH_SIZE
        total_rows = len(df)
        batches = (total_rows + batch_size - 1) // batch_size

        logger.info(f"Writing {total_rows} records in {batches} batches")

        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i + batch_size]
            batch_num = (i // batch_size) + 1

            if_exists = 'replace' if i == 0 else 'append'

            self.write_table(batch, table_name, if_exists=if_exists, batch_size=None)
            logger.info(f"Wrote batch {batch_num}/{batches}")

    def _table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        try:
            inspector = inspect(self.db.get_engine())
            return table_name in inspector.get_table_names()

        except Exception as e:
            logger.warning(f"Error checking table existence: {e}")
            return False

    def truncate_table(self, table_name: str):
        """
        Truncate (empty) a table.

        Args:
            table_name: Name of table to truncate
        """
        try:
            with self.db.get_engine().connect() as conn:
                # Use DELETE instead of TRUNCATE for better compatibility
                conn.execute(f"DELETE FROM {table_name}")
                conn.commit()

            logger.info(f"Truncated table: {table_name}")

        except Exception as e:
            logger.error(f"Failed to truncate table {table_name}: {e}")
            raise

    def drop_table(self, table_name: str):
        """
        Drop a table if it exists.

        Args:
            table_name: Name of table to drop
        """
        try:
            with self.db.get_engine().connect() as conn:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.commit()

            logger.info(f"Dropped table: {table_name}")

        except Exception as e:
            logger.error(f"Failed to drop table {table_name}: {e}")
            raise
