"""
Database connector for reading data from SQL databases.
Wraps config.db_config for use by agents.
"""
import pandas as pd
from typing import Optional
from sqlalchemy import text
from config.db_config import get_db_connection
from utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseConnector:
    """
    Database connector for reading and querying data.
    Uses SQLAlchemy for database-agnostic access.
    """

    def __init__(self):
        """Initialize database connector."""
        self.db = get_db_connection()

    def read_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Read entire table into a DataFrame.

        Args:
            table_name: Name of the table to read
            limit: Optional limit on number of rows

        Returns:
            DataFrame with table contents
        """
        try:
            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"

            logger.info(f"Reading table: {table_name}")
            df = pd.read_sql(query, self.db.get_engine())

            logger.info(f"Read {len(df)} records from {table_name}")
            return df

        except Exception as e:
            logger.error(f"Failed to read table {table_name}: {e}")
            raise

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a custom SQL query and return results.

        Args:
            query: SQL query string

        Returns:
            DataFrame with query results
        """
        try:
            logger.debug(f"Executing query: {query[:100]}...")
            df = pd.read_sql(query, self.db.get_engine())

            logger.info(f"Query returned {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        try:
            # Reflect metadata to check table existence
            self.db.metadata.reflect(bind=self.db.get_engine())
            return table_name in self.db.metadata.tables

        except Exception as e:
            logger.warning(f"Error checking table existence: {e}")
            return False

    def get_row_count(self, table_name: str) -> int:
        """
        Get the number of rows in a table.

        Args:
            table_name: Name of the table

        Returns:
            Number of rows
        """
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            with self.db.get_engine().connect() as conn:
                result = conn.execute(text(query))
                count = result.scalar()

            return count

        except Exception as e:
            logger.error(f"Failed to get row count: {e}")
            return 0
