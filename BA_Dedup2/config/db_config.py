"""
Database connection factory and utilities.
Supports SQLite, SQL Server, PostgreSQL, and other SQLAlchemy-compatible databases.
"""
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseConnection:
    """
    Database connection manager using SQLAlchemy.
    Provides connection pooling and session management.
    """

    def __init__(self, connection_string=None):
        """
        Initialize database connection.

        Args:
            connection_string: SQLAlchemy connection string. If None, uses settings.DB_CONNECTION_STRING
        """
        self.connection_string = connection_string or settings.DB_CONNECTION_STRING
        self.engine = None
        self.Session = None
        self.metadata = MetaData()

    def connect(self):
        """Establish database connection and create session factory."""
        try:
            # Special handling for SQLite in-memory databases
            if 'sqlite' in self.connection_string.lower() and ':memory:' in self.connection_string:
                self.engine = create_engine(
                    self.connection_string,
                    connect_args={'check_same_thread': False},
                    poolclass=StaticPool
                )
            else:
                self.engine = create_engine(self.connection_string)

            self.Session = sessionmaker(bind=self.engine)
            self.metadata.reflect(bind=self.engine)

            logger.info(f"Connected to database: {self._mask_connection_string()}")
            return self.engine

        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def get_session(self):
        """Get a new database session."""
        if not self.Session:
            self.connect()
        return self.Session()

    def get_engine(self):
        """Get the database engine."""
        if not self.engine:
            self.connect()
        return self.engine

    def _mask_connection_string(self):
        """Mask sensitive information in connection string for logging."""
        if '@' in self.connection_string:
            parts = self.connection_string.split('@')
            if '://' in parts[0]:
                protocol = parts[0].split('://')[0]
                return f"{protocol}://***@{parts[1]}"
        return self.connection_string.split('://')[0] + '://***'

    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


# Global database connection instance
db = DatabaseConnection()


def get_db_connection():
    """Get the global database connection instance."""
    return db


def create_tables(metadata):
    """
    Create all tables defined in metadata.

    Args:
        metadata: SQLAlchemy MetaData object with table definitions
    """
    engine = db.get_engine()
    metadata.create_all(engine)
    logger.info("Database tables created successfully")
