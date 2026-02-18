"""
Migration script to add cluster_id and similarity_score columns to existing tables.
Run this once to update the database schema.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.db_connector import DatabaseConnector
from utils.logger import get_logger

logger = get_logger('migration')


def main():
    """Add cluster_id and similarity_score columns to ba_source_records table."""

    logger.info("=" * 80)
    logger.info("MIGRATION: Adding cluster_id and similarity_score columns")
    logger.info("=" * 80)
    logger.info("")

    db = DatabaseConnector()
    engine = db.db.get_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    try:
        # Check if columns already exist
        cursor.execute("PRAGMA table_info(ba_source_records)")
        columns = [row[1] for row in cursor.fetchall()]

        if 'cluster_id' in columns:
            logger.info("✓ Column 'cluster_id' already exists")
        else:
            logger.info("Adding column 'cluster_id' to ba_source_records...")
            cursor.execute("""
                ALTER TABLE ba_source_records
                ADD COLUMN cluster_id INTEGER
            """)
            logger.info("✓ Added column 'cluster_id'")

        if 'similarity_score' in columns:
            logger.info("✓ Column 'similarity_score' already exists")
        else:
            logger.info("Adding column 'similarity_score' to ba_source_records...")
            cursor.execute("""
                ALTER TABLE ba_source_records
                ADD COLUMN similarity_score REAL
            """)
            logger.info("✓ Added column 'similarity_score'")

        conn.commit()
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ MIGRATION COMPLETE")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        conn.rollback()
        return 1

    finally:
        conn.close()

    return 0


if __name__ == '__main__':
    sys.exit(main())
