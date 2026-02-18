"""
Database migration: Add versioning tables for merge tracking and recovery.

Run this script to add versioning capabilities to an existing BA_Dedup2 database.

Usage:
    python db/migrations/add_versioning_tables.py [database_path]

If no database_path is provided, uses default from config.
"""
import sys
import sqlite3
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.logger import get_logger

logger = get_logger(__name__)


def migrate_add_versioning_tables(db_path: str):
    """
    Add versioning tables to database.

    Creates:
    - ba_merge_operations: High-level merge tracking
    - ba_record_versions: Detailed record snapshots
    - ba_merge_relationships: Which records were merged together

    Args:
        db_path: Path to SQLite database file
    """
    logger.info(f"Starting migration: Add versioning tables to {db_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Check if tables already exist
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='ba_merge_operations'
        """)

        if cursor.fetchone():
            logger.warning("Versioning tables already exist. Skipping migration.")
            print("Versioning tables already exist. No migration needed.")
            return

        # Table 1: Merge operations (high-level tracking)
        logger.info("Creating ba_merge_operations table...")
        cursor.execute("""
            CREATE TABLE ba_merge_operations (
                operation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                operation_type TEXT,  -- 'auto_merge', 'manual_merge', 'undo'
                user_id TEXT,
                cluster_id INTEGER,
                record_count INTEGER,
                golden_record_id TEXT,
                is_undone INTEGER DEFAULT 0,
                undone_by_operation_id INTEGER,
                notes TEXT
            )
        """)

        # Table 2: Record versions (detailed snapshots)
        logger.info("Creating ba_record_versions table...")
        cursor.execute("""
            CREATE TABLE ba_record_versions (
                version_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id INTEGER,
                record_id TEXT,
                version_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                version_type TEXT,  -- 'before_merge', 'after_merge', 'golden'
                record_data TEXT,  -- JSON snapshot of record
                FOREIGN KEY (operation_id) REFERENCES ba_merge_operations(operation_id)
            )
        """)

        # Table 3: Merge relationships (which records were merged)
        logger.info("Creating ba_merge_relationships table...")
        cursor.execute("""
            CREATE TABLE ba_merge_relationships (
                relationship_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id INTEGER,
                source_record_id TEXT,
                target_record_id TEXT,  -- golden record ID
                similarity_score REAL,
                FOREIGN KEY (operation_id) REFERENCES ba_merge_operations(operation_id)
            )
        """)

        # Create indexes for performance
        logger.info("Creating indexes...")
        cursor.execute("""
            CREATE INDEX idx_merge_ops_timestamp
            ON ba_merge_operations(operation_timestamp)
        """)

        cursor.execute("""
            CREATE INDEX idx_merge_ops_cluster
            ON ba_merge_operations(cluster_id)
        """)

        cursor.execute("""
            CREATE INDEX idx_record_versions_operation
            ON ba_record_versions(operation_id)
        """)

        cursor.execute("""
            CREATE INDEX idx_record_versions_record
            ON ba_record_versions(record_id)
        """)

        cursor.execute("""
            CREATE INDEX idx_merge_relationships_operation
            ON ba_merge_relationships(operation_id)
        """)

        cursor.execute("""
            CREATE INDEX idx_merge_relationships_source
            ON ba_merge_relationships(source_record_id)
        """)

        conn.commit()

        logger.info("Migration complete! Versioning tables created successfully.")
        print("\nMigration successful!")
        print("Created tables:")
        print("  - ba_merge_operations")
        print("  - ba_record_versions")
        print("  - ba_merge_relationships")
        print("Created indexes for performance")

    except Exception as e:
        conn.rollback()
        logger.error(f"Migration failed: {str(e)}")
        print(f"\nMigration failed: {str(e)}")
        raise

    finally:
        conn.close()


def verify_migration(db_path: str):
    """
    Verify that migration was successful.

    Args:
        db_path: Path to database
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check all tables exist
    expected_tables = [
        'ba_merge_operations',
        'ba_record_versions',
        'ba_merge_relationships'
    ]

    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table'
        ORDER BY name
    """)

    existing_tables = [row[0] for row in cursor.fetchall()]

    all_exist = all(table in existing_tables for table in expected_tables)

    if all_exist:
        print("\nVerification: All versioning tables exist")
        return True
    else:
        missing = [t for t in expected_tables if t not in existing_tables]
        print(f"\nVerification failed: Missing tables: {missing}")
        return False


if __name__ == '__main__':
    # Get database path from command line or use default
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        # Use default from config
        try:
            from config import settings
            db_path = settings.DATABASE_PATH
        except:
            db_path = 'ba_dedup.db'

    print(f"Database: {db_path}")
    print("="*80)

    # Run migration
    migrate_add_versioning_tables(db_path)

    # Verify
    verify_migration(db_path)

    print("\nYou can now use the MergeVersionManager for tracking and recovery!")
