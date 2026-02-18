"""
Create version tracking table for backup/restore workflow.
Tracks each deduplication run and associated backups.
"""
import sqlite3
from pathlib import Path

def create_version_tracking_table(db_path: str = 'ba_dedup.db'):
    """Create table for tracking deduplication runs and backups."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create version tracking table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ba_run_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Version identification
            version_timestamp TEXT NOT NULL,  -- YYYYMMDD_HHMMSS format
            version_name TEXT,                -- Optional: user-friendly name

            -- Backup information
            backup_file TEXT NOT NULL,        -- Path to backup file
            backup_size_mb REAL,              -- Backup file size in MB

            -- Run metadata
            description TEXT,                 -- What was being tested/fixed
            run_by TEXT,                      -- Username or identifier
            run_status TEXT DEFAULT 'backup_created',  -- backup_created, run_completed, restored

            -- Before-run statistics
            source_records_before INTEGER,
            review_queue_before INTEGER,
            pending_reviews_before INTEGER,

            -- After-run statistics (populated after run completes)
            source_records_after INTEGER,
            review_queue_after INTEGER,
            pending_reviews_after INTEGER,
            auto_merged_count INTEGER,
            flagged_for_review_count INTEGER,

            -- Configuration used
            fuzzy_threshold INTEGER,          -- e.g., 85
            blocking_strategy TEXT,           -- e.g., 'state_zip'
            review_keywords TEXT,             -- CSV list of keywords used

            -- Timestamps
            backup_created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            run_completed_date TIMESTAMP,
            restored_date TIMESTAMP,

            -- Notes and issues
            notes TEXT,                       -- User notes about this run
            issues_found TEXT,                -- Problems discovered during review
            code_changes TEXT                 -- What was fixed before next run
        )
    """)

    # Create indexes
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_version_timestamp
        ON ba_run_versions(version_timestamp)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_run_status
        ON ba_run_versions(run_status)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_backup_created_date
        ON ba_run_versions(backup_created_date DESC)
    """)

    # Create view for recent runs
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS recent_runs AS
        SELECT
            id,
            version_timestamp,
            version_name,
            description,
            run_status,
            backup_file,
            backup_size_mb,
            pending_reviews_before,
            flagged_for_review_count,
            backup_created_date,
            issues_found
        FROM ba_run_versions
        ORDER BY backup_created_date DESC
        LIMIT 20
    """)

    conn.commit()

    print('='*80)
    print('VERSION TRACKING TABLE CREATED')
    print('='*80)
    print('Table: ba_run_versions')
    print('View: recent_runs (shows last 20 runs)')
    print('')
    print('Indexes created:')
    print('  - idx_version_timestamp')
    print('  - idx_run_status')
    print('  - idx_backup_created_date')
    print('='*80)

    conn.close()

if __name__ == '__main__':
    # Get database path
    project_root = Path(__file__).parent.parent.parent
    db_path = project_root / 'ba_dedup.db'

    print(f'Creating version tracking table in: {db_path}')
    create_version_tracking_table(str(db_path))
    print('\nMigration complete!')
