"""
Create human_review_queue table for records requiring manual review.
"""
import sqlite3
from pathlib import Path

def create_human_review_table(db_path: str = 'ba_dedup.db'):
    """Create table for human review queue."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create human review queue table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS human_review_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Original record data
            source_record_id INTEGER,
            name_original TEXT,
            name_parsed TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            phone TEXT,
            email TEXT,
            contact_person TEXT,

            -- Review metadata
            review_keywords TEXT,  -- Comma-separated list of keywords that triggered review
            review_reason TEXT,    -- Why this record needs review
            flagged_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            -- Review workflow fields
            review_status TEXT DEFAULT 'pending',  -- pending, approved, rejected, merged
            reviewed_by TEXT,
            reviewed_date TIMESTAMP,
            review_notes TEXT,

            -- Decision fields
            decision TEXT,  -- keep_separate, merge_with_cluster, delete, etc.
            merge_with_cluster_id INTEGER,  -- If merging, which cluster to merge into
            merge_with_record_id INTEGER,   -- If merging, specific record ID

            -- Audit fields
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create indexes for common queries
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_review_status
        ON human_review_queue(review_status)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_review_keywords
        ON human_review_queue(review_keywords)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_flagged_date
        ON human_review_queue(flagged_date)
    """)

    # Create view for pending reviews
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS pending_reviews AS
        SELECT
            id,
            name_parsed,
            city,
            state,
            zip,
            review_keywords,
            review_reason,
            flagged_date,
            CASE
                WHEN address IS NOT NULL AND address != '' THEN 'Yes'
                ELSE 'No'
            END as has_address,
            CASE
                WHEN phone IS NOT NULL AND phone != '' THEN 'Yes'
                ELSE 'No'
            END as has_phone
        FROM human_review_queue
        WHERE review_status = 'pending'
        ORDER BY flagged_date DESC
    """)

    conn.commit()

    print('='*80)
    print('HUMAN REVIEW TABLE CREATED')
    print('='*80)
    print('Table: human_review_queue')
    print('View: pending_reviews (shows only pending records)')
    print('')
    print('Indexes created:')
    print('  - idx_review_status')
    print('  - idx_review_keywords')
    print('  - idx_flagged_date')
    print('='*80)

    conn.close()

if __name__ == '__main__':
    # Get database path
    project_root = Path(__file__).parent.parent.parent
    db_path = project_root / 'ba_dedup.db'

    print(f'Creating human review table in: {db_path}')
    create_human_review_table(str(db_path))
    print('\nMigration complete!')
