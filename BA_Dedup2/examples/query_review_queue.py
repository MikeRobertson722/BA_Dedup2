"""
Example queries for human review queue.
Demonstrates how to interact with the review database for UI development.
"""
import sqlite3
import pandas as pd
from datetime import datetime

DB_PATH = 'ba_dedup.db'

def get_pending_reviews(limit=50, offset=0):
    """Get pending reviews (paginated)."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT * FROM pending_reviews
        ORDER BY flagged_date DESC
        LIMIT ? OFFSET ?
    """
    df = pd.read_sql_query(query, conn, params=(limit, offset))
    conn.close()
    return df

def get_review_by_id(review_id):
    """Get single review record by ID."""
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM human_review_queue WHERE id = ?"
    df = pd.read_sql_query(query, conn, params=(review_id,))
    conn.close()
    return df.iloc[0] if len(df) > 0 else None

def approve_record(review_id, reviewed_by, notes=''):
    """Approve a record (keep as separate entity)."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE human_review_queue
        SET review_status = 'approved',
            decision = 'keep_separate',
            reviewed_by = ?,
            reviewed_date = CURRENT_TIMESTAMP,
            review_notes = ?,
            updated_date = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (reviewed_by, notes, review_id))
    conn.commit()
    rows_updated = cursor.rowcount
    conn.close()
    return rows_updated

def mark_for_merge(review_id, merge_with_cluster_id, reviewed_by, notes=''):
    """Mark record to be merged with another cluster."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE human_review_queue
        SET review_status = 'merged',
            decision = 'merge_with_cluster',
            merge_with_cluster_id = ?,
            reviewed_by = ?,
            reviewed_date = CURRENT_TIMESTAMP,
            review_notes = ?,
            updated_date = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (merge_with_cluster_id, reviewed_by, notes, review_id))
    conn.commit()
    rows_updated = cursor.rowcount
    conn.close()
    return rows_updated

def get_statistics():
    """Get review queue statistics."""
    conn = sqlite3.connect(DB_PATH)

    # Overall stats
    stats = {}
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM human_review_queue")
    stats['total'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM human_review_queue WHERE review_status = 'pending'")
    stats['pending'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM human_review_queue WHERE review_status = 'approved'")
    stats['approved'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM human_review_queue WHERE review_status = 'merged'")
    stats['merged'] = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM human_review_queue WHERE review_status = 'rejected'")
    stats['rejected'] = cursor.fetchone()[0]

    conn.close()
    return stats

def search_reviews(search_term):
    """Search reviews by name."""
    conn = sqlite3.connect(DB_PATH)
    query = """
        SELECT * FROM human_review_queue
        WHERE name_parsed LIKE ?
          AND review_status = 'pending'
        ORDER BY name_parsed
    """
    df = pd.read_sql_query(query, conn, params=(f'%{search_term}%',))
    conn.close()
    return df

# Example usage
if __name__ == '__main__':
    print('='*80)
    print('HUMAN REVIEW QUEUE - EXAMPLE QUERIES')
    print('='*80)

    # 1. Get statistics
    stats = get_statistics()
    print(f'\nStatistics:')
    print(f'  Total records: {stats["total"]:,}')
    print(f'  Pending: {stats["pending"]:,}')
    print(f'  Approved: {stats["approved"]:,}')
    print(f'  Merged: {stats["merged"]:,}')
    print(f'  Rejected: {stats["rejected"]:,}')

    # 2. Get first 10 pending reviews
    print(f'\n{"="*80}')
    print('FIRST 10 PENDING REVIEWS')
    print('='*80)
    pending = get_pending_reviews(limit=10)
    if len(pending) > 0:
        print(pending[['id', 'name_parsed', 'city', 'state', 'review_keywords']].to_string(index=False))

    # 3. Search example
    print(f'\n{"="*80}')
    print('SEARCH EXAMPLE: "TRUST"')
    print('='*80)
    results = search_reviews('TRUST')
    print(f'Found {len(results):,} matching records')
    if len(results) > 0:
        print(results[['id', 'name_parsed', 'city', 'state']].head(5).to_string(index=False))

    # 4. Example: Approve a record (commented out to avoid changing data)
    # review_id = 1
    # result = approve_record(review_id, 'demo_user', 'Verified as legitimate separate entity')
    # print(f'\nApproved record {review_id}')

    print(f'\n{"="*80}')
    print('For more examples, see: db/REVIEW_QUERIES.sql')
    print('='*80)
