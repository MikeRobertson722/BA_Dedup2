# Human Review System - Implementation Guide

## Overview

The human review system automatically flags sensitive entity types (Trusts, Estates, Departments, etc.) and stores them in a database table for manual review through a future UI.

---

## Database Schema

### Table: `human_review_queue`

| Column | Type | Description |
|--------|------|-------------|
| **id** | INTEGER | Primary key (auto-increment) |
| **source_record_id** | INTEGER | Original record ID from source data |
| **name_original** | TEXT | Original business name from source |
| **name_parsed** | TEXT | Parsed/normalized name |
| **address** | TEXT | Street address |
| **city** | TEXT | City |
| **state** | TEXT | State |
| **zip** | TEXT | ZIP code |
| **phone** | TEXT | Phone number |
| **email** | TEXT | Email address |
| **contact_person** | TEXT | Contact person name |
| **review_keywords** | TEXT | Comma-separated keywords that triggered review |
| **review_reason** | TEXT | Explanation of why review is needed |
| **flagged_date** | TIMESTAMP | When record was flagged |
| **review_status** | TEXT | Status: `pending`, `approved`, `rejected`, `merged` |
| **reviewed_by** | TEXT | Username of reviewer |
| **reviewed_date** | TIMESTAMP | When review was completed |
| **review_notes** | TEXT | Reviewer's notes/comments |
| **decision** | TEXT | Decision: `keep_separate`, `merge_with_cluster`, `delete` |
| **merge_with_cluster_id** | INTEGER | If merging, target cluster ID |
| **merge_with_record_id** | INTEGER | If merging, specific record ID |

### View: `pending_reviews`

Simplified view showing only pending reviews with key fields.

---

## Current Statistics

```
Total records: 457
Pending: 457
Approved: 0
Merged: 0
Rejected: 0
```

### Breakdown by Keyword

| Keyword | Count | Description |
|---------|-------|-------------|
| **TRUST** | 235 | Trust entities (e.g., "Smith Family Trust") |
| **ESTATE** | 98 | Estate entities |
| **TR** | 88 | Trust abbreviations |
| **TRUSTEE** | 24 | Trustee designations |
| **DEPT** | 8 | Department abbreviations |
| **DEPARTMENT** | 2 | Department names |
| **DIVISION** | 1 | Division names |

---

## Files Created

### 1. Database Migration
**File:** `db/migrations/create_human_review_table.py`
- Creates `human_review_queue` table
- Creates `pending_reviews` view
- Creates indexes for performance

**Run:** `python db/migrations/create_human_review_table.py`

### 2. Review Keywords Configuration
**File:** `config/review_keywords.py`
- Configurable list of keywords requiring review
- Reason descriptions for each keyword
- Easy to add new keywords

### 3. Deduplication Script
**File:** `run_dedup_with_db_review.py`
- Runs fuzzy deduplication
- Flags records for review
- Populates database table
- Creates backup CSV files

**Run:** `python run_dedup_with_db_review.py`

### 4. SQL Query Reference
**File:** `db/REVIEW_QUERIES.sql`
- 15+ example queries for UI development
- Common operations (approve, reject, merge)
- Search and filter examples
- Statistics queries

### 5. Python Query Examples
**File:** `examples/query_review_queue.py`
- Python functions for database operations
- Example usage patterns
- Ready to integrate into UI

**Run:** `python examples/query_review_queue.py`

---

## Adding New Review Keywords

Edit `config/review_keywords.py`:

```python
HUMAN_REVIEW_KEYWORDS = [
    'TRUST',
    'TRUSTEE',
    'ESTATE',
    'DEPARTMENT',
    'DIVISION',

    # Add new keywords here:
    'FOUNDATION',
    'CHARITY',
    'EXECUTOR',
    'ADMINISTRATOR',
    'BENEFICIARY',
    'CONSERVATOR',
    # ... etc.
]

# Add reason descriptions:
REVIEW_REASONS = {
    'FOUNDATION': 'Foundation entities require verification',
    'CHARITY': 'Charitable organizations require verification',
    # ... etc.
}
```

Then re-run: `python run_dedup_with_db_review.py`

---

## Example UI Workflow

### 1. Display Pending Reviews

```sql
SELECT * FROM pending_reviews
ORDER BY flagged_date DESC
LIMIT 50 OFFSET 0;  -- Pagination
```

### 2. Show Record Details

```sql
SELECT * FROM human_review_queue
WHERE id = 123;
```

### 3. Approve Record (Keep Separate)

```sql
UPDATE human_review_queue
SET review_status = 'approved',
    decision = 'keep_separate',
    reviewed_by = 'john_doe',
    reviewed_date = CURRENT_TIMESTAMP,
    review_notes = 'Verified as legitimate entity'
WHERE id = 123;
```

### 4. Mark for Merging

```sql
UPDATE human_review_queue
SET review_status = 'merged',
    decision = 'merge_with_cluster',
    merge_with_cluster_id = 5678,
    reviewed_by = 'john_doe',
    reviewed_date = CURRENT_TIMESTAMP,
    review_notes = 'Duplicate of cluster 5678'
WHERE id = 123;
```

### 5. Bulk Operations

```sql
-- Approve all similar records
UPDATE human_review_queue
SET review_status = 'approved',
    reviewed_by = 'john_doe',
    reviewed_date = CURRENT_TIMESTAMP
WHERE name_parsed = '2003 COMSTOCK CHILDRENS TRUST FBO'
  AND review_status = 'pending';
```

---

## Python Integration Example

```python
import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('ba_dedup.db')

# Get pending reviews
df = pd.read_sql_query("""
    SELECT * FROM pending_reviews
    ORDER BY flagged_date DESC
    LIMIT 50
""", conn)

# Approve a record
cursor = conn.cursor()
cursor.execute("""
    UPDATE human_review_queue
    SET review_status = 'approved',
        decision = 'keep_separate',
        reviewed_by = ?,
        reviewed_date = CURRENT_TIMESTAMP
    WHERE id = ?
""", ('john_doe', 123))
conn.commit()

conn.close()
```

---

## Known Potential Duplicates in Review Queue

These records appear multiple times and may actually BE duplicates:

| Name | Count | Locations |
|------|-------|-----------|
| **2003 COMSTOCK CHILDRENS TRUST FBO** | 3 | Dallas, TX (3×) |
| **ALVRONE SATER TRUSTEE** | 4 | Evansville, IN (2×), Dallas, TX (2×) |
| **ALBERT L STIFFLER IRREVOCABLE TRUST** | 3 | Indiana, PA (2×), Omaha, NE |

These should be high priority for review as they may legitimately need merging.

---

## UI Development Recommendations

### Must-Have Features

1. **List View**
   - Paginated table of pending reviews
   - Sort by: date, name, keyword type
   - Filter by: keyword, state, status

2. **Detail View**
   - Full record information
   - Review history
   - Action buttons: Approve, Reject, Merge

3. **Search**
   - Search by name, city, state
   - Filter by review keyword

4. **Dashboard**
   - Statistics (pending, approved, rejected)
   - Breakdown by keyword type
   - Reviewer performance metrics

5. **Bulk Actions**
   - Select multiple records
   - Bulk approve/reject
   - Find similar records

### Nice-to-Have Features

1. **Duplicate Detection**
   - Show similar records in review queue
   - Suggest potential merges

2. **Review History**
   - Audit log of all decisions
   - Ability to undo reviews

3. **Notes/Comments**
   - Add notes to records
   - Tag records for follow-up

4. **Assignment**
   - Assign records to specific reviewers
   - Track workload distribution

---

## Database Maintenance

### Backup Before Review Sessions

```bash
sqlite3 ba_dedup.db ".backup ba_dedup_backup_$(date +%Y%m%d).db"
```

### Archive Completed Reviews

```sql
-- Archive reviews older than 30 days
CREATE TABLE IF NOT EXISTS human_review_archive AS
SELECT * FROM human_review_queue WHERE 1=0;

INSERT INTO human_review_archive
SELECT * FROM human_review_queue
WHERE review_status IN ('approved', 'rejected', 'merged')
  AND reviewed_date < datetime('now', '-30 days');

DELETE FROM human_review_queue
WHERE id IN (SELECT id FROM human_review_archive);
```

### Query Performance

All common queries are indexed:
- `idx_review_status` - Fast filtering by status
- `idx_review_keywords` - Fast filtering by keyword
- `idx_flagged_date` - Fast sorting by date

---

## Next Steps

1. ✅ **Database table created** - Ready for use
2. ✅ **457 records populated** - Trusts, Estates, Departments flagged
3. ✅ **Query examples provided** - SQL and Python
4. ⏳ **Build review UI** - Use queries from `db/REVIEW_QUERIES.sql`
5. ⏳ **Add more keywords** - Expand `config/review_keywords.py` as needed
6. ⏳ **Process reviews** - Human review and decisions
7. ⏳ **Export approved records** - Merge back into golden records

---

## Support Files

| File | Purpose |
|------|---------|
| `db/REVIEW_QUERIES.sql` | SQL query reference |
| `examples/query_review_queue.py` | Python query examples |
| `config/review_keywords.py` | Keyword configuration |
| `db/migrations/create_human_review_table.py` | Database schema |
| `run_dedup_with_db_review.py` | Full deduplication with review |

---

## Contact & Questions

For questions about the review system, see:
- SQL queries: `db/REVIEW_QUERIES.sql`
- Python examples: `examples/query_review_queue.py`
- Configuration: `config/review_keywords.py`

**Database:** `ba_dedup.db`
**Table:** `human_review_queue`
**View:** `pending_reviews`
