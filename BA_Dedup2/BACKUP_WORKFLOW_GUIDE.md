# Backup & Restore Workflow Guide

## Overview

The BA deduplication system now includes **automatic backup/restore** functionality to support iterative development and testing. This enables you to:

1. Add new records
2. Run deduplication
3. Manually review results
4. **Rollback if issues found**
5. Fix code
6. Re-run with corrections
7. Verify improvements

---

## Quick Start

### Running Deduplication (Auto-Creates Backup)

```bash
python run_dedup_with_db_review.py
```

**What happens:**
1. Creates backup automatically before running
2. Prompts for optional description: `"Testing new fuzzy threshold"`, `"First production run"`, etc.
3. Records backup metadata (timestamp, stats, description)
4. Runs deduplication
5. Updates version tracking with results
6. Shows restore command at end

### Restoring Previous Version

```bash
# Interactive mode (recommended)
python restore_backup.py

# Direct mode (if you know the backup file)
python restore_backup.py backups/ba_dedup_backup_20260218_143022.db
```

---

## Complete Iterative Workflow

### Step 1: Add New Records

Add records to `input/sample_data.csv` or import to database.

### Step 2: Run Deduplication

```bash
python run_dedup_with_db_review.py
```

**Example output:**
```
================================================================================
FUZZY DEDUPLICATION WITH DATABASE-BACKED HUMAN REVIEW
================================================================================

Step 1: Creating backup...

Description of this run (optional, press Enter to skip): Testing increased threshold to 90%

================================================================================
CREATING BACKUP
================================================================================
Current state:
  Source records: 10,051
  Review queue total: 457
  Pending reviews: 457

Backing up to: backups\ba_dedup_backup_20260218_143022.db
Backup created: ba_dedup_backup_20260218_143022.db
Size: 2.34 MB
Version ID: 3
================================================================================

Backup created: backups\ba_dedup_backup_20260218_143022.db
Version ID: 3

If you need to undo this run, use:
  python restore_backup.py

Continuing with deduplication...

[... deduplication runs ...]

================================================================================
COMPLETE
================================================================================
Auto-merged records: 8,556
Records in review queue: 457

================================================================================
BACKUP & RESTORE INFO
================================================================================
Backup created: backups\ba_dedup_backup_20260218_143022.db
Version ID: 3

To undo this run:
  python restore_backup.py

Or restore specific backup:
  python restore_backup.py backups\ba_dedup_backup_20260218_143022.db
================================================================================
```

### Step 3: Manual Review

Review the results:

```bash
# Query pending reviews
python examples/query_review_queue.py

# Check merge results
python show_merge_results.py

# Review output files
# - output/golden_records_auto_merged.csv
# - output/human_review_backup.csv
```

### Step 4: Document Issues (If Found)

If you find problems during review:

```bash
python add_issue_notes.py
```

**Example:**
```
Enter run number to add notes: 1

Issues found: Threshold too aggressive - merged different companies with similar names

Code changes: Reduce fuzzy threshold from 90 back to 85, add industry verification
```

### Step 5: Restore Previous State

```bash
python restore_backup.py
```

**Example:**
```
================================================================================
BACKUP RESTORE UTILITY
================================================================================

Available backups:

#    ID    Timestamp        Status          Size (MB)  Pending  Description
----------------------------------------------------------------------------------------------------
1    3     20260218_143022  run_completed   2.34       457      Testing increased threshold...
2    2     20260218_120015  run_completed   2.34       457      First production run
3    1     20260218_095500  backup_created  2.34       457      Initial baseline

Enter backup number to restore (or "q" to quit): 1

================================================================================
SELECTED BACKUP
================================================================================
ID: 3
Timestamp: 20260218_143022
File: backups\ba_dedup_backup_20260218_143022.db
Size: 2.34 MB
Description: Testing increased threshold to 90%
Status: run_completed
Pending reviews before: 457

Issues found: Threshold too aggressive - merged different companies

Are you sure you want to restore this backup? (yes/no): yes

================================================================================
RESTORING BACKUP
================================================================================
Backup file: backups\ba_dedup_backup_20260218_143022.db
Backup size: 2.34 MB

Current state (will be overwritten):
  Source records: 8,200
  Review queue: 480
  Pending reviews: 480

Creating safety backup: safety_backup_before_restore_20260218_144530.db

Restoring ba_dedup_backup_20260218_143022.db...

Restored state:
  Source records: 10,051
  Review queue: 457
  Pending reviews: 457

Backup restored successfully!
Safety backup saved: safety_backup_before_restore_20260218_144530.db
================================================================================
```

### Step 6: Fix Code

Make the necessary code changes based on issues found:

```python
# Example: Adjust fuzzy threshold in run_dedup_with_db_review.py
threshold = 85  # Changed from 90 back to 85
```

### Step 7: Re-Run with Fixes

```bash
python run_dedup_with_db_review.py
```

**Enter description:** `"Fixed threshold - reduced to 85"`

### Step 8: Verify Improvements

```bash
# Review results again
python examples/query_review_queue.py
python show_merge_results.py

# Compare with previous run
# Check if issues are resolved
```

---

## Database Schema

### Version Tracking Table: `ba_run_versions`

| Column | Description |
|--------|-------------|
| **id** | Version ID (auto-increment) |
| **version_timestamp** | YYYYMMDD_HHMMSS format |
| **version_name** | Optional user-friendly name |
| **backup_file** | Path to backup file |
| **backup_size_mb** | Backup size in MB |
| **description** | What was being tested/fixed |
| **run_status** | `backup_created`, `run_completed`, `restored` |
| **source_records_before** | Records before run |
| **review_queue_before** | Review queue count before |
| **pending_reviews_before** | Pending count before |
| **source_records_after** | Records after run |
| **review_queue_after** | Review queue after run |
| **pending_reviews_after** | Pending after run |
| **auto_merged_count** | How many auto-merged |
| **flagged_for_review_count** | How many flagged |
| **fuzzy_threshold** | Threshold used (optional) |
| **blocking_strategy** | Strategy used (optional) |
| **backup_created_date** | When backup was created |
| **run_completed_date** | When run finished |
| **restored_date** | When restored (if applicable) |
| **issues_found** | Problems discovered |
| **code_changes** | Fixes applied |

---

## Utility Scripts

### 1. `restore_backup.py`

**Interactive mode:**
```bash
python restore_backup.py
```

Shows list of backups, allows selection, confirms before restoring.

**Direct mode:**
```bash
python restore_backup.py backups/ba_dedup_backup_20260218_143022.db
```

Restores specific backup directly.

### 2. `add_issue_notes.py`

**Interactive mode:**
```bash
python add_issue_notes.py
```

Select a run version and add notes about issues found.

**Direct mode:**
```bash
python add_issue_notes.py 3 "Found duplicate merge issue" "Added industry field check"
```

### 3. `utils/backup_manager.py`

Python API for backup/restore operations:

```python
from utils.backup_manager import BackupManager

manager = BackupManager()

# Create backup
backup_file, version_id = manager.create_backup(
    description="Testing new blocking strategy",
    version_name="v1.2-test"
)

# List backups
backups = manager.list_backups(limit=10)

# Restore backup
manager.restore_backup("backups/ba_dedup_backup_20260218_143022.db")

# Add issue notes
manager.add_issue_notes(
    version_id=3,
    issues_found="Merged different companies",
    code_changes="Added industry verification"
)
```

---

## Backup Storage

**Location:** `backups/` directory (auto-created)

**Naming:** `ba_dedup_backup_YYYYMMDD_HHMMSS.db`

**Size:** Typically 2-5 MB for 10K records

**Safety:** Every restore creates a safety backup first: `safety_backup_before_restore_YYYYMMDD_HHMMSS.db`

---

## SQL Queries for Version History

### View Recent Runs

```sql
SELECT * FROM recent_runs;
```

### View All Versions

```sql
SELECT
    id,
    version_timestamp,
    description,
    run_status,
    pending_reviews_before,
    flagged_for_review_count,
    issues_found,
    backup_created_date
FROM ba_run_versions
ORDER BY backup_created_date DESC;
```

### Find Runs with Issues

```sql
SELECT
    id,
    version_timestamp,
    description,
    issues_found,
    code_changes
FROM ba_run_versions
WHERE issues_found IS NOT NULL
ORDER BY backup_created_date DESC;
```

### Compare Before/After Stats

```sql
SELECT
    id,
    version_timestamp,
    description,
    pending_reviews_before,
    pending_reviews_after,
    (pending_reviews_after - pending_reviews_before) as change,
    auto_merged_count,
    flagged_for_review_count
FROM ba_run_versions
WHERE run_status = 'run_completed'
ORDER BY backup_created_date DESC;
```

---

## Best Practices

### 1. Always Add Descriptions

When running deduplication, add meaningful descriptions:
- **Good:** `"Testing 90% threshold with state+zip blocking"`
- **Good:** `"First run after fixing name parser"`
- **Bad:** `"test"`, `""` (empty)

### 2. Review Before Continuing

Always review results before making changes:
```bash
python examples/query_review_queue.py
python show_merge_results.py
```

### 3. Document Issues Immediately

When you find problems, document them right away:
```bash
python add_issue_notes.py
```

This helps track what needs to be fixed.

### 4. Clean Up Old Backups

Periodically clean up old backup files:
```bash
# Keep last 10 backups, delete older ones
ls -t backups/*.db | tail -n +11 | xargs rm
```

### 5. Safety Backups

Restore operations create safety backups automatically. These can be deleted after verifying restore worked correctly.

---

## Troubleshooting

### Backup Fails

**Problem:** Cannot create backup (disk space, permissions)

**Solution:**
- Check disk space: `df -h` (Unix) or `dir backups` (Windows)
- Check permissions on `backups/` directory
- Manually create: `sqlite3 ba_dedup.db ".backup backups/manual_backup.db"`

### Restore Fails

**Problem:** Restore operation fails

**Solution:**
- Check backup file exists: `ls backups/`
- Verify backup file integrity: `sqlite3 backups/ba_dedup_backup_20260218_143022.db "PRAGMA integrity_check;"`
- Use safety backup if restore corrupted database

### Version Table Missing

**Problem:** `ba_run_versions` table doesn't exist

**Solution:**
```bash
python db/migrations/create_version_tracking_table.py
```

---

## Migration to Production

When moving to production:

1. **Create baseline backup** before any production runs
2. **Test restore process** on non-production database first
3. **Set up automated backups** (daily/weekly cron job)
4. **Monitor backup sizes** - ensure disk space available
5. **Archive old backups** to long-term storage

**Example backup cron job:**
```bash
# Daily backup at 2 AM
0 2 * * * cd /path/to/BA_Dedup2 && python -c "from utils.backup_manager import BackupManager; BackupManager().create_backup('Daily automated backup')"
```

---

## Summary

The backup/restore system provides:

✅ **Automatic backups** before each deduplication run
✅ **Version tracking** with metadata and statistics
✅ **Easy restore** with interactive or direct mode
✅ **Issue documentation** to track problems and fixes
✅ **Safety backups** when restoring
✅ **Complete audit trail** of all runs and changes

This enables confident experimentation and rapid iteration without fear of data loss.

---

## Quick Reference

```bash
# Run deduplication (creates backup automatically)
python run_dedup_with_db_review.py

# Restore previous version
python restore_backup.py

# Add issue notes
python add_issue_notes.py

# View backup list
python utils/backup_manager.py

# Create manual backup
python -c "from utils.backup_manager import create_backup; create_backup('Manual backup')"

# Direct restore
python restore_backup.py backups/ba_dedup_backup_20260218_143022.db
```
