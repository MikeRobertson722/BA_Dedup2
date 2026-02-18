# Iterative Development Workflow - Quick Start

## Your Exact Workflow Is Now Automated

You asked for this workflow:

1. ✅ **Add new records** for merging
2. ✅ **Run process** (auto-creates backup)
3. ✅ **Manual review** results
4. ✅ **Undo back to previous state**
5. ✅ **Fix code**
6. ✅ **Re-run process** (auto-creates new backup)
7. ✅ **Manual review** to verify fixes

**This is now fully implemented and ready to use!**

---

## The 6-Step Process

### Step 1: Add New Records

Add records to your input file:
```bash
# Edit input/sample_data.csv
# Or import directly to database
```

### Step 2: Run Deduplication

```bash
python run_dedup_with_db_review.py
```

**What happens automatically:**
- ✅ Creates backup before running
- ✅ Prompts for description (optional)
- ✅ Records all metadata
- ✅ Runs deduplication
- ✅ Shows restore command when done

**Example:**
```
Description of this run (optional, press Enter to skip): Testing threshold 90%

Creating backup...
Backup created: backups\ba_dedup_backup_20260218_093156.db
Version ID: 1

[... deduplication runs ...]

To undo this run:
  python restore_backup.py
```

### Step 3: Manual Review

Review the results:

```bash
# Check what was merged
python show_merge_results.py

# Check what was flagged for review
python examples/query_review_queue.py

# Check output files
# - output/golden_records_auto_merged.csv
# - output/human_review_backup.csv
```

### Step 4: Undo (If Issues Found)

If you find problems:

```bash
python restore_backup.py
```

**Interactive menu:**
```
#    ID    Timestamp        Status          Flagged  Description
1    3     20260218_093156  run_completed   480      Testing threshold 90%
2    2     20260218_085500  run_completed   457      First production run
3    1     20260218_080000  backup_created  457      Initial baseline

Enter backup number to restore: 1

Are you sure? (yes/no): yes

[Restores database to exact state before run]
```

**Safety:** Creates safety backup before restoring (just in case)

### Step 5: Fix Code

Make your corrections:

```python
# Example: Reduce threshold
threshold = 85  # Was 90, too aggressive

# Example: Fix blocking strategy
strategy = SmartBlockingStrategy(max_missing_data_pairs=25000)

# Example: Add new validation
if industry_field_matches:
    # ... additional logic
```

### Step 6: Re-Run with Fixes

```bash
python run_dedup_with_db_review.py
```

**Enter description:** `"Fixed: Reduced threshold to 85"`

**Result:** Creates new backup, runs again, can compare with previous version

---

## Documenting Issues (Optional)

After manual review, document what you found:

```bash
python add_issue_notes.py
```

**Example:**
```
Enter run number: 1

Issues found: Merged different oil companies with similar names

Code changes: Added industry field verification before merging
```

**Why document?**
- Track what went wrong
- Remember what needs fixing
- Compare improvements over time
- Build institutional knowledge

---

## Real Example

Let's say you're testing a new fuzzy threshold:

```bash
# Step 1: Edit code - change threshold to 90%
# Step 2: Run process
python run_dedup_with_db_review.py
# Description: "Testing 90% threshold"

# Step 3: Review
python show_merge_results.py
# Oh no! It merged "Texaco Oil" with "Texas Oil Co" - different companies!

# Step 4: Undo
python restore_backup.py
# Select backup #1, restore complete

# Step 5: Fix code - change threshold back to 85%
# Add industry verification logic

# Step 6: Re-run
python run_dedup_with_db_review.py
# Description: "Fixed: 85% threshold + industry check"

# Verify: Now Texaco and Texas stay separate - perfect!
```

---

## Behind the Scenes

### Automatic Backup

Every time you run `run_dedup_with_db_review.py`:

1. Creates full database backup (`ba_dedup.db` → `backups/ba_dedup_backup_TIMESTAMP.db`)
2. Records metadata in `ba_run_versions` table:
   - Timestamp
   - Description
   - Stats before run (records, pending reviews)
   - Stats after run (auto-merged, flagged)
   - Issues found (if documented)
   - Code changes (if documented)

### Easy Restore

When you run `restore_backup.py`:

1. Shows list of all backups with metadata
2. Let you select which version to restore
3. Creates safety backup of current state
4. Copies selected backup over current database
5. Updates version tracking

### No Data Loss

- Every restore creates safety backup first
- All backups preserved in `backups/` directory
- Version history tracked in database
- Can restore any previous state at any time

---

## Quick Reference Commands

```bash
# Run deduplication (auto-backup)
python run_dedup_with_db_review.py

# Restore previous version
python restore_backup.py

# Document issues found
python add_issue_notes.py

# Review results
python show_merge_results.py
python examples/query_review_queue.py

# List all backups
python utils/backup_manager.py

# Direct restore (if you know the file)
python restore_backup.py backups/ba_dedup_backup_20260218_093156.db

# Create manual backup anytime
python -c "from utils.backup_manager import create_backup; create_backup('Manual backup')"
```

---

## Tips for Success

### 1. Always Add Descriptions

When prompted for description, be specific:
- ✅ **Good:** "Testing 90% threshold with state+zip blocking"
- ✅ **Good:** "First production run after name parser fix"
- ❌ **Bad:** "test", "run 1", "" (empty)

**Why?** Helps you remember what each run was testing when reviewing history

### 2. Review Before Making Changes

Don't skip Step 3 (manual review)! Always check:
- `show_merge_results.py` - What got merged?
- `query_review_queue.py` - What got flagged?
- Output CSV files - Does data look correct?

### 3. Document Issues Immediately

When you find problems, document them right away with `add_issue_notes.py`. Your future self will thank you.

### 4. Start with Small Changes

When testing code fixes:
- Make one change at a time
- Run, review, iterate
- Don't change multiple things at once (hard to identify what fixed/broke)

### 5. Keep Successful Runs

When you get a good result:
- Note the version ID
- Document what worked
- This becomes your new baseline

---

## Version History Example

After several iterations, your version history might look like:

| ID | Timestamp | Description | Status | Result |
|----|-----------|-------------|--------|--------|
| 5 | 20260218_153000 | ✅ Final: 85% + industry check | run_completed | Perfect! |
| 4 | 20260218_145500 | 85% threshold retry | run_completed | Better but missed some |
| 3 | 20260218_143000 | Testing 90% threshold | run_completed | Too aggressive - merged wrong companies |
| 2 | 20260218_135000 | First production run | run_completed | Baseline |
| 1 | 20260218_130000 | Initial test | backup_created | Starting point |

**Result:** Clear audit trail of what was tested, what worked, what didn't

---

## Need More Details?

See **BACKUP_WORKFLOW_GUIDE.md** for:
- Complete technical documentation
- Database schema details
- SQL queries for version history
- Python API reference
- Troubleshooting guide
- Production deployment tips

---

## Summary

✅ **Automatic backups** before each run
✅ **Easy restore** with interactive menu
✅ **Version tracking** for audit trail
✅ **Issue documentation** for tracking problems
✅ **Safety backups** prevent accidental data loss
✅ **No manual steps** - fully automated

**Your iterative development workflow is now production-ready!**

Run `python run_dedup_with_db_review.py` to start testing with confidence.
