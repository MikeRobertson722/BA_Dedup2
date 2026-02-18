# Group ID and Matching Percentage Columns

## Overview

Added **`cluster_id`** (group ID) and **`similarity_score`** (matching percentage) columns to both source and deduplicated tables. These columns track which records are duplicates and how confident the match is.

---

## What Was Added

### 1. **ba_source_records** Table
Two new columns added:
- **`cluster_id`** - The duplicate group ID this record belongs to
  - `-1` = unique record (no duplicates found)
  - `0, 1, 2, ...` = duplicate group numbers
- **`similarity_score`** - Match confidence as a decimal (0.0 to 1.0)
  - `0.0` = unique record
  - `0.95 - 1.0` = high confidence match (auto-merged)

### 2. **business_associates_deduplicated** Table
Same two columns:
- **`cluster_id`** - The group ID for this golden record
- **`similarity_score`** - Highest similarity score in the cluster

---

## How It Works

### During Pipeline Execution

1. **Import** - Records imported with `cluster_id = NULL` and `similarity_score = NULL`
2. **Matching** - Fuzzy + AI matching assigns cluster IDs and scores
3. **Update Source** - Source records table updated with cluster assignments
4. **Merging** - Golden records created with cluster_id and max similarity_score
5. **Save** - Deduplicated table saved with cluster info

---

## Querying the Data

### View All Source Records with Groups
```sql
SELECT
    source_record_id,
    name,
    city,
    state,
    cluster_id as group_id,
    ROUND(similarity_score * 100, 1) as match_percent
FROM ba_source_records
ORDER BY cluster_id;
```

### View Duplicate Groups
```sql
SELECT
    cluster_id as group_id,
    COUNT(*) as record_count,
    GROUP_CONCAT(name, ' | ') as names,
    MAX(similarity_score) * 100 as max_match_percent
FROM ba_source_records
WHERE cluster_id >= 0
GROUP BY cluster_id
ORDER BY cluster_id;
```

### View Deduplicated Records with Scores
```sql
SELECT
    name,
    address,
    city,
    state,
    zip,
    cluster_id as group_id,
    ROUND(similarity_score * 100, 1) as match_percent
FROM business_associates_deduplicated
ORDER BY cluster_id;
```

### Find All Records in a Specific Group
```sql
SELECT
    source_record_id,
    name,
    address,
    cluster_id,
    similarity_score * 100 as match_percent
FROM ba_source_records
WHERE cluster_id = 0  -- Change to desired group number
ORDER BY similarity_score DESC;
```

---

## Example Results

### Source Records Table
| source_record_id | name | cluster_id | similarity_score |
|------------------|------|------------|------------------|
| IMP_xxx_R0001 | ABC Medical Group | 0 | 1.00 |
| IMP_xxx_R0002 | ABC Medical Group | 0 | 1.00 |
| IMP_xxx_R0003 | A.B.C. Medical Group | 0 | 0.967 |
| IMP_xxx_R0004 | Springfield Medical Associates | 1 | 0.964 |
| IMP_xxx_R0005 | Springfield Medical Assoc | 1 | 0.964 |
| IMP_xxx_R0008 | Lakeview Clinic | -1 | 0.0 |

### Deduplicated Records Table
| name | cluster_id | similarity_score |
|------|------------|------------------|
| ABC Medical Group | 0 | 1.00 |
| Springfield Medical Associates | 1 | 0.964 |
| Lakeview Clinic | -1 | 0.0 |

---

## Understanding the Values

### Cluster ID (Group ID)
- **-1**: Record is unique, no duplicates found
- **0, 1, 2, ...**: Records with the same number are duplicates of each other

### Similarity Score (Match Percent)
- **0.0% (0.00)**: Unique record
- **75-95% (0.75-0.95)**: Uncertain match, sent to AI for analysis
- **95-100% (0.95-1.00)**: High confidence match, auto-merged
- **100% (1.00)**: Perfect match

---

## Query Script

Use the provided script to view all groups and scores:

```bash
cd BA_Dedup2
python examples/query_groups_and_scores.py
```

This displays:
1. All source records with cluster assignments
2. Summary of duplicate groups
3. Deduplicated records
4. Detailed view of first group
5. Summary statistics

---

## Files Modified

1. **data/import_tracker.py**
   - Added `cluster_id` and `similarity_score` columns to ba_source_records table
   - Added `update_source_records_with_clusters()` method

2. **agents/merge_agent.py**
   - Modified to keep `cluster_id` and `similarity_score` in final output

3. **examples/run_full_pipeline_with_tracking.py**
   - Added step to update source records with cluster assignments
   - Added step to save deduplicated records to database

4. **examples/query_groups_and_scores.py** (NEW)
   - Demonstrates how to query and view groups and scores

5. **utils/migrate_add_cluster_columns.py** (NEW)
   - Migration script to add columns to existing database

---

## Migration

If you have an existing database, run the migration script once:

```bash
python utils/migrate_add_cluster_columns.py
```

This adds the new columns to the `ba_source_records` table without losing existing data.

---

## Benefits

1. **Track Duplicates**: Easily see which records are duplicates
2. **Audit Confidence**: Know how confident the system was in each match
3. **Group Analysis**: Analyze duplicate groups for quality control
4. **Data Lineage**: Trace from source records to golden records
5. **Quality Metrics**: Calculate average match confidence, group sizes, etc.

---

## Example Analysis Queries

### Average Match Confidence by Group
```sql
SELECT
    cluster_id,
    COUNT(*) as records,
    ROUND(AVG(similarity_score) * 100, 1) as avg_match_percent,
    ROUND(MAX(similarity_score) * 100, 1) as max_match_percent
FROM ba_source_records
WHERE cluster_id >= 0
GROUP BY cluster_id
ORDER BY avg_match_percent DESC;
```

### Uncertain Matches (Near Threshold)
```sql
SELECT
    source_record_id,
    name,
    cluster_id,
    ROUND(similarity_score * 100, 1) as match_percent
FROM ba_source_records
WHERE similarity_score BETWEEN 0.90 AND 0.96
ORDER BY similarity_score;
```

### Count Records by Group Size
```sql
SELECT
    group_size,
    COUNT(*) as num_groups
FROM (
    SELECT cluster_id, COUNT(*) as group_size
    FROM ba_source_records
    WHERE cluster_id >= 0
    GROUP BY cluster_id
)
GROUP BY group_size
ORDER BY group_size;
```

---

## Summary

✅ **cluster_id** column tracks duplicate groups
✅ **similarity_score** column shows match confidence
✅ Both tables have these columns (source + deduplicated)
✅ Populated automatically during pipeline execution
✅ Query script provided for easy viewing
✅ Migration script for existing databases

**All records now have full group tracking and confidence scores!**
