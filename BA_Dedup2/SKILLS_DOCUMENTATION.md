## Reusable Skills System

## Overview

The BA Deduplication system now provides **reusable skills** - standalone, callable functions extracted from agents. This makes the deduplication logic:

- ✅ **Modular** - Use individual functions independently
- ✅ **Testable** - Test each skill in isolation
- ✅ **Reusable** - Call from scripts, notebooks, APIs
- ✅ **Composable** - Mix and match skills for custom workflows
- ✅ **Maintainable** - Single source of truth for core logic

---

## Architecture

### Before: Agent-Based

```python
# Agent encapsulates all logic
agent = ValidationAgent(config)
df = agent.run(data)  # Black box
```

### After: Skills-Based

```python
# Import only what you need
from skills import standardize_name, check_required_fields

# Call skills directly
df = standardize_name(df)
df, errors = check_required_fields(df, ['name', 'address'])
```

**Agents still exist** and use skills internally. Skills provide an **additional API** for direct use.

---

## Skill Categories

### 1. Ingestion Skills (`skills.ingestion_skills`)

**Purpose:** Read data from various sources

| Skill | Description | Example |
|-------|-------------|---------|
| `read_csv_file` | Read CSV file | `df = read_csv_file('input/data.csv')` |
| `read_excel_file` | Read Excel file | `df = read_excel_file('input/data.xlsx')` |
| `read_database_table` | Read database table | `df = read_database_table('ba_source_records')` |
| `apply_field_mappings` | Rename columns | `df = apply_field_mappings(df, {'Business Name': 'name'})` |
| `normalize_column_names` | Standardize column names | `df = normalize_column_names(df)` |
| `ingest_data` | Complete ingestion pipeline | `df = ingest_data('csv', 'input/data.csv')` |

### 2. Validation Skills (`skills.validation_skills`)

**Purpose:** Validate and standardize data

| Skill | Description | Example |
|-------|-------------|---------|
| `check_required_fields` | Check for required fields | `df, errors = check_required_fields(df, ['name'])` |
| `add_optional_fields` | Add missing columns | `df = add_optional_fields(df, {'status': 'active'})` |
| `standardize_name` | Standardize name field | `df = standardize_name(df)` |
| `standardize_address` | Standardize address field | `df = standardize_address(df)` |
| `standardize_phone` | Standardize phone field | `df = standardize_phone(df)` |
| `standardize_zip` | Standardize ZIP field | `df = standardize_zip(df)` |
| `standardize_email` | Standardize email field | `df = standardize_email(df)` |
| `parse_name_components` | Parse first/middle/last/suffix | `df = parse_name_components(df)` |
| `validate_data_quality` | Check data quality | `df, warnings = validate_data_quality(df)` |
| `remove_exact_duplicates` | Remove exact duplicates | `df, count = remove_exact_duplicates(df)` |
| `validate_all` | Complete validation pipeline | `df, errors = validate_all(df, ['name'], standardize_all=True)` |

### 3. Matching Skills (`skills.matching_skills`)

**Purpose:** Find duplicate records

| Skill | Description | Example |
|-------|-------------|---------|
| `fuzzy_match_names` | Compare two names | `is_match, score = fuzzy_match_names("John", "Jon")` |
| `fuzzy_match_addresses` | Compare two addresses | `is_match, score = fuzzy_match_addresses(addr1, addr2)` |
| `generate_candidate_pairs` | Generate blocking pairs | `pairs = generate_candidate_pairs(df)` |
| `calculate_similarity_scores` | Score candidate pairs | `matches = calculate_similarity_scores(df, pairs)` |
| `cluster_duplicates` | Cluster using Union-Find | `df = cluster_duplicates(df, pairs)` |
| `find_duplicates` | Complete matching pipeline | `df = find_duplicates(df, threshold=0.85)` |
| `get_cluster_records` | Get records in cluster | `records = get_cluster_records(df, cluster_id=100)` |
| `get_all_clusters` | Get all clusters | `clusters = get_all_clusters(df)` |

### 4. Merge Skills (`skills.merge_skills`)

**Purpose:** Create golden records

| Skill | Description | Example |
|-------|-------------|---------|
| `calculate_completeness_score` | Score record completeness | `score = calculate_completeness_score(record)` |
| `select_best_values` | Pick best values from cluster | `values = select_best_values(cluster_df)` |
| `create_golden_record` | Create single golden record | `golden = create_golden_record(cluster_df, id)` |
| `merge_cluster` | Merge cluster to golden + locations | `golden, locs = merge_cluster(cluster_df, id)` |
| `merge_all_clusters` | Merge all clusters | `golden_df, locs = merge_all_clusters(df)` |
| `deduplicate_and_merge` | Complete dedup + merge pipeline | `golden, locs = deduplicate_and_merge(df)` |
| `get_cluster_summary` | Get cluster statistics | `summary = get_cluster_summary(df)` |

### 5. Output Skills (`skills.output_skills`)

**Purpose:** Export results and generate reports

| Skill | Description | Example |
|-------|-------------|---------|
| `export_golden_records` | Export to CSV | `export_golden_records(df, 'output/golden.csv')` |
| `export_duplicate_report` | Export duplicate report | `export_duplicate_report(df, 'output/dupes.csv')` |
| `generate_statistics` | Generate stats dictionary | `stats = generate_statistics(orig_df, golden_df)` |
| `generate_summary_report` | Create text summary | `generate_summary_report(orig, golden, 'output/summary.txt')` |
| `create_excel_report` | Create Excel with multiple sheets | `create_excel_report(golden, locs, 'output/report.xlsx')` |
| `export_all` | Export all outputs | `files = export_all(orig, golden, locs, 'output/')` |
| `print_statistics` | Print stats to console | `print_statistics(orig_df, golden_df)` |

---

## Usage Examples

### Example 1: Simple Deduplication (One-Liner)

```python
from skills import ingest_data, validate_all, find_duplicates, merge_all_clusters, export_all

# Load data
df = ingest_data('csv', 'input/sample_data.csv')

# Validate and standardize
df, errors = validate_all(df, required_fields=['name'], standardize_all=True)

# Find duplicates
df = find_duplicates(df, threshold=0.85)

# Merge clusters
golden_records, all_locations = merge_all_clusters(df)

# Export results
files = export_all(df, golden_records, all_locations)

print(f"Done! Golden records: {len(golden_records)}")
```

### Example 2: Custom Workflow with Granular Control

```python
from skills import (
    read_csv_file,
    standardize_name,
    standardize_address,
    generate_candidate_pairs,
    calculate_similarity_scores,
    cluster_duplicates,
    create_golden_record
)

# Read data
df = read_csv_file('input/data.csv')

# Custom standardization (only name and address)
df = standardize_name(df, name_column='business_name')
df = standardize_address(df, address_column='street_address')

# Custom matching with specific fields
pairs = generate_candidate_pairs(df, blocking_fields=['state', 'city'])
matches = calculate_similarity_scores(
    df,
    pairs,
    match_fields=['business_name_normalized'],
    threshold=0.90  # Higher threshold
)

# Cluster
pairs_only = [(idx1, idx2) for idx1, idx2, score in matches]
df = cluster_duplicates(df, pairs_only)

# Create golden records manually
golden_records = []
for cluster_id in df[df['cluster_id'] != -1]['cluster_id'].unique():
    cluster_df = df[df['cluster_id'] == cluster_id]
    golden = create_golden_record(cluster_df, cluster_id, strategy='best_values')
    golden_records.append(golden)

print(f"Created {len(golden_records)} golden records with custom logic")
```

### Example 3: Name Matching Only

```python
from skills import fuzzy_match_names

# Compare individual names
is_match, score = fuzzy_match_names("John Smith", "Jon Smith", threshold=0.85)
print(f"Match: {is_match}, Score: {score:.2f}")

# Batch comparison
names = ["John Smith", "Jane Doe", "Bob Johnson"]
for i, name1 in enumerate(names):
    for name2 in names[i+1:]:
        is_match, score = fuzzy_match_names(name1, name2)
        if is_match:
            print(f"  {name1} ~ {name2} ({score:.2f})")
```

### Example 4: Jupyter Notebook Analysis

```python
# In Jupyter notebook
from skills import read_csv_file, find_duplicates, get_cluster_summary

# Load data
df = read_csv_file('data.csv')

# Find duplicates
df = find_duplicates(df, threshold=0.85)

# Analyze clusters
summary = get_cluster_summary(df)

# Display top clusters
print("Top 10 largest clusters:")
print(summary.head(10))

# Interactive exploration
from skills.matching_skills import get_cluster_records

# Look at specific cluster
cluster_100 = get_cluster_records(df, cluster_id=100)
display(cluster_100[['name', 'address', 'city', 'state']])
```

### Example 5: API Integration

```python
from fastapi import FastAPI
from skills import validate_all, find_duplicates

app = FastAPI()

@app.post("/deduplicate")
def deduplicate_api(data: dict):
    """API endpoint for deduplication."""
    import pandas as pd

    # Convert input to DataFrame
    df = pd.DataFrame(data['records'])

    # Validate
    df, errors = validate_all(
        df,
        required_fields=['name'],
        standardize_all=True
    )

    # Find duplicates
    df = find_duplicates(df, threshold=data.get('threshold', 0.85))

    # Return results
    return {
        'total_records': len(df),
        'duplicate_count': (df['cluster_id'] != -1).sum(),
        'clusters': df[['cluster_id', 'name', 'address']].to_dict('records')
    }
```

### Example 6: Testing Individual Skills

```python
import pytest
import pandas as pd
from skills import standardize_name, fuzzy_match_names

def test_standardize_name():
    """Test name standardization."""
    df = pd.DataFrame({'name': ['Dr. John Smith', 'Jane Doe, MD']})

    df = standardize_name(df)

    assert 'name_no_title' in df.columns
    assert 'name_first' in df.columns
    assert df.loc[0, 'name_no_title'] == 'John Smith'
    assert df.loc[0, 'name_title'] == 'Dr'

def test_fuzzy_match_names():
    """Test fuzzy name matching."""
    is_match, score = fuzzy_match_names("John Smith", "Jon Smith", threshold=0.85)

    assert is_match == True
    assert score > 0.85

    is_match2, score2 = fuzzy_match_names("John Smith", "Jane Doe", threshold=0.85)
    assert is_match2 == False
```

---

## Best Practices

### 1. Import What You Need

```python
# Good - specific imports
from skills import find_duplicates, merge_all_clusters

# Avoid - importing everything
from skills import *
```

### 2. Chain Skills for Pipelines

```python
# Pipeline pattern
df = (
    ingest_data('csv', 'input/data.csv')
    .pipe(standardize_name)
    .pipe(standardize_address)
    .pipe(lambda df: find_duplicates(df, threshold=0.85))
)
```

### 3. Handle Errors Gracefully

```python
from skills import check_required_fields

df, errors = check_required_fields(df, ['name', 'address'], drop_invalid=True)

if errors:
    for error in errors:
        logging.warning(f"Validation issue: {error}")
```

### 4. Use Type Hints

```python
import pandas as pd
from typing import Tuple, List

def my_custom_workflow(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Custom workflow using skills."""
    from skills import validate_all

    df, errors = validate_all(df, ['name'])
    return df, errors
```

---

## Skill Composition Patterns

### Pattern 1: Pipeline

```python
def deduplication_pipeline(input_path: str, threshold: float = 0.85):
    """Complete deduplication pipeline."""
    from skills import ingest_data, validate_all, find_duplicates, merge_all_clusters

    df = ingest_data('csv', input_path)
    df, _ = validate_all(df, ['name'], standardize_all=True)
    df = find_duplicates(df, threshold=threshold)
    golden, locations = merge_all_clusters(df)

    return golden, locations
```

### Pattern 2: Conditional Processing

```python
def smart_deduplication(df: pd.DataFrame, data_quality: str):
    """Adjust processing based on data quality."""
    from skills import validate_all, find_duplicates

    if data_quality == 'high':
        threshold = 0.90
        drop_invalid = False
    else:
        threshold = 0.85
        drop_invalid = True

    df, _ = validate_all(df, ['name'], drop_invalid=drop_invalid)
    df = find_duplicates(df, threshold=threshold)

    return df
```

### Pattern 3: Batch Processing

```python
def batch_deduplicate(file_list: List[str]):
    """Process multiple files."""
    from skills import read_csv_file, find_duplicates, export_golden_records

    all_results = []

    for file_path in file_list:
        df = read_csv_file(file_path)
        df = find_duplicates(df, threshold=0.85)
        export_golden_records(df, f'output/golden_{Path(file_path).stem}.csv')
        all_results.append(df)

    return all_results
```

---

## Integration with Agents

**Agents still work** and use skills internally:

```python
# Old way (still works)
from agents.validation_agent import ValidationAgent

agent = ValidationAgent(config)
df = agent.run(data)

# New way (using skills directly)
from skills import validate_all

df, errors = validate_all(data, ['name'], standardize_all=True)
```

**When to use agents:**
- Running complete pipeline with configuration
- Need agent lifecycle (logging, stats, error handling)
- Using orchestrator/pipeline framework

**When to use skills:**
- Custom workflows
- Testing individual functions
- API/notebook integration
- Fine-grained control

---

## API Reference

### Complete Skill Listing

```python
from skills import (
    # Ingestion (6 skills)
    read_csv_file,
    read_excel_file,
    read_database_table,
    apply_field_mappings,
    normalize_column_names,
    ingest_data,

    # Validation (11 skills)
    check_required_fields,
    add_optional_fields,
    standardize_name,
    standardize_address,
    standardize_phone,
    standardize_zip,
    standardize_email,
    parse_name_components,
    validate_data_quality,
    remove_exact_duplicates,
    validate_all,

    # Matching (8 skills)
    fuzzy_match_names,
    fuzzy_match_addresses,
    generate_candidate_pairs,
    calculate_similarity_scores,
    cluster_duplicates,
    find_duplicates,
    get_cluster_records,
    get_all_clusters,

    # Merge (7 skills)
    calculate_completeness_score,
    select_best_values,
    create_golden_record,
    merge_cluster,
    merge_all_clusters,
    deduplicate_and_merge,
    get_cluster_summary,

    # Output (7 skills)
    export_golden_records,
    export_duplicate_report,
    generate_statistics,
    generate_summary_report,
    create_excel_report,
    export_all,
    print_statistics,
)
```

---

## Testing Skills

Run the example:

```bash
python examples/use_skills_example.py
```

This demonstrates both complete pipeline and granular control approaches.

---

## Migration Guide

### From Agent-Based to Skills-Based

**Before:**

```python
from agents.ingestion_agent import IngestionAgent
from agents.validation_agent import ValidationAgent
from agents.matching_agent import MatchingAgent

ingestion = IngestionAgent(config)
validation = ValidationAgent(config)
matching = MatchingAgent(config)

df = ingestion.run(None)
df = validation.run(df)
df = matching.run(df)
```

**After:**

```python
from skills import ingest_data, validate_all, find_duplicates

df = ingest_data('csv', 'input/data.csv')
df, errors = validate_all(df, ['name'], standardize_all=True)
df = find_duplicates(df, threshold=0.85)
```

---

## Summary

✅ **39 reusable skills** across 5 categories
✅ **Modular** - Use independently or composed
✅ **Testable** - Each skill is a pure function
✅ **Backward compatible** - Agents still work
✅ **Well-documented** - Examples and API reference
✅ **Production-ready** - Extracted from proven agent code

**Get started:**

```python
from skills import ingest_data, find_duplicates, merge_all_clusters, export_all

df = ingest_data('csv', 'input/sample_data.csv')
df = find_duplicates(df, threshold=0.85)
golden, locations = merge_all_clusters(df)
files = export_all(df, golden, locations)

print("Done!")
```

See [examples/use_skills_example.py](examples/use_skills_example.py) for complete working examples.
