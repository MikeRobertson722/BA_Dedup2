# Skills - Reusable Deduplication Functions

## Quick Start

```python
from skills import ingest_data, find_duplicates, merge_all_clusters, export_all

# Load data
df = ingest_data('csv', 'input/sample_data.csv')

# Find duplicates
df = find_duplicates(df, threshold=0.85)

# Merge clusters
golden, locations = merge_all_clusters(df)

# Export results
files = export_all(df, golden, locations, output_dir='output/')

print(f"Done! {len(golden)} unique businesses from {len(df)} records")
```

## What Are Skills?

Skills are **reusable, standalone functions** extracted from deduplication agents. Instead of using agents as black boxes, you can now:

- ✅ Call individual functions directly
- ✅ Compose custom workflows
- ✅ Test functions in isolation
- ✅ Use in scripts, notebooks, APIs

## Skill Categories

### 📥 Ingestion Skills
Read data from CSV, Excel, or database

```python
from skills import read_csv_file, normalize_column_names

df = read_csv_file('input/data.csv')
df = normalize_column_names(df)
```

### ✅ Validation Skills
Validate and standardize data

```python
from skills import standardize_name, standardize_address, validate_all

df = standardize_name(df)
df = standardize_address(df)
df, errors = validate_all(df, required_fields=['name'])
```

### 🔍 Matching Skills
Find duplicate records

```python
from skills import find_duplicates, fuzzy_match_names

df = find_duplicates(df, threshold=0.85)

is_match, score = fuzzy_match_names("John Smith", "Jon Smith")
```

### 🔗 Merge Skills
Create golden records from duplicates

```python
from skills import merge_all_clusters, create_golden_record

golden, locations = merge_all_clusters(df, strategy='most_complete')
```

### 📊 Output Skills
Export results and generate reports

```python
from skills import export_all, print_statistics

files = export_all(df, golden, locations)
print_statistics(df, golden)
```

## Complete Example

See [examples/use_skills_example.py](../examples/use_skills_example.py) for a working example.

## Documentation

- **Complete Documentation:** [SKILLS_DOCUMENTATION.md](../SKILLS_DOCUMENTATION.md)
- **All 39 Skills:** API reference with examples
- **Usage Patterns:** Pipeline, conditional, batch processing
- **Integration Guide:** Agents vs Skills, when to use each

## Skill Files

- `ingestion_skills.py` - Data reading and field mapping
- `validation_skills.py` - Validation and standardization
- `matching_skills.py` - Fuzzy matching and clustering
- `merge_skills.py` - Golden record creation
- `output_skills.py` - Export and reporting

## Key Benefits

| Feature | Description |
|---------|-------------|
| **Modular** | Use only what you need |
| **Testable** | Test individual functions |
| **Composable** | Build custom workflows |
| **Reusable** | Call from anywhere |
| **Documented** | Examples for every skill |
| **Compatible** | Agents still work |

## Quick Reference

```python
# Complete one-liner deduplication
from skills import ingest_data, validate_all, find_duplicates, merge_all_clusters, export_all

df = ingest_data('csv', 'input/data.csv')
df, _ = validate_all(df, ['name'], standardize_all=True)
df = find_duplicates(df, threshold=0.85)
golden, locations = merge_all_clusters(df)
files = export_all(df, golden, locations)
```

## Need Help?

- See [SKILLS_DOCUMENTATION.md](../SKILLS_DOCUMENTATION.md) for complete documentation
- Run `python examples/use_skills_example.py` for working examples
- Each skill has docstrings with usage examples
