# Configuring Deduplication Settings

This workflow describes how to tune match fields, similarity thresholds, and merge rules.

## Similarity Threshold

The similarity threshold determines how similar two records must be to be considered duplicates.

### Understanding Thresholds

- **0.95-1.0**: Very strict - Only near-exact matches
- **0.85-0.94**: Strict - Common typos and abbreviations (default: 0.85)
- **0.75-0.84**: Moderate - More variation allowed
- **0.60-0.74**: Loose - High recall, more false positives

### Setting Threshold

**Via Command Line:**
```bash
python main.py --input data.csv --threshold 0.90
```

**Via .env File:**
```bash
SIMILARITY_THRESHOLD=0.90
```

**Via config/settings.py:**
```python
SIMILARITY_THRESHOLD = 0.90
```

### Tuning Guidelines

**Too many false positives?** → Increase threshold
```bash
python main.py --input data.csv --threshold 0.92
```

**Missing obvious duplicates?** → Decrease threshold
```bash
python main.py --input data.csv --threshold 0.80
```

## Match Fields

Match fields determine which attributes are compared when finding duplicates.

### Default Match Fields

```python
MATCH_FIELDS = ['name', 'address', 'city', 'state', 'zip']
```

### Customizing Match Fields

**Via .env File:**
```bash
MATCH_FIELDS=name,address,zip
```

**Via Agent Config:**
```python
matching_agent = MatchingAgent(config={
    'match_fields': ['name', 'address', 'zip']
})
```

### Field Weights

Modify `agents/matching_agent.py` to adjust field importance:

```python
def _calculate_similarities(self, ...):
    # Custom weights
    weights = {
        'name': 0.50,      # Name is most important
        'address': 0.30,   # Address is second
        'city': 0.10,
        'state': 0.05,
        'zip': 0.05
    }
```

**Guidelines:**
- **Name**: Most distinctive field (weight: 0.40-0.50)
- **Address**: Important for location (weight: 0.30-0.40)
- **City/State/ZIP**: Geographic verification (weight: 0.05-0.10 each)

## Blocking Strategy

Blocking reduces comparisons by only comparing records with shared attributes.

### Default Blocking

```python
blocking_fields = ['zip_normalized', 'state']
```

Only compares records with same ZIP or same state.

### Custom Blocking

Modify `agents/matching_agent.py`:

```python
def __init__(self, config):
    self.blocking_fields = config.get('blocking_fields', ['state', 'city'])
```

**Blocking Field Guidelines:**
- Use fields with low cardinality (state, city, zip prefix)
- Avoid blocking on highly unique fields (email, phone)
- Use multiple blocking passes for better coverage

### Advanced Blocking

```python
# Block by multiple criteria
def _generate_candidate_pairs(self, df):
    pairs_1 = self._block_by_field(df, 'zip_normalized')
    pairs_2 = self._block_by_field(df, 'state')
    pairs_3 = self._block_by_name_prefix(df)

    # Combine all pairs
    all_pairs = set(pairs_1) | set(pairs_2) | set(pairs_3)
    return list(all_pairs)
```

## Merge Strategy

Determines how duplicate records are combined into a single golden record.

### Available Strategies

**1. most_complete (default)**
- Chooses record with most fields filled
- Fills missing fields from other records

```bash
python main.py --input data.csv --merge-strategy most_complete
```

**2. most_recent**
- Chooses most recently updated record
- Requires 'updated_date' or 'created_date' field

```bash
python main.py --input data.csv --merge-strategy most_recent
```

**3. first**
- Chooses first record encountered
- Simple but less sophisticated

```bash
python main.py --input data.csv --merge-strategy first
```

### Important Fields for Merge

Specify which fields matter most for completeness scoring:

**Via Agent Config:**
```python
merge_agent = MergeAgent(config={
    'merge_strategy': 'most_complete',
    'important_fields': ['name', 'address', 'phone', 'email']
})
```

## Advanced Configuration

### Custom Similarity Function

Modify `agents/matching_agent.py`:

```python
def _calculate_name_similarity(self, name1, name2):
    # Use different fuzzy matching algorithm
    from thefuzz import fuzz

    # Try multiple algorithms, take best
    ratio = fuzz.ratio(name1, name2) / 100.0
    partial = fuzz.partial_ratio(name1, name2) / 100.0
    token_sort = fuzz.token_sort_ratio(name1, name2) / 100.0

    return max(ratio, partial, token_sort)
```

### Field-Specific Matching

```python
# Exact match for critical fields
if rec1['tax_id'] != '' and rec2['tax_id'] != '':
    if rec1['tax_id'] == rec2['tax_id']:
        return 1.0  # Definite match
```

### Minimum Field Requirements

```python
# Require minimum similarity on key fields
if similarities['name'] < 0.70:
    continue  # Skip this pair, name too different

if similarities['address'] < 0.50:
    continue  # Skip this pair, address too different
```

## Configuration Examples

### Healthcare Provider Deduplication

```python
# High confidence required
SIMILARITY_THRESHOLD = 0.90
MATCH_FIELDS = ['name', 'address', 'zip', 'tax_id']
MERGE_STRATEGY = 'most_complete'
BLOCKING_FIELDS = ['zip', 'state']
```

### Vendor Master Deduplication

```python
# More lenient for variations
SIMILARITY_THRESHOLD = 0.82
MATCH_FIELDS = ['name', 'city', 'state']
MERGE_STRATEGY = 'most_recent'
BLOCKING_FIELDS = ['state']
```

### Address Standardization Focus

```python
# Emphasize location matching
SIMILARITY_THRESHOLD = 0.85
MATCH_FIELDS = ['address', 'city', 'state', 'zip']
MERGE_STRATEGY = 'most_complete'
BLOCKING_FIELDS = ['city', 'state']

# Custom weights in matching_agent.py
weights = {
    'name': 0.20,
    'address': 0.50,
    'city': 0.15,
    'state': 0.10,
    'zip': 0.05
}
```

## Testing Configuration Changes

### A/B Testing Different Thresholds

```bash
# Test multiple thresholds
python main.py --input data.csv --threshold 0.80 --output results_080
python main.py --input data.csv --threshold 0.85 --output results_085
python main.py --input data.csv --threshold 0.90 --output results_090

# Compare results
python scripts/compare_results.py results_080 results_085 results_090
```

### Validation Script

```python
import pandas as pd

# Load results
results = pd.read_sql("SELECT * FROM ba_deduplicated", engine)

# Check deduplication effectiveness
original_count = 100  # Original record count
deduplicated_count = len(results)
reduction_rate = (original_count - deduplicated_count) / original_count

print(f"Reduction rate: {reduction_rate:.2%}")
print(f"Records removed: {original_count - deduplicated_count}")

# Manually review sample of merged records
sample = results[results['_merged_from_count'] > 1].sample(10)
print(sample[['name', 'address', '_merged_from_count']])
```

## Best Practices

1. **Start Conservative**: Begin with higher threshold (0.90), lower if needed
2. **Test with Known Duplicates**: Create test set with verified duplicates
3. **Manual Review**: Always review sample of merged records
4. **Iterate**: Adjust settings based on false positives/negatives
5. **Document**: Keep track of which settings work best for your data
6. **Backup**: Always keep original data before deduplication
