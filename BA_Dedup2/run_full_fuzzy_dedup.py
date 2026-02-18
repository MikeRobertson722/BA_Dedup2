"""
Full fuzzy matching deduplication pipeline.
Handles name variations, abbreviations, spacing, special characters.
"""
import pandas as pd
import sys
from pathlib import Path
import re
from fuzzywuzzy import fuzz
from collections import defaultdict

sys.path.insert(0, str(Path.cwd()))

from utils.logger import get_logger
from utils.smart_blocking import SmartBlockingStrategy

logger = get_logger(__name__)

print('='*80)
print('FULL FUZZY MATCHING DEDUPLICATION PIPELINE')
print('='*80)

# Load data
df = pd.read_csv('input/sample_data.csv')
print(f'\nStarting with: {len(df):,} records')

# Step 1: Remove exact duplicates
df = df.drop_duplicates()
print(f'After removing exact duplicates: {len(df):,} records')

# Step 2: Smart name parsing (handles "Last, First" but preserves "Company, LLC")
def normalize_name(name):
    """Parse and normalize business names."""
    if pd.isna(name) or name == '':
        return name

    name_str = str(name).strip()

    # Remove line feeds, carriage returns, extra whitespace
    name_str = re.sub(r'[\r\n]+', ' ', name_str)
    name_str = re.sub(r'\s+', ' ', name_str).strip()

    # Company legal entity suffixes
    entity_suffixes = [
        'LLC', 'L.L.C', 'LP', 'L.P', 'INC', 'CORP', 'LTD',
        'PLLC', 'PC', 'PA', 'CORPORATION', 'COMPANY', 'CO',
        'INCORPORATED', 'LIMITED', 'LLP', 'TRUST', 'TR'
    ]

    # Check if name contains comma
    if ',' in name_str:
        parts = name_str.split(',', 1)
        if len(parts) == 2:
            first_part = parts[0].strip()
            second_part = parts[1].strip()

            # Check if second part is a legal entity suffix
            second_upper = second_part.upper().replace('.', '')
            if second_upper in entity_suffixes:
                return name_str

            # Check if first part looks like a company
            company_indicators = ['PROPERTIES', 'ASSOCIATES', 'PARTNERS', 'GROUP',
                                 'VENTURES', 'HOLDINGS', 'MANAGEMENT', 'SERVICES',
                                 'ENERGY', 'OIL', 'GAS', 'PETROLEUM', 'MEDICAL',
                                 'HEALTH', 'HOSPITAL', 'CLINIC', 'CARE']

            first_upper = first_part.upper()
            if any(indicator in first_upper for indicator in company_indicators):
                return name_str

            # Check if starts with digits
            if first_part and first_part[0].isdigit():
                return name_str

            # Otherwise, likely "Last, First" person name
            if second_part and len(second_part) > 1:
                return f'{second_part} {first_part}'

    return name_str

# Step 3: Advanced normalization for fuzzy matching
def create_match_key(name):
    """Create normalized key for fuzzy matching."""
    if pd.isna(name) or name == '':
        return ''

    s = str(name).upper()

    # Remove all special characters, punctuation
    s = re.sub(r'[^A-Z0-9\s]', '', s)

    # Remove common business suffixes for matching
    suffixes = ['LLC', 'INC', 'CORP', 'LTD', 'LP', 'CORPORATION',
                'INCORPORATED', 'COMPANY', 'LIMITED', 'CO']
    for suffix in suffixes:
        s = re.sub(r'\b' + suffix + r'\b', '', s)

    # Normalize abbreviations
    abbrevs = {
        'ASSOC': 'ASSOCIATES',
        'ASSOCS': 'ASSOCIATES',
        'GRP': 'GROUP',
        'CTR': 'CENTER',
        'CNTR': 'CENTER',
        'HOSP': 'HOSPITAL',
        'MED': 'MEDICAL',
        'MGMT': 'MANAGEMENT',
        'SVCS': 'SERVICES'
    }
    for abbr, full in abbrevs.items():
        s = re.sub(r'\b' + abbr + r'\b', full, s)

    # Remove extra whitespace
    s = re.sub(r'\s+', ' ', s).strip()

    return s

# Apply normalizations
df['name_original'] = df['name']
df['name_parsed'] = df['name'].apply(normalize_name)
df['name_match_key'] = df['name_parsed'].apply(create_match_key)

print(f'\nApplied smart name parsing and normalization')

# Step 4: Add normalized ZIP for blocking
df['zip_normalized'] = df['zip'].astype(str).str.replace(r'\D', '', regex=True).str[:5]

# Step 5: Smart blocking to generate candidate pairs
print(f'\nGenerating candidate pairs with smart blocking...')
strategy = SmartBlockingStrategy(max_missing_data_pairs=50000)
candidate_pairs = strategy.generate_candidate_pairs(df)

print(f'Generated {len(candidate_pairs):,} candidate pairs')

# Step 6: Fuzzy matching on candidate pairs
print(f'\nPerforming fuzzy matching (similarity threshold: 85%)...')

matches = []
threshold = 85

for i, (idx1, idx2) in enumerate(candidate_pairs):
    if i % 10000 == 0:
        print(f'  Processed {i:,}/{len(candidate_pairs):,} pairs...', end='\r')

    name1 = df.loc[idx1, 'name_match_key']
    name2 = df.loc[idx2, 'name_match_key']

    # Skip if either name is empty
    if not name1 or not name2:
        continue

    # Calculate similarity
    similarity = fuzz.token_sort_ratio(name1, name2)

    if similarity >= threshold:
        matches.append((idx1, idx2, similarity))

print(f'\nFound {len(matches):,} matching pairs above {threshold}% similarity')

# Step 7: Build clusters using Union-Find
class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        px, py = self.find(x), self.find(y)
        if px != py:
            self.parent[px] = py

uf = UnionFind()
for idx1, idx2, score in matches:
    uf.union(idx1, idx2)

# Assign cluster IDs
df['cluster_id'] = df.index.map(lambda x: uf.find(x))

# Count clusters
clusters = df.groupby('cluster_id').size()
multi_record_clusters = clusters[clusters > 1]

print(f'\n{"="*80}')
print('CLUSTERING RESULTS')
print('='*80)
print(f'Total unique clusters: {len(clusters):,}')
print(f'Clusters with multiple records: {len(multi_record_clusters):,}')
print(f'Total records in clusters: {multi_record_clusters.sum():,}')
print(f'Duplicates to be merged: {multi_record_clusters.sum() - len(multi_record_clusters):,}')

# Step 8: Create golden records (most complete per cluster)
def completeness_score(row):
    score = 0
    for col in ['address', 'city', 'state', 'zip', 'phone', 'email', 'contact_person']:
        if pd.notna(row.get(col)) and str(row.get(col)).strip() != '':
            score += 1
    return score

df['_completeness'] = df.apply(completeness_score, axis=1)

# Get golden record for each cluster (most complete)
golden_records = df.sort_values('_completeness', ascending=False).drop_duplicates('cluster_id', keep='first')

print(f'\n{"="*80}')
print('FINAL RESULTS')
print('='*80)
print(f'Starting records: {len(df):,}')
print(f'Golden records (unique businesses): {len(golden_records):,}')
print(f'Duplicates merged: {len(df) - len(golden_records):,}')
print(f'Deduplication rate: {(len(df) - len(golden_records)) / len(df) * 100:.1f}%')

# Show example merges
print(f'\n{"="*80}')
print('EXAMPLE NAME VARIATIONS MERGED')
print('='*80)

example_clusters = multi_record_clusters.nlargest(10)
for cluster_id in example_clusters.index[:5]:
    cluster_recs = df[df['cluster_id'] == cluster_id]
    unique_names = cluster_recs['name_parsed'].unique()
    if len(unique_names) > 1:
        print(f'\nCluster {cluster_id}: {len(cluster_recs)} records merged')
        for name in unique_names[:5]:
            print(f'  - {name}')
        if len(unique_names) > 5:
            print(f'  ... and {len(unique_names) - 5} more variations')

# Step 9: Save results
Path('output').mkdir(exist_ok=True)

# Save golden records
golden_clean = golden_records[['name_parsed', 'address', 'city', 'state', 'zip',
                                'phone', 'email', 'contact_person']].copy()
golden_clean = golden_clean.rename(columns={'name_parsed': 'name'})
golden_clean.to_csv('output/golden_records_fuzzy.csv', index=False)

# Save all locations with cluster mapping
locations = df[['cluster_id', 'name_original', 'name_parsed', 'address',
                'city', 'state', 'zip', 'phone', 'email', 'contact_person']].copy()
locations = locations.sort_values(['cluster_id', 'state', 'city'])
locations.to_csv('output/all_locations_with_clusters.csv', index=False)

# Save merge summary
merge_summary = df.groupby('cluster_id').agg({
    'name_parsed': 'first',
    'name_original': lambda x: ' | '.join(x.unique()[:5]),
    'cluster_id': 'count'
}).rename(columns={'cluster_id': 'location_count', 'name_original': 'name_variations'})
merge_summary = merge_summary.sort_values('location_count', ascending=False)
merge_summary.to_csv('output/merge_summary.csv')

print(f'\n{"="*80}')
print('FILES SAVED')
print('='*80)
print(f'1. output/golden_records_fuzzy.csv')
print(f'   - {len(golden_clean):,} unique businesses (golden records)')
print(f'')
print(f'2. output/all_locations_with_clusters.csv')
print(f'   - {len(locations):,} locations with cluster IDs')
print(f'   - Use cluster_id to see which records were merged')
print(f'')
print(f'3. output/merge_summary.csv')
print(f'   - Summary of all merges with name variations')
print(f'   - Shows which names were consolidated per cluster')

print(f'\n{"="*80}')
print('DEDUPLICATION COMPLETE!')
print('='*80)
