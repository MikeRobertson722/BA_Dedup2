"""
Fuzzy matching deduplication with human review flagging.
Flags Trusts, Departments, and other sensitive entity types for manual review.
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
from config.review_keywords import HUMAN_REVIEW_KEYWORDS, get_review_reason

logger = get_logger(__name__)

print('='*80)
print('FUZZY DEDUPLICATION WITH HUMAN REVIEW FLAGGING')
print('='*80)

# Load data
df = pd.read_csv('input/sample_data.csv')
print(f'\nStarting with: {len(df):,} records')

# Remove exact duplicates
df = df.drop_duplicates()
print(f'After removing exact duplicates: {len(df):,} records')

# Smart name parsing (same as before)
def normalize_name(name):
    """Parse and normalize business names."""
    if pd.isna(name) or name == '':
        return name

    name_str = str(name).strip()
    name_str = re.sub(r'[\r\n]+', ' ', name_str)
    name_str = re.sub(r'\s+', ' ', name_str).strip()

    entity_suffixes = [
        'LLC', 'L.L.C', 'LP', 'L.P', 'INC', 'CORP', 'LTD',
        'PLLC', 'PC', 'PA', 'CORPORATION', 'COMPANY', 'CO',
        'INCORPORATED', 'LIMITED', 'LLP', 'TRUST', 'TR'
    ]

    if ',' in name_str:
        parts = name_str.split(',', 1)
        if len(parts) == 2:
            first_part = parts[0].strip()
            second_part = parts[1].strip()

            second_upper = second_part.upper().replace('.', '')
            if second_upper in entity_suffixes:
                return name_str

            company_indicators = ['PROPERTIES', 'ASSOCIATES', 'PARTNERS', 'GROUP',
                                 'VENTURES', 'HOLDINGS', 'MANAGEMENT', 'SERVICES',
                                 'ENERGY', 'OIL', 'GAS', 'PETROLEUM', 'MEDICAL',
                                 'HEALTH', 'HOSPITAL', 'CLINIC', 'CARE']

            first_upper = first_part.upper()
            if any(indicator in first_upper for indicator in company_indicators):
                return name_str

            if first_part and first_part[0].isdigit():
                return name_str

            if second_part and len(second_part) > 1:
                return f'{second_part} {first_part}'

    return name_str

def create_match_key(name):
    """Create normalized key for fuzzy matching."""
    if pd.isna(name) or name == '':
        return ''

    s = str(name).upper()
    s = re.sub(r'[^A-Z0-9\s]', '', s)

    suffixes = ['LLC', 'INC', 'CORP', 'LTD', 'LP', 'CORPORATION',
                'INCORPORATED', 'COMPANY', 'LIMITED', 'CO']
    for suffix in suffixes:
        s = re.sub(r'\b' + suffix + r'\b', '', s)

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

    s = re.sub(r'\s+', ' ', s).strip()
    return s

# Apply normalizations
df['name_original'] = df['name']
df['name_parsed'] = df['name'].apply(normalize_name)
df['name_match_key'] = df['name_parsed'].apply(create_match_key)

# NEW: Flag records that need human review
def check_needs_review(name):
    """Check if name contains keywords requiring human review."""
    if pd.isna(name) or name == '':
        return False, []

    name_upper = str(name).upper()
    matched_keywords = []

    for keyword in HUMAN_REVIEW_KEYWORDS:
        # Use word boundaries to avoid partial matches
        pattern = r'\b' + re.escape(keyword.upper()) + r'\b'
        if re.search(pattern, name_upper):
            matched_keywords.append(keyword)

    return len(matched_keywords) > 0, matched_keywords

df['needs_review'], df['review_keywords'] = zip(*df['name_parsed'].apply(check_needs_review))

# Separate into auto-process and human-review groups
auto_process = df[~df['needs_review']].copy()
human_review = df[df['needs_review']].copy()

print(f'\n{"="*80}')
print('HUMAN REVIEW CLASSIFICATION')
print('='*80)
print(f'Records for automatic processing: {len(auto_process):,}')
print(f'Records requiring human review: {len(human_review):,}')

if len(human_review) > 0:
    print(f'\nHuman review breakdown:')
    for keyword in HUMAN_REVIEW_KEYWORDS:
        count = sum(keyword in str(kw_list) for kw_list in human_review['review_keywords'])
        if count > 0:
            print(f'  - {keyword}: {count:,} records')

    print(f'\nExample records requiring review:')
    for idx, row in human_review.head(10).iterrows():
        keywords = ', '.join(row['review_keywords'])
        print(f'  - {row["name_parsed"]} (contains: {keywords})')

# Process auto-process records with fuzzy matching
print(f'\n{"="*80}')
print('PROCESSING AUTO-MERGE RECORDS')
print('='*80)

if len(auto_process) > 0:
    # Add normalized ZIP for blocking
    auto_process['zip_normalized'] = auto_process['zip'].astype(str).str.replace(r'\D', '', regex=True).str[:5]

    # Smart blocking
    print(f'\nGenerating candidate pairs with smart blocking...')
    strategy = SmartBlockingStrategy(max_missing_data_pairs=50000)
    candidate_pairs = strategy.generate_candidate_pairs(auto_process)
    print(f'Generated {len(candidate_pairs):,} candidate pairs')

    # Fuzzy matching
    print(f'\nPerforming fuzzy matching (similarity threshold: 85%)...')
    matches = []
    threshold = 85

    for i, (idx1, idx2) in enumerate(candidate_pairs):
        if i % 10000 == 0:
            print(f'  Processed {i:,}/{len(candidate_pairs):,} pairs...', end='\r')

        name1 = auto_process.loc[idx1, 'name_match_key']
        name2 = auto_process.loc[idx2, 'name_match_key']

        if not name1 or not name2:
            continue

        similarity = fuzz.token_sort_ratio(name1, name2)

        if similarity >= threshold:
            matches.append((idx1, idx2, similarity))

    print(f'\nFound {len(matches):,} matching pairs above {threshold}% similarity')

    # Build clusters
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

    auto_process['cluster_id'] = auto_process.index.map(lambda x: uf.find(x))

    clusters = auto_process.groupby('cluster_id').size()
    multi_record_clusters = clusters[clusters > 1]

    print(f'\nClustering results:')
    print(f'  Total clusters: {len(clusters):,}')
    print(f'  Multi-record clusters: {len(multi_record_clusters):,}')
    print(f'  Duplicates merged: {multi_record_clusters.sum() - len(multi_record_clusters):,}')

    # Create golden records
    def completeness_score(row):
        score = 0
        for col in ['address', 'city', 'state', 'zip', 'phone', 'email', 'contact_person']:
            if pd.notna(row.get(col)) and str(row.get(col)).strip() != '':
                score += 1
        return score

    auto_process['_completeness'] = auto_process.apply(completeness_score, axis=1)
    golden_auto = auto_process.sort_values('_completeness', ascending=False).drop_duplicates('cluster_id', keep='first')

    print(f'\nGolden records (auto-merged): {len(golden_auto):,}')
else:
    golden_auto = pd.DataFrame()
    auto_process['cluster_id'] = None
    print('\nNo records for automatic processing')

# For human review records, assign unique cluster IDs (no auto-merge)
if len(human_review) > 0:
    human_review['cluster_id'] = range(len(auto_process), len(auto_process) + len(human_review))
    human_review['_completeness'] = human_review.apply(completeness_score if len(auto_process) > 0 else lambda x: 0, axis=1)

# Combine results
all_processed = pd.concat([auto_process, human_review], ignore_index=True)

print(f'\n{"="*80}')
print('FINAL RESULTS')
print('='*80)
print(f'Total input records: {len(df):,}')
print(f'Auto-merged golden records: {len(golden_auto):,}')
print(f'Human review records (not merged): {len(human_review):,}')
print(f'Total unique entities: {len(golden_auto) + len(human_review):,}')

# Save results
Path('output').mkdir(exist_ok=True)

# 1. Golden records (auto-merged only)
if len(golden_auto) > 0:
    golden_clean = golden_auto[['name_parsed', 'address', 'city', 'state', 'zip',
                                 'phone', 'email', 'contact_person']].copy()
    golden_clean = golden_clean.rename(columns={'name_parsed': 'name'})
    golden_clean.to_csv('output/golden_records_auto_merged.csv', index=False)

# 2. Human review records
if len(human_review) > 0:
    review_output = human_review[['name_original', 'name_parsed', 'address', 'city', 'state', 'zip',
                                   'phone', 'email', 'contact_person', 'review_keywords']].copy()
    review_output['review_reason'] = review_output['review_keywords'].apply(
        lambda kw_list: ' | '.join([get_review_reason(kw) for kw in kw_list])
    )
    review_output = review_output.rename(columns={'name_parsed': 'name'})
    review_output.to_csv('output/HUMAN_REVIEW_REQUIRED.csv', index=False)

# 3. All locations with cluster mapping
locations = all_processed[['cluster_id', 'name_original', 'name_parsed', 'address',
                            'city', 'state', 'zip', 'phone', 'email', 'contact_person',
                            'needs_review', 'review_keywords']].copy()
locations = locations.sort_values(['needs_review', 'cluster_id', 'state', 'city'])
locations.to_csv('output/all_locations_with_review_flags.csv', index=False)

# 4. Summary report
with open('output/DEDUPLICATION_SUMMARY.txt', 'w') as f:
    f.write('='*80 + '\n')
    f.write('FUZZY DEDUPLICATION SUMMARY WITH HUMAN REVIEW\n')
    f.write('='*80 + '\n\n')

    f.write(f'Total input records: {len(df):,}\n')
    f.write(f'Records after removing exact duplicates: {len(df):,}\n\n')

    f.write('AUTOMATIC PROCESSING:\n')
    f.write(f'  Records auto-processed: {len(auto_process):,}\n')
    f.write(f'  Golden records created: {len(golden_auto):,}\n')
    f.write(f'  Duplicates auto-merged: {len(auto_process) - len(golden_auto):,}\n\n')

    f.write('HUMAN REVIEW REQUIRED:\n')
    f.write(f'  Total records flagged: {len(human_review):,}\n')
    if len(human_review) > 0:
        f.write(f'  Breakdown by keyword:\n')
        for keyword in HUMAN_REVIEW_KEYWORDS:
            count = sum(keyword in str(kw_list) for kw_list in human_review['review_keywords'])
            if count > 0:
                f.write(f'    - {keyword}: {count:,} records\n')

    f.write(f'\n')
    f.write('FILES CREATED:\n')
    f.write('  1. golden_records_auto_merged.csv - Automatically deduplicated records\n')
    f.write('  2. HUMAN_REVIEW_REQUIRED.csv - Records needing manual review\n')
    f.write('  3. all_locations_with_review_flags.csv - Complete data with flags\n')
    f.write('  4. DEDUPLICATION_SUMMARY.txt - This summary file\n')

print(f'\n{"="*80}')
print('FILES SAVED')
print('='*80)
print('1. output/golden_records_auto_merged.csv')
print(f'   - {len(golden_auto):,} automatically deduplicated businesses')
print('')
print('2. output/HUMAN_REVIEW_REQUIRED.csv')
print(f'   - {len(human_review):,} records requiring manual review')
print(f'   - Contains Trusts, Departments, and other flagged entities')
print('')
print('3. output/all_locations_with_review_flags.csv')
print(f'   - {len(locations):,} total locations with review flags')
print('')
print('4. output/DEDUPLICATION_SUMMARY.txt')
print('   - Summary report of all processing')
print('='*80)

if len(human_review) > 0:
    print(f'\n[ACTION REQUIRED] {len(human_review):,} records need human review!')
    print('Please review: output/HUMAN_REVIEW_REQUIRED.csv')
