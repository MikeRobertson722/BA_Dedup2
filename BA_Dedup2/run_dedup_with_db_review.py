"""
Fuzzy matching deduplication with database-backed human review.
Stores review records in database table for easy UI integration.
"""
import pandas as pd
import sqlite3
import sys
from pathlib import Path
import re
from fuzzywuzzy import fuzz
from datetime import datetime

sys.path.insert(0, str(Path.cwd()))

from utils.logger import get_logger
from utils.smart_blocking import SmartBlockingStrategy
from config.review_keywords import HUMAN_REVIEW_KEYWORDS, get_review_reason

logger = get_logger(__name__)

# Database path
DB_PATH = 'ba_dedup.db'

# Union-Find data structure for clustering
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

print('='*80)
print('FUZZY DEDUPLICATION WITH DATABASE-BACKED HUMAN REVIEW')
print('='*80)

print('\nStep 1: Loading data...')
# Read the data directly
df = pd.read_csv('input/sample_data.csv')
df['source_record_id'] = [f"R{idx+1:04d}" for idx in range(len(df))]
print(f'\nStarting with: {len(df):,} records')

# Remove exact duplicates
df = df.drop_duplicates()
print(f'After removing exact duplicates: {len(df):,} records')

# Smart name parsing (reuse from previous script)
def normalize_name(name):
    if pd.isna(name) or name == '':
        return name
    name_str = str(name).strip()
    name_str = re.sub(r'[\r\n]+', ' ', name_str)
    name_str = re.sub(r'\s+', ' ', name_str).strip()

    entity_suffixes = ['LLC', 'L.L.C', 'LP', 'L.P', 'INC', 'CORP', 'LTD',
                       'PLLC', 'PC', 'PA', 'CORPORATION', 'COMPANY', 'CO',
                       'INCORPORATED', 'LIMITED', 'LLP', 'TRUST', 'TR']

    if ',' in name_str:
        parts = name_str.split(',', 1)
        if len(parts) == 2:
            first_part, second_part = parts[0].strip(), parts[1].strip()
            second_upper = second_part.upper().replace('.', '')
            if second_upper in entity_suffixes:
                return name_str
            company_indicators = ['PROPERTIES', 'ASSOCIATES', 'PARTNERS', 'GROUP',
                                 'VENTURES', 'HOLDINGS', 'MANAGEMENT', 'SERVICES',
                                 'ENERGY', 'OIL', 'GAS', 'PETROLEUM', 'MEDICAL',
                                 'HEALTH', 'HOSPITAL', 'CLINIC', 'CARE']
            if any(ind in first_part.upper() for ind in company_indicators):
                return name_str
            if first_part and first_part[0].isdigit():
                return name_str
            if second_part and len(second_part) > 1:
                return f'{second_part} {first_part}'
    return name_str

def create_match_key(name):
    if pd.isna(name) or name == '':
        return ''
    s = str(name).upper()
    s = re.sub(r'[^A-Z0-9\s]', '', s)
    suffixes = ['LLC', 'INC', 'CORP', 'LTD', 'LP', 'CORPORATION',
                'INCORPORATED', 'COMPANY', 'LIMITED', 'CO']
    for suffix in suffixes:
        s = re.sub(r'\b' + suffix + r'\b', '', s)
    abbrevs = {'ASSOC': 'ASSOCIATES', 'GRP': 'GROUP', 'CTR': 'CENTER',
               'HOSP': 'HOSPITAL', 'MED': 'MEDICAL', 'MGMT': 'MANAGEMENT'}
    for abbr, full in abbrevs.items():
        s = re.sub(r'\b' + abbr + r'\b', full, s)
    return re.sub(r'\s+', ' ', s).strip()

# Apply normalizations
df['name_original'] = df['name']
df['name_parsed'] = df['name'].apply(normalize_name)
df['name_match_key'] = df['name_parsed'].apply(create_match_key)

# Flag records for review
def check_needs_review(name):
    if pd.isna(name) or name == '':
        return False, []
    name_upper = str(name).upper()
    matched_keywords = []
    for keyword in HUMAN_REVIEW_KEYWORDS:
        pattern = r'\b' + re.escape(keyword.upper()) + r'\b'
        if re.search(pattern, name_upper):
            matched_keywords.append(keyword)
    return len(matched_keywords) > 0, matched_keywords

df['needs_review'], df['review_keywords'] = zip(*df['name_parsed'].apply(check_needs_review))

# Separate groups
auto_process = df[~df['needs_review']].copy()
human_review = df[df['needs_review']].copy()

print(f'\n{"="*80}')
print('CLASSIFICATION')
print('='*80)
print(f'Auto-process: {len(auto_process):,}')
print(f'Human review: {len(human_review):,}')

# Process human review records to find potential duplicate groups
human_review_matches = []
human_review_match_scores = {}

if len(human_review) > 0:
    print(f'\nFinding potential duplicates in human review records...')
    human_review['zip_normalized'] = human_review['zip'].astype(str).str.replace(r'\D', '', regex=True).str[:5]

    # Generate candidate pairs for human review records
    hr_strategy = SmartBlockingStrategy(max_missing_data_pairs=50000)
    hr_candidate_pairs = hr_strategy.generate_candidate_pairs(human_review)
    print(f'Generated {len(hr_candidate_pairs):,} human review candidate pairs')

    # Fuzzy match human review records
    threshold = 75  # Lower threshold for human review to catch more potential duplicates

    for i, (idx1, idx2) in enumerate(hr_candidate_pairs):
        if i % 5000 == 0 and i > 0:
            print(f'  {i:,}/{len(hr_candidate_pairs):,}...', end='\r')
        name1 = human_review.loc[idx1, 'name_match_key']
        name2 = human_review.loc[idx2, 'name_match_key']
        if not name1 or not name2:
            continue
        score = fuzz.token_sort_ratio(name1, name2)
        if score >= threshold:
            human_review_matches.append((idx1, idx2))
            human_review_match_scores[(idx1, idx2)] = score

    print(f'\nFound {len(human_review_matches):,} potential duplicate pairs in human review')

    # Build clusters for human review records
    hr_uf = UnionFind()
    for idx1, idx2 in human_review_matches:
        hr_uf.union(idx1, idx2)

    # Assign cluster IDs to human review records
    hr_clusters = {}
    for idx in human_review.index:
        root = hr_uf.find(idx)
        if root not in hr_clusters:
            hr_clusters[root] = []
        hr_clusters[root].append(idx)

    # Assign cluster_id column
    human_review['cluster_id'] = 0
    for cluster_id, (root, indices) in enumerate(hr_clusters.items(), start=1):
        for idx in indices:
            human_review.loc[idx, 'cluster_id'] = cluster_id

    num_hr_clusters = len([c for c in hr_clusters.values() if len(c) > 1])
    print(f'Created {num_hr_clusters} human review clusters')

# Process auto records with fuzzy matching
if len(auto_process) > 0:
    auto_process['zip_normalized'] = auto_process['zip'].astype(str).str.replace(r'\D', '', regex=True).str[:5]

    print(f'\nGenerating candidate pairs...')
    strategy = SmartBlockingStrategy(max_missing_data_pairs=50000)
    candidate_pairs = strategy.generate_candidate_pairs(auto_process)
    print(f'Generated {len(candidate_pairs):,} pairs')

    print(f'\nFuzzy matching...')
    matches = []
    match_scores = {}  # Store similarity scores: (idx1, idx2) -> score
    threshold = 85

    for i, (idx1, idx2) in enumerate(candidate_pairs):
        if i % 10000 == 0:
            print(f'  {i:,}/{len(candidate_pairs):,}...', end='\r')
        name1 = auto_process.loc[idx1, 'name_match_key']
        name2 = auto_process.loc[idx2, 'name_match_key']
        if not name1 or not name2:
            continue
        score = fuzz.token_sort_ratio(name1, name2)
        if score >= threshold:
            matches.append((idx1, idx2))
            match_scores[(idx1, idx2)] = score

    print(f'\nFound {len(matches):,} matches')

    # Build clusters using UnionFind
    uf = UnionFind()
    for idx1, idx2 in matches:
        uf.union(idx1, idx2)

    auto_process['cluster_id'] = auto_process.index.map(lambda x: uf.find(x))

    # Golden records
    def completeness_score(row):
        return sum(1 for col in ['address', 'city', 'state', 'zip', 'phone', 'email', 'contact_person']
                   if pd.notna(row.get(col)) and str(row.get(col)).strip())

    auto_process['_completeness'] = auto_process.apply(completeness_score, axis=1)
    golden_auto = auto_process.sort_values('_completeness', ascending=False).drop_duplicates('cluster_id')

    print(f'Golden records: {len(golden_auto):,}')
else:
    golden_auto = pd.DataFrame()

# Insert human review records into database
print(f'\n{"="*80}')
print('POPULATING DATABASE')
print('='*80)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Clear existing pending reviews (optional - comment out to keep historical data)
cursor.execute("DELETE FROM human_review_queue WHERE review_status = 'pending'")
print(f'Cleared existing pending reviews')

# Insert new review records with cluster information
inserted_count = 0
if len(human_review) > 0:
    for idx, row in human_review.iterrows():
        keywords_str = ','.join(row['review_keywords'])
        reasons = [get_review_reason(kw) for kw in row['review_keywords']]
        reason_str = ' | '.join(reasons)

        # Add cluster information if this record is part of a duplicate group
        cluster_id = row.get('cluster_id', 0)
        cluster_note = ''
        if cluster_id > 0:
            cluster_note = f' [Potential duplicate group #{cluster_id}]'
            reason_str = reason_str + cluster_note

        cursor.execute("""
            INSERT INTO human_review_queue (
                source_record_id, name_original, name_parsed,
                address, city, state, zip, phone, email, contact_person,
                review_keywords, review_reason, review_status, merge_with_cluster_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """, (
            int(idx),
            row['name_original'],
            row['name_parsed'],
            row.get('address'),
            row.get('city'),
            row.get('state'),
            row.get('zip'),
            row.get('phone'),
            row.get('email'),
            row.get('contact_person'),
            keywords_str,
            reason_str,
            int(cluster_id) if cluster_id > 0 else None
        ))
        inserted_count += 1

conn.commit()

print(f'Inserted {inserted_count:,} records into human_review_queue')

# Get statistics
cursor.execute("SELECT review_status, COUNT(*) FROM human_review_queue GROUP BY review_status")
status_counts = cursor.fetchall()

print(f'\nDatabase statistics (review queue):')
for status, count in status_counts:
    print(f'  {status}: {count:,} records')

# Write golden records to business_associates_deduplicated table
if len(golden_auto) > 0:
    print(f'\nWriting golden records to database...')

    # Clear existing records (optional - comment out to keep historical data)
    cursor.execute("DELETE FROM business_associates_deduplicated")

    # Insert golden records
    golden_inserted = 0
    for idx, row in golden_auto.iterrows():
        cursor.execute("""
            INSERT INTO business_associates_deduplicated (
                name, address, city, state, zip, phone, email, contact_person,
                cluster_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.get('name_parsed'),
            row.get('address'),
            row.get('city'),
            row.get('state'),
            row.get('zip'),
            row.get('phone'),
            row.get('email'),
            row.get('contact_person'),
            int(row.get('cluster_id')) if pd.notna(row.get('cluster_id')) else None
        ))
        golden_inserted += 1

    conn.commit()
    print(f'Inserted {golden_inserted:,} golden records into business_associates_deduplicated')

conn.close()

# Save output files
Path('output').mkdir(exist_ok=True)

if len(golden_auto) > 0:
    golden_clean = golden_auto[['name_parsed', 'address', 'city', 'state', 'zip',
                                 'phone', 'email', 'contact_person']].copy()
    golden_clean = golden_clean.rename(columns={'name_parsed': 'name'})
    golden_clean.to_csv('output/golden_records_auto_merged.csv', index=False)

# Export enhanced human review CSV with grouping and similarity scores
if len(human_review) > 0:
    # Create a detailed review export
    review_export = []

    # Get all clusters with multiple records (potential duplicates)
    clustered = human_review[human_review['cluster_id'] > 0].copy()

    if len(clustered) > 0:
        # Sort by cluster_id to group duplicates together
        clustered = clustered.sort_values('cluster_id')

        for cluster_id, cluster_group in clustered.groupby('cluster_id'):
            # Add a separator row for each cluster
            review_export.append({
                'group_id': f'GROUP-{int(cluster_id)}',
                'record_id': '---',
                'similarity': '---',
                'name_original': f'=== POTENTIAL DUPLICATE GROUP #{int(cluster_id)} ({len(cluster_group)} records) ===',
                'name_parsed': '',
                'address': '',
                'city': '',
                'state': '',
                'zip': '',
                'phone': '',
                'email': '',
                'contact_person': '',
                'review_keywords': ''
            })

            # Add each record in the cluster
            indices = cluster_group.index.tolist()
            for i, idx in enumerate(indices):
                row = cluster_group.loc[idx]

                # Calculate similarity to first record in cluster
                similarity_pct = ''
                if i > 0:
                    first_idx = indices[0]
                    pair_key = (min(first_idx, idx), max(first_idx, idx))
                    if pair_key in human_review_match_scores:
                        similarity_pct = f'{human_review_match_scores[pair_key]}%'

                review_export.append({
                    'group_id': f'GROUP-{int(cluster_id)}',
                    'record_id': int(idx),
                    'similarity': similarity_pct,
                    'name_original': row['name_original'],
                    'name_parsed': row['name_parsed'],
                    'address': row.get('address', ''),
                    'city': row.get('city', ''),
                    'state': row.get('state', ''),
                    'zip': row.get('zip', ''),
                    'phone': row.get('phone', ''),
                    'email': row.get('email', ''),
                    'contact_person': row.get('contact_person', ''),
                    'review_keywords': ','.join(row['review_keywords'])
                })

            # Add blank row between groups
            review_export.append({
                'group_id': '',
                'record_id': '',
                'similarity': '',
                'name_original': '',
                'name_parsed': '',
                'address': '',
                'city': '',
                'state': '',
                'zip': '',
                'phone': '',
                'email': '',
                'contact_person': '',
                'review_keywords': ''
            })

    # Add ungrouped records (not part of any duplicate cluster)
    ungrouped = human_review[human_review['cluster_id'] == 0].copy()
    if len(ungrouped) > 0:
        review_export.append({
            'group_id': 'UNGROUPED',
            'record_id': '---',
            'similarity': '---',
            'name_original': f'=== UNGROUPED RECORDS ({len(ungrouped)} records) ===',
            'name_parsed': '',
            'address': '',
            'city': '',
            'state': '',
            'zip': '',
            'phone': '',
            'email': '',
            'contact_person': '',
            'review_keywords': ''
        })

        for idx, row in ungrouped.iterrows():
            review_export.append({
                'group_id': 'UNGROUPED',
                'record_id': int(idx),
                'similarity': '',
                'name_original': row['name_original'],
                'name_parsed': row['name_parsed'],
                'address': row.get('address', ''),
                'city': row.get('city', ''),
                'state': row.get('state', ''),
                'zip': row.get('zip', ''),
                'phone': row.get('phone', ''),
                'email': row.get('email', ''),
                'contact_person': row.get('contact_person', ''),
                'review_keywords': ','.join(row['review_keywords'])
            })

    # Save enhanced CSV
    review_df = pd.DataFrame(review_export)
    review_df.to_csv('output/human_review_GROUPED.csv', index=False)

    print(f'\nExported human review file:')
    print(f'  output/human_review_GROUPED.csv (grouped by potential duplicates with similarity %)')

print(f'\n{"="*80}')
print('COMPLETE')
print('='*80)
print(f'Auto-merged records: {len(golden_auto):,}')
print(f'Records in review queue: {inserted_count:,}')

# Show human review grouping statistics
if len(human_review) > 0:
    num_groups = len(human_review[human_review['cluster_id'] > 0]['cluster_id'].unique())
    num_grouped_records = len(human_review[human_review['cluster_id'] > 0])
    num_ungrouped = len(human_review[human_review['cluster_id'] == 0])

    print(f'\nHuman Review Grouping:')
    print(f'  Potential duplicate groups: {num_groups}')
    print(f'  Records in groups: {num_grouped_records:,}')
    print(f'  Ungrouped records: {num_ungrouped:,}')

print(f'\nDatabase: {DB_PATH}')
print(f'Table: human_review_queue')
print(f'\nQuery pending reviews:')
print(f'  SELECT * FROM pending_reviews;')
print('='*80)
