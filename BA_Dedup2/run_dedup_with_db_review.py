"""
Fuzzy matching deduplication with database-backed human review.
Stores review records in database table for easy UI integration.

INCLUDES AUTOMATIC BACKUP before each run for easy rollback.
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
from utils.backup_manager import BackupManager

logger = get_logger(__name__)

# Database path
DB_PATH = 'ba_dedup.db'

print('='*80)
print('FUZZY DEDUPLICATION WITH DATABASE-BACKED HUMAN REVIEW')
print('='*80)

# CREATE BACKUP BEFORE RUNNING
print('\nStep 1: Creating backup...')
backup_manager = BackupManager(db_path=DB_PATH)

# Get optional description from user
description = input('\nDescription of this run (optional, press Enter to skip): ').strip()
if not description:
    description = 'Automated deduplication run'

backup_file, version_id = backup_manager.create_backup(
    description=description,
    run_by='dedup_script'
)

print(f'\nBackup created: {backup_file}')
print(f'Version ID: {version_id}')
print('\nIf you need to undo this run, use:')
print(f'  python restore_backup.py')
print('\nContinuing with deduplication...\n')

# Load data
df = pd.read_csv('input/sample_data.csv')
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

# Process auto records with fuzzy matching
if len(auto_process) > 0:
    auto_process['zip_normalized'] = auto_process['zip'].astype(str).str.replace(r'\D', '', regex=True).str[:5]

    print(f'\nGenerating candidate pairs...')
    strategy = SmartBlockingStrategy(max_missing_data_pairs=50000)
    candidate_pairs = strategy.generate_candidate_pairs(auto_process)
    print(f'Generated {len(candidate_pairs):,} pairs')

    print(f'\nFuzzy matching...')
    matches = []
    threshold = 85

    for i, (idx1, idx2) in enumerate(candidate_pairs):
        if i % 10000 == 0:
            print(f'  {i:,}/{len(candidate_pairs):,}...', end='\r')
        name1 = auto_process.loc[idx1, 'name_match_key']
        name2 = auto_process.loc[idx2, 'name_match_key']
        if not name1 or not name2:
            continue
        if fuzz.token_sort_ratio(name1, name2) >= threshold:
            matches.append((idx1, idx2))

    print(f'\nFound {len(matches):,} matches')

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

# Insert new review records
inserted_count = 0
if len(human_review) > 0:
    for idx, row in human_review.iterrows():
        keywords_str = ','.join(row['review_keywords'])
        reasons = [get_review_reason(kw) for kw in row['review_keywords']]
        reason_str = ' | '.join(reasons)

        cursor.execute("""
            INSERT INTO human_review_queue (
                source_record_id, name_original, name_parsed,
                address, city, state, zip, phone, email, contact_person,
                review_keywords, review_reason, review_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
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
            reason_str
        ))
        inserted_count += 1

conn.commit()

print(f'Inserted {inserted_count:,} records into human_review_queue')

# Get statistics
cursor.execute("SELECT review_status, COUNT(*) FROM human_review_queue GROUP BY review_status")
status_counts = cursor.fetchall()

print(f'\nDatabase statistics:')
for status, count in status_counts:
    print(f'  {status}: {count:,} records')

conn.close()

# Save output files
Path('output').mkdir(exist_ok=True)

if len(golden_auto) > 0:
    golden_clean = golden_auto[['name_parsed', 'address', 'city', 'state', 'zip',
                                 'phone', 'email', 'contact_person']].copy()
    golden_clean = golden_clean.rename(columns={'name_parsed': 'name'})
    golden_clean.to_csv('output/golden_records_auto_merged.csv', index=False)

# Also export CSV for backup
if len(human_review) > 0:
    review_csv = human_review[['name_original', 'name_parsed', 'address', 'city', 'state',
                                'zip', 'phone', 'email', 'contact_person', 'review_keywords']].copy()
    review_csv.to_csv('output/human_review_backup.csv', index=False)

print(f'\n{"="*80}')
print('COMPLETE')
print('='*80)
print(f'Auto-merged records: {len(golden_auto):,}')
print(f'Records in review queue: {inserted_count:,}')
print(f'\nDatabase: {DB_PATH}')
print(f'Table: human_review_queue')
print(f'\nQuery pending reviews:')
print(f'  SELECT * FROM pending_reviews;')

# Update version tracking with completion stats
backup_manager.update_run_completion(
    version_id=version_id,
    notes=f'Auto-merged: {len(golden_auto)}, Flagged for review: {inserted_count}'
)

print(f'\n{"="*80}')
print('BACKUP & RESTORE INFO')
print('='*80)
print(f'Backup created: {backup_file}')
print(f'Version ID: {version_id}')
print(f'\nTo undo this run:')
print(f'  python restore_backup.py')
print(f'\nOr restore specific backup:')
print(f'  python restore_backup.py {backup_file}')
print('='*80)
