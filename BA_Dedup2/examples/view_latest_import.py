"""View the latest import with groups and scores."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from data.db_connector import DatabaseConnector

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

db = DatabaseConnector()
engine = db.db.get_engine()

# Get the latest import
latest_import = pd.read_sql(
    "SELECT import_id FROM ba_imports ORDER BY import_date DESC LIMIT 1",
    engine
)

if latest_import.empty:
    print("No imports found")
    sys.exit(1)

import_id = latest_import.iloc[0]['import_id']
print(f"\n{'='*80}")
print(f"LATEST IMPORT: {import_id}")
print(f"{'='*80}\n")

# Get source records with groups
query = f"""
SELECT
    source_record_id,
    name,
    city,
    state,
    zip,
    cluster_id as group_id,
    ROUND(similarity_score * 100, 1) as match_percent
FROM ba_source_records
WHERE import_id = '{import_id}'
ORDER BY cluster_id, source_record_id
"""

df = pd.read_sql(query, engine)

# Show duplicate groups
duplicates = df[df['group_id'] >= 0]
if not duplicates.empty:
    print("DUPLICATE GROUPS:")
    print("-" * 80)
    for group_id in sorted(duplicates['group_id'].unique()):
        group = duplicates[duplicates['group_id'] == group_id]
        print(f"\nGroup {int(group_id)}: {len(group)} records, {group['match_percent'].max()}% confidence")
        for _, row in group.iterrows():
            print(f"  - {row['name']} ({row['city']}, {row['state']})")
    print()

# Show unique records
unique = df[df['group_id'] == -1]
print(f"\nUNIQUE RECORDS: {len(unique)}")
print("-" * 80)
for _, row in unique.iterrows():
    print(f"  - {row['name']} ({row['city']}, {row['state']})")

# Summary
print(f"\n{'='*80}")
print("SUMMARY:")
print(f"  Total records: {len(df)}")
print(f"  Duplicate groups: {duplicates['group_id'].nunique() if not duplicates.empty else 0}")
print(f"  Records in groups: {len(duplicates)}")
print(f"  Unique records: {len(unique)}")
if not duplicates.empty:
    print(f"  Average match confidence: {duplicates['match_percent'].mean():.1f}%")
print(f"{'='*80}\n")
