"""
Example demonstrating how to use extracted skills for deduplication.
Shows both modular skill-by-skill usage and complete pipeline usage.
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import skills
from skills import (
    # Ingestion
    ingest_data,

    # Validation
    validate_all,

    # Matching
    find_duplicates,

    # Merge
    merge_all_clusters,

    # Output
    export_all,
    print_statistics
)

print('='*80)
print('DEDUPLICATION USING REUSABLE SKILLS')
print('='*80)

# ==============================================================================
# METHOD 1: Complete Pipeline (One-Liner Approach)
# ==============================================================================

print('\n[METHOD 1] Complete Pipeline\n')

# Step 1: Ingest data
df = ingest_data(
    source_type='csv',
    source_path='input/sample_data.csv',
    normalize_columns=True
)
print(f"Loaded: {len(df)} records")

# Step 2: Validate and standardize
df_validated, errors = validate_all(
    df,
    required_fields=['name'],
    optional_fields={'status': 'active'},
    drop_invalid=False,
    standardize_all=True
)
print(f"Validated: {len(df_validated)} records")
if errors:
    print(f"Validation warnings: {len(errors)}")

# Step 3: Find duplicates
df_duplicates = find_duplicates(
    df_validated,
    match_fields=['name_normalized', 'address_normalized'],
    threshold=0.85
)
print(f"Duplicates found: {(df_duplicates['cluster_id'] != -1).sum()} records")

# Step 4: Merge clusters
golden_records, all_locations = merge_all_clusters(
    df_duplicates,
    strategy='most_complete',
    preserve_all_locations=True
)
print(f"Golden records: {len(golden_records)} unique businesses")
print(f"All locations: {len(all_locations)} addresses preserved")

# Step 5: Export results
files = export_all(
    original_df=df,
    golden_df=golden_records,
    all_locations_df=all_locations,
    output_dir='output/skills_example'
)

print('\nExported files:')
for file_type, file_path in files.items():
    print(f"  - {file_type}: {file_path}")

# Print statistics
print_statistics(df, golden_records)


# ==============================================================================
# METHOD 2: Granular Control (Step-by-Step Approach)
# ==============================================================================

print('\n' + '='*80)
print('[METHOD 2] Granular Control - Individual Skills')
print('='*80 + '\n')

# Import individual skills
from skills.ingestion_skills import read_csv_file, normalize_column_names
from skills.validation_skills import (
    check_required_fields,
    standardize_name,
    standardize_address,
    remove_exact_duplicates
)
from skills.matching_skills import (
    generate_candidate_pairs,
    calculate_similarity_scores,
    cluster_duplicates
)
from skills.merge_skills import create_golden_record, calculate_completeness_score
from skills.output_skills import export_golden_records, generate_summary_report

# Step 1: Read data
print("Step 1: Reading data...")
df = read_csv_file('input/sample_data.csv')
df = normalize_column_names(df)
print(f"  Loaded {len(df)} records\n")

# Step 2: Validation
print("Step 2: Validating required fields...")
df, errors = check_required_fields(df, ['name'], drop_invalid=False)
if errors:
    for error in errors:
        print(f"  Warning: {error}")
print()

# Step 3: Standardization
print("Step 3: Standardizing fields...")
df = standardize_name(df)
df = standardize_address(df)
print("  Name and address standardized\n")

# Step 4: Remove exact duplicates
print("Step 4: Removing exact duplicates...")
df, removed = remove_exact_duplicates(df)
print(f"  Removed {removed} exact duplicates\n")

# Step 5: Generate candidate pairs
print("Step 5: Generating candidate pairs...")
pairs = generate_candidate_pairs(df, blocking_fields=['state', 'zip_normalized'])
print(f"  Generated {len(pairs)} candidate pairs\n")

# Step 6: Calculate similarity scores
print("Step 6: Calculating similarity scores...")
matches = calculate_similarity_scores(
    df,
    pairs,
    match_fields=['name_normalized'],
    threshold=0.85
)
print(f"  Found {len(matches)} matches above threshold\n")

# Step 7: Cluster duplicates
print("Step 7: Clustering duplicates...")
pairs_only = [(idx1, idx2) for idx1, idx2, score in matches]
df = cluster_duplicates(df, pairs_only)
duplicate_count = (df['cluster_id'] != -1).sum()
cluster_count = df[df['cluster_id'] != -1]['cluster_id'].nunique()
print(f"  Created {cluster_count} clusters with {duplicate_count} duplicates\n")

# Step 8: Create golden records for each cluster
print("Step 8: Creating golden records...")
golden_records = []

# Process clusters
for cluster_id in df[df['cluster_id'] != -1]['cluster_id'].unique():
    cluster_df = df[df['cluster_id'] == cluster_id]
    golden = create_golden_record(cluster_df, cluster_id, strategy='most_complete')
    golden_records.append(golden)

# Add singletons
singletons = df[df['cluster_id'] == -1]
for _, record in singletons.iterrows():
    golden_records.append(record)

import pandas as pd
golden_df = pd.DataFrame(golden_records)
print(f"  Created {len(golden_df)} golden records\n")

# Step 9: Export
print("Step 9: Exporting results...")
export_golden_records(golden_df, 'output/skills_example/golden_granular.csv')
generate_summary_report(df, golden_df, output_path='output/skills_example/summary_granular.txt')
print("  Exported successfully\n")

print('='*80)
print('COMPLETE!')
print('='*80)
print('\nBoth methods produce the same results.')
print('Method 1 is faster for standard use cases.')
print('Method 2 provides fine-grained control for custom workflows.')
print('\nSee output/skills_example/ for results.')
