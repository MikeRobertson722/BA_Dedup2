import pandas as pd

# Load the results
df = pd.read_csv('output/all_locations_with_clusters.csv')
golden = pd.read_csv('output/golden_records_fuzzy.csv')

# Create merge summary (fix the NaN issue)
merge_summary = df.groupby('cluster_id').agg({
    'name_parsed': 'first',
    'name_original': lambda x: ' | '.join([str(n) for n in x.unique()[:5] if pd.notna(n)]),
    'cluster_id': 'count'
}).rename(columns={'cluster_id': 'location_count', 'name_original': 'name_variations'})

merge_summary = merge_summary.sort_values('location_count', ascending=False)
merge_summary.to_csv('output/merge_summary.csv')

print('='*80)
print('FUZZY MATCHING DEDUPLICATION - FINAL RESULTS')
print('='*80)
print(f'\nStarting records: {len(df):,}')
print(f'Golden records (unique businesses): {len(golden):,}')
print(f'Duplicates merged: {len(df) - len(golden):,}')
print(f'Deduplication rate: {(len(df) - len(golden)) / len(df) * 100:.1f}%')

# Show top businesses by location count
print('\n' + '='*80)
print('TOP 20 BUSINESSES BY NUMBER OF LOCATIONS/VARIATIONS MERGED')
print('='*80)
print(merge_summary.head(20)[['name_parsed', 'location_count']].to_string())

# Show example name variations
print('\n'+'='*80)
print('EXAMPLE NAME VARIATIONS THAT WERE SUCCESSFULLY MERGED')
print('='*80)

multi_location = merge_summary[merge_summary['location_count'] > 1]
for idx, (cluster_id, row) in enumerate(multi_location.head(15).iterrows()):
    print(f'\n{idx+1}. {row["name_parsed"]} ({row["location_count"]} records)')
    variations = row['name_variations'].split(' | ')
    unique_vars = [v for v in variations if v != 'nan' and v.strip()]
    for var in unique_vars[:5]:
        print(f'   - {var}')
    if len(unique_vars) > 5:
        print(f'   ... and {len(unique_vars) - 5} more')

# Show specific examples user mentioned
print('\n'+'='*80)
print('VERIFYING SPECIFIC EXAMPLES USER REQUESTED')
print('='*80)

test_cases = [
    ('AMOCO PRODUCTION', 'Should merge AMOCO PRODUCTION COMPANY and AMOCO PRODUCTION CO'),
    ('ABC MEDICAL', 'Should merge A.B.C. Medical Group and ABC Medical Group'),
    ('NORTHSHORE PHYSICIANS', 'Should merge NorthShore and Northshore variations'),
    ('AMERICAN HEART', 'Should merge abbreviations'),
]

for search_term, description in test_cases:
    matching = merge_summary[merge_summary['name_parsed'].str.contains(search_term, case=False, na=False)]
    if len(matching) > 0:
        print(f'\n[YES] {description}')
        for cluster_id, row in matching.head(2).iterrows():
            print(f'  Golden record: {row["name_parsed"]}')
            print(f'  Variations: {row["name_variations"][:200]}...')
    else:
        print(f'\n[NO] {description} - No matches found')

print('\n' + '='*80)
print('FILES SAVED')
print('='*80)
print('1. output/golden_records_fuzzy.csv')
print(f'   - {len(golden):,} unique businesses (one per company)')
print('')
print('2. output/all_locations_with_clusters.csv')
print(f'   - {len(df):,} locations with cluster_id mapping')
print(f'   - All original addresses preserved for correspondence')
print('')
print('3. output/merge_summary.csv')
print(f'   - Summary showing which name variations were merged')
print('='*80)
