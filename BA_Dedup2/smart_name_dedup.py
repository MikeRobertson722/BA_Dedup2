"""
Smart name parsing and deduplication.
Handles "Last, First" person names while preserving "Company, LLC" business names.
"""
import pandas as pd
from pathlib import Path

print('='*80)
print('RE-RUNNING WITH IMPROVED NAME PARSING')
print('='*80)

# Load the data
df = pd.read_csv('input/sample_data.csv')
df_dedup = df.drop_duplicates()

# Smart name parser: handles "Last, First" but preserves "Company, LLC"
def normalize_name(name):
    if pd.isna(name) or name == '':
        return name

    name_str = str(name).strip()

    # Company legal entity suffixes (don't reformat these)
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
                # This is a company name, keep as-is
                return name_str

            # Check if first part looks like a company (all caps, contains keywords)
            company_indicators = ['PROPERTIES', 'ASSOCIATES', 'PARTNERS', 'GROUP',
                                 'VENTURES', 'HOLDINGS', 'MANAGEMENT', 'SERVICES',
                                 'ENERGY', 'OIL', 'GAS', 'PETROLEUM', 'MEDICAL',
                                 'HEALTH', 'HOSPITAL', 'CLINIC', 'CARE']

            first_upper = first_part.upper()
            if any(indicator in first_upper for indicator in company_indicators):
                # Likely a company name, keep as-is
                return name_str

            # Check if first part is all digits (like "123 MAIN ST, LLC")
            if first_part.replace(' ', '').replace('-', '').isdigit():
                return name_str

            # Check if first part starts with digits (like "101 ARCH PROPERTIES")
            if first_part and first_part[0].isdigit():
                return name_str

            # Otherwise, likely "Last, First" person name
            # But only if second part looks like a first name (not empty, not just initials)
            if second_part and len(second_part) > 1:
                return f'{second_part} {first_part}'

    return name_str

# Apply name normalization
df_dedup['name_original'] = df_dedup['name']
df_dedup['name'] = df_dedup['name'].apply(normalize_name)

# Count reformatted
name_changed = df_dedup['name'] != df_dedup['name_original']
reformatted = name_changed.sum()
print(f'\nReformatted {reformatted:,} person names from "Last, First" to "First Last"')

# Show examples
print('\nExamples of person name reformatting:')
person_examples = df_dedup[
    name_changed & df_dedup['name_original'].notna()
][['name_original', 'name']].head(15)

for idx, row in person_examples.iterrows():
    print(f'  "{row["name_original"]}" -> "{row["name"]}"')

# Verify company names were NOT reformatted
print('\nVerifying company names with commas were preserved:')
has_comma = df_dedup['name_original'].str.contains(',', na=False)
has_entity = df_dedup['name_original'].str.contains('LLC|LP|INC|CORP|PROPERTIES', case=False, na=False)
company_check = df_dedup[has_comma & has_entity][['name_original', 'name']].head(10)

if len(company_check) > 0:
    preserved_count = (company_check['name'] == company_check['name_original']).sum()
    print(f'Preserved {preserved_count}/{len(company_check)} company names with legal suffixes')

    for idx, row in company_check.iterrows():
        status = 'OK' if row['name'] == row['name_original'] else 'CHANGED'
        print(f'  [{status}] "{row["name_original"]}"')
else:
    print('No company names with commas found in sample')

# Now deduplicate
df_dedup['name_normalized'] = df_dedup['name'].str.upper().str.strip()

def completeness_score(row):
    score = 0
    for col in ['address', 'city', 'state', 'zip', 'phone', 'email']:
        if pd.notna(row.get(col)) and row.get(col) != '':
            score += 1
    return score

df_dedup['_completeness'] = df_dedup.apply(completeness_score, axis=1)

golden_records = df_dedup.sort_values('_completeness', ascending=False).drop_duplicates('name_normalized', keep='first')

print(f'\n{"="*80}')
print('DEDUPLICATION RESULTS')
print('='*80)
print(f'Unique businesses (with smart name parsing): {len(golden_records):,}')
print(f'Duplicates merged: {len(df_dedup) - len(golden_records):,}')

# Save
Path('output').mkdir(exist_ok=True)
golden_clean = golden_records.drop(columns=['name_normalized', '_completeness', 'name_original'])
golden_clean.to_csv('output/deduplicated_businesses_final.csv', index=False)

locations = df_dedup[['name_original', 'name', 'address', 'city', 'state', 'zip', 'phone', 'email']].copy()
locations = locations.sort_values(['name', 'state', 'city'])
locations.to_csv('output/business_locations_final.csv', index=False)

print(f'\nFiles saved:')
print(f'  - output/deduplicated_businesses_final.csv ({len(golden_clean):,} businesses)')
print(f'  - output/business_locations_final.csv ({len(locations):,} locations)')
