"""
Load DEC_BA_MASTER.CSV into SQLite database.
Creates dec_ba_master table with all DEC Business Associate records.
"""
import re
import pandas as pd
import sqlite3
from pathlib import Path

# Paths
DB_PATH = 'ba_dedup.db'
CSV_FILE = 'input/DEC_BA_MASTER.CSV'

print('='*80)
print('LOADING DEC BA MASTER DATA INTO DATABASE')
print('='*80)

# Check file exists
if not Path(CSV_FILE).exists():
    print(f'ERROR: File not found: {CSV_FILE}')
    exit(1)

# Load CSV
print(f'\nReading CSV file: {CSV_FILE}')
df = pd.read_csv(CSV_FILE, dtype=str, low_memory=False)
df = df.fillna('')

print(f'Loaded {len(df):,} records')
print(f'Columns: {", ".join(df.columns.tolist())}')

# Clean embedded newlines (critical for address fields)
print('\nCleaning embedded newlines in address fields...')
for col in ['ADDRADDRESS', 'ADDRCITY', 'ADDRSTATE', 'HDRNAME', 'ADDRCONTACT']:
    if col in df.columns:
        before = df[col].str.contains('\r|\n', regex=True).sum()
        df[col] = df[col].str.replace('\r\n', ' ', regex=False)
        df[col] = df[col].str.replace('\n', ' ', regex=False)
        df[col] = df[col].str.replace('\r', ' ', regex=False)
        df[col] = df[col].str.replace(r'\s+', ' ', regex=True).str.strip()
        if before > 0:
            print(f'  {col}: cleaned {before:,} records with embedded newlines')

# Clean SSN: strip to digits only
print('\nCleaning SSN field (stripping to digits only)...')
if 'SSN' in df.columns:
    before_example = df['SSN'].head(3).tolist()
    df['SSN'] = df['SSN'].apply(lambda x: re.sub(r'[^0-9]', '', str(x)) if x else '')
    after_example = df['SSN'].head(3).tolist()
    print(f'  Before: {before_example}')
    print(f'  After:  {after_example}')

# Connect to database
print(f'\nConnecting to database: {DB_PATH}')
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create table
print('Creating dec_ba_master table...')
cursor.execute('DROP TABLE IF EXISTS dec_ba_master')
cursor.execute("""
    CREATE TABLE dec_ba_master (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hdrcode TEXT,
        ssn TEXT,
        hdrname TEXT,
        addrsubcode TEXT,
        addrcontact TEXT,
        addrcity TEXT,
        addrstate TEXT,
        addrzipcode TEXT,
        addrcountry TEXT,
        countryname TEXT,
        addraddress TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

# Insert data
print(f'\nInserting {len(df):,} records into database...')
batch_size = 10000
for i in range(0, len(df), batch_size):
    batch = df.iloc[i:i+batch_size]
    batch.to_sql('dec_ba_master', conn, if_exists='append', index=False,
                 dtype={
                     'hdrcode': 'TEXT',
                     'ssn': 'TEXT',
                     'hdrname': 'TEXT',
                     'addrsubcode': 'TEXT',
                     'addrcontact': 'TEXT',
                     'addrcity': 'TEXT',
                     'addrstate': 'TEXT',
                     'addrzipcode': 'TEXT',
                     'addrcountry': 'TEXT',
                     'countryname': 'TEXT',
                     'addraddress': 'TEXT'
                 })
    print(f'  Inserted {min(i+batch_size, len(df)):,}/{len(df):,} records', end='\r')

print(f'\n\nInserted {len(df):,} records')

# Create indexes for fast lookups
print('\nCreating indexes...')
cursor.execute('CREATE INDEX idx_dec_ssn ON dec_ba_master(ssn)')
print('  Created index on SSN')
cursor.execute('CREATE INDEX idx_dec_hdrcode ON dec_ba_master(hdrcode)')
print('  Created index on HDRCODE')
cursor.execute('CREATE INDEX idx_dec_name ON dec_ba_master(hdrname)')
print('  Created index on HDRNAME')

conn.commit()

# Verify
cursor.execute('SELECT COUNT(*) FROM dec_ba_master')
count = cursor.fetchone()[0]
print(f'\nVerification: {count:,} records in database')

# Show sample
cursor.execute('SELECT * FROM dec_ba_master LIMIT 3')
print('\nSample records:')
for row in cursor.fetchall():
    print(f'  {row[1]} | {row[3]} | {row[6]}, {row[7]} {row[8]}')

conn.close()

print(f'\n{"="*80}')
print('COMPLETE')
print('='*80)
print(f'Database: {DB_PATH}')
print(f'Table: dec_ba_master')
print(f'Records: {count:,}')
print('\nQuery examples:')
print('  SELECT * FROM dec_ba_master WHERE ssn = "407049128";')
print('  SELECT * FROM dec_ba_master WHERE hdrname LIKE "%BEVINS%";')
print('  SELECT COUNT(*) FROM dec_ba_master GROUP BY addrstate;')
print('='*80)
