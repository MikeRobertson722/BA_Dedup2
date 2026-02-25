"""Quick Snowflake connection test."""
import os
from dotenv import load_dotenv

load_dotenv()

import snowflake.connector

conn_params = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'role': os.getenv('SNOWFLAKE_ROLE'),
}

# Remove empty values
conn_params = {k: v for k, v in conn_params.items() if v}

print("Connecting with:")
for k, v in conn_params.items():
    if k == 'password':
        print(f"  {k}: {'*' * len(v)}")
    else:
        print(f"  {k}: {v}")

print("\nAttempting browser-based SSO login (a browser window should open)...")
try:
    conn = snowflake.connector.connect(**conn_params, authenticator='externalbrowser')
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
    row = cur.fetchone()
    print(f"\nConnected successfully!")
    print(f"  User:      {row[0]}")
    print(f"  Role:      {row[1]}")
    print(f"  Warehouse: {row[2]}")
    print(f"  Database:  {row[3]}")
    print(f"  Schema:    {row[4]}")

    # Check if schema exists
    cur.execute(f"SHOW SCHEMAS LIKE '{os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')}' IN DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
    schemas = cur.fetchall()
    if schemas:
        print(f"\n  Schema '{os.getenv('SNOWFLAKE_SCHEMA')}' exists.")
    else:
        print(f"\n  Schema '{os.getenv('SNOWFLAKE_SCHEMA')}' does NOT exist yet.")

    cur.close()
    conn.close()
    print("\nConnection closed. You're all set!")
except Exception as e:
    print(f"\nConnection failed: {e}")
