# Adding a New Data Source

This workflow describes how to add support for a new data source type (e.g., PostgreSQL, API endpoint, etc.).

## Steps

### 1. Extend Data Connector

If adding a new database type, extend `data/db_connector.py`:

```python
def read_from_postgres(table_name: str) -> pd.DataFrame:
    """Read data from PostgreSQL."""
    connection_string = "postgresql://user:password@host:port/database"
    engine = create_engine(connection_string)
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)
```

### 2. Update IngestionAgent

Modify `agents/ingestion_agent.py` to support the new source type:

```python
def execute(self, data: pd.DataFrame = None) -> pd.DataFrame:
    if self.source_type == 'postgres':
        df = self._read_postgres()
    elif self.source_type == 'api':
        df = self._read_api()
    # ... existing types ...

    return df

def _read_postgres(self) -> pd.DataFrame:
    """Read from PostgreSQL database."""
    # Implementation
    pass
```

### 3. Update Configuration

Add configuration options to `config/settings.py`:

```python
# PostgreSQL Configuration
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE', 'ba_dedup')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', '')
```

### 4. Update .env.example

Add environment variable examples to `.env.example`:

```bash
# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=ba_dedup
POSTGRES_USER=user
POSTGRES_PASSWORD=your_password
```

### 5. Update CLI Arguments

Add new options to `main.py`:

```python
parser.add_argument(
    '--postgres-host',
    type=str,
    help='PostgreSQL host'
)

parser.add_argument(
    '--postgres-port',
    type=int,
    default=5432,
    help='PostgreSQL port'
)
```

### 6. Test New Data Source

Create a test script:

```python
from agents.ingestion_agent import IngestionAgent

agent = IngestionAgent(config={
    'source_type': 'postgres',
    'table_name': 'business_associates'
})

df = agent.run(None)
print(f"Loaded {len(df)} records from PostgreSQL")
```

## Common Data Sources

### API Endpoint

```python
def _read_api(self) -> pd.DataFrame:
    import requests
    response = requests.get(self.api_url)
    data = response.json()
    return pd.DataFrame(data)
```

### JSON File

```python
def _read_json(self) -> pd.DataFrame:
    return pd.read_json(self.source_path)
```

### Parquet File

```python
def _read_parquet(self) -> pd.DataFrame:
    return pd.read_parquet(self.source_path)
```

## Best Practices

- Always validate connection strings
- Use connection pooling for databases
- Handle authentication securely
- Implement retry logic for network sources
- Add comprehensive error handling
- Test with realistic data volumes
