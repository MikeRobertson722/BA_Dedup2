# BA Deduplication Pipeline

A Python-based agentic workflow system for identifying and merging duplicate Business Associate records using fuzzy matching and probabilistic deduplication.

## Features

- **Multiple Input Sources**: CSV, Excel, or database tables
- **Fuzzy Matching**: Identifies duplicates despite typos, abbreviations, and variations
- **Intelligent Merging**: Creates "golden records" from duplicate clusters
- **Resume Capability**: Continue from last checkpoint after failures
- **Configurable**: Tune matching sensitivity and merge strategies
- **Audit Trail**: Track which records were merged
- **Extensible**: Easy to add new agents and data sources

## Quick Start

### Installation

1. Clone or download this project
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment:
   ```bash
   cp .env.example .env
   # Edit .env with your settings (optional - defaults work for testing)
   ```

### Run with Sample Data

```bash
python main.py --input input/sample_data.csv
```

This will:
1. Read Business Associate records from CSV
2. Validate and standardize the data
3. Identify duplicates using fuzzy matching
4. Merge duplicates into golden records
5. Write results to SQLite database (`ba_dedup.db`)

### View Results

```bash
# Using SQLite CLI
sqlite3 ba_dedup.db "SELECT * FROM business_associates_deduplicated LIMIT 10"

# Or with Python
python -c "import pandas as pd; import sqlite3; conn = sqlite3.connect('ba_dedup.db'); print(pd.read_sql('SELECT * FROM business_associates_deduplicated', conn))"
```

## Usage

### Basic Commands

```bash
# Run with CSV input
python main.py --input data/business_associates.csv

# Run with Excel input
python main.py --input data/business_associates.xlsx --input-type excel

# Run with database table
python main.py --input-type database --table ba_raw

# Custom similarity threshold (0-1, default: 0.85)
python main.py --input data.csv --threshold 0.90

# Output to CSV instead of database
python main.py --input data.csv --output-type csv --output results/deduplicated.csv
```

### Advanced Options

```bash
# Verbose logging
python main.py --input data.csv --verbose

# Reset and run from scratch
python main.py --input data.csv --reset

# Resume from last checkpoint
python main.py --resume

# Custom merge strategy
python main.py --input data.csv --merge-strategy most_complete
```

See `python main.py --help` for all options.

## Configuration

### Environment Variables

Create a `.env` file (copy from `.env.example`):

```bash
# Similarity threshold (0-1)
SIMILARITY_THRESHOLD=0.85

# Fields to use for matching
MATCH_FIELDS=name,address,city,state,zip

# Merge strategy (most_complete, most_recent, first)
MERGE_STRATEGY=most_complete

# Database connection
DB_CONNECTION_STRING=sqlite:///ba_dedup.db

# Output table names
OUTPUT_TABLE=business_associates_deduplicated
OUTPUT_AUDIT_TABLE=ba_dedup_audit
```

### Tuning Deduplication

**Similarity Threshold**:
- `0.95-1.0`: Very strict - Only near-exact matches
- `0.85-0.94`: Strict - Common typos/abbreviations (default)
- `0.75-0.84`: Moderate - More variation allowed
- `0.60-0.74`: Loose - High recall, more false positives

**Match Fields**:
Specify which fields to compare when finding duplicates. Common combinations:
- `name,address,zip`: Focus on key identifiers
- `name,city,state`: Geographic matching
- `name,address,city,state,zip`: Comprehensive (default)

**Merge Strategies**:
- `most_complete`: Choose record with most fields filled (default)
- `most_recent`: Choose most recently updated record
- `first`: Choose first record encountered

## Architecture

### Pipeline Stages

```
Input → Ingestion → Validation → Matching → Merge → Output
```

1. **Ingestion**: Read data from CSV, Excel, or database
2. **Validation**: Check required fields, standardize formats
3. **Matching**: Find duplicate pairs using fuzzy matching
4. **Merge**: Resolve duplicates into golden records
5. **Output**: Write deduplicated results

### Agents

The pipeline uses specialized agents for each stage:

- **IngestionAgent**: Reads and normalizes input data
- **ValidationAgent**: Validates data quality and standardizes formatting
- **MatchingAgent**: Identifies duplicates using fuzzy matching and blocking
- **MergeAgent**: Merges duplicate records using configured strategy
- **OutputAgent**: Writes results and audit trail

### Workflow Engine

Orchestrates agent execution with:
- Sequential data flow between agents
- Error handling and retries
- State persistence for resume capability
- Progress logging and monitoring

## Data Format

### Expected Input Columns

Required:
- `name`: Business Associate name
- `address`: Street address

Optional (but recommended):
- `city`: City name
- `state`: State/province code
- `zip`: ZIP/postal code
- `phone`: Phone number
- `email`: Email address
- `contact_person`: Contact name

### Example Input CSV

```csv
name,address,city,state,zip,phone,email
ABC Medical Group,123 Main Street,Springfield,IL,62701,217-555-0101,contact@abc.com
ABC Medical Grp,123 Main St,Springfield,IL,62701,(217) 555-0101,info@abc.com
XYZ Health Center,456 Oak Avenue,Chicago,IL,60601,312-555-0202,contact@xyz.com
```

### Output Format

The deduplicated output includes all original fields plus:
- Merged records appear only once
- Audit table tracks which records were combined

## Logs and State

### Log Files

- **Location**: `logs/ba_dedup.log`
- **Content**: Detailed execution logs with timestamps
- **Levels**: DEBUG, INFO, WARNING, ERROR

View logs:
```bash
tail -f logs/ba_dedup.log
grep ERROR logs/ba_dedup.log
```

### State Management

- **Location**: `state/pipeline_state.json`
- **Purpose**: Track progress, enable resume
- **Content**: Step status, record counts, errors

Check state:
```python
from state.state_manager import StateManager
state = StateManager()
print(state.get_summary())
```

## Examples

### Example 1: Healthcare Providers

```bash
python main.py \
  --input providers.csv \
  --threshold 0.90 \
  --merge-strategy most_complete \
  --output ba_providers_clean
```

### Example 2: Vendor Master Data

```bash
python main.py \
  --input vendors.xlsx \
  --input-type excel \
  --threshold 0.85 \
  --match-fields name,city,state \
  --output vendors_deduplicated
```

### Example 3: Database to Database

```bash
python main.py \
  --input-type database \
  --table business_associates_raw \
  --output-type database \
  --output business_associates_clean \
  --threshold 0.88
```

## Development

### Project Structure

```
BA_Dedup2/
├── agents/           # Processing agents
├── workflows/        # Orchestration
├── data/            # Data access layer
├── config/          # Configuration
├── state/           # State management
├── utils/           # Utilities
└── .agent/workflows/ # How-to guides
```

### Adding Custom Agents

See `.agent/workflows/add_agent.md` for detailed instructions.

```python
from agents.base_agent import BaseAgent

class MyAgent(BaseAgent):
    def execute(self, data):
        # Your logic here
        return processed_data
```

### Extending Data Sources

See `.agent/workflows/add_data_source.md` for instructions on adding support for new data sources (PostgreSQL, APIs, etc.).

## Troubleshooting

### No duplicates found

**Problem**: Pipeline finds 0 duplicate pairs

**Solutions**:
1. Lower similarity threshold: `--threshold 0.75`
2. Check that input data has actual duplicates
3. Verify data normalization is working
4. Review logs for blocking issues

### Too many false positives

**Problem**: Unrelated records being merged

**Solutions**:
1. Raise similarity threshold: `--threshold 0.92`
2. Add more match fields
3. Review and adjust field weights in `agents/matching_agent.py`

### Pipeline fails with database error

**Problem**: "Failed to connect to database"

**Solutions**:
1. Check `.env` database settings
2. Ensure database is accessible
3. Verify connection string format
4. Check firewall/network settings

### Out of memory errors

**Problem**: Pipeline crashes with large datasets

**Solutions**:
1. Increase batch size in config
2. Optimize blocking strategy
3. Process data in chunks
4. Use more specific blocking fields

## Performance

### Benchmarks

Approximate processing speeds (single thread, typical hardware):

- Small (< 10K records): < 1 minute
- Medium (10K-100K records): 5-30 minutes
- Large (100K-1M records): 1-4 hours

### Optimization Tips

1. **Use effective blocking**: Reduce unnecessary comparisons
2. **Increase batch size**: For better database write performance
3. **Optimize match fields**: Use fewer, more distinctive fields
4. **Parallel processing**: Modify matching agent for multi-threading

## License

This project is provided as-is for internal use.

## Support

For issues or questions:

1. Check `.agent/workflows/` guides
2. Review `CLAUDE.md` for development documentation
3. Inspect logs in `logs/ba_dedup.log`
4. Use `--verbose` flag for detailed output

## Changelog

### Version 1.0
- Initial release
- CSV, Excel, and database input support
- Fuzzy matching deduplication
- Multiple merge strategies
- Resume capability
- Audit trail tracking
