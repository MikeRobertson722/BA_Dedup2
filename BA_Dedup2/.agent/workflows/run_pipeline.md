# Running the BA Deduplication Pipeline

This workflow describes how to run the deduplication pipeline end-to-end.

## Prerequisites

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

## Basic Usage

### Run with CSV Input

```bash
python main.py --input input/sample_data.csv --input-type csv
```

### Run with Excel Input

```bash
python main.py --input data/business_associates.xlsx --input-type excel
```

### Run with Database Input

```bash
python main.py --input-type database --table business_associates_raw
```

## Advanced Options

### Custom Similarity Threshold

```bash
python main.py --input input/sample_data.csv --threshold 0.90
```

Higher threshold (e.g., 0.90) = stricter matching, fewer duplicates found
Lower threshold (e.g., 0.75) = looser matching, more duplicates found

### Custom Merge Strategy

```bash
python main.py --input input/sample_data.csv --merge-strategy most_complete
```

Available strategies:
- `most_complete`: Choose record with most fields filled (default)
- `most_recent`: Choose most recently updated record
- `first`: Choose first record encountered

### Custom Output

```bash
# Output to specific table
python main.py --input input/sample_data.csv --output ba_deduplicated_final

# Output to CSV
python main.py --input input/sample_data.csv --output-type csv --output results/deduplicated.csv

# Output to Excel
python main.py --input input/sample_data.csv --output-type excel --output results/deduplicated.xlsx
```

### Reset and Run from Scratch

```bash
python main.py --input input/sample_data.csv --reset
```

### Resume from Last Checkpoint

```bash
python main.py --input input/sample_data.csv --resume
```

### Verbose Logging

```bash
python main.py --input input/sample_data.csv --verbose
```

## Custom Workflow

### Create Custom Workflow JSON

```json
{
  "name": "custom_pipeline",
  "steps": [
    {
      "name": "ingestion",
      "agent": "ingestion",
      "config": {}
    },
    {
      "name": "validation",
      "agent": "validation",
      "config": {"drop_invalid": true}
    }
  ]
}
```

### Run with Custom Workflow

```bash
python main.py --workflow custom_workflow.json --input input/sample_data.csv
```

## Monitoring Progress

The pipeline logs progress to:
- Console: Real-time progress updates
- Log file: `logs/ba_dedup.log` with detailed information
- State file: `state/pipeline_state.json` with checkpoint data

## Common Commands

```bash
# Quick test with sample data
python main.py --input input/sample_data.csv

# Production run with high threshold
python main.py --input data/production_bas.csv --threshold 0.92 --output ba_production_clean

# Resume after failure
python main.py --resume

# Full reset and reprocess
python main.py --input input/sample_data.csv --reset --verbose
```

## Troubleshooting

### Pipeline fails with "No records ingested"
- Check input file path exists
- Verify file format (CSV/Excel)
- Check file has data rows

### Too many/few duplicates found
- Adjust `--threshold` parameter
- Review match fields in config
- Check data normalization

### Output not appearing
- Verify database connection (check .env)
- Check output path permissions (for file output)
- Review logs in `logs/ba_dedup.log`
