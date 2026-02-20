# BA Deduplication - Claude Project Guide

## Communication Rules

- Be direct and honest. Do not compliment ideas just to be agreeable.
- If an idea is bad, say so and explain why.
- If an approach has problems, lead with the problems.
- No filler phrases like "great question", "solid plan", "that's smart".
- Assume the user has no ego about this - they want the project to succeed.
- Give your actual opinion, not the polite version.

This guide helps Claude understand and work with the BA Deduplication pipeline project.

## Project Overview

**Purpose**: Identify and merge duplicate Business Associate (BA) records using fuzzy matching and probabilistic deduplication.

**Architecture**: Agentic workflow pipeline with specialized agents for each stage of processing.

**Key Technologies**:
- `pandas` - Data manipulation
- `thefuzz` - Fuzzy string matching
- `recordlinkage` - Probabilistic deduplication
- `sqlalchemy` - Database access
- `openpyxl` - Excel file handling

## Project Structure

```
BA_Dedup2/
├── main.py                  # Entry point - CLI interface
├── requirements.txt         # Python dependencies
├── .env.example            # Environment configuration template
├── README.md               # User documentation
├── CLAUDE.md              # This file
│
├── config/                 # Configuration
│   ├── settings.py         # Load and manage settings
│   └── db_config.py       # Database connection factory
│
├── agents/                 # Processing agents
│   ├── base_agent.py       # Abstract base class for all agents
│   ├── ingestion_agent.py  # Read data from sources
│   ├── validation_agent.py # Validate and standardize data
│   ├── matching_agent.py   # Find duplicates using fuzzy matching
│   ├── merge_agent.py      # Merge duplicates into golden records
│   └── output_agent.py     # Write results to destination
│
├── workflows/              # Workflow orchestration
│   ├── workflow_engine.py  # Orchestrates agent execution
│   └── definitions/
│       └── data_pipeline.json  # Pipeline definition
│
├── data/                   # Data access layer
│   ├── db_connector.py     # Database operations
│   ├── file_reader.py      # File reading (CSV, Excel)
│   └── table_writer.py     # Database writing
│
├── state/                  # State management
│   └── state_manager.py    # Track pipeline progress, enable resume
│
├── utils/                  # Utilities
│   ├── logger.py           # Structured logging
│   └── helpers.py          # Helper functions (normalization, retry, etc.)
│
├── .agent/workflows/       # How-to guides for common tasks
│   ├── add_agent.md        # How to add a new agent
│   ├── add_data_source.md  # How to add a new data source
│   ├── run_pipeline.md     # How to run the pipeline
│   ├── debug_pipeline.md   # How to debug issues
│   └── configure_dedup.md  # How to tune deduplication settings
│
├── input/                  # Input data directory
│   └── sample_data.csv     # Sample data with intentional duplicates
│
└── logs/                   # Log files
    └── ba_dedup.log        # Pipeline execution logs
```

## Core Concepts

### Agents

All agents inherit from `BaseAgent` and implement the `execute()` method:

1. **IngestionAgent**: Reads data from CSV, Excel, or database
2. **ValidationAgent**: Validates required fields, standardizes formats
3. **MatchingAgent**: Finds duplicate pairs using fuzzy matching and blocking
4. **MergeAgent**: Resolves duplicates into golden records
5. **OutputAgent**: Writes results to destination

### Workflow Engine

Orchestrates agent execution in sequence:
- Manages data handoff between agents
- Handles errors and retries
- Persists state for resume capability
- Logs execution progress

### Deduplication Strategy

1. **Blocking**: Reduce comparisons by only comparing records with shared attributes (e.g., same ZIP code)
2. **Fuzzy Matching**: Calculate similarity scores for candidate pairs using `thefuzz`
3. **Clustering**: Group connected duplicates into clusters
4. **Merging**: Resolve each cluster into a single golden record

### Configuration

Settings can be configured via:
1. Environment variables (`.env` file)
2. Command-line arguments
3. Direct modification of `config/settings.py`

## Common Tasks

### Running the Pipeline

```bash
# With CSV input
python main.py --input input/sample_data.csv

# With custom threshold
python main.py --input input/sample_data.csv --threshold 0.90

# With database input
python main.py --input-type database --table business_associates_raw
```

### Adding a New Agent

See `.agent/workflows/add_agent.md` for detailed instructions.

Quick steps:
1. Create agent class inheriting from `BaseAgent`
2. Implement `execute()` method
3. Register in `agents/__init__.py`
4. Add to `workflow_engine.py` registry
5. Update workflow definition JSON

### Debugging

```bash
# View logs
tail -f logs/ba_dedup.log

# Check state
python -c "from state.state_manager import StateManager; print(StateManager().get_summary())"

# Run with verbose logging
python main.py --input input/sample_data.csv --verbose
```

### Tuning Deduplication

See `.agent/workflows/configure_dedup.md` for detailed guidance.

Key parameters:
- `SIMILARITY_THRESHOLD`: 0.85 (default) - Higher = stricter matching
- `MATCH_FIELDS`: Which fields to compare
- `MERGE_STRATEGY`: How to resolve duplicates ('most_complete', 'most_recent', 'first')

## Development Conventions

### Code Style

- Use type hints for function parameters and returns
- Document classes and functions with docstrings
- Use meaningful variable names
- Keep functions focused and single-purpose

### Agent Development

- Always inherit from `BaseAgent`
- Use `self.logger` for logging (never `print()`)
- Implement `validate()` for custom validation logic
- Handle errors gracefully with try/except
- Return DataFrames from `execute()`

### Logging

- Use appropriate log levels:
  - `DEBUG`: Detailed diagnostic information
  - `INFO`: Normal progress messages
  - `WARNING`: Unexpected but handled situations
  - `ERROR`: Error conditions

```python
self.logger.info("Processing started")
self.logger.debug(f"Processing {len(df)} records")
self.logger.warning("Missing optional field")
self.logger.error(f"Processing failed: {error}")
```

### Testing

- Test agents individually before integration
- Use sample data from `input/sample_data.csv`
- Verify duplicate detection on known duplicates
- Check that golden records have complete data

## Data Flow

```
Input Source (CSV/Excel/DB)
    ↓
IngestionAgent (read & normalize column names)
    ↓
ValidationAgent (validate, standardize, clean)
    ↓
MatchingAgent (find duplicate clusters)
    ↓
MergeAgent (create golden records)
    ↓
OutputAgent (write to destination)
    ↓
Output Table/File
```

## Key Files to Modify

### Adding Features

- **New agent**: Create in `agents/`, register in `workflow_engine.py`
- **New data source**: Extend `data/file_reader.py` or `data/db_connector.py`
- **New configuration**: Add to `config/settings.py` and `.env.example`

### Tuning Deduplication

- **Similarity threshold**: `config/settings.py` → `SIMILARITY_THRESHOLD`
- **Match fields**: `config/settings.py` → `MATCH_FIELDS`
- **Fuzzy matching logic**: `agents/matching_agent.py` → `_calculate_similarities()`
- **Merge strategy**: `config/settings.py` → `MERGE_STRATEGY`

### Workflow Changes

- **Agent order**: `workflows/definitions/data_pipeline.json`
- **Agent configs**: Update agent `config` in workflow JSON
- **Error handling**: `workflows/workflow_engine.py`

## Troubleshooting

### Common Issues

**No duplicates found**:
- Lower similarity threshold (try 0.75-0.80)
- Check data normalization in `ValidationAgent`
- Verify blocking fields have values

**Too many false positives**:
- Raise similarity threshold (try 0.90-0.95)
- Adjust field weights in `MatchingAgent`
- Add more match fields

**Pipeline fails to resume**:
- Check `state/pipeline_state.json`
- Use `--reset` to start fresh
- Review logs in `logs/ba_dedup.log`

**Performance issues**:
- Optimize blocking strategy
- Use batch processing for large datasets
- Consider parallel processing for matching

## Best Practices

1. **Always test with sample data first** before running on production data
2. **Back up original data** before deduplication
3. **Review merge results** - Manually verify a sample of merged records
4. **Start conservative** - Use higher threshold, lower if needed
5. **Document configuration changes** - Keep track of what works
6. **Use version control** - Track changes to code and config
7. **Monitor logs** - Watch for warnings and errors
8. **Incremental improvements** - Test one change at a time

## Resources

- Fuzzy matching documentation: [thefuzz](https://github.com/seatgeek/thefuzz)
- Record linkage guide: [recordlinkage](https://recordlinkage.readthedocs.io/)
- SQLAlchemy docs: [sqlalchemy](https://docs.sqlalchemy.org/)

## Getting Help

1. Check `.agent/workflows/` guides for specific tasks
2. Review `logs/ba_dedup.log` for detailed error messages
3. Use `--verbose` flag for debug-level logging
4. Inspect `state/pipeline_state.json` for pipeline state
5. Test agents individually before full pipeline run
