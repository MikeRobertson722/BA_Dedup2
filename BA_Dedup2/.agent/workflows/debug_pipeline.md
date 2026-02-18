# Debugging the Pipeline

This workflow describes how to diagnose failures, inspect state, and re-run steps.

## Checking Pipeline State

### View Current State

```python
from state.state_manager import StateManager

state = StateManager()
summary = state.get_summary()

print(f"Status: {summary['status']}")
print(f"Current step: {summary['current_step']}")
print(f"Completed steps: {summary['completed_steps']}")
print(f"Failed steps: {summary['failed_steps']}")
```

### View Step Results

```python
validation_result = state.get_step_result('validation')
print(validation_result)
```

## Viewing Logs

### View Recent Logs

```bash
tail -f logs/ba_dedup.log
```

### Search for Errors

```bash
grep ERROR logs/ba_dedup.log
grep -A 5 "Failed" logs/ba_dedup.log
```

### View Specific Agent Logs

```bash
grep "agent.matching" logs/ba_dedup.log
```

## Common Issues

### Issue: Ingestion Agent Fails

**Symptoms:**
- "File not found" error
- "Failed to read" error

**Debugging:**
```python
from agents.ingestion_agent import IngestionAgent
import pandas as pd

agent = IngestionAgent(config={
    'source_type': 'csv',
    'source_path': 'input/sample_data.csv'
})

try:
    result = agent.run(None)
    print(f"Success: {len(result)} records")
except Exception as e:
    print(f"Error: {e}")
```

**Solutions:**
- Verify file path is correct
- Check file encoding (use UTF-8)
- Ensure file has header row
- Check file permissions

### Issue: Validation Agent Rejects Records

**Symptoms:**
- "Missing required field" warning
- Many records dropped

**Debugging:**
```python
from agents.validation_agent import ValidationAgent

agent = ValidationAgent()
result = agent.run(test_data)

print(f"Validation errors: {agent.validation_errors}")
```

**Solutions:**
- Review required fields in config
- Check source data quality
- Consider setting `drop_invalid: false` in config

### Issue: Matching Agent Finds No Duplicates

**Symptoms:**
- "Found 0 duplicate pairs" message
- cluster_id all -1

**Debugging:**
```python
from agents.matching_agent import MatchingAgent

agent = MatchingAgent(config={'similarity_threshold': 0.70})
result = agent.run(validated_data)

# Check how many pairs were generated
print(f"Threshold: {agent.similarity_threshold}")
print(f"Duplicate clusters: {result['cluster_id'].nunique() - 1}")
```

**Solutions:**
- Lower similarity threshold (try 0.75 or 0.80)
- Check data normalization
- Verify blocking fields have values
- Review match fields configuration

### Issue: Matching Agent Finds Too Many Duplicates

**Symptoms:**
- Many false positives
- Unrelated records grouped together

**Debugging:**
```python
# Analyze false positive cluster
cluster_data = result[result['cluster_id'] == 0]
print(cluster_data[['name', 'address', 'city']])
```

**Solutions:**
- Raise similarity threshold (try 0.90 or 0.95)
- Add more match fields
- Improve blocking strategy
- Review fuzzy matching weights

### Issue: Database Connection Fails

**Symptoms:**
- "Failed to connect to database" error
- Connection timeout

**Debugging:**
```python
from config.db_config import DatabaseConnection

db = DatabaseConnection()
try:
    engine = db.connect()
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")
```

**Solutions:**
- Check .env database settings
- Verify database is running
- Test connection string separately
- Check firewall/network access

## Inspecting Data at Each Step

### Create Debug Script

```python
from workflows.workflow_engine import WorkflowEngine

# Run workflow
engine = WorkflowEngine()
result = engine.run()

# Inspect results after each step
print("\n=== INGESTION ===")
ingestion_result = engine.get_step_result('ingestion')
print(f"Records: {len(ingestion_result)}")
print(f"Columns: {list(ingestion_result.columns)}")

print("\n=== VALIDATION ===")
validation_result = engine.get_step_result('validation')
print(f"Records: {len(validation_result)}")
print(f"Normalized fields: {[c for c in validation_result.columns if 'normalized' in c]}")

print("\n=== MATCHING ===")
matching_result = engine.get_step_result('matching')
duplicates = matching_result[matching_result['cluster_id'] != -1]
print(f"Duplicate records: {len(duplicates)}")
print(f"Unique clusters: {duplicates['cluster_id'].nunique()}")

print("\n=== MERGE ===")
merge_result = engine.get_step_result('merge')
print(f"Final records: {len(merge_result)}")
```

## Re-running Failed Steps

### Reset State and Retry

```python
from state.state_manager import StateManager

state = StateManager()
state.reset()

# Run pipeline again
```

### Skip Completed Steps

The pipeline automatically resumes from the last failed step:

```bash
python main.py --resume
```

## Performance Debugging

### Check Step Execution Times

```bash
grep "completed:" logs/ba_dedup.log
```

### Identify Slow Steps

```python
from state.state_manager import StateManager

state = StateManager()
for step_name, step_data in state.state['step_results'].items():
    if 'started_at' in step_data and 'completed_at' in step_data:
        import datetime
        start = datetime.datetime.fromisoformat(step_data['started_at'])
        end = datetime.datetime.fromisoformat(step_data['completed_at'])
        duration = (end - start).total_seconds()
        print(f"{step_name}: {duration:.2f}s")
```

## Testing Individual Agents

```python
import pandas as pd
from agents.matching_agent import MatchingAgent

# Create test data
test_data = pd.DataFrame({
    'name': ['ABC Corp', 'ABC Corporation', 'XYZ Inc'],
    'address': ['123 Main St', '123 Main Street', '456 Oak Ave'],
    'city': ['Springfield', 'Springfield', 'Chicago'],
    'state': ['IL', 'IL', 'IL'],
    'zip': ['62701', '62701', '60601']
})

# Add normalized fields
test_data['name_normalized'] = test_data['name'].str.lower()
test_data['address_normalized'] = test_data['address'].str.lower()

# Test matching
agent = MatchingAgent(config={'similarity_threshold': 0.85})
result = agent.run(test_data)

print(result[['name', 'cluster_id', 'similarity_score']])
```

## Best Practices

- Always check logs first (`logs/ba_dedup.log`)
- Test agents individually before full pipeline run
- Use verbose mode for detailed output
- Save intermediate results for analysis
- Reset state when changing configuration
- Use version control for configuration changes
