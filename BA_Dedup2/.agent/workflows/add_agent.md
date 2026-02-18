# Adding a New Agent

This workflow describes how to create and register a new agent in the BA Deduplication pipeline.

## Steps

### 1. Create Agent Class

Create a new file in `agents/` directory (e.g., `agents/my_new_agent.py`):

```python
from agents.base_agent import BaseAgent
import pandas as pd
from typing import Dict, Any

class MyNewAgent(BaseAgent):
    """Description of what this agent does."""

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__('my_new_agent', config)
        # Initialize agent-specific attributes

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Main execution logic.

        Args:
            data: Input DataFrame

        Returns:
            Processed DataFrame
        """
        self.logger.info("Executing my new agent")

        # Your processing logic here
        result = data.copy()

        return result

    def validate(self, result: pd.DataFrame) -> bool:
        """Optional: Custom validation logic."""
        if not super().validate(result):
            return False

        # Add custom validation
        return True
```

### 2. Register Agent

Add your agent to `agents/__init__.py`:

```python
from agents.my_new_agent import MyNewAgent

__all__ = [
    # ... existing agents ...
    'MyNewAgent'
]
```

### 3. Register in Workflow Engine

Add your agent to the registry in `workflows/workflow_engine.py`:

```python
AGENT_REGISTRY = {
    # ... existing agents ...
    'my_new_agent': MyNewAgent
}
```

### 4. Add to Workflow Definition

Update `workflows/definitions/data_pipeline.json` to include your agent:

```json
{
  "steps": [
    {
      "name": "my_new_step",
      "agent": "my_new_agent",
      "description": "Description of what this step does",
      "config": {},
      "retry_on_failure": false
    }
  ]
}
```

### 5. Test Your Agent

Create a simple test script:

```python
from agents.my_new_agent import MyNewAgent
import pandas as pd

# Create test data
test_data = pd.DataFrame({
    'name': ['Test 1', 'Test 2'],
    'value': [100, 200]
})

# Initialize and run agent
agent = MyNewAgent()
result = agent.run(test_data)

print(f"Input: {len(test_data)} records")
print(f"Output: {len(result)} records")
```

## Best Practices

- Always inherit from `BaseAgent`
- Implement the `execute()` method
- Use `self.logger` for logging
- Handle errors gracefully
- Document your agent with docstrings
- Test with sample data before integrating
