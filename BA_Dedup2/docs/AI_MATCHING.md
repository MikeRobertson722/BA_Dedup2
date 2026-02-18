# AI-Powered Matching Agent

## Overview

The AI Matching Agent uses Claude's language model to intelligently identify duplicate Business Associate records. Unlike traditional fuzzy matching, it leverages semantic understanding to:

- **Understand nicknames and variations**: "Mike" = "Michael", "Bob" = "Robert"
- **Handle abbreviations**: "ABC Medical" = "A.B.C. Medical Group"
- **Recognize address variations**: "Suite 100" = "Ste 100" = "#100"
- **Account for missing data**: One record may be more complete than another
- **Apply business logic**: Understand context and healthcare-specific conventions

## Setup

### 1. Get an Anthropic API Key

1. Go to [https://console.anthropic.com/](https://console.anthropic.com/)
2. Sign up or log in
3. Create an API key
4. Copy the key

### 2. Configure the Agent

Add to your `.env` file:

```env
# AI-Powered Matching Settings
ANTHROPIC_API_KEY=sk-ant-your-api-key-here
AI_MATCHING_ENABLED=true
AI_MODEL=claude-sonnet-4-20250514
```

### 3. Install Required Package

```bash
pip install anthropic
```

## Usage

### Option 1: Standalone Testing

Use the test script to try AI matching on your data:

```bash
cd BA_Dedup2
python examples/test_ai_matching.py
```

### Option 2: Programmatic Usage

```python
from agents.ai_matching_agent import AIMatchingAgent
import pandas as pd

# Load your data
data = pd.read_csv('input/sample_data.csv')

# Configure AI agent
ai_agent = AIMatchingAgent({
    'api_key': 'your-api-key',
    'model': 'claude-sonnet-4-20250514',
    'similarity_threshold': 0.80,  # 80% confidence threshold
    'batch_size': 10,  # Process 10 pairs per API call
    'fields_to_compare': ['name', 'address', 'city', 'state', 'zip']
})

# Find duplicates
result = ai_agent.execute(data)

# View duplicates
duplicates = result[result['cluster_id'] != -1]
print(f"Found {len(duplicates)} duplicate records in {duplicates['cluster_id'].nunique()} clusters")
```

### Option 3: Integration with Pipeline

Create a custom workflow that uses AI matching instead of fuzzy matching:

```json
{
  "name": "ba_dedup_ai_workflow",
  "description": "BA deduplication with AI-powered matching",
  "steps": [
    {
      "name": "ingestion",
      "agent": "ingestion",
      "config": {
        "source_type": "csv",
        "source_path": "input/sample_data.csv"
      }
    },
    {
      "name": "validation",
      "agent": "validation",
      "config": {}
    },
    {
      "name": "ai_matching",
      "agent": "ai_matching",
      "config": {
        "similarity_threshold": 0.80,
        "batch_size": 10
      }
    },
    {
      "name": "merge",
      "agent": "merge",
      "config": {
        "merge_strategy": "most_complete"
      }
    },
    {
      "name": "output",
      "agent": "output",
      "config": {
        "output_type": "database",
        "output_table": "business_associates_ai_deduplicated"
      }
    }
  ]
}
```

## Configuration Options

### Agent Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_key` | string | env: ANTHROPIC_API_KEY | Anthropic API key |
| `model` | string | claude-sonnet-4-20250514 | Claude model to use |
| `similarity_threshold` | float | 0.80 | Confidence threshold (0.0-1.0) |
| `batch_size` | int | 10 | Pairs processed per API call |
| `fields_to_compare` | list | ['name', 'address', ...] | Fields sent to AI |

### Model Recommendations

- **claude-sonnet-4-20250514**: Best balance of speed and accuracy (recommended)
- **claude-opus-4-20250514**: Highest accuracy, slower and more expensive
- **claude-haiku-3-5-20241022**: Fastest and cheapest, lower accuracy

## Cost Considerations

AI matching uses API calls, which incur costs based on tokens:

- **Cost per 1M tokens** (Sonnet): ~$3 input, ~$15 output
- **Average cost per pair**: ~$0.001-0.002
- **For 100 records**: ~$0.50-2.00 (depending on batch size and data)

### Cost Optimization Tips:

1. **Use blocking first**: Generate candidate pairs using state/zip blocking before AI analysis
2. **Increase batch size**: Process more pairs per API call (up to ~20 pairs)
3. **Use Haiku for simple cases**: Switch to Haiku model for straightforward deduplication
4. **Cache results**: The agent doesn't re-analyze pairs that have been checked

## Example Output

The AI agent adds these columns to your data:

- `cluster_id`: Cluster number for duplicates (-1 for unique records)
- `similarity_score`: AI confidence score (0.0-1.0)
- `ai_reasoning`: Text explanation of why records were matched

Example:

```
Cluster 0:
  Name: Mike Robertson
  Address: 9215 delacorte
  City: missouri city
  State: TX
  Confidence: 0.95
  AI Reasoning: Same person - 'Mike' is nickname for 'Michael'. Same address with minor variation ('delacorte' vs 'delacorte ln.'). Same city and state.

  Name: Michael Robertson
  Address: 9215 delacorte ln.
  City: missouri city
  State: TX
  ZIP: 77459
  Confidence: 0.95
  AI Reasoning: Same person - 'Mike' is nickname for 'Michael'. Same address with minor variation ('delacorte' vs 'delacorte ln.'). Same city and state.
```

## Comparison: AI vs Fuzzy Matching

| Feature | Fuzzy Matching | AI Matching |
|---------|----------------|-------------|
| Speed | Very Fast | Slower (API calls) |
| Cost | Free | ~$0.001-0.002 per pair |
| Accuracy | Good (85-90%) | Excellent (95-98%) |
| Nickname handling | Manual mapping | Automatic understanding |
| Context awareness | None | High |
| Explainability | Score only | Reasoning provided |
| Offline capable | Yes | No (requires API) |

## Best Practices

1. **Start with fuzzy matching** for initial deduplication
2. **Use AI for edge cases**: Records that fuzzy matching misses or is uncertain about
3. **Review AI reasoning**: Check the explanations for quality assurance
4. **Tune threshold**: Start at 0.80, adjust based on false positives/negatives
5. **Monitor costs**: Track API usage for large datasets

## Troubleshooting

### Error: "Anthropic API key not configured"

- Check that `ANTHROPIC_API_KEY` is set in your `.env` file
- Make sure the key starts with `sk-ant-`
- Verify the `.env` file is in the correct directory

### Error: "Could not find JSON in AI response"

- The AI's response format may have changed
- Check the logs for the raw response
- Try increasing `max_tokens` if response is truncated

### High costs

- Reduce `batch_size` to avoid token limits
- Use blocking to reduce candidate pairs
- Switch to Haiku model for cost savings
- Consider using AI only for uncertain matches (hybrid approach)

## Future Enhancements

- [ ] Hybrid approach: Use fuzzy matching first, AI for edge cases
- [ ] Caching: Store AI decisions to avoid re-analysis
- [ ] Fine-tuning: Custom prompts for specific business rules
- [ ] Confidence calibration: Adjust thresholds based on validation feedback
- [ ] Multi-language support: Handle international names and addresses

## Support

For issues or questions about AI matching:
- Check the logs in `logs/ba_dedup.log`
- Review API usage in Anthropic console
- See main README.md for general troubleshooting
