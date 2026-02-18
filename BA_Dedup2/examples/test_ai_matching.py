"""
Example script to test AI-powered matching agent.
Demonstrates how to use Claude AI for intelligent duplicate detection.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from agents.ai_matching_agent import AIMatchingAgent
from agents.ingestion_agent import IngestionAgent
from agents.validation_agent import ValidationAgent
from utils.logger import get_logger

logger = get_logger('test_ai_matching')


def main():
    """Test AI matching on sample data."""

    # Step 1: Load data
    logger.info("Loading sample data...")
    ingestion_agent = IngestionAgent({
        'source_type': 'csv',
        'source_path': 'input/sample_data.csv'
    })
    data = ingestion_agent.execute()
    logger.info(f"Loaded {len(data)} records")

    # Step 2: Validate/normalize data
    logger.info("Validating data...")
    validation_agent = ValidationAgent()
    data = validation_agent.execute(data)
    logger.info(f"Validated {len(data)} records")

    # Step 3: Use AI to find duplicates
    logger.info("\n" + "="*80)
    logger.info("Starting AI-Powered Duplicate Detection")
    logger.info("="*80)

    # NOTE: You need to set ANTHROPIC_API_KEY in your .env file first!
    try:
        ai_agent = AIMatchingAgent({
            'similarity_threshold': 0.80,  # 80% confidence threshold
            'batch_size': 5,  # Process 5 pairs at a time
            'fields_to_compare': ['name', 'address', 'city', 'state', 'zip']
        })

        result = ai_agent.execute(data)

        # Show results
        duplicates = result[result['cluster_id'] != -1]
        logger.info(f"\nResults:")
        logger.info(f"  Total records: {len(result)}")
        logger.info(f"  Duplicates found: {len(duplicates)}")
        logger.info(f"  Clusters: {duplicates['cluster_id'].nunique()}")

        # Show some example matches with AI reasoning
        if len(duplicates) > 0:
            logger.info("\nExample AI-identified duplicates:")
            for cluster_id in duplicates['cluster_id'].unique()[:3]:
                cluster = result[result['cluster_id'] == cluster_id]
                logger.info(f"\n--- Cluster {cluster_id} ---")
                for _, row in cluster.iterrows():
                    logger.info(f"  Name: {row['name']}")
                    logger.info(f"  Address: {row['address']}")
                    logger.info(f"  Confidence: {row['similarity_score']:.2f}")
                    if row['ai_reasoning']:
                        logger.info(f"  AI Reasoning: {row['ai_reasoning']}")
                    logger.info("")

    except ValueError as e:
        logger.error(f"\n{e}")
        logger.info("\nTo use AI matching:")
        logger.info("1. Get an API key from https://console.anthropic.com/")
        logger.info("2. Add it to your .env file: ANTHROPIC_API_KEY=your-key-here")
        logger.info("3. Set AI_MATCHING_ENABLED=true")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
