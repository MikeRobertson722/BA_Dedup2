"""
Test Hybrid Matching - Combines fuzzy + AI matching.
Demonstrates the optimal approach for accuracy and cost efficiency.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from workflows.workflow_engine import WorkflowEngine
from state.state_manager import StateManager
from utils.logger import get_logger

logger = get_logger('test_hybrid')


def main():
    """Test hybrid matching workflow."""

    logger.info("=" * 80)
    logger.info("HYBRID MATCHING TEST: Fuzzy + AI")
    logger.info("=" * 80)
    logger.info("")
    logger.info("This test demonstrates the hybrid approach:")
    logger.info("1. Fuzzy matching finds obvious duplicates (fast, free)")
    logger.info("2. AI analyzes uncertain cases (accurate, low cost)")
    logger.info("3. Best of both worlds!")
    logger.info("")

    # Check if AI is configured
    import os
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key or api_key == 'your-api-key-here':
        logger.warning("⚠️  ANTHROPIC_API_KEY not configured")
        logger.info("")
        logger.info("The hybrid workflow will run with fuzzy matching only.")
        logger.info("To enable AI analysis of uncertain cases:")
        logger.info("1. Get an API key from https://console.anthropic.com/")
        logger.info("2. Add to .env: ANTHROPIC_API_KEY=your-key-here")
        logger.info("3. Set AI_MATCHING_ENABLED=true")
        logger.info("")
        input("Press Enter to continue with fuzzy-only mode, or Ctrl+C to exit...")

    # Initialize state manager
    state_manager = StateManager()
    state_manager.reset()  # Start fresh

    # Load hybrid workflow
    workflow_file = Path(__file__).parent.parent / 'workflows' / 'definitions' / 'hybrid_pipeline.json'

    try:
        # Create workflow engine
        logger.info(f"Loading hybrid workflow: {workflow_file}")
        engine = WorkflowEngine(
            workflow_file=str(workflow_file),
            state_manager=state_manager
        )

        # Run pipeline
        logger.info("\nStarting hybrid pipeline execution...\n")
        result = engine.run()

        # Get hybrid matching statistics
        hybrid_agent = engine.get_agent('hybrid_matching')
        if hybrid_agent:
            stats = hybrid_agent['agent'].get_stats()

            logger.info("\n" + "=" * 80)
            logger.info("HYBRID MATCHING STATISTICS")
            logger.info("=" * 80)
            logger.info(f"Total candidate pairs analyzed: {stats['total_pairs']}")
            logger.info(f"")
            logger.info(f"Fuzzy Matching Results:")
            logger.info(f"  High confidence matches: {stats['high_confidence_pairs']} pairs")
            logger.info(f"  Uncertain cases: {stats['uncertain_pairs']} pairs")
            logger.info(f"  Low confidence (rejected): {stats['low_confidence_pairs']} pairs")
            logger.info(f"")

            if stats['ai_analyzed_pairs'] > 0:
                logger.info(f"AI Analysis of Uncertain Cases:")
                logger.info(f"  Pairs analyzed: {stats['ai_analyzed_pairs']}")
                logger.info(f"  Confirmed as duplicates: {stats['ai_confirmed_pairs']}")
                logger.info(f"  Rejected as non-duplicates: {stats['ai_rejected_pairs']}")
                logger.info(f"")
                logger.info(f"Cost Savings:")
                logger.info(f"  Only {stats['uncertain_pairs']} pairs sent to AI")
                logger.info(f"  vs. {stats['total_pairs']} pairs if using AI-only")
                logger.info(f"  Reduction: {100 * (1 - stats['uncertain_pairs']/stats['total_pairs']):.1f}%")
            else:
                logger.info(f"AI Analysis: Skipped (not enabled or no uncertain cases)")

        # Show summary
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 80)
        summary = engine.get_summary()
        logger.info(f"Total records processed: {len(result)}")
        logger.info(f"Steps executed: {summary['steps_executed']}")

        # Show deduplication results
        merge_agent = engine.get_agent('merge')
        if merge_agent and merge_agent['agent'].merge_audit:
            audit = merge_agent['agent'].merge_audit
            logger.info(f"\nDeduplication Results:")
            logger.info(f"  Duplicate clusters merged: {len(audit)}")
            total_duplicates = sum(a['source_record_count'] for a in audit)
            logger.info(f"  Total duplicate records: {total_duplicates}")
            logger.info(f"  Records after deduplication: {len(result)}")

        logger.info("\n" + "=" * 80)
        logger.info("✅ HYBRID PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)

        return 0

    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
