"""
BA Deduplication Pipeline - Main Entry Point
Orchestrates the end-to-end deduplication workflow for Business Associate records.
"""
import argparse
import sys
from pathlib import Path

from workflows.workflow_engine import WorkflowEngine
from state.state_manager import StateManager
from utils.logger import get_logger
from config import settings

logger = get_logger('main')


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='BA Deduplication Pipeline - Identify and merge duplicate Business Associate records',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with CSV input
  python main.py --input input/sample_data.csv --input-type csv

  # Run with Excel input
  python main.py --input data/business_associates.xlsx --input-type excel

  # Run with database input
  python main.py --input-type database --table business_associates_raw

  # Run with custom similarity threshold
  python main.py --input input/sample_data.csv --threshold 0.90

  # Reset state and run from scratch
  python main.py --input input/sample_data.csv --reset
        """
    )

    # Input options
    parser.add_argument(
        '--input', '-i',
        type=str,
        help='Path to input file (CSV or Excel)'
    )

    parser.add_argument(
        '--input-type',
        type=str,
        choices=['csv', 'excel', 'database'],
        help='Input source type (default: from .env or csv)'
    )

    parser.add_argument(
        '--table',
        type=str,
        help='Database table name (for database input type)'
    )

    # Output options
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output destination (table name or file path)'
    )

    parser.add_argument(
        '--output-type',
        type=str,
        choices=['database', 'csv', 'excel'],
        default='database',
        help='Output destination type (default: database)'
    )

    # Processing options
    parser.add_argument(
        '--threshold', '-t',
        type=float,
        help=f'Similarity threshold for matching (0-1, default: {settings.SIMILARITY_THRESHOLD})'
    )

    parser.add_argument(
        '--merge-strategy',
        type=str,
        choices=['most_complete', 'most_recent', 'first'],
        help=f'Strategy for merging duplicates (default: {settings.MERGE_STRATEGY})'
    )

    # Workflow options
    parser.add_argument(
        '--workflow', '-w',
        type=str,
        help='Path to custom workflow definition JSON file'
    )

    parser.add_argument(
        '--reset', '-r',
        action='store_true',
        help='Reset pipeline state and run from scratch'
    )

    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from last checkpoint (default behavior)'
    )

    # Logging options
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help=f'Logging level (default: {settings.LOG_LEVEL})'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output (equivalent to --log-level DEBUG)'
    )

    return parser.parse_args()


def build_workflow_config(args):
    """Build workflow configuration from arguments."""
    config = {}

    # Input configuration
    if args.input:
        config['source_path'] = args.input
        config['source_type'] = args.input_type or 'csv'

    if args.input_type:
        config['source_type'] = args.input_type

    if args.table:
        config['table_name'] = args.table

    # Output configuration
    if args.output:
        if args.output_type == 'database':
            config['output_table'] = args.output
        else:
            config['output_path'] = args.output

    config['output_type'] = args.output_type

    # Processing configuration
    if args.threshold:
        config['similarity_threshold'] = args.threshold

    if args.merge_strategy:
        config['merge_strategy'] = args.merge_strategy

    return config


def update_workflow_definition(workflow_def, config):
    """Update workflow definition with config from arguments."""
    # Update agent configs based on provided config
    for step in workflow_def.get('steps', []):
        agent_name = step['agent']

        if agent_name == 'ingestion':
            if 'source_type' in config:
                step['config']['source_type'] = config['source_type']
            if 'source_path' in config:
                step['config']['source_path'] = config['source_path']
            if 'table_name' in config:
                step['config']['table_name'] = config['table_name']

        elif agent_name == 'matching':
            if 'similarity_threshold' in config:
                step['config']['similarity_threshold'] = config['similarity_threshold']

        elif agent_name == 'merge':
            if 'merge_strategy' in config:
                step['config']['merge_strategy'] = config['merge_strategy']

        elif agent_name == 'output':
            if 'output_type' in config:
                step['config']['output_type'] = config['output_type']
            if 'output_table' in config:
                step['config']['output_table'] = config['output_table']
            if 'output_path' in config:
                step['config']['output_path'] = config['output_path']

    return workflow_def


def main():
    """Main entry point for BA deduplication pipeline."""
    try:
        # Parse arguments
        args = parse_arguments()

        # Set log level
        if args.verbose:
            settings.LOG_LEVEL = 'DEBUG'
        elif args.log_level:
            settings.LOG_LEVEL = args.log_level

        logger.info("=" * 80)
        logger.info("BA Deduplication Pipeline")
        logger.info("=" * 80)

        # Initialize state manager
        state_manager = StateManager()

        # Reset state if requested
        if args.reset:
            logger.info("Resetting pipeline state...")
            state_manager.reset()

        # Build configuration from arguments
        config = build_workflow_config(args)

        # Log configuration
        logger.info("Configuration:")
        for key, value in config.items():
            logger.info(f"  {key}: {value}")

        # Initialize workflow engine
        if args.workflow:
            logger.info(f"Loading custom workflow: {args.workflow}")
            workflow_engine = WorkflowEngine(
                workflow_file=args.workflow,
                state_manager=state_manager
            )
        else:
            logger.info("Using default workflow")
            workflow_engine = WorkflowEngine(state_manager=state_manager)

        # Update workflow with config
        workflow_def = workflow_engine.workflow_def
        workflow_def = update_workflow_definition(workflow_def, config)

        # Re-initialize agents with updated config
        workflow_engine.agents = workflow_engine._initialize_agents()

        # Run pipeline
        logger.info("\nStarting pipeline execution...")
        result = workflow_engine.run()

        # Print summary
        logger.info("\n" + "=" * 80)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 80)

        summary = workflow_engine.get_summary()
        logger.info(f"\nSummary:")
        logger.info(f"  Total records processed: {len(result)}")
        logger.info(f"  Steps executed: {summary['steps_executed']}")

        # Print agent statistics
        logger.info(f"\nAgent Statistics:")
        for step_name, stats in summary['agent_stats'].items():
            logger.info(f"  {step_name}:")
            logger.info(f"    Execution count: {stats['execution_count']}")
            logger.info(f"    Error count: {stats['error_count']}")

        # Check for merge agent to show deduplication stats
        merge_agent = workflow_engine.get_agent('merge')
        if merge_agent and merge_agent.merge_audit:
            logger.info(f"\nDeduplication Results:")
            logger.info(f"  Duplicate clusters merged: {len(merge_agent.merge_audit)}")
            total_duplicates = sum(audit['source_record_count'] for audit in merge_agent.merge_audit)
            logger.info(f"  Total duplicate records: {total_duplicates}")
            logger.info(f"  Records after deduplication: {len(result)}")

        logger.info("\n" + "=" * 80)

        return 0

    except KeyboardInterrupt:
        logger.warning("\nPipeline interrupted by user")
        return 130

    except Exception as e:
        logger.error(f"\nPipeline failed with error: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
