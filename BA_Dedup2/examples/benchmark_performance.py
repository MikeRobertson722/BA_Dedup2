"""
Performance Benchmark Suite for BA Dedup System.

Tests performance with datasets of varying sizes:
- 1K records (small)
- 10K records (medium)
- 50K records (large)
- 100K records (very large)

Tracks metrics:
- Execution time per phase
- Memory usage (current, peak, delta)
- Database query counts
- Throughput (records/second)

Usage:
    python examples/benchmark_performance.py [dataset_size]

    dataset_size: 1000, 10000, 50000, 100000 (default: all)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
import tempfile
import sqlite3
from datetime import datetime
import json
from typing import Dict, List

from utils.performance import (
    PerformanceMetrics, Timer, MemoryProfiler,
    get_metrics, reset_metrics, print_metrics_summary
)
from agents.ingestion_agent import IngestionAgent
from agents.validation_agent import ValidationAgent
from agents.matching_agent import MatchingAgent
from agents.merge_agent import MergeAgent
from agents.output_agent import OutputAgent
from utils.logger import get_logger

logger = get_logger(__name__)


def generate_test_dataset(size: int, duplicate_rate: float = 0.3) -> pd.DataFrame:
    """
    Generate synthetic test dataset with controlled duplicates.

    Args:
        size: Number of records to generate
        duplicate_rate: Percentage of records that are duplicates (0.0-1.0)

    Returns:
        DataFrame with test data
    """
    np.random.seed(42)  # For reproducibility

    # Base names, addresses, cities
    first_names = ['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'David', 'Emma', 'Frank']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
    streets = ['Main St', 'Oak Ave', 'Elm St', 'Park Blvd', 'Washington Ave', 'Maple Dr']
    cities = ['Austin', 'Houston', 'Dallas', 'San Antonio', 'Fort Worth', 'El Paso']
    states = ['TX', 'CA', 'NY', 'FL', 'IL']

    records = []

    # Calculate number of unique and duplicate records
    num_duplicates = int(size * duplicate_rate)
    num_unique = size - num_duplicates

    # Generate unique records
    for i in range(num_unique):
        first = np.random.choice(first_names)
        last = np.random.choice(last_names)
        street_num = np.random.randint(100, 9999)
        street = np.random.choice(streets)

        record = {
            'name': f"{first} {last}",
            'address': f"{street_num} {street}",
            'city': np.random.choice(cities),
            'state': np.random.choice(states),
            'zip': f"{np.random.randint(10000, 99999)}",
            'phone': f"{np.random.randint(200, 999)}-{np.random.randint(100, 999)}-{np.random.randint(1000, 9999)}",
            'email': f"{first.lower()}.{last.lower()}@example.com"
        }
        records.append(record)

    # Generate duplicates (variations of existing records)
    duplicate_indices = np.random.choice(num_unique, num_duplicates, replace=True)
    for idx in duplicate_indices:
        original = records[idx].copy()

        # Introduce variations
        variation_type = np.random.choice(['name', 'address', 'exact'])

        if variation_type == 'name':
            # Name variation (abbreviation, middle initial, etc.)
            original['name'] = original['name'].replace('John', 'J.')
        elif variation_type == 'address':
            # Address variation (St vs Street, different ZIP+4, etc.)
            original['address'] = original['address'].replace('St', 'Street')
            if '-' not in original['zip']:
                original['zip'] = f"{original['zip']}-{np.random.randint(1000, 9999)}"
        # else: exact duplicate

        records.append(original)

    # Shuffle records
    df = pd.DataFrame(records)
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    return df


def run_benchmark(dataset_size: int, output_dir: str = 'benchmarks') -> Dict:
    """
    Run complete benchmark for a given dataset size.

    Args:
        dataset_size: Number of records
        output_dir: Directory to save results

    Returns:
        Dict with benchmark results
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"BENCHMARK: {dataset_size:,} records")
    logger.info(f"{'='*80}\n")

    # Reset metrics
    reset_metrics()
    metrics = get_metrics()
    metrics.start()

    # Create temporary database
    temp_db = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.db')
    db_path = temp_db.name
    temp_db.close()
    conn = sqlite3.connect(db_path)

    # Generate test data
    with Timer("1. Generate Test Data", verbose=True):
        df = generate_test_dataset(dataset_size)
        logger.info(f"   Generated {len(df):,} records")

    # Save to CSV
    temp_csv = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
    csv_path = temp_csv.name
    temp_csv.close()
    df.to_csv(csv_path, index=False)

    try:
        # Phase 1: Ingestion
        with Timer("2. Ingestion Agent", verbose=True):
            with MemoryProfiler("Ingestion"):
                ingestion_agent = IngestionAgent()
                df_ingested = ingestion_agent.execute(csv_path)
                logger.info(f"   Ingested: {len(df_ingested):,} records")

        # Phase 2: Validation
        with Timer("3. Validation Agent", verbose=True):
            with MemoryProfiler("Validation"):
                validation_agent = ValidationAgent()
                df_validated = validation_agent.execute(df_ingested)
                logger.info(f"   Validated: {len(df_validated):,} records")

        # Phase 3: Matching
        with Timer("4. Matching Agent", verbose=True):
            with MemoryProfiler("Matching"):
                matching_agent = MatchingAgent(config={'match_method': 'fuzzy'})
                df_matched = matching_agent.execute(df_validated)
                clusters = len(df_matched[df_matched['cluster_id'] != -1]['cluster_id'].unique())
                logger.info(f"   Matched: {clusters} duplicate clusters found")

        # Phase 4: Merge
        with Timer("5. Merge Agent", verbose=True):
            with MemoryProfiler("Merge"):
                merge_agent = MergeAgent(
                    config={'enable_versioning': False},  # Disable for performance
                    db_connection=conn
                )
                df_merged = merge_agent.execute(df_matched)
                logger.info(f"   Merged: {len(df_merged):,} unique records")

        # Phase 5: Output
        with Timer("6. Output Agent", verbose=True):
            with MemoryProfiler("Output"):
                output_agent = OutputAgent()
                df_final = output_agent.execute(df_merged)
                logger.info(f"   Final: {len(df_final):,} records")

        # Set record counts
        metrics.set_record_counts(len(df), len(df_final))

        # Success
        success = True

    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        success = False

    finally:
        # Close database
        conn.close()

        # Clean up temporary files
        Path(db_path).unlink(missing_ok=True)
        Path(csv_path).unlink(missing_ok=True)

    # End metrics collection
    metrics.end()

    # Save results
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    result_file = output_path / f"benchmark_{dataset_size}_{timestamp}.json"
    metrics.export_json(str(result_file))

    # Print summary
    print_metrics_summary()

    return {
        'dataset_size': dataset_size,
        'success': success,
        'metrics': metrics.metrics,
        'result_file': str(result_file)
    }


def run_all_benchmarks():
    """Run benchmarks for all dataset sizes."""
    sizes = [1000, 10000, 50000, 100000]
    results = []

    print("\n" + "="*80)
    print(" " * 20 + "BA DEDUP PERFORMANCE BENCHMARK SUITE")
    print("="*80)
    print(f"\nRunning benchmarks for dataset sizes: {', '.join(str(s) for s in sizes)}")
    print("This may take several minutes...\n")

    for size in sizes:
        try:
            result = run_benchmark(size)
            results.append(result)
        except Exception as e:
            logger.error(f"Failed benchmark for {size}: {e}")
            results.append({
                'dataset_size': size,
                'success': False,
                'error': str(e)
            })

        # Brief pause between benchmarks
        import time
        time.sleep(2)

    # Generate comparison report
    print("\n" + "="*80)
    print(" " * 25 + "BENCHMARK COMPARISON")
    print("="*80)
    print(f"\n{'Dataset Size':>15} | {'Duration':>12} | {'Memory (MB)':>12} | {'Throughput':>15} | {'Status':>8}")
    print("-" * 80)

    for result in results:
        if result['success']:
            m = result['metrics']
            size = result['dataset_size']
            duration = m['total_duration']
            memory = m['memory']['delta_mb']
            throughput = size / duration if duration > 0 else 0
            status = "✓ PASS"
        else:
            size = result['dataset_size']
            duration = 0
            memory = 0
            throughput = 0
            status = "✗ FAIL"

        print(f"{size:>15,} | {duration:>10.2f}s | {memory:>10.1f} MB | {throughput:>10.0f} rec/s | {status:>8}")

    print("\n" + "="*80)
    print("\nBenchmark results saved to: benchmarks/")
    print()


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        # Run specific benchmark
        try:
            size = int(sys.argv[1])
            run_benchmark(size)
        except ValueError:
            print(f"Error: Invalid dataset size '{sys.argv[1]}'")
            print("Usage: python benchmark_performance.py [1000|10000|50000|100000]")
            sys.exit(1)
    else:
        # Run all benchmarks
        run_all_benchmarks()


if __name__ == '__main__':
    main()
