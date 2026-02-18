"""
Performance monitoring and benchmarking utilities.

Provides decorators and utilities for tracking execution time, memory usage,
and database queries. Essential for optimization and regression testing.
"""
import time
import functools
import tracemalloc
import psutil
import os
from typing import Any, Callable, Dict, Optional
from datetime import datetime
import json
from pathlib import Path
from utils.logger import get_logger

logger = get_logger(__name__)


class PerformanceMetrics:
    """
    Collects and manages performance metrics for the pipeline.

    Tracks:
    - Execution time per operation
    - Memory usage (current, peak, delta)
    - Database query counts
    - Record counts and throughput
    """

    def __init__(self):
        """Initialize metrics collector."""
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'total_duration': 0,
            'operations': [],
            'memory': {
                'start_mb': 0,
                'end_mb': 0,
                'peak_mb': 0,
                'delta_mb': 0
            },
            'database': {
                'total_queries': 0,
                'slow_queries': []
            },
            'records': {
                'input_count': 0,
                'output_count': 0,
                'duplicates_removed': 0
            }
        }
        self.operation_stack = []  # Track nested operations

    def start(self):
        """Start collecting metrics."""
        self.metrics['start_time'] = datetime.now().isoformat()
        self.metrics['memory']['start_mb'] = self._get_memory_usage_mb()
        tracemalloc.start()

    def end(self):
        """End collecting metrics and calculate totals."""
        self.metrics['end_time'] = datetime.now().isoformat()
        self.metrics['memory']['end_mb'] = self._get_memory_usage_mb()
        self.metrics['memory']['delta_mb'] = (
            self.metrics['memory']['end_mb'] - self.metrics['memory']['start_mb']
        )

        # Get peak memory from tracemalloc
        current, peak = tracemalloc.get_traced_memory()
        self.metrics['memory']['peak_mb'] = peak / 1024 / 1024
        tracemalloc.stop()

        # Calculate total duration
        if self.metrics['start_time'] and self.metrics['end_time']:
            start = datetime.fromisoformat(self.metrics['start_time'])
            end = datetime.fromisoformat(self.metrics['end_time'])
            self.metrics['total_duration'] = (end - start).total_seconds()

    def add_operation(self, name: str, duration: float, memory_delta_mb: float = 0,
                     record_count: Optional[int] = None, details: Optional[Dict] = None):
        """
        Add operation metrics.

        Args:
            name: Operation name
            duration: Execution time in seconds
            memory_delta_mb: Memory change in MB
            record_count: Number of records processed
            details: Additional operation details
        """
        operation = {
            'name': name,
            'duration': duration,
            'memory_delta_mb': memory_delta_mb,
            'timestamp': datetime.now().isoformat()
        }

        if record_count is not None:
            operation['record_count'] = record_count
            operation['throughput'] = record_count / duration if duration > 0 else 0

        if details:
            operation['details'] = details

        self.metrics['operations'].append(operation)

    def add_query(self, query_time: float, query: Optional[str] = None):
        """
        Add database query metrics.

        Args:
            query_time: Query execution time in seconds
            query: Optional SQL query text
        """
        self.metrics['database']['total_queries'] += 1

        # Track slow queries (> 1 second)
        if query_time > 1.0:
            self.metrics['database']['slow_queries'].append({
                'duration': query_time,
                'query': query[:200] if query else 'N/A',  # Truncate long queries
                'timestamp': datetime.now().isoformat()
            })

    def set_record_counts(self, input_count: int, output_count: int):
        """Set input/output record counts."""
        self.metrics['records']['input_count'] = input_count
        self.metrics['records']['output_count'] = output_count
        self.metrics['records']['duplicates_removed'] = input_count - output_count

    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary for display."""
        return {
            'total_duration': f"{self.metrics['total_duration']:.2f}s",
            'memory_usage': f"{self.metrics['memory']['delta_mb']:.1f} MB",
            'peak_memory': f"{self.metrics['memory']['peak_mb']:.1f} MB",
            'total_queries': self.metrics['database']['total_queries'],
            'slow_queries': len(self.metrics['database']['slow_queries']),
            'duplicates_removed': self.metrics['records']['duplicates_removed'],
            'operations': len(self.metrics['operations'])
        }

    def export_json(self, filepath: str):
        """Export metrics to JSON file."""
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(self.metrics, f, indent=2)
        logger.info(f"Metrics exported to {filepath}")

    def _get_memory_usage_mb(self) -> float:
        """Get current process memory usage in MB."""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024


# Global metrics instance
_global_metrics = None


def get_metrics() -> PerformanceMetrics:
    """Get or create global metrics instance."""
    global _global_metrics
    if _global_metrics is None:
        _global_metrics = PerformanceMetrics()
    return _global_metrics


def reset_metrics():
    """Reset global metrics."""
    global _global_metrics
    _global_metrics = PerformanceMetrics()


def track_time(operation_name: Optional[str] = None):
    """
    Decorator to track execution time of a function.

    Args:
        operation_name: Custom name for operation (defaults to function name)

    Usage:
        @track_time()
        def my_function():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = operation_name or func.__name__

            # Track memory before
            mem_before = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

            # Execute function
            start_time = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start_time

            # Track memory after
            mem_after = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            mem_delta = mem_after - mem_before

            # Add to metrics
            metrics = get_metrics()
            metrics.add_operation(name, duration, mem_delta)

            logger.debug(f"⏱️  {name}: {duration:.3f}s, Δmem: {mem_delta:+.1f}MB")

            return result
        return wrapper
    return decorator


def track_memory(operation_name: Optional[str] = None):
    """
    Decorator to track memory usage of a function.

    Args:
        operation_name: Custom name for operation

    Usage:
        @track_memory()
        def memory_intensive_function():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = operation_name or func.__name__

            # Start tracemalloc if not running
            if not tracemalloc.is_tracing():
                tracemalloc.start()

            # Get snapshot before
            tracemalloc.reset_peak()
            mem_before = tracemalloc.get_traced_memory()[0] / 1024 / 1024

            # Execute function
            result = func(*args, **kwargs)

            # Get snapshot after
            mem_after, peak_mem = tracemalloc.get_traced_memory()
            mem_after = mem_after / 1024 / 1024
            peak_mem = peak_mem / 1024 / 1024
            mem_delta = mem_after - mem_before

            logger.debug(f"💾 {name}: Peak: {peak_mem:.1f}MB, Δmem: {mem_delta:+.1f}MB")

            return result
        return wrapper
    return decorator


class Timer:
    """
    Context manager for timing code blocks.

    Usage:
        with Timer("my_operation"):
            # code to time
            ...
    """

    def __init__(self, name: str, verbose: bool = True):
        """
        Initialize timer.

        Args:
            name: Operation name
            verbose: Print timing info when done
        """
        self.name = name
        self.verbose = verbose
        self.start_time = None
        self.duration = None

    def __enter__(self):
        """Start timing."""
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End timing and log."""
        self.duration = time.time() - self.start_time

        # Add to metrics
        metrics = get_metrics()
        metrics.add_operation(self.name, self.duration)

        if self.verbose:
            logger.info(f"[TIME] {self.name}: {self.duration:.3f}s")


class MemoryProfiler:
    """
    Context manager for profiling memory usage of code blocks.

    Usage:
        with MemoryProfiler("my_operation"):
            # code to profile
            ...
    """

    def __init__(self, name: str, verbose: bool = True):
        """
        Initialize profiler.

        Args:
            name: Operation name
            verbose: Print memory info when done
        """
        self.name = name
        self.verbose = verbose
        self.mem_before = None
        self.mem_after = None
        self.peak_mem = None

    def __enter__(self):
        """Start profiling."""
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        tracemalloc.reset_peak()
        self.mem_before = tracemalloc.get_traced_memory()[0] / 1024 / 1024
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End profiling and log."""
        self.mem_after, self.peak_mem = tracemalloc.get_traced_memory()
        self.mem_after = self.mem_after / 1024 / 1024
        self.peak_mem = self.peak_mem / 1024 / 1024
        mem_delta = self.mem_after - self.mem_before

        if self.verbose:
            logger.info(f"[MEM] {self.name}: Peak: {self.peak_mem:.1f}MB, Delta: {mem_delta:+.1f}MB")


def benchmark_function(func: Callable, iterations: int = 10,
                       warmup: int = 2, *args, **kwargs) -> Dict[str, float]:
    """
    Benchmark a function with multiple iterations.

    Args:
        func: Function to benchmark
        iterations: Number of iterations to run
        warmup: Number of warmup runs (not counted)
        *args: Arguments to pass to function
        **kwargs: Keyword arguments to pass to function

    Returns:
        Dict with timing statistics (min, max, mean, median)
    """
    import statistics

    # Warmup runs
    for _ in range(warmup):
        func(*args, **kwargs)

    # Timed runs
    times = []
    for _ in range(iterations):
        start = time.time()
        func(*args, **kwargs)
        times.append(time.time() - start)

    return {
        'min': min(times),
        'max': max(times),
        'mean': statistics.mean(times),
        'median': statistics.median(times),
        'stdev': statistics.stdev(times) if len(times) > 1 else 0,
        'iterations': iterations
    }


def print_metrics_summary():
    """Print formatted summary of current metrics."""
    metrics = get_metrics()
    summary = metrics.get_summary()

    print("\n" + "=" * 80)
    print("PERFORMANCE METRICS SUMMARY")
    print("=" * 80)
    print(f"Total Duration:      {summary['total_duration']}")
    print(f"Memory Usage:        {summary['memory_usage']}")
    print(f"Peak Memory:         {summary['peak_memory']}")
    print(f"Database Queries:    {summary['total_queries']}")
    print(f"Slow Queries:        {summary['slow_queries']}")
    print(f"Duplicates Removed:  {summary['duplicates_removed']}")
    print(f"Operations Tracked:  {summary['operations']}")
    print("=" * 80)

    # Print top 5 slowest operations
    if metrics.metrics['operations']:
        print("\nTop 5 Slowest Operations:")
        print("-" * 80)
        sorted_ops = sorted(metrics.metrics['operations'],
                          key=lambda x: x['duration'], reverse=True)[:5]
        for i, op in enumerate(sorted_ops, 1):
            print(f"{i}. {op['name']}: {op['duration']:.3f}s")
            if 'throughput' in op:
                print(f"   Throughput: {op['throughput']:.0f} records/sec")

    print()
