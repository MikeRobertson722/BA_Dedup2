"""
Structured logging utility for BA Deduplication pipeline.
Provides console and file logging with timestamps and levels.

Enhanced with performance monitoring (Priority 4):
- Memory usage tracking per step
- Execution time tracking
- Metrics export for analysis
"""
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
from config import settings

try:
    import psutil
    import os
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


def get_logger(name, log_file=None):
    """
    Get or create a logger with console and file handlers.

    Args:
        name: Logger name (typically __name__ from calling module)
        log_file: Optional log file path. If None, uses settings.LOG_FILE

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper()))

        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        console_formatter = logging.Formatter(
            '%(levelname)s - %(message)s'
        )

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # File handler
        log_path = log_file or settings.LOG_FILE
        log_path = Path(log_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)

    return logger


class PipelineLogger:
    """
    Specialized logger for tracking pipeline execution progress.
    Provides structured logging of agent execution, data flow, and errors.

    Enhanced with performance monitoring (Priority 4):
    - Memory usage tracking per step
    - Database query counting
    - Metrics collection and export
    """

    def __init__(self, pipeline_name='ba_dedup', enable_performance_tracking=False):
        """
        Initialize pipeline logger.

        Args:
            pipeline_name: Name of the pipeline
            enable_performance_tracking: Enable detailed performance metrics
        """
        self.logger = get_logger(f'pipeline.{pipeline_name}')
        self.pipeline_name = pipeline_name
        self.start_time = None
        self.step_times = {}

        # Performance tracking (Priority 4)
        self.enable_performance_tracking = enable_performance_tracking
        self.step_memory = {}
        self.performance_metrics = {
            'steps': [],
            'memory': {
                'start_mb': 0,
                'peak_mb': 0,
                'current_mb': 0
            },
            'queries': {
                'total': 0,
                'slow_queries': []
            }
        } if enable_performance_tracking else None

    def start_pipeline(self):
        """Log pipeline start."""
        self.start_time = datetime.now()
        self.logger.info(f"=" * 80)
        self.logger.info(f"Starting pipeline: {self.pipeline_name}")
        self.logger.info(f"Start time: {self.start_time}")

        # Track initial memory
        if self.enable_performance_tracking and PSUTIL_AVAILABLE:
            mem_mb = self._get_memory_mb()
            self.performance_metrics['memory']['start_mb'] = mem_mb
            self.performance_metrics['memory']['current_mb'] = mem_mb
            self.logger.info(f"Initial memory: {mem_mb:.1f} MB")

        self.logger.info(f"=" * 80)

    def end_pipeline(self, success=True, export_metrics: bool = False,
                    metrics_path: Optional[str] = None):
        """
        Log pipeline completion.

        Args:
            success: Whether pipeline completed successfully
            export_metrics: Export performance metrics to JSON
            metrics_path: Path to export metrics (default: logs/metrics_{timestamp}.json)
        """
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds() if self.start_time else 0
        status = "COMPLETED" if success else "FAILED"

        self.logger.info(f"=" * 80)
        self.logger.info(f"Pipeline {status}: {self.pipeline_name}")
        self.logger.info(f"End time: {end_time}")
        self.logger.info(f"Total duration: {duration:.2f} seconds")

        # Track final memory and calculate peak
        if self.enable_performance_tracking and PSUTIL_AVAILABLE:
            final_mem = self._get_memory_mb()
            start_mem = self.performance_metrics['memory']['start_mb']
            delta_mem = final_mem - start_mem
            peak_mem = self.performance_metrics['memory']['peak_mb']

            self.logger.info(f"Memory usage: {delta_mem:+.1f} MB (Start: {start_mem:.1f} MB, Peak: {peak_mem:.1f} MB)")
            self.logger.info(f"Database queries: {self.performance_metrics['queries']['total']}")

            # Export metrics if requested
            if export_metrics:
                self._export_metrics(metrics_path, duration, success)

        self.logger.info(f"=" * 80)

    def start_step(self, step_name):
        """Log step start."""
        self.step_times[step_name] = datetime.now()

        # Track step memory start
        if self.enable_performance_tracking and PSUTIL_AVAILABLE:
            self.step_memory[step_name] = self._get_memory_mb()

        self.logger.info(f"\n{'-' * 60}")
        self.logger.info(f"Starting step: {step_name}")
        self.logger.info(f"{'-' * 60}")

    def end_step(self, step_name, record_count=None, success=True):
        """Log step completion."""
        start_time = self.step_times.get(step_name)
        duration = (datetime.now() - start_time).total_seconds() if start_time else 0
        status = "[OK]" if success else "[FAIL]"

        msg = f"{status} Step completed: {step_name} ({duration:.2f}s)"
        if record_count is not None:
            msg += f" - {record_count} records"

        # Track memory delta for this step
        if self.enable_performance_tracking and PSUTIL_AVAILABLE:
            current_mem = self._get_memory_mb()
            start_mem = self.step_memory.get(step_name, current_mem)
            delta_mem = current_mem - start_mem

            # Update peak memory
            if current_mem > self.performance_metrics['memory']['peak_mb']:
                self.performance_metrics['memory']['peak_mb'] = current_mem

            # Store step metrics
            step_metrics = {
                'name': step_name,
                'duration': duration,
                'memory_delta_mb': delta_mem,
                'success': success,
                'timestamp': datetime.now().isoformat()
            }
            if record_count is not None:
                step_metrics['record_count'] = record_count
                step_metrics['throughput'] = record_count / duration if duration > 0 else 0

            self.performance_metrics['steps'].append(step_metrics)

            msg += f" | mem_delta: {delta_mem:+.1f} MB"

        self.logger.info(msg)

    def log_agent_execution(self, agent_name, action, details=None):
        """Log agent-specific actions."""
        msg = f"[{agent_name}] {action}"
        if details:
            msg += f" - {details}"
        self.logger.info(msg)

    def log_data_stats(self, step_name, stats_dict):
        """Log data statistics."""
        self.logger.info(f"Data stats for {step_name}:")
        for key, value in stats_dict.items():
            self.logger.info(f"  {key}: {value}")

    def log_error(self, error, context=None):
        """Log error with context."""
        self.logger.error(f"Error: {error}")
        if context:
            self.logger.error(f"Context: {context}")

    def log_warning(self, warning, context=None):
        """Log warning with context."""
        self.logger.warning(f"Warning: {warning}")
        if context:
            self.logger.warning(f"Context: {context}")

    # PRIORITY 4: Performance monitoring methods

    def track_query(self, query_time: float, query: Optional[str] = None):
        """
        Track database query execution.

        Args:
            query_time: Query execution time in seconds
            query: Optional SQL query text
        """
        if not self.enable_performance_tracking:
            return

        self.performance_metrics['queries']['total'] += 1

        # Track slow queries (> 1 second)
        if query_time > 1.0:
            self.performance_metrics['queries']['slow_queries'].append({
                'duration': query_time,
                'query': query[:200] if query else 'N/A',
                'timestamp': datetime.now().isoformat()
            })
            self.logger.warning(f"Slow query detected: {query_time:.3f}s")

    def log_performance_summary(self):
        """Print performance summary at end of pipeline."""
        if not self.enable_performance_tracking or not self.performance_metrics:
            return

        self.logger.info("\n" + "=" * 80)
        self.logger.info("PERFORMANCE SUMMARY")
        self.logger.info("=" * 80)

        # Memory stats
        mem = self.performance_metrics['memory']
        self.logger.info(f"Memory: Start={mem['start_mb']:.1f} MB, "
                       f"Peak={mem['peak_mb']:.1f} MB, "
                       f"Delta={mem['peak_mb']-mem['start_mb']:+.1f} MB")

        # Query stats
        queries = self.performance_metrics['queries']
        self.logger.info(f"Database Queries: {queries['total']} total, "
                       f"{len(queries['slow_queries'])} slow (>1s)")

        # Top slowest steps
        if self.performance_metrics['steps']:
            sorted_steps = sorted(self.performance_metrics['steps'],
                                key=lambda x: x['duration'], reverse=True)[:5]
            self.logger.info("\nTop 5 Slowest Steps:")
            for i, step in enumerate(sorted_steps, 1):
                self.logger.info(f"  {i}. {step['name']}: {step['duration']:.2f}s "
                              f"(Δmem: {step.get('memory_delta_mb', 0):+.1f} MB)")

        self.logger.info("=" * 80)

    def _get_memory_mb(self) -> float:
        """Get current process memory usage in MB."""
        if not PSUTIL_AVAILABLE:
            return 0.0
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024

    def _export_metrics(self, metrics_path: Optional[str], duration: float, success: bool):
        """Export performance metrics to JSON file."""
        import json

        if metrics_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            metrics_path = f"logs/metrics_{self.pipeline_name}_{timestamp}.json"

        # Prepare export data
        export_data = {
            'pipeline_name': self.pipeline_name,
            'timestamp': datetime.now().isoformat(),
            'success': success,
            'duration': duration,
            **self.performance_metrics
        }

        # Ensure directory exists
        Path(metrics_path).parent.mkdir(parents=True, exist_ok=True)

        # Write to file
        with open(metrics_path, 'w') as f:
            json.dump(export_data, f, indent=2)

        self.logger.info(f"Performance metrics exported to: {metrics_path}")
