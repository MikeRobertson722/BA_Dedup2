"""
Data type optimization utilities for memory efficiency.

Automatically converts DataFrame columns to optimal data types:
- Repeated string values → category dtype (20-40% memory savings)
- Large integers → smaller int types (int8, int16, int32 vs int64)
- Float precision optimization

Typical memory savings: 20-50% for BA dedup datasets.
"""
import pandas as pd
import numpy as np
from typing import Dict, Any
from utils.logger import get_logger

logger = get_logger(__name__)


def optimize_dataframe_dtypes(df: pd.DataFrame, categorical_threshold: float = 0.5,
                             verbose: bool = False) -> pd.DataFrame:
    """
    Optimize DataFrame column data types for memory efficiency.

    Args:
        df: DataFrame to optimize
        categorical_threshold: Convert to category if unique values / total rows < threshold
        verbose: Print optimization details

    Returns:
        Optimized DataFrame
    """
    if verbose:
        memory_before = df.memory_usage(deep=True).sum() / 1024 / 1024
        logger.info(f"Optimizing dtypes for DataFrame with {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"Memory before: {memory_before:.2f} MB")

    optimizations = []

    for col in df.columns:
        original_dtype = df[col].dtype
        optimized = False

        # Skip already optimal types
        if original_dtype in [np.int8, np.int16, pd.CategoricalDtype]:
            continue

        # Optimize object (string) columns
        if original_dtype == 'object':
            optimized_dtype, saved_mb = _optimize_object_column(df, col, categorical_threshold)
            if optimized_dtype:
                optimizations.append({
                    'column': col,
                    'original_dtype': str(original_dtype),
                    'optimized_dtype': optimized_dtype,
                    'memory_saved_mb': saved_mb
                })
                optimized = True

        # Optimize integer columns
        elif np.issubdtype(original_dtype, np.integer):
            optimized_dtype, saved_mb = _optimize_integer_column(df, col)
            if optimized_dtype:
                optimizations.append({
                    'column': col,
                    'original_dtype': str(original_dtype),
                    'optimized_dtype': optimized_dtype,
                    'memory_saved_mb': saved_mb
                })
                optimized = True

        # Optimize float columns
        elif np.issubdtype(original_dtype, np.floating):
            optimized_dtype, saved_mb = _optimize_float_column(df, col)
            if optimized_dtype:
                optimizations.append({
                    'column': col,
                    'original_dtype': str(original_dtype),
                    'optimized_dtype': optimized_dtype,
                    'memory_saved_mb': saved_mb
                })
                optimized = True

    if verbose and optimizations:
        memory_after = df.memory_usage(deep=True).sum() / 1024 / 1024
        total_saved = memory_before - memory_after
        savings_pct = (total_saved / memory_before) * 100 if memory_before > 0 else 0

        logger.info(f"\nOptimization results:")
        logger.info(f"Memory after:  {memory_after:.2f} MB")
        logger.info(f"Memory saved:  {total_saved:.2f} MB ({savings_pct:.1f}%)")
        logger.info(f"Optimizations: {len(optimizations)} columns")

        for opt in optimizations:
            logger.info(f"  {opt['column']}: {opt['original_dtype']} → {opt['optimized_dtype']} "
                      f"({opt['memory_saved_mb']:.2f} MB saved)")

    return df


def _optimize_object_column(df: pd.DataFrame, col: str,
                           threshold: float) -> tuple[str | None, float]:
    """
    Optimize object (string) column to category if appropriate.

    Args:
        df: DataFrame
        col: Column name
        threshold: Categorical threshold

    Returns:
        Tuple of (optimized dtype name, memory saved in MB) or (None, 0) if no optimization
    """
    try:
        # Calculate unique ratio
        num_unique = df[col].nunique()
        num_total = len(df[col])
        unique_ratio = num_unique / num_total if num_total > 0 else 0

        # Convert to category if low unique ratio
        if unique_ratio < threshold:
            memory_before = df[col].memory_usage(deep=True) / 1024 / 1024
            df[col] = df[col].astype('category')
            memory_after = df[col].memory_usage(deep=True) / 1024 / 1024
            memory_saved = memory_before - memory_after

            return 'category', memory_saved

    except Exception as e:
        logger.warning(f"Could not optimize column {col}: {e}")

    return None, 0


def _optimize_integer_column(df: pd.DataFrame, col: str) -> tuple[str | None, float]:
    """
    Optimize integer column to smallest suitable int type.

    Args:
        df: DataFrame
        col: Column name

    Returns:
        Tuple of (optimized dtype name, memory saved in MB) or (None, 0) if no optimization
    """
    try:
        memory_before = df[col].memory_usage(deep=True) / 1024 / 1024

        # Get min and max values
        col_min = df[col].min()
        col_max = df[col].max()

        # Determine smallest suitable type
        if col_min >= -128 and col_max <= 127:
            new_dtype = np.int8
        elif col_min >= -32768 and col_max <= 32767:
            new_dtype = np.int16
        elif col_min >= -2147483648 and col_max <= 2147483647:
            new_dtype = np.int32
        else:
            return None, 0  # Already at int64 or needs int64

        # Convert if smaller than current
        if df[col].dtype != new_dtype:
            df[col] = df[col].astype(new_dtype)
            memory_after = df[col].memory_usage(deep=True) / 1024 / 1024
            memory_saved = memory_before - memory_after

            return str(new_dtype), memory_saved

    except Exception as e:
        logger.warning(f"Could not optimize column {col}: {e}")

    return None, 0


def _optimize_float_column(df: pd.DataFrame, col: str) -> tuple[str | None, float]:
    """
    Optimize float column to float32 if appropriate.

    Args:
        df: DataFrame
        col: Column name

    Returns:
        Tuple of (optimized dtype name, memory saved in MB) or (None, 0) if no optimization
    """
    try:
        # Only optimize float64 → float32 if data fits
        if df[col].dtype == np.float64:
            memory_before = df[col].memory_usage(deep=True) / 1024 / 1024

            # Check if values fit in float32 range
            col_min = df[col].min()
            col_max = df[col].max()

            if col_min >= np.finfo(np.float32).min and col_max <= np.finfo(np.float32).max:
                df[col] = df[col].astype(np.float32)
                memory_after = df[col].memory_usage(deep=True) / 1024 / 1024
                memory_saved = memory_before - memory_after

                return 'float32', memory_saved

    except Exception as e:
        logger.warning(f"Could not optimize column {col}: {e}")

    return None, 0


def get_memory_usage_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Get detailed memory usage summary for DataFrame.

    Args:
        df: DataFrame to analyze

    Returns:
        Dict with memory usage breakdown
    """
    memory_usage = df.memory_usage(deep=True)
    total_mb = memory_usage.sum() / 1024 / 1024

    column_usage = []
    for col in df.columns:
        col_mb = memory_usage[col] / 1024 / 1024
        col_pct = (col_mb / total_mb * 100) if total_mb > 0 else 0

        column_usage.append({
            'column': col,
            'dtype': str(df[col].dtype),
            'memory_mb': col_mb,
            'memory_pct': col_pct,
            'unique_values': df[col].nunique() if df[col].dtype == 'object' else None
        })

    # Sort by memory usage
    column_usage.sort(key=lambda x: x['memory_mb'], reverse=True)

    return {
        'total_memory_mb': total_mb,
        'num_rows': len(df),
        'num_columns': len(df.columns),
        'columns': column_usage
    }


def print_memory_usage_summary(df: pd.DataFrame):
    """Print formatted memory usage summary."""
    summary = get_memory_usage_summary(df)

    print("\n" + "=" * 80)
    print("DATAFRAME MEMORY USAGE SUMMARY")
    print("=" * 80)
    print(f"Total Memory:  {summary['total_memory_mb']:.2f} MB")
    print(f"Rows:          {summary['num_rows']:,}")
    print(f"Columns:       {summary['num_columns']}")
    print("\nTop 10 Memory Consumers:")
    print("-" * 80)
    print(f"{'Column':<30} {'dtype':<15} {'Memory (MB)':>12} {'%':>8} {'Unique':>10}")
    print("-" * 80)

    for col_info in summary['columns'][:10]:
        unique_str = f"{col_info['unique_values']:,}" if col_info['unique_values'] is not None else "N/A"
        print(f"{col_info['column']:<30} {col_info['dtype']:<15} "
              f"{col_info['memory_mb']:>12.2f} {col_info['memory_pct']:>7.1f}% {unique_str:>10}")

    print("=" * 80 + "\n")


# Suggested categorical columns for BA dedup datasets
BA_CATEGORICAL_COLUMNS = [
    'state',  # Only 50 values (US states)
    'city',  # Typically <1000 unique values
    'entity_type',  # Business types
    'merge_strategy',  # Limited options
    'match_method'  # Limited options
]


def optimize_ba_dataframe(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """
    Optimize BA dedup DataFrame with domain-specific optimizations.

    Args:
        df: BA DataFrame to optimize
        verbose: Print optimization details

    Returns:
        Optimized DataFrame
    """
    # Apply general optimizations
    df = optimize_dataframe_dtypes(df, verbose=verbose)

    # Force specific columns to category if they exist
    for col in BA_CATEGORICAL_COLUMNS:
        if col in df.columns and df[col].dtype != 'category':
            try:
                df[col] = df[col].astype('category')
                if verbose:
                    logger.info(f"Converted {col} to category (BA domain-specific optimization)")
            except Exception as e:
                logger.warning(f"Could not convert {col} to category: {e}")

    # Optimize cluster_id and similarity_score if present
    if 'cluster_id' in df.columns:
        try:
            # cluster_id is typically small integers
            df['cluster_id'] = df['cluster_id'].astype(np.int32)
        except Exception:
            pass

    if 'similarity_score' in df.columns:
        try:
            # similarity_score is 0-1, float32 is sufficient
            df['similarity_score'] = df['similarity_score'].astype(np.float32)
        except Exception:
            pass

    return df
