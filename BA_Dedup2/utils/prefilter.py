"""
Pre-filtering utilities to identify records that need deduplication.
Reduces processing by identifying obvious uniques before expensive fuzzy matching.
"""
import pandas as pd
from typing import Dict, Tuple
from utils.logger import get_logger

logger = get_logger(__name__)


def identify_dedup_candidates(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Pre-filter records to identify which need deduplication.

    Strategy:
    1. Records with unique SSN token → likely unique (if SSN present and valid)
    2. Records with unique exact match key (name+address+zip) → unique
    3. Everything else → needs fuzzy matching

    Args:
        df: DataFrame with normalized fields

    Returns:
        Dict with keys:
            - 'needs_dedup': Records that need fuzzy matching
            - 'unique_already': Records that are obviously unique
    """
    logger.info(f"Pre-filtering {len(df)} records to identify dedup candidates...")

    needs_dedup_list = []
    unique_already_list = []

    # Strategy 1: Group by SSN token (if available)
    if 'ssn_token' in df.columns:
        # Records with valid SSN token
        has_ssn = df[df['ssn_token'].notna() & (df['ssn_token'] != '')].copy()
        no_ssn = df[~df.index.isin(has_ssn.index)].copy()

        if len(has_ssn) > 0:
            # Count occurrences of each SSN token
            ssn_counts = has_ssn['ssn_token'].value_counts()
            unique_ssns = ssn_counts[ssn_counts == 1].index
            duplicate_ssns = ssn_counts[ssn_counts > 1].index

            # SSNs appearing once = probably unique
            unique_by_ssn = has_ssn[has_ssn['ssn_token'].isin(unique_ssns)]
            duplicate_by_ssn = has_ssn[has_ssn['ssn_token'].isin(duplicate_ssns)]

            logger.info(f"SSN-based filtering: {len(unique_by_ssn)} unique, {len(duplicate_by_ssn)} potential duplicates")

            unique_already_list.append(unique_by_ssn)
            needs_dedup_list.append(duplicate_by_ssn)

        # Records without SSN need fuzzy matching
        if len(no_ssn) > 0:
            needs_dedup_list.append(no_ssn)
            logger.info(f"{len(no_ssn)} records without SSN need fuzzy matching")
    else:
        # No SSN available, use other strategies
        no_ssn = df.copy()

    # Strategy 2: Exact match key (name + address + zip)
    # This catches obvious duplicates and obvious uniques
    if len(no_ssn) > 0 or 'ssn_token' not in df.columns:
        working_df = no_ssn if 'ssn_token' in df.columns else df.copy()

        # Create exact match key
        working_df['_exact_match_key'] = (
            working_df.get('name_normalized', '').fillna('').astype(str) + '|' +
            working_df.get('address_normalized', '').fillna('').astype(str) + '|' +
            working_df.get('zip_normalized', '').fillna('').astype(str)
        )

        # Count occurrences
        exact_counts = working_df['_exact_match_key'].value_counts()
        unique_keys = exact_counts[exact_counts == 1].index
        duplicate_keys = exact_counts[exact_counts > 1].index

        # Exact matches appearing once = unique
        unique_by_exact = working_df[working_df['_exact_match_key'].isin(unique_keys)]
        duplicate_by_exact = working_df[working_df['_exact_match_key'].isin(duplicate_keys)]

        logger.info(f"Exact match filtering: {len(unique_by_exact)} unique, {len(duplicate_by_exact)} need fuzzy matching")

        if len(unique_by_exact) > 0:
            unique_already_list.append(unique_by_exact.drop(columns=['_exact_match_key']))

        if len(duplicate_by_exact) > 0:
            needs_dedup_list.append(duplicate_by_exact.drop(columns=['_exact_match_key']))

    # Combine results
    needs_dedup = pd.concat(needs_dedup_list, ignore_index=True) if needs_dedup_list else pd.DataFrame()
    unique_already = pd.concat(unique_already_list, ignore_index=True) if unique_already_list else pd.DataFrame()

    # Remove duplicates (record might be in both lists)
    if len(needs_dedup) > 0:
        needs_dedup = needs_dedup.drop_duplicates()
    if len(unique_already) > 0:
        unique_already = unique_already.drop_duplicates()

    logger.info(f"Pre-filtering complete:")
    logger.info(f"  - {len(unique_already)} records are obviously unique (skip fuzzy matching)")
    logger.info(f"  - {len(needs_dedup)} records need fuzzy matching")
    logger.info(f"  - Saved {len(unique_already)} comparisons ({len(unique_already) * len(df) / 2:.0f} potential pairs avoided)")

    return {
        'needs_dedup': needs_dedup,
        'unique_already': unique_already
    }


def estimate_comparison_savings(total_records: int, unique_count: int) -> Dict[str, float]:
    """
    Estimate computational savings from pre-filtering.

    Args:
        total_records: Total number of records
        unique_count: Number of records marked as unique

    Returns:
        Dict with savings statistics
    """
    # Without pre-filtering: n*(n-1)/2 comparisons
    total_comparisons_without = (total_records * (total_records - 1)) / 2

    # With pre-filtering: only compare records needing dedup
    dedup_count = total_records - unique_count
    comparisons_with = (dedup_count * (dedup_count - 1)) / 2

    # Savings
    comparisons_saved = total_comparisons_without - comparisons_with
    percent_saved = (comparisons_saved / total_comparisons_without * 100) if total_comparisons_without > 0 else 0

    return {
        'total_records': total_records,
        'unique_records': unique_count,
        'dedup_records': dedup_count,
        'comparisons_without_prefilter': total_comparisons_without,
        'comparisons_with_prefilter': comparisons_with,
        'comparisons_saved': comparisons_saved,
        'percent_saved': percent_saved
    }


def apply_batch_limit(df: pd.DataFrame, max_batch_size: int = 5000) -> list:
    """
    Split large DataFrames into manageable batches.

    Prevents memory issues and performance degradation from comparing
    too many records at once.

    Args:
        df: DataFrame to batch
        max_batch_size: Maximum records per batch

    Returns:
        List of DataFrame batches
    """
    if len(df) <= max_batch_size:
        return [df]

    batches = []
    num_batches = (len(df) + max_batch_size - 1) // max_batch_size

    for i in range(num_batches):
        start_idx = i * max_batch_size
        end_idx = min((i + 1) * max_batch_size, len(df))
        batch = df.iloc[start_idx:end_idx].copy()
        batches.append(batch)

    logger.info(f"Split {len(df)} records into {len(batches)} batches of max {max_batch_size} records")

    return batches
