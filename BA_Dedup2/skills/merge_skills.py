"""
Merge Skills - Reusable golden record creation and merging functions.
Extracted from MergeAgent for standalone use.
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any
from utils.logger import get_logger

logger = get_logger(__name__)


def calculate_completeness_score(record: pd.Series, score_fields: List[str] = None) -> int:
    """
    Calculate completeness score for a record (higher = more complete).

    Args:
        record: Pandas Series representing a record
        score_fields: List of fields to consider for scoring
                     Default: ['address', 'city', 'state', 'zip', 'phone', 'email', 'contact_person']

    Returns:
        Completeness score (number of non-null fields)

    Example:
        score = calculate_completeness_score(df.iloc[0])
        print(f"Record completeness: {score}/7")
    """
    if score_fields is None:
        score_fields = ['address', 'city', 'state', 'zip', 'phone', 'email', 'contact_person']

    score = sum(
        1 for field in score_fields
        if field in record.index and pd.notna(record[field]) and str(record[field]).strip()
    )

    return score


def select_best_values(cluster_df: pd.DataFrame, value_fields: List[str] = None) -> Dict[str, Any]:
    """
    Select the best (most complete, most frequent) values from a cluster.

    Args:
        cluster_df: DataFrame with all records in a cluster
        value_fields: Fields to extract best values for
                     Default: ['name', 'address', 'city', 'state', 'zip', 'phone', 'email', 'contact_person']

    Returns:
        Dictionary of field -> best value

    Example:
        cluster_records = df[df['cluster_id'] == 100]
        best_values = select_best_values(cluster_records)
        print(f"Best name: {best_values['name']}")
    """
    if value_fields is None:
        value_fields = ['name', 'address', 'city', 'state', 'zip', 'phone', 'email', 'contact_person']

    best_values = {}

    for field in value_fields:
        if field not in cluster_df.columns:
            best_values[field] = ''
            continue

        # Filter out null/empty values
        non_null = cluster_df[field].dropna()
        non_null = non_null[non_null.astype(str).str.strip() != '']

        if len(non_null) == 0:
            best_values[field] = ''
        elif len(non_null) == 1:
            best_values[field] = non_null.iloc[0]
        else:
            # Use most common value (mode)
            # If tie, use first occurrence
            best_values[field] = non_null.mode().iloc[0]

    return best_values


def create_golden_record(
    cluster_df: pd.DataFrame,
    cluster_id: int,
    strategy: str = 'most_complete'
) -> pd.Series:
    """
    Create a golden record from a cluster of duplicates.

    Args:
        cluster_df: DataFrame with all records in the cluster
        cluster_id: Cluster ID
        strategy: Merging strategy:
                 - 'most_complete': Select record with most non-null fields (default)
                 - 'first': Use first record
                 - 'best_values': Select best value for each field independently

    Returns:
        Pandas Series representing the golden record

    Example:
        cluster_records = df[df['cluster_id'] == 100]
        golden = create_golden_record(cluster_records, cluster_id=100, strategy='most_complete')
        print(golden['name'])
    """
    if len(cluster_df) == 0:
        raise ValueError("Cluster is empty")

    if strategy == 'first':
        # Simply use the first record
        golden_record = cluster_df.iloc[0].copy()

    elif strategy == 'most_complete':
        # Select the record with the most complete data
        cluster_df['_completeness'] = cluster_df.apply(calculate_completeness_score, axis=1)
        golden_record = cluster_df.loc[cluster_df['_completeness'].idxmax()].copy()
        golden_record = golden_record.drop('_completeness')

    elif strategy == 'best_values':
        # Select best value for each field independently
        best_values = select_best_values(cluster_df)
        golden_record = pd.Series(best_values)
        golden_record['cluster_id'] = cluster_id

    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    # Ensure cluster_id is set
    golden_record['cluster_id'] = cluster_id

    return golden_record


def merge_cluster(
    cluster_df: pd.DataFrame,
    cluster_id: int,
    strategy: str = 'most_complete',
    preserve_all_locations: bool = True
) -> Tuple[pd.Series, pd.DataFrame]:
    """
    Merge a cluster into a single golden record, optionally preserving all locations.

    Args:
        cluster_df: DataFrame with all records in the cluster
        cluster_id: Cluster ID
        strategy: Merging strategy ('most_complete', 'first', 'best_values')
        preserve_all_locations: If True, return all original locations

    Returns:
        Tuple of (golden_record: Series, all_locations: DataFrame)

    Example:
        cluster_records = df[df['cluster_id'] == 100]
        golden, locations = merge_cluster(cluster_records, cluster_id=100)
        print(f"Golden record: {golden['name']}")
        print(f"All locations: {len(locations)}")
    """
    # Create golden record
    golden_record = create_golden_record(cluster_df, cluster_id, strategy)

    # Preserve all locations if requested
    if preserve_all_locations:
        all_locations = cluster_df.copy()
    else:
        all_locations = pd.DataFrame()

    logger.debug(f"Merged cluster {cluster_id}: {len(cluster_df)} records -> 1 golden record")

    return golden_record, all_locations


def merge_all_clusters(
    df: pd.DataFrame,
    strategy: str = 'most_complete',
    preserve_all_locations: bool = True
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merge all clusters into golden records.

    Args:
        df: Input DataFrame with cluster_id column
        strategy: Merging strategy ('most_complete', 'first', 'best_values')
        preserve_all_locations: If True, return all original locations

    Returns:
        Tuple of (golden_records: DataFrame, all_locations: DataFrame)

    Example:
        # After duplicate detection
        df = find_duplicates(df)

        # Merge all clusters
        golden_records, all_locations = merge_all_clusters(df, strategy='most_complete')

        print(f"Original: {len(df)} records")
        print(f"Golden: {len(golden_records)} unique businesses")
        print(f"All locations: {len(all_locations)} addresses preserved")
    """
    if 'cluster_id' not in df.columns:
        raise ValueError("DataFrame must have 'cluster_id' column")

    logger.info(f"Merging all clusters in {len(df)} records")

    golden_records = []
    all_locations_list = []

    # Process clusters
    clustered = df[df['cluster_id'] != -1]
    for cluster_id in clustered['cluster_id'].unique():
        cluster_df = df[df['cluster_id'] == cluster_id]

        golden_record, locations = merge_cluster(
            cluster_df,
            cluster_id,
            strategy,
            preserve_all_locations
        )

        golden_records.append(golden_record)

        if preserve_all_locations:
            all_locations_list.append(locations)

    # Add singleton records (not in any cluster)
    singletons = df[df['cluster_id'] == -1].copy()
    if len(singletons) > 0:
        golden_records.extend([row for _, row in singletons.iterrows()])

        if preserve_all_locations:
            all_locations_list.append(singletons)

    # Convert to DataFrames
    golden_df = pd.DataFrame(golden_records)
    all_locations_df = pd.concat(all_locations_list, ignore_index=True) if all_locations_list else pd.DataFrame()

    logger.info(f"Merge complete:")
    logger.info(f"  - {len(golden_df)} golden records (unique businesses)")
    logger.info(f"  - {len(all_locations_df)} total locations preserved")

    return golden_df, all_locations_df


def deduplicate_and_merge(
    df: pd.DataFrame,
    match_fields: List[str] = None,
    blocking_fields: List[str] = None,
    threshold: float = 0.85,
    merge_strategy: str = 'most_complete',
    preserve_all_locations: bool = True
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Complete deduplication and merging pipeline.

    Args:
        df: Input DataFrame
        match_fields: Fields to use for matching
        blocking_fields: Fields to use for blocking
        threshold: Similarity threshold
        merge_strategy: Merging strategy
        preserve_all_locations: Whether to preserve all locations

    Returns:
        Tuple of (golden_records, all_locations)

    Example:
        # One-liner deduplication and merging
        from skills.matching_skills import find_duplicates
        from skills.merge_skills import deduplicate_and_merge

        # Find duplicates
        df = find_duplicates(df, threshold=0.85)

        # Merge clusters
        golden_records, all_locations = merge_all_clusters(df)

        # Or do both in one call:
        golden, locations = deduplicate_and_merge(df, threshold=0.85)
    """
    logger.info("Starting complete deduplication and merge pipeline")

    # Import here to avoid circular dependency
    from skills.matching_skills import find_duplicates

    # Step 1: Find duplicates
    df_with_clusters = find_duplicates(df, match_fields, blocking_fields, threshold)

    # Step 2: Merge clusters
    golden_records, all_locations = merge_all_clusters(
        df_with_clusters,
        merge_strategy,
        preserve_all_locations
    )

    logger.info("Deduplication and merge pipeline complete")

    return golden_records, all_locations


def get_cluster_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get summary statistics for all clusters.

    Args:
        df: DataFrame with cluster_id column

    Returns:
        DataFrame with cluster statistics

    Example:
        summary = get_cluster_summary(df)
        print(summary.head())
        # cluster_id | record_count | has_phone | has_email | completeness_avg
    """
    if 'cluster_id' not in df.columns:
        raise ValueError("DataFrame must have 'cluster_id' column")

    clustered = df[df['cluster_id'] != -1].copy()

    if len(clustered) == 0:
        return pd.DataFrame()

    summary = clustered.groupby('cluster_id').agg({
        'cluster_id': 'count',  # Record count
    }).rename(columns={'cluster_id': 'record_count'})

    # Add completeness statistics
    clustered['_completeness'] = clustered.apply(calculate_completeness_score, axis=1)
    completeness_stats = clustered.groupby('cluster_id')['_completeness'].agg(['mean', 'max'])
    summary['completeness_avg'] = completeness_stats['mean']
    summary['completeness_max'] = completeness_stats['max']

    # Add data availability flags
    for field in ['phone', 'email', 'address']:
        if field in clustered.columns:
            has_field = clustered.groupby('cluster_id')[field].apply(
                lambda x: x.notna().any()
            )
            summary[f'has_{field}'] = has_field

    summary = summary.reset_index()
    summary = summary.sort_values('record_count', ascending=False)

    return summary
