"""
Matching Skills - Reusable fuzzy matching and clustering functions.
Extracted from MatchingAgent for standalone use.
"""
import pandas as pd
import numpy as np
from typing import List, Tuple, Dict
from thefuzz import fuzz
from utils.logger import get_logger
from utils.smart_blocking import SmartBlockingStrategy

logger = get_logger(__name__)


def fuzzy_match_names(name1: str, name2: str, threshold: float = 0.85) -> Tuple[bool, float]:
    """
    Compare two names using fuzzy matching.

    Args:
        name1: First name to compare
        name2: Second name to compare
        threshold: Minimum similarity threshold (0-1)

    Returns:
        Tuple of (is_match: bool, similarity_score: float)

    Example:
        is_match, score = fuzzy_match_names("John Smith", "Jon Smith", threshold=0.85)
        print(f"Match: {is_match}, Score: {score:.2f}")
    """
    if not name1 or not name2:
        return False, 0.0

    score = fuzz.token_sort_ratio(str(name1), str(name2)) / 100.0
    is_match = score >= threshold

    return is_match, score


def fuzzy_match_addresses(addr1: str, addr2: str, threshold: float = 0.80) -> Tuple[bool, float]:
    """
    Compare two addresses using fuzzy matching.

    Args:
        addr1: First address to compare
        addr2: Second address to compare
        threshold: Minimum similarity threshold (0-1)

    Returns:
        Tuple of (is_match: bool, similarity_score: float)

    Example:
        is_match, score = fuzzy_match_addresses(
            "123 Main St",
            "123 Main Street",
            threshold=0.80
        )
    """
    if not addr1 or not addr2:
        return False, 0.0

    score = fuzz.token_sort_ratio(str(addr1), str(addr2)) / 100.0
    is_match = score >= threshold

    return is_match, score


def apply_blocking_strategy(
    df: pd.DataFrame,
    blocking_fields: List[str] = None,
    max_pairs: int = 50000
) -> List[Tuple[int, int]]:
    """
    Generate candidate pairs using smart blocking strategy.

    Args:
        df: Input DataFrame with records
        blocking_fields: Fields to use for blocking (default: ['state', 'zip_normalized'])
        max_pairs: Maximum number of pairs to generate

    Returns:
        List of (index1, index2) candidate pairs

    Example:
        pairs = apply_blocking_strategy(df, blocking_fields=['state', 'zip'], max_pairs=100000)
        print(f"Generated {len(pairs)} candidate pairs")
    """
    if blocking_fields is None:
        blocking_fields = ['state', 'zip_normalized']

    logger.info(f"Applying blocking strategy on fields: {blocking_fields}")

    strategy = SmartBlockingStrategy(max_missing_data_pairs=max_pairs)
    candidate_pairs = strategy.generate_candidate_pairs(df)

    logger.info(f"Generated {len(candidate_pairs)} candidate pairs")

    return candidate_pairs


def generate_candidate_pairs(
    df: pd.DataFrame,
    blocking_fields: List[str] = None,
    max_pairs: int = 50000
) -> List[Tuple[int, int]]:
    """
    Alias for apply_blocking_strategy for backward compatibility.

    Args:
        df: Input DataFrame
        blocking_fields: Fields to use for blocking
        max_pairs: Maximum pairs to generate

    Returns:
        List of candidate pairs

    Example:
        pairs = generate_candidate_pairs(df, blocking_fields=['state', 'city'])
    """
    return apply_blocking_strategy(df, blocking_fields, max_pairs)


def calculate_similarity_scores(
    df: pd.DataFrame,
    candidate_pairs: List[Tuple[int, int]],
    match_fields: List[str] = None,
    threshold: float = 0.85
) -> List[Tuple[int, int, float]]:
    """
    Calculate similarity scores for candidate pairs.

    Args:
        df: Input DataFrame
        candidate_pairs: List of (index1, index2) pairs to compare
        match_fields: Fields to use for matching (default: ['name_normalized', 'address_normalized'])
        threshold: Minimum similarity threshold

    Returns:
        List of (index1, index2, score) for pairs above threshold

    Example:
        pairs = generate_candidate_pairs(df)
        matches = calculate_similarity_scores(df, pairs, threshold=0.85)
        print(f"Found {len(matches)} matching pairs")
    """
    if match_fields is None:
        match_fields = ['name_normalized', 'address_normalized']

    logger.info(f"Calculating similarity scores for {len(candidate_pairs)} pairs")
    logger.info(f"Match fields: {match_fields}, Threshold: {threshold}")

    matches = []

    for i, (idx1, idx2) in enumerate(candidate_pairs):
        if i % 10000 == 0 and i > 0:
            logger.debug(f"Processed {i}/{len(candidate_pairs)} pairs...")

        scores = []

        # Calculate similarity for each match field
        for field in match_fields:
            if field not in df.columns:
                continue

            val1 = df.loc[idx1, field]
            val2 = df.loc[idx2, field]

            if pd.isna(val1) or pd.isna(val2) or str(val1).strip() == '' or str(val2).strip() == '':
                continue

            # Use token_sort_ratio for fuzzy matching
            score = fuzz.token_sort_ratio(str(val1), str(val2)) / 100.0
            scores.append(score)

        # Calculate average score across all fields
        if scores:
            avg_score = sum(scores) / len(scores)

            if avg_score >= threshold:
                matches.append((idx1, idx2, avg_score))

    logger.info(f"Found {len(matches)} matching pairs above threshold {threshold}")

    return matches


def cluster_duplicates(df: pd.DataFrame, duplicate_pairs: List[Tuple[int, int]]) -> pd.DataFrame:
    """
    Cluster duplicate pairs into groups using Union-Find algorithm.

    Args:
        df: Input DataFrame
        duplicate_pairs: List of (index1, index2) duplicate pairs

    Returns:
        DataFrame with 'cluster_id' column added (-1 for singletons)

    Example:
        matches = calculate_similarity_scores(df, pairs)
        pairs_only = [(idx1, idx2) for idx1, idx2, score in matches]
        df = cluster_duplicates(df, pairs_only)
        print(f"Created {df['cluster_id'].nunique()} clusters")
    """
    logger.info(f"Clustering {len(duplicate_pairs)} duplicate pairs")

    # Union-Find data structure
    class UnionFind:
        def __init__(self):
            self.parent = {}

        def find(self, x):
            if x not in self.parent:
                self.parent[x] = x
            if self.parent[x] != x:
                self.parent[x] = self.find(self.parent[x])
            return self.parent[x]

        def union(self, x, y):
            px, py = self.find(x), self.find(y)
            if px != py:
                self.parent[px] = py

    # Build clusters
    uf = UnionFind()
    for idx1, idx2 in duplicate_pairs:
        uf.union(idx1, idx2)

    # Assign cluster IDs
    df['cluster_id'] = df.index.map(lambda x: uf.find(x) if x in uf.parent else -1)

    # Count clusters and duplicates
    duplicate_count = (df['cluster_id'] != -1).sum()
    cluster_count = df[df['cluster_id'] != -1]['cluster_id'].nunique()

    logger.info(f"Created {cluster_count} clusters containing {duplicate_count} duplicate records")

    return df


def find_duplicates(
    df: pd.DataFrame,
    match_fields: List[str] = None,
    blocking_fields: List[str] = None,
    threshold: float = 0.85,
    max_pairs: int = 50000
) -> pd.DataFrame:
    """
    Complete duplicate detection pipeline.

    Args:
        df: Input DataFrame
        match_fields: Fields to use for matching
        blocking_fields: Fields to use for blocking
        threshold: Similarity threshold
        max_pairs: Maximum candidate pairs

    Returns:
        DataFrame with cluster_id column added

    Example:
        # Simple duplicate detection
        df = find_duplicates(df, threshold=0.85)

        # With custom fields
        df = find_duplicates(
            df,
            match_fields=['name_normalized', 'phone_normalized'],
            blocking_fields=['state', 'zip_normalized'],
            threshold=0.90
        )
    """
    logger.info(f"Starting duplicate detection on {len(df)} records")

    # Step 1: Generate candidate pairs
    candidate_pairs = generate_candidate_pairs(df, blocking_fields, max_pairs)

    # Step 2: Calculate similarity scores
    matches = calculate_similarity_scores(df, candidate_pairs, match_fields, threshold)

    # Extract pairs without scores
    duplicate_pairs = [(idx1, idx2) for idx1, idx2, score in matches]

    # Step 3: Cluster duplicates
    df = cluster_duplicates(df, duplicate_pairs)

    # Summary
    duplicate_count = (df['cluster_id'] != -1).sum()
    cluster_count = df[df['cluster_id'] != -1]['cluster_id'].nunique()

    logger.info(f"Duplicate detection complete:")
    logger.info(f"  - {duplicate_count} duplicate records")
    logger.info(f"  - {cluster_count} clusters")
    logger.info(f"  - {len(df) - duplicate_count} unique records")

    return df


def get_cluster_records(df: pd.DataFrame, cluster_id: int) -> pd.DataFrame:
    """
    Get all records belonging to a specific cluster.

    Args:
        df: DataFrame with cluster_id column
        cluster_id: Cluster ID to retrieve

    Returns:
        DataFrame with all records in the cluster

    Example:
        # Get all records in cluster 100
        cluster_records = get_cluster_records(df, cluster_id=100)
        print(f"Cluster contains {len(cluster_records)} records")
    """
    if 'cluster_id' not in df.columns:
        raise ValueError("DataFrame must have 'cluster_id' column")

    cluster_records = df[df['cluster_id'] == cluster_id].copy()

    return cluster_records


def get_all_clusters(df: pd.DataFrame) -> Dict[int, pd.DataFrame]:
    """
    Get all clusters as a dictionary.

    Args:
        df: DataFrame with cluster_id column

    Returns:
        Dictionary mapping cluster_id to DataFrame of records

    Example:
        clusters = get_all_clusters(df)
        for cluster_id, records in clusters.items():
            print(f"Cluster {cluster_id}: {len(records)} records")
    """
    if 'cluster_id' not in df.columns:
        raise ValueError("DataFrame must have 'cluster_id' column")

    clusters = {}
    for cluster_id in df[df['cluster_id'] != -1]['cluster_id'].unique():
        clusters[cluster_id] = get_cluster_records(df, cluster_id)

    logger.info(f"Retrieved {len(clusters)} clusters")

    return clusters
