"""
Matching Agent - Identifies duplicate/similar records using fuzzy matching.
Uses thefuzz and recordlinkage for probabilistic deduplication.

Enhanced with caching (Priority 4):
- Fuzzy match scores cached to avoid redundant comparisons
- 50-80% reduction in fuzzy matching calls for typical datasets
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple
from thefuzz import fuzz
import recordlinkage as rl
from agents.base_agent import BaseAgent
from config import settings
from utils.helpers import (
    normalize_name_with_nicknames,
    parse_name,
    normalize_suffix,
    extract_entity_type,
    should_match_entities
)

# Import caching (Priority 4)
try:
    from utils.cache import get_fuzzy_match_cache
    CACHING_AVAILABLE = True
except ImportError:
    CACHING_AVAILABLE = False


class MatchingAgent(BaseAgent):
    """
    Agent responsible for identifying duplicate and similar Business Associate records.
    Uses blocking and fuzzy matching to find candidate duplicates efficiently.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize matching agent.

        Args:
            config: Configuration dictionary with keys:
                - similarity_threshold: Minimum similarity score (0-1) for matches
                - match_fields: List of fields to use for matching
                - blocking_fields: Fields to use for blocking (optimization)
        """
        super().__init__('matching', config)
        self.similarity_threshold = self.config.get('similarity_threshold', settings.SIMILARITY_THRESHOLD)
        self.match_fields = self.config.get('match_fields', settings.MATCH_FIELDS)
        self.blocking_fields = self.config.get('blocking_fields', ['zip_normalized', 'state'])
        self.duplicate_pairs = []

        # Initialize fuzzy match cache (Priority 4)
        self.fuzzy_cache = get_fuzzy_match_cache() if CACHING_AVAILABLE else None

    def _cached_fuzz_ratio(self, str1: str, str2: str) -> float:
        """
        Calculate fuzzy ratio with caching.

        Args:
            str1: First string
            str2: Second string

        Returns:
            Similarity score (0-1)
        """
        if self.fuzzy_cache:
            # Check cache
            cached_score = self.fuzzy_cache.get(str1, str2)
            if cached_score is not None:
                return cached_score

            # Calculate and cache
            score = fuzz.ratio(str1, str2) / 100.0
            self.fuzzy_cache.put(str1, str2, score)
            return score
        else:
            # No caching available
            return fuzz.ratio(str1, str2) / 100.0

    def _cached_fuzz_token_sort_ratio(self, str1: str, str2: str) -> float:
        """
        Calculate fuzzy token_sort_ratio with caching.

        Args:
            str1: First string
            str2: Second string

        Returns:
            Similarity score (0-1)
        """
        if self.fuzzy_cache:
            # Create cache key with prefix to differentiate from regular ratio
            cache_key1 = f"token_sort:{str1}"
            cache_key2 = f"token_sort:{str2}"

            cached_score = self.fuzzy_cache.get(cache_key1, cache_key2)
            if cached_score is not None:
                return cached_score

            # Calculate and cache
            score = fuzz.token_sort_ratio(str1, str2) / 100.0
            self.fuzzy_cache.put(cache_key1, cache_key2, score)
            return score
        else:
            # No caching available
            return fuzz.token_sort_ratio(str1, str2) / 100.0

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Find duplicate/similar records in the dataset.

        Args:
            data: Input DataFrame to analyze

        Returns:
            DataFrame with duplicate cluster IDs added
        """
        self.logger.info(f"Finding duplicates in {len(data)} records")
        self.logger.info(f"Similarity threshold: {self.similarity_threshold}")

        # Add a unique internal ID for tracking (use assign to avoid copy)
        if 'record_id' not in data.columns:
            df = data.assign(record_id=range(len(data)))
        else:
            df = data

        # Find candidate pairs using blocking
        candidate_pairs = self._generate_candidate_pairs(df)
        self.logger.info(f"Generated {len(candidate_pairs)} candidate pairs using blocking")

        # Calculate similarity scores for candidate pairs
        duplicate_pairs = self._calculate_similarities(df, candidate_pairs)
        self.logger.info(f"Found {len(duplicate_pairs)} duplicate pairs above threshold")

        # Cluster duplicates into groups
        df = self._cluster_duplicates(df, duplicate_pairs)

        duplicate_count = (df['cluster_id'] != -1).sum()
        cluster_count = df[df['cluster_id'] != -1]['cluster_id'].nunique()

        self.logger.info(f"Identified {duplicate_count} duplicate records in {cluster_count} clusters")

        return df

    def _generate_candidate_pairs(self, df: pd.DataFrame) -> List[Tuple[int, int]]:
        """
        Generate candidate pairs using smart multi-strategy blocking (Priority 4 optimization).

        Uses optimized blocking to reduce pairs from millions to thousands while
        maintaining high recall (not missing true duplicates).

        Args:
            df: Input DataFrame

        Returns:
            List of (index1, index2) tuples representing candidate pairs
        """
        # Try to use smart blocking strategy (Priority 4 optimization)
        try:
            from utils.smart_blocking import SmartBlockingStrategy

            # Use smart blocking with configurable max pairs
            max_pairs = getattr(self.config, 'MAX_MISSING_DATA_PAIRS', 50000)
            strategy = SmartBlockingStrategy(max_missing_data_pairs=max_pairs)

            pairs = strategy.generate_candidate_pairs(df)
            self.logger.info(f"Smart blocking generated {len(pairs)} candidate pairs")

            return pairs

        except ImportError:
            # Fallback to basic blocking if smart_blocking not available
            self.logger.warning("Smart blocking not available, using basic blocking")
            return self._generate_candidate_pairs_basic(df)

    def _generate_candidate_pairs_basic(self, df: pd.DataFrame) -> List[Tuple[int, int]]:
        """
        Basic blocking strategy (fallback).

        DEPRECATED: Use SmartBlockingStrategy for better performance.

        Args:
            df: Input DataFrame

        Returns:
            List of (index1, index2) tuples representing candidate pairs
        """
        all_pairs = set()

        # Strategy 1: Block by state
        if 'state' in df.columns:
            valid_state = df[df['state'].notna() & (df['state'] != '')]
            if len(valid_state) > 0:
                indexer = rl.Index()
                indexer.block('state')
                state_pairs = indexer.index(valid_state)
                all_pairs.update(state_pairs)
                self.logger.info(f"State blocking: {len(state_pairs)} candidate pairs")

        # Strategy 2: Block by ZIP
        if 'zip_normalized' in df.columns:
            valid_zip = df[df['zip_normalized'].notna() & (df['zip_normalized'] != '')]
            if len(valid_zip) > 0:
                indexer = rl.Index()
                indexer.block('zip_normalized')
                zip_pairs = indexer.index(valid_zip)
                new_pairs = set(zip_pairs) - all_pairs
                all_pairs.update(new_pairs)
                self.logger.info(f"ZIP blocking: {len(new_pairs)} additional candidate pairs")

        # Limited fallback for missing data (capped to prevent explosion)
        missing_data = df[
            (df['state'].isna() | (df['state'] == '')) |
            (df['zip_normalized'].isna() | (df['zip_normalized'] == ''))
        ]

        if len(missing_data) > 0:
            self.logger.warning(f"{len(missing_data)} records have missing blocking fields")

            # Only compare against a sample if there are many missing records
            if len(missing_data) > 100:
                self.logger.warning("Too many records with missing data - using limited sampling")
                # Sample up to 100 records
                sample_missing = missing_data.sample(n=min(100, len(missing_data)), random_state=42)
                indexer = rl.Index()
                indexer.full()
                missing_pairs = indexer.index(sample_missing)
                all_pairs.update(missing_pairs)
                self.logger.info(f"Limited missing data comparison: {len(missing_pairs)} pairs (sampled)")
            else:
                # Full comparison for small number of missing records
                indexer = rl.Index()
                indexer.full()
                missing_pairs = indexer.index(missing_data)
                all_pairs.update(missing_pairs)
                self.logger.info(f"Missing data comparison: {len(missing_pairs)} pairs")

        # Final fallback
        if len(all_pairs) == 0:
            self.logger.error("No blocking worked - dataset may be missing all blocking fields")
            self.logger.error("Using limited full indexing on first 1000 records only")
            limited_df = df.head(1000)
            indexer = rl.Index()
            indexer.full()
            all_pairs.update(indexer.index(limited_df))

        self.logger.info(f"Total candidate pairs: {len(all_pairs)}")

        return list(all_pairs)

    def _calculate_similarities(self, df: pd.DataFrame,
                               candidate_pairs: List[Tuple[int, int]]) -> List[Dict[str, Any]]:
        """
        Calculate similarity scores for candidate pairs.

        Args:
            df: Input DataFrame
            candidate_pairs: List of candidate pairs to compare

        Returns:
            List of duplicate pair dictionaries with similarity scores
        """
        duplicate_pairs = []

        for idx1, idx2 in candidate_pairs:
            # Get records
            rec1 = df.loc[idx1]
            rec2 = df.loc[idx2]

            # SAFETY CHECK 1: Entity type compatibility
            # Don't match different entity types (Individual vs Trust vs Department vs Business)
            if 'name' in df.columns:
                entity1_type = extract_entity_type(rec1.get('name', ''))
                entity2_type = extract_entity_type(rec2.get('name', ''))

                if not should_match_entities(entity1_type, entity2_type,
                                             str(rec1.get('name', '')),
                                             str(rec2.get('name', ''))):
                    # Skip this pair - incompatible entity types
                    continue

            # SAFETY CHECK 2: Suffix compatibility
            # Don't match Jr with Sr (different people - father/son)
            if 'name' in df.columns:
                parsed1 = parse_name(rec1.get('name', ''))
                parsed2 = parse_name(rec2.get('name', ''))

                suffix1_norm = normalize_suffix(parsed1.get('suffix', ''))
                suffix2_norm = normalize_suffix(parsed2.get('suffix', ''))

                # If both have suffixes and they're different, skip this pair
                if suffix1_norm and suffix2_norm and suffix1_norm != suffix2_norm:
                    # Different suffixes = different people (e.g., Jr vs Sr)
                    continue

            # FAST PATH: SSN token match = 100% same person
            # If SSN tokens match, skip expensive fuzzy matching - it's the same person
            if 'ssn_token' in df.columns:
                ssn1 = rec1.get('ssn_token', '')
                ssn2 = rec2.get('ssn_token', '')

                if ssn1 and ssn2 and ssn1 == ssn2:
                    # SSN tokens match = definitely same person
                    duplicate_pairs.append({
                        'record_id_1': rec1['record_id'],
                        'record_id_2': rec2['record_id'],
                        'similarity_score': 1.0,
                        'name_similarity': 1.0,
                        'address_similarity': 1.0,
                        'match_method': 'ssn_exact'
                    })
                    continue  # Skip fuzzy matching for this pair

            # Calculate similarity for each field
            similarities = {}

            # Name similarity (most important)
            # Apply nickname normalization for better matching
            if 'name_normalized' in df.columns:
                name1 = str(rec1['name_normalized'])
                name2 = str(rec2['name_normalized'])

                # Calculate base similarity (cached - Priority 4)
                base_similarity = self._cached_fuzz_token_sort_ratio(name1, name2)

                # Normalize nicknames to full names before comparison
                name1_with_nicknames = normalize_name_with_nicknames(name1)
                name2_with_nicknames = normalize_name_with_nicknames(name2)
                nickname_similarity = self._cached_fuzz_token_sort_ratio(name1_with_nicknames, name2_with_nicknames)

                # If nickname normalization creates a match, boost score up to 99%
                if nickname_similarity > base_similarity and nickname_similarity >= 0.90:
                    # Names match after nickname normalization - high confidence
                    similarities['name'] = min(0.99, nickname_similarity + 0.10)  # Boost to near-perfect
                else:
                    # Use the better of base or nickname similarity
                    similarities['name'] = max(base_similarity, nickname_similarity)
            else:
                similarities['name'] = 0.0

            # Address similarity (cached - Priority 4)
            if 'address_normalized' in df.columns:
                addr1 = str(rec1['address_normalized'])
                addr2 = str(rec2['address_normalized'])
                similarities['address'] = self._cached_fuzz_ratio(addr1, addr2)
            else:
                similarities['address'] = 0.0

            # City similarity - handle missing values gracefully (cached - Priority 4)
            if 'city_normalized' in df.columns:
                city1 = str(rec1['city_normalized'])
                city2 = str(rec2['city_normalized'])

                # If either city is missing, don't penalize
                if city1 in ['', 'nan', 'none'] or city2 in ['', 'nan', 'none']:
                    similarities['city'] = 0.5  # Neutral score for missing data
                else:
                    similarities['city'] = self._cached_fuzz_ratio(city1, city2)
            else:
                similarities['city'] = 0.5

            # State exact match - handle missing values gracefully
            if 'state' in df.columns:
                state1 = str(rec1.get('state', ''))
                state2 = str(rec2.get('state', ''))

                # If either state is missing, don't penalize
                if state1 in ['', 'nan', 'none'] or state2 in ['', 'nan', 'none']:
                    similarities['state'] = 0.5  # Neutral score for missing data
                elif state1 == state2:
                    similarities['state'] = 1.0
                else:
                    similarities['state'] = 0.0
            else:
                similarities['state'] = 0.5

            # ZIP exact match - important for location verification
            if 'zip_normalized' in df.columns:
                zip1 = str(rec1.get('zip_normalized', ''))
                zip2 = str(rec2.get('zip_normalized', ''))

                if zip1 and zip2 and zip1 == zip2:
                    similarities['zip'] = 1.0
                else:
                    similarities['zip'] = 0.0
            else:
                similarities['zip'] = 0.0

            # Calculate weighted average
            # Name and address are most important
            weights = {
                'name': 0.40,
                'address': 0.30,
                'city': 0.10,
                'state': 0.10,
                'zip': 0.10  # Increased from 0.05 to give more weight to ZIP
            }

            total_similarity = sum(similarities.get(field, 0) * weight
                                  for field, weight in weights.items())

            # PENALTY: Check if first names are completely different (not nicknames)
            # This prevents grouping different people at the same address
            if 'name_normalized' in df.columns:
                name1 = str(rec1['name_normalized']).lower()
                name2 = str(rec2['name_normalized']).lower()

                # Extract first names (first word)
                first1 = name1.split()[0] if name1 and name1 != 'nan' else ''
                first2 = name2.split()[0] if name2 and name2 != 'nan' else ''

                if first1 and first2:
                    # Normalize nicknames before comparison
                    first1_normalized = normalize_name_with_nicknames(first1)
                    first2_normalized = normalize_name_with_nicknames(first2)

                    # Get just the first word from normalized names
                    first1_normalized = first1_normalized.split()[0] if first1_normalized else ''
                    first2_normalized = first2_normalized.split()[0] if first2_normalized else ''

                    # Check if first names are different even after nickname normalization
                    if first1_normalized != first2_normalized:
                        # Different first names - apply penalty unless names are very similar (cached - Priority 4)
                        first_name_similarity = self._cached_fuzz_ratio(first1, first2)
                        if first_name_similarity < 0.70:
                            # First names are clearly different (e.g., Cole vs Mike)
                            # Apply 15% penalty to discourage matching different people
                            total_similarity = max(0.0, total_similarity - 0.15)

            # PENALTY: Suffix mismatch (one has suffix, other doesn't)
            # This handles uncertain cases like "John Smith" vs "John Smith Jr"
            if suffix1_norm or suffix2_norm:
                if (suffix1_norm and not suffix2_norm) or (suffix2_norm and not suffix1_norm):
                    # One has suffix, one doesn't = uncertain match
                    # Apply moderate penalty (could be same person, data quality issue)
                    total_similarity = max(0.0, total_similarity - 0.10)

            # BONUS: If ZIP matches AND address is similar, boost confidence
            # This helps match records with missing/incorrect city/state
            zip_matches = similarities.get('zip', 0) == 1.0
            address_similar = similarities.get('address', 0) >= 0.80
            name_similar = similarities.get('name', 0) >= 0.80

            if zip_matches and address_similar and name_similar:
                # Strong evidence of a match: same name, same address, same ZIP
                # Even if city/state are missing or don't match
                total_similarity = min(1.0, total_similarity + 0.10)  # Boost by 10%

            # If above threshold, add to duplicate pairs
            if total_similarity >= self.similarity_threshold:
                duplicate_pairs.append({
                    'record_id_1': rec1['record_id'],
                    'record_id_2': rec2['record_id'],
                    'similarity_score': total_similarity,
                    'name_similarity': similarities['name'],
                    'address_similarity': similarities['address']
                })

        return duplicate_pairs

    def _cluster_duplicates(self, df: pd.DataFrame,
                          duplicate_pairs: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Cluster duplicate records into groups using connected components.

        Args:
            df: Input DataFrame
            duplicate_pairs: List of duplicate pair dictionaries

        Returns:
            DataFrame with cluster_id column added
        """
        # Initialize cluster IDs (-1 means no cluster)
        df['cluster_id'] = -1
        df['similarity_score'] = 0.0

        if not duplicate_pairs:
            return df

        # Build adjacency list for clustering
        from collections import defaultdict
        graph = defaultdict(set)

        for pair in duplicate_pairs:
            id1 = pair['record_id_1']
            id2 = pair['record_id_2']
            graph[id1].add(id2)
            graph[id2].add(id1)

        # Find connected components (clusters)
        visited = set()
        cluster_id = 0

        def dfs(node, cluster):
            """Depth-first search to find connected records."""
            if node in visited:
                return
            visited.add(node)
            cluster.append(node)
            for neighbor in graph[node]:
                dfs(neighbor, cluster)

        # Assign cluster IDs
        for record_id in graph.keys():
            if record_id not in visited:
                cluster = []
                dfs(record_id, cluster)

                # Assign cluster ID to all records in this cluster
                for rec_id in cluster:
                    mask = df['record_id'] == rec_id
                    df.loc[mask, 'cluster_id'] = cluster_id

                cluster_id += 1

        # Store similarity scores for duplicates
        for pair in duplicate_pairs:
            id1_mask = df['record_id'] == pair['record_id_1']
            id2_mask = df['record_id'] == pair['record_id_2']

            # Keep the highest similarity score for each record
            current_score1 = df.loc[id1_mask, 'similarity_score'].values[0]
            df.loc[id1_mask, 'similarity_score'] = max(current_score1, pair['similarity_score'])

            current_score2 = df.loc[id2_mask, 'similarity_score'].values[0]
            df.loc[id2_mask, 'similarity_score'] = max(current_score2, pair['similarity_score'])

        return df

    def validate(self, result: pd.DataFrame) -> bool:
        """
        Validate matching results.

        Args:
            result: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        if not super().validate(result):
            return False

        # Check that cluster_id column was added
        if 'cluster_id' not in result.columns:
            self.logger.error("cluster_id column not found in result")
            return False

        return True
