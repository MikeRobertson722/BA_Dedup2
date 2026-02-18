"""
Hybrid Matching Agent - Combines fuzzy matching with AI-powered matching.
Uses fuzzy matching first for obvious duplicates (fast, free),
then applies AI matching to uncertain cases for higher accuracy.
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple
from agents.matching_agent import MatchingAgent
from agents.ai_matching_agent import AIMatchingAgent
from agents.base_agent import BaseAgent
from config import settings


class HybridMatchingAgent(BaseAgent):
    """
    Agent that combines fuzzy matching with AI matching for optimal accuracy and cost.

    Strategy:
    1. Use fuzzy matching on all records (fast, free)
    2. Identify "uncertain" matches (scores between thresholds)
    3. Use AI to re-evaluate uncertain cases
    4. Combine results for final clustering
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize hybrid matching agent.

        Args:
            config: Configuration dictionary with keys:
                - fuzzy_threshold_high: High confidence threshold (auto-accept)
                - fuzzy_threshold_low: Low confidence threshold (auto-reject)
                - ai_threshold: AI confidence threshold for matches
                - ai_enabled: Whether AI matching is enabled
                - use_ai_for_uncertain: Use AI only for uncertain cases (recommended)
        """
        super().__init__('hybrid_matching', config)

        # Fuzzy matching thresholds
        self.fuzzy_threshold_high = self.config.get('fuzzy_threshold_high', 0.95)  # Auto-merge only 95%+
        self.fuzzy_threshold_low = self.config.get('fuzzy_threshold_low', 0.75)

        # AI settings
        self.ai_threshold = self.config.get('ai_threshold', 0.80)
        self.ai_enabled = self.config.get('ai_enabled', settings.AI_MATCHING_ENABLED)
        self.use_ai_for_uncertain = self.config.get('use_ai_for_uncertain', True)

        # Initialize sub-agents
        self.fuzzy_agent = MatchingAgent({
            'similarity_threshold': self.fuzzy_threshold_low,  # Lower threshold to catch more candidates
            'match_fields': self.config.get('match_fields', settings.MATCH_FIELDS),
            'blocking_fields': self.config.get('blocking_fields', ['state'])  # Use state blocking (more inclusive)
        })

        if self.ai_enabled:
            try:
                self.ai_agent = AIMatchingAgent({
                    'api_key': self.config.get('api_key', settings.ANTHROPIC_API_KEY),
                    'model': self.config.get('ai_model', settings.AI_MODEL),
                    'similarity_threshold': self.ai_threshold,
                    'batch_size': self.config.get('ai_batch_size', 10)
                })
            except ValueError as e:
                self.logger.warning(f"AI matching disabled: {e}")
                self.ai_enabled = False
                self.ai_agent = None
        else:
            self.ai_agent = None
            self.logger.info("AI matching disabled in configuration")

        self.stats = {
            'total_pairs': 0,
            'high_confidence_pairs': 0,
            'uncertain_pairs': 0,
            'ai_analyzed_pairs': 0,
            'ai_confirmed_pairs': 0,
            'ai_rejected_pairs': 0,
            'low_confidence_pairs': 0
        }

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Find duplicates using hybrid fuzzy + AI approach.

        Args:
            data: Input DataFrame to analyze

        Returns:
            DataFrame with duplicate cluster IDs added
        """
        self.logger.info("=" * 80)
        self.logger.info("HYBRID MATCHING: Fuzzy + AI")
        self.logger.info("=" * 80)
        self.logger.info(f"Fuzzy thresholds: {self.fuzzy_threshold_low} - {self.fuzzy_threshold_high}")
        self.logger.info(f"AI threshold: {self.ai_threshold}")
        self.logger.info(f"AI enabled: {self.ai_enabled}")

        # Add record IDs if not present (use assign to avoid copy)
        if 'record_id' not in data.columns:
            df = data.assign(record_id=range(len(data)))
        else:
            df = data

        # Phase 1: Fuzzy Matching
        self.logger.info("\n--- Phase 1: Fuzzy Matching ---")
        df = self.fuzzy_agent.execute(df)

        # Get fuzzy matching results
        fuzzy_pairs = self._extract_fuzzy_pairs(df)
        self.stats['total_pairs'] = len(fuzzy_pairs)

        # Categorize pairs by confidence
        high_conf_pairs, uncertain_pairs, low_conf_pairs = self._categorize_pairs(fuzzy_pairs)

        self.stats['high_confidence_pairs'] = len(high_conf_pairs)
        self.stats['uncertain_pairs'] = len(uncertain_pairs)
        self.stats['low_confidence_pairs'] = len(low_conf_pairs)

        self.logger.info(f"Fuzzy results:")
        self.logger.info(f"  High confidence (≥{self.fuzzy_threshold_high}): {len(high_conf_pairs)} pairs")
        self.logger.info(f"  Uncertain ({self.fuzzy_threshold_low}-{self.fuzzy_threshold_high}): {len(uncertain_pairs)} pairs")
        self.logger.info(f"  Low confidence (<{self.fuzzy_threshold_low}): {len(low_conf_pairs)} pairs")

        # Phase 2: AI Matching for Uncertain Cases
        final_pairs = high_conf_pairs.copy()  # Start with high confidence matches

        if self.ai_enabled and len(uncertain_pairs) > 0:
            self.logger.info("\n--- Phase 2: AI Analysis of Uncertain Cases ---")
            self.logger.info(f"Analyzing {len(uncertain_pairs)} uncertain pairs with AI...")

            ai_pairs = self._ai_analyze_uncertain(df, uncertain_pairs)

            self.stats['ai_analyzed_pairs'] = len(uncertain_pairs)
            self.stats['ai_confirmed_pairs'] = len(ai_pairs)
            self.stats['ai_rejected_pairs'] = len(uncertain_pairs) - len(ai_pairs)

            self.logger.info(f"AI results:")
            self.logger.info(f"  Confirmed as duplicates: {len(ai_pairs)} pairs")
            self.logger.info(f"  Rejected as non-duplicates: {len(uncertain_pairs) - len(ai_pairs)} pairs")

            # Add AI-confirmed pairs to final results
            final_pairs.extend(ai_pairs)
        elif self.ai_enabled and len(uncertain_pairs) == 0:
            self.logger.info("\n--- Phase 2: Skipped (No uncertain cases) ---")
        elif not self.ai_enabled and len(uncertain_pairs) > 0:
            self.logger.info("\n--- Phase 2: Skipped (AI disabled) ---")
            self.logger.info(f"Note: {len(uncertain_pairs)} uncertain pairs not analyzed")
            # Optionally include uncertain pairs as matches
            if self.config.get('include_uncertain_without_ai', False):
                final_pairs.extend(uncertain_pairs)

        # Phase 3: Final Clustering
        self.logger.info("\n--- Phase 3: Final Clustering ---")
        df = self._cluster_duplicates(df, final_pairs)

        duplicate_count = (df['cluster_id'] != -1).sum()
        cluster_count = df[df['cluster_id'] != -1]['cluster_id'].nunique()

        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"HYBRID MATCHING COMPLETE")
        self.logger.info(f"{'='*80}")
        self.logger.info(f"Final results:")
        self.logger.info(f"  Total duplicate records: {duplicate_count}")
        self.logger.info(f"  Duplicate clusters: {cluster_count}")
        self.logger.info(f"  Unique records: {len(df) - duplicate_count}")

        # Add stats to dataframe as metadata
        df.attrs['matching_stats'] = self.stats

        return df

    def _extract_fuzzy_pairs(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Extract pair information from fuzzy matching results.

        Args:
            df: DataFrame with cluster_id and similarity_score from fuzzy matching

        Returns:
            List of pair dictionaries with scores
        """
        pairs = []

        # Group by cluster_id
        for cluster_id in df[df['cluster_id'] != -1]['cluster_id'].unique():
            cluster = df[df['cluster_id'] == cluster_id]

            # Get all pairs within this cluster
            indices = cluster.index.tolist()
            for i in range(len(indices)):
                for j in range(i + 1, len(indices)):
                    idx1, idx2 = indices[i], indices[j]

                    # Use the average similarity score from the records
                    score1 = cluster.loc[idx1, 'similarity_score'] if 'similarity_score' in cluster.columns else 0.85
                    score2 = cluster.loc[idx2, 'similarity_score'] if 'similarity_score' in cluster.columns else 0.85
                    avg_score = (score1 + score2) / 2

                    pairs.append({
                        'record_id_1': cluster.loc[idx1, 'record_id'],
                        'record_id_2': cluster.loc[idx2, 'record_id'],
                        'similarity_score': avg_score,
                        'idx1': idx1,
                        'idx2': idx2
                    })

        return pairs

    def _categorize_pairs(self, pairs: List[Dict[str, Any]]) -> Tuple[List, List, List]:
        """
        Categorize pairs into high confidence, uncertain, and low confidence.

        Args:
            pairs: List of pair dictionaries with similarity scores

        Returns:
            Tuple of (high_confidence, uncertain, low_confidence) pair lists
        """
        high_confidence = []
        uncertain = []
        low_confidence = []

        for pair in pairs:
            score = pair['similarity_score']

            if score >= self.fuzzy_threshold_high:
                high_confidence.append(pair)
            elif score >= self.fuzzy_threshold_low:
                uncertain.append(pair)
            else:
                low_confidence.append(pair)

        return high_confidence, uncertain, low_confidence

    def _ai_analyze_uncertain(self, df: pd.DataFrame,
                             uncertain_pairs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Use AI to analyze uncertain pairs.

        Args:
            df: Input DataFrame
            uncertain_pairs: List of uncertain pairs to analyze

        Returns:
            List of AI-confirmed duplicate pairs
        """
        if not self.ai_agent:
            return []

        # Prepare candidate pairs for AI (convert to tuple format)
        candidate_pairs = [(pair['idx1'], pair['idx2']) for pair in uncertain_pairs]

        # Analyze with AI
        ai_pairs = self.ai_agent._ai_analyze_pairs(df, candidate_pairs)

        return ai_pairs

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
        from collections import defaultdict

        # Reset cluster info
        df['cluster_id'] = -1
        df['similarity_score'] = 0.0
        df['match_method'] = 'unique'  # Track which method found the match

        if not duplicate_pairs:
            return df

        # Build adjacency list
        graph = defaultdict(set)
        pair_info = {}  # Store info about each pair

        for pair in duplicate_pairs:
            id1 = pair['record_id_1']
            id2 = pair['record_id_2']
            graph[id1].add(id2)
            graph[id2].add(id1)

            # Store pair info
            pair_key = tuple(sorted([id1, id2]))
            pair_info[pair_key] = pair

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

        # Store match info
        for pair in duplicate_pairs:
            id1_mask = df['record_id'] == pair['record_id_1']
            id2_mask = df['record_id'] == pair['record_id_2']

            # Update similarity scores and method
            current_score1 = df.loc[id1_mask, 'similarity_score'].values[0]
            if pair['similarity_score'] > current_score1:
                df.loc[id1_mask, 'similarity_score'] = pair['similarity_score']
                method = 'ai' if 'ai_reasoning' in pair else 'fuzzy'
                df.loc[id1_mask, 'match_method'] = method

            current_score2 = df.loc[id2_mask, 'similarity_score'].values[0]
            if pair['similarity_score'] > current_score2:
                df.loc[id2_mask, 'similarity_score'] = pair['similarity_score']
                method = 'ai' if 'ai_reasoning' in pair else 'fuzzy'
                df.loc[id2_mask, 'match_method'] = method

        return df

    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about hybrid matching performance.

        Returns:
            Dictionary with matching statistics
        """
        return self.stats

    def validate(self, result: pd.DataFrame) -> bool:
        """
        Validate hybrid matching results.

        Args:
            result: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        if not super().validate(result):
            return False

        if 'cluster_id' not in result.columns:
            self.logger.error("cluster_id column not found in result")
            return False

        return True
