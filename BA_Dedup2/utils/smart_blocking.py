"""
Smart Blocking Strategies for BA Deduplication.

Optimized blocking that reduces candidate pairs from millions to thousands
while maintaining high recall (not missing true duplicates).

Key improvements:
- Multiple overlapping blocking keys (state, ZIP, city, phone prefix, name tokens)
- Phonetic blocking for name variations
- Limited fallback for missing data
- Configurable max pairs to prevent explosions
"""
import pandas as pd
import recordlinkage as rl
from typing import List, Tuple, Set
from utils.logger import get_logger

logger = get_logger(__name__)


class SmartBlockingStrategy:
    """
    Multi-strategy blocking approach for efficient duplicate detection.

    Uses cascading strategies with increasing coverage but decreasing precision:
    1. SSN token (exact matches)
    2. State blocking (geographic)
    3. ZIP blocking (more specific geographic)
    4. City blocking (for records missing state/ZIP)
    5. Phone prefix blocking (area code + exchange)
    6. Name token blocking (first 3 words)
    7. Limited full comparison (last resort, capped at max_pairs)
    """

    def __init__(self, max_missing_data_pairs: int = 50000):
        """
        Initialize smart blocking strategy.

        Args:
            max_missing_data_pairs: Maximum pairs to generate for missing data fallback
        """
        self.max_missing_data_pairs = max_missing_data_pairs

    def generate_candidate_pairs(self, df: pd.DataFrame) -> List[Tuple[int, int]]:
        """
        Generate candidate pairs using smart multi-strategy blocking.

        Args:
            df: Input DataFrame

        Returns:
            List of (index1, index2) tuples representing candidate pairs
        """
        all_pairs = set()

        # STRATEGY 1: SSN Token Blocking (if available)
        pairs = self._block_by_ssn_token(df)
        if pairs:
            all_pairs.update(pairs)
            logger.info(f"SSN token blocking: {len(pairs)} candidate pairs")

        # STRATEGY 2: State Blocking
        pairs = self._block_by_state(df)
        if pairs:
            new_pairs = set(pairs) - all_pairs
            all_pairs.update(new_pairs)
            logger.info(f"State blocking: {len(new_pairs)} additional candidate pairs")

        # STRATEGY 3: ZIP Blocking
        pairs = self._block_by_zip(df)
        if pairs:
            new_pairs = set(pairs) - all_pairs
            all_pairs.update(new_pairs)
            logger.info(f"ZIP blocking: {len(new_pairs)} additional candidate pairs")

        # STRATEGY 4: City Blocking (for records missing state/ZIP)
        pairs = self._block_by_city(df)
        if pairs:
            new_pairs = set(pairs) - all_pairs
            all_pairs.update(new_pairs)
            logger.info(f"City blocking: {len(new_pairs)} additional candidate pairs")

        # STRATEGY 5: Phone Prefix Blocking (area code + exchange)
        pairs = self._block_by_phone_prefix(df)
        if pairs:
            new_pairs = set(pairs) - all_pairs
            all_pairs.update(new_pairs)
            logger.info(f"Phone prefix blocking: {len(new_pairs)} additional candidate pairs")

        # STRATEGY 6: Name Token Blocking (first significant word)
        # DISABLED BY DEFAULT - generates too many pairs (can add 1M+ pairs)
        # Only enable if geographic blocking is insufficient
        # pairs = self._block_by_name_token(df)
        # if pairs:
        #     new_pairs = set(pairs) - all_pairs
        #     all_pairs.update(new_pairs)
        #     logger.info(f"Name token blocking: {len(new_pairs)} additional candidate pairs")

        # STRATEGY 7: Limited Missing Data Fallback
        pairs = self._limited_missing_data_fallback(df, all_pairs)
        if pairs:
            new_pairs = set(pairs) - all_pairs
            all_pairs.update(new_pairs)
            logger.info(f"Limited missing data fallback: {len(new_pairs)} additional candidate pairs")

        logger.info(f"Total candidate pairs: {len(all_pairs)}")
        return list(all_pairs)

    def _block_by_ssn_token(self, df: pd.DataFrame) -> Set[Tuple[int, int]]:
        """Block by SSN token (exact matches only)."""
        if 'ssn_token' not in df.columns:
            return set()

        valid_ssn = df[df['ssn_token'].notna() & (df['ssn_token'] != '')]
        if len(valid_ssn) == 0:
            return set()

        indexer = rl.Index()
        indexer.block('ssn_token')
        return set(indexer.index(valid_ssn))

    def _block_by_state(self, df: pd.DataFrame) -> Set[Tuple[int, int]]:
        """
        Block by state + ZIP combination (much more restrictive).

        Prevents generating millions of pairs for states with many records.
        Falls back to state+city if ZIP not available.
        """
        if 'state' not in df.columns:
            return set()

        # PRIORITY: Use state+ZIP combination (most restrictive)
        zip_col = 'zip_normalized' if 'zip_normalized' in df.columns else 'zip'
        if zip_col in df.columns:
            valid_data = df[
                (df['state'].notna()) & (df['state'] != '') &
                (df[zip_col].notna()) & (df[zip_col] != '')
            ]

            if len(valid_data) > 0:
                # Create combined blocking key
                df_temp = valid_data.copy()
                df_temp['state_zip'] = df_temp['state'].astype(str) + '|' + df_temp[zip_col].astype(str)

                indexer = rl.Index()
                indexer.block('state_zip')
                pairs = set(indexer.index(df_temp))
                logger.info(f"State+ZIP blocking: {len(pairs)} pairs from {len(valid_data)} records")
                return pairs

        # FALLBACK: Use state+city if ZIP not available
        if 'city' in df.columns:
            valid_data = df[
                (df['state'].notna()) & (df['state'] != '') &
                (df['city'].notna()) & (df['city'] != '')
            ]

            if len(valid_data) > 0:
                df_temp = valid_data.copy()
                df_temp['state_city'] = df_temp['state'].astype(str) + '|' + df_temp['city'].astype(str)

                indexer = rl.Index()
                indexer.block('state_city')
                pairs = set(indexer.index(df_temp))
                logger.info(f"State+City blocking: {len(pairs)} pairs from {len(valid_data)} records")
                return pairs

        # LAST RESORT: State-only (will generate many pairs - warn user)
        valid_state = df[df['state'].notna() & (df['state'] != '')]
        if len(valid_state) == 0:
            return set()

        state_counts = valid_state['state'].value_counts()
        max_state = state_counts.idxmax()
        max_count = state_counts.max()
        estimated_pairs = (max_count * (max_count - 1)) // 2

        logger.warning(
            f"Using state-only blocking (ZIP/city unavailable). "
            f"Largest state: {max_state} with {max_count:,} records "
            f"= ~{estimated_pairs:,} pairs just for that state!"
        )

        indexer = rl.Index()
        indexer.block('state')
        return set(indexer.index(valid_state))

    def _block_by_zip(self, df: pd.DataFrame) -> Set[Tuple[int, int]]:
        """Block by ZIP code (use zip_normalized if available)."""
        zip_col = 'zip_normalized' if 'zip_normalized' in df.columns else 'zip'

        if zip_col not in df.columns:
            return set()

        valid_zip = df[df[zip_col].notna() & (df[zip_col] != '')]
        if len(valid_zip) == 0:
            return set()

        indexer = rl.Index()
        indexer.block(zip_col)
        return set(indexer.index(valid_zip))

    def _block_by_city(self, df: pd.DataFrame) -> Set[Tuple[int, int]]:
        """
        Block by city (especially useful for records missing state/ZIP).

        Only blocks records that don't have valid state or ZIP.
        """
        if 'city' not in df.columns:
            return set()

        # Only use city blocking for records missing state/ZIP
        missing_geo = df[
            ((df['state'].isna()) | (df['state'] == '')) |
            ((df.get('zip_normalized', df.get('zip', pd.Series())).isna()) |
             (df.get('zip_normalized', df.get('zip', pd.Series())) == ''))
        ]

        valid_city = missing_geo[missing_geo['city'].notna() & (missing_geo['city'] != '')]
        if len(valid_city) == 0:
            return set()

        indexer = rl.Index()
        indexer.block('city')
        return set(indexer.index(valid_city))

    def _block_by_phone_prefix(self, df: pd.DataFrame) -> Set[Tuple[int, int]]:
        """
        Block by phone area code + exchange (first 6 digits: NXXNXX).

        Useful for finding records with similar phone numbers (same location/office).
        """
        if 'phone' not in df.columns:
            return set()

        # Extract phone prefix (area code + exchange)
        df_with_prefix = df.copy()
        df_with_prefix['phone_prefix'] = df_with_prefix['phone'].astype(str).str.replace(r'\D', '', regex=True).str[:6]

        valid_phone = df_with_prefix[
            (df_with_prefix['phone_prefix'].notna()) &
            (df_with_prefix['phone_prefix'] != '') &
            (df_with_prefix['phone_prefix'].str.len() >= 6)
        ]

        if len(valid_phone) == 0:
            return set()

        indexer = rl.Index()
        indexer.block('phone_prefix')
        return set(indexer.index(valid_phone))

    def _block_by_name_token(self, df: pd.DataFrame) -> Set[Tuple[int, int]]:
        """
        Block by first significant word in name (skip common prefixes like "Dr", "The", etc.).

        Helps find records with similar names even if blocking fields are missing.
        """
        if 'name' not in df.columns:
            return set()

        # Extract first significant token
        df_with_token = df.copy()

        # Common prefixes to skip
        skip_words = {'dr', 'the', 'a', 'an', 'dba', 'inc', 'llc', 'corp', 'ltd'}

        def get_first_token(name):
            if pd.isna(name) or name == '':
                return ''

            # Clean and split
            tokens = str(name).lower().replace('.', '').replace(',', '').split()

            # Find first significant token
            for token in tokens:
                if len(token) >= 3 and token not in skip_words:
                    return token

            # Fallback to first token
            return tokens[0] if tokens else ''

        df_with_token['name_token'] = df_with_token['name'].apply(get_first_token)

        valid_token = df_with_token[
            (df_with_token['name_token'].notna()) &
            (df_with_token['name_token'] != '') &
            (df_with_token['name_token'].str.len() >= 3)
        ]

        if len(valid_token) == 0:
            return set()

        indexer = rl.Index()
        indexer.block('name_token')
        return set(indexer.index(valid_token))

    def _limited_missing_data_fallback(self, df: pd.DataFrame, existing_pairs: Set) -> Set[Tuple[int, int]]:
        """
        Limited fallback for records not covered by any blocking strategy.

        Caps the number of pairs to prevent explosion while ensuring all records
        get at least some comparisons.

        Args:
            df: Input DataFrame
            existing_pairs: Pairs already generated by other strategies

        Returns:
            Set of additional pairs (capped at max_missing_data_pairs)
        """
        # Find records not covered by existing pairs
        covered_indices = set()
        for idx1, idx2 in existing_pairs:
            covered_indices.add(idx1)
            covered_indices.add(idx2)

        uncovered = df[~df.index.isin(covered_indices)]

        if len(uncovered) == 0:
            return set()

        logger.info(f"Found {len(uncovered)} records not covered by blocking strategies")

        # Strategy: Compare each uncovered record against a sample of other records
        # Use sorted index order for deterministic sampling
        uncovered_indices = sorted(uncovered.index.tolist())
        all_indices = sorted(df.index.tolist())

        # Calculate how many comparisons per uncovered record
        max_comparisons_per_record = min(100, len(all_indices))

        # Generate limited pairs
        limited_pairs = set()

        for unc_idx in uncovered_indices:
            # Compare against up to max_comparisons_per_record other records
            # Use step to sample evenly across dataset
            step = max(1, len(all_indices) // max_comparisons_per_record)

            for other_idx in all_indices[::step]:
                if unc_idx != other_idx:
                    pair = (min(unc_idx, other_idx), max(unc_idx, other_idx))
                    limited_pairs.add(pair)

                # Cap total pairs
                if len(limited_pairs) >= self.max_missing_data_pairs:
                    logger.warning(
                        f"Reached max_missing_data_pairs limit ({self.max_missing_data_pairs}). "
                        f"Some records may not be fully compared."
                    )
                    return limited_pairs

        return limited_pairs


def estimate_blocking_effectiveness(df: pd.DataFrame, strategy: SmartBlockingStrategy = None) -> dict:
    """
    Estimate how effective blocking will be on this dataset.

    Args:
        df: Input DataFrame
        strategy: Blocking strategy to use (creates default if None)

    Returns:
        Dict with effectiveness metrics
    """
    if strategy is None:
        strategy = SmartBlockingStrategy()

    # Count records covered by each blocking field
    coverage = {}

    if 'state' in df.columns:
        coverage['state'] = (df['state'].notna() & (df['state'] != '')).sum()

    if 'zip' in df.columns or 'zip_normalized' in df.columns:
        zip_col = 'zip_normalized' if 'zip_normalized' in df.columns else 'zip'
        coverage['zip'] = (df[zip_col].notna() & (df[zip_col] != '')).sum()

    if 'city' in df.columns:
        coverage['city'] = (df['city'].notna() & (df['city'] != '')).sum()

    if 'phone' in df.columns:
        coverage['phone'] = (df['phone'].notna() & (df['phone'] != '')).sum()

    if 'name' in df.columns:
        coverage['name'] = (df['name'].notna() & (df['name'] != '')).sum()

    # Estimate pairs
    total_records = len(df)
    full_pairs = (total_records * (total_records - 1)) // 2

    return {
        'total_records': total_records,
        'full_cartesian_pairs': full_pairs,
        'field_coverage': coverage,
        'coverage_percentages': {k: f"{v/total_records*100:.1f}%" for k, v in coverage.items()}
    }
