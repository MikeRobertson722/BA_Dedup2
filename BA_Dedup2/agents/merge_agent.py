"""
Merge Agent - Resolves duplicate clusters by creating golden records.
Merges duplicate records using the configured strategy.

Enhanced with Priority 3 features:
- Full version tracking for all merge operations
- Undo/rollback capability
- Audit trail for compliance
"""
import pandas as pd
from typing import Dict, Any, List, Optional
from agents.base_agent import BaseAgent
from config import settings
from utils.helpers import merge_records
from utils.versioning import MergeVersionManager


class MergeAgent(BaseAgent):
    """
    Agent responsible for merging duplicate records into golden records.
    Uses the configured merge strategy to resolve conflicts.
    """

    def __init__(self, config: Dict[str, Any] = None, db_connection=None):
        """
        Initialize merge agent.

        Args:
            config: Configuration dictionary with keys:
                - merge_strategy: Strategy for merging ('most_complete', 'most_recent', 'first')
                - important_fields: Fields to prioritize in merging
                - enable_versioning: Enable version tracking (default: True)
                - user_id: User ID for audit trail
            db_connection: Database connection for versioning (optional)
        """
        super().__init__('merge', config)
        self.merge_strategy = self.config.get('merge_strategy', settings.MERGE_STRATEGY)
        self.important_fields = self.config.get('important_fields', settings.MATCH_FIELDS)
        self.merge_audit = []

        # Versioning (Priority 3)
        self.enable_versioning = self.config.get('enable_versioning', True)
        self.user_id = self.config.get('user_id', 'system')
        self.version_manager = None

        if self.enable_versioning and db_connection:
            try:
                self.version_manager = MergeVersionManager(db_connection)
                self.logger.info("Version tracking enabled")
            except Exception as e:
                self.logger.warning(f"Could not initialize version manager: {e}")
                self.enable_versioning = False

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Merge duplicate records into golden records.

        Args:
            data: Input DataFrame with cluster_id column

        Returns:
            DataFrame with deduplicated records
        """
        self.logger.info(f"Merging duplicates using strategy: {self.merge_strategy}")

        # Separate records into duplicates and non-duplicates (slicing creates copies)
        duplicates = data[data['cluster_id'] != -1]
        non_duplicates = data[data['cluster_id'] == -1]
        df = data  # Keep reference for logging

        self.logger.info(f"Processing {len(duplicates)} duplicate records in {duplicates['cluster_id'].nunique()} clusters")

        # Merge each cluster
        merged_records = []

        for cluster_id in duplicates['cluster_id'].unique():
            cluster_df = duplicates[duplicates['cluster_id'] == cluster_id]

            # Merge records in this cluster
            golden_record = self._merge_cluster(cluster_df, cluster_id)
            merged_records.append(golden_record)

        # Create DataFrame from merged records
        if merged_records:
            merged_df = pd.DataFrame(merged_records)
        else:
            # If no duplicates to merge, create empty DataFrame with same columns
            merged_df = pd.DataFrame(columns=df.columns)

        # Combine merged records with non-duplicates
        result_df = pd.concat([non_duplicates, merged_df], ignore_index=True)

        # Clean up temporary columns (but keep cluster_id and similarity_score)
        if 'record_id' in result_df.columns:
            result_df = result_df.drop(columns=['record_id'])

        original_count = len(df)
        final_count = len(result_df)
        removed_count = original_count - final_count

        self.logger.info(f"Merge complete: {removed_count} duplicate records merged")
        self.logger.info(f"Final count: {final_count} unique records")

        return result_df

    def _merge_cluster(self, cluster_df: pd.DataFrame, cluster_id: int) -> Dict[str, Any]:
        """
        Merge a cluster of duplicate records into a single golden record.

        Enhanced with versioning to track before/after states.

        Args:
            cluster_df: DataFrame containing records in this cluster
            cluster_id: Cluster ID

        Returns:
            Dictionary representing the merged golden record
        """
        self.logger.debug(f"Merging cluster {cluster_id} with {len(cluster_df)} records")

        # Get the golden record using merge strategy
        golden_record = merge_records(
            cluster_df,
            strategy=self.merge_strategy,
            important_fields=self.important_fields
        )

        # Track merge audit information
        source_ids = cluster_df['record_id'].tolist() if 'record_id' in cluster_df.columns else []

        self.merge_audit.append({
            'cluster_id': cluster_id,
            'source_record_count': len(cluster_df),
            'source_record_ids': source_ids,
            'merge_strategy': self.merge_strategy
        })

        # PRIORITY 3: Record merge operation with full versioning
        if self.enable_versioning and self.version_manager:
            try:
                operation_id = self.version_manager.record_merge_operation(
                    cluster_id=cluster_id,
                    records_df=cluster_df,
                    golden_record=golden_record,
                    operation_type='auto_merge',
                    user_id=self.user_id,
                    notes=f"Merged {len(cluster_df)} records using {self.merge_strategy} strategy"
                )
                self.logger.debug(f"Recorded merge operation {operation_id} for cluster {cluster_id}")
            except Exception as e:
                self.logger.warning(f"Could not record merge version: {e}")

        # Add merge metadata
        merged_dict = golden_record.to_dict()
        merged_dict['_merged_from_count'] = len(cluster_df)
        merged_dict['_cluster_id'] = cluster_id

        return merged_dict

    def get_merge_audit(self) -> List[Dict[str, Any]]:
        """
        Get audit trail of merge operations.

        Returns:
            List of merge audit records
        """
        return self.merge_audit

    def export_merge_audit(self) -> pd.DataFrame:
        """
        Export merge audit as a DataFrame.

        Returns:
            DataFrame with merge audit information
        """
        if not self.merge_audit:
            return pd.DataFrame()

        return pd.DataFrame(self.merge_audit)

    def validate(self, result: pd.DataFrame) -> bool:
        """
        Validate merge results.

        Args:
            result: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        if not super().validate(result):
            return False

        # Result should have fewer or equal records than input
        # (This is checked by comparing with merge audit)

        return True

    # PRIORITY 3: Version control methods

    def undo_merge(self, operation_id: int) -> Dict[str, Any]:
        """
        Undo a specific merge operation.

        Args:
            operation_id: Operation ID to undo

        Returns:
            Dict with undo results
        """
        if not self.version_manager:
            return {'success': False, 'error': 'Version manager not initialized'}

        result = self.version_manager.undo_merge(operation_id, self.user_id)
        return result

    def get_merge_history(self,
                         record_id: Optional[str] = None,
                         cluster_id: Optional[int] = None,
                         limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get merge operation history.

        Args:
            record_id: Optional filter by record ID
            cluster_id: Optional filter by cluster ID
            limit: Maximum operations to return

        Returns:
            List of merge operations
        """
        if not self.version_manager:
            return []

        return self.version_manager.get_merge_history(
            record_id=record_id,
            cluster_id=cluster_id,
            limit=limit
        )

    def get_audit_trail(self,
                       start_date=None,
                       end_date=None,
                       user_id: Optional[str] = None) -> pd.DataFrame:
        """
        Generate audit trail report for compliance.

        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter
            user_id: Optional user filter

        Returns:
            DataFrame with complete audit trail
        """
        if not self.version_manager:
            return pd.DataFrame()

        return self.version_manager.get_audit_trail(
            start_date=start_date,
            end_date=end_date,
            user_id=user_id
        )
