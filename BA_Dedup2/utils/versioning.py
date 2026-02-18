"""
Versioning and recovery utilities for Business Associate deduplication.
Provides merge history tracking, undo/rollback, and point-in-time recovery.
"""
import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from utils.logger import get_logger

logger = get_logger(__name__)


class MergeVersionManager:
    """
    Manages merge versions and provides undo/rollback capabilities.

    Features:
    - Track all merge operations
    - Store before/after snapshots
    - Undo individual merges
    - Rollback to specific point in time
    - Audit trail for compliance
    """

    def __init__(self, db_connection):
        """
        Initialize version manager.

        Args:
            db_connection: Database connection (sqlite3 or SQLAlchemy)
        """
        self.db = db_connection
        self._ensure_version_tables()

    def _ensure_version_tables(self):
        """Create version tracking tables if they don't exist."""
        cursor = self.db.cursor()

        # Table 1: Merge operations (high-level tracking)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ba_merge_operations (
                operation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                operation_type TEXT,  -- 'auto_merge', 'manual_merge', 'undo'
                user_id TEXT,
                cluster_id INTEGER,
                record_count INTEGER,
                golden_record_id TEXT,
                is_undone INTEGER DEFAULT 0,
                undone_by_operation_id INTEGER,
                notes TEXT
            )
        """)

        # Table 2: Record versions (detailed snapshots)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ba_record_versions (
                version_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id INTEGER,
                record_id TEXT,
                version_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                version_type TEXT,  -- 'before_merge', 'after_merge', 'golden'
                record_data TEXT,  -- JSON snapshot of record
                FOREIGN KEY (operation_id) REFERENCES ba_merge_operations(operation_id)
            )
        """)

        # Table 3: Merge relationships (which records were merged)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ba_merge_relationships (
                relationship_id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id INTEGER,
                source_record_id TEXT,
                target_record_id TEXT,  -- golden record ID
                similarity_score REAL,
                FOREIGN KEY (operation_id) REFERENCES ba_merge_operations(operation_id)
            )
        """)

        self.db.commit()
        logger.info("Version tracking tables initialized")

    def record_merge_operation(self,
                               cluster_id: int,
                               records_df: pd.DataFrame,
                               golden_record: pd.Series,
                               operation_type: str = 'auto_merge',
                               user_id: Optional[str] = None,
                               notes: Optional[str] = None) -> int:
        """
        Record a merge operation with full versioning.

        Args:
            cluster_id: Cluster ID being merged
            records_df: DataFrame of all records in cluster
            golden_record: The golden/merged record
            operation_type: Type of operation ('auto_merge', 'manual_merge')
            user_id: User who performed the merge
            notes: Optional notes about the merge

        Returns:
            operation_id: Unique ID for this merge operation
        """
        cursor = self.db.cursor()

        # 1. Create merge operation record
        cursor.execute("""
            INSERT INTO ba_merge_operations
            (operation_type, user_id, cluster_id, record_count, golden_record_id, notes)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            operation_type,
            user_id or 'system',
            cluster_id,
            len(records_df),
            golden_record.get('record_id', ''),
            notes
        ))

        operation_id = cursor.lastrowid

        # 2. Store before-merge snapshots for all records (batch insert)
        before_merge_data = []
        for idx, record in records_df.iterrows():
            record_snapshot = record.to_dict()
            # Convert any non-JSON-serializable types
            for key, val in record_snapshot.items():
                if pd.isna(val):
                    record_snapshot[key] = None
                elif isinstance(val, (pd.Timestamp, datetime)):
                    record_snapshot[key] = str(val)

            before_merge_data.append((
                operation_id,
                record.get('record_id', ''),
                'before_merge',
                json.dumps(record_snapshot)
            ))

        # Batch insert all before-merge snapshots
        if before_merge_data:
            cursor.executemany("""
                INSERT INTO ba_record_versions
                (operation_id, record_id, version_type, record_data)
                VALUES (?, ?, ?, ?)
            """, before_merge_data)

        # 3. Store golden record snapshot
        golden_snapshot = golden_record.to_dict()
        for key, val in golden_snapshot.items():
            if pd.isna(val):
                golden_snapshot[key] = None
            elif isinstance(val, (pd.Timestamp, datetime)):
                golden_snapshot[key] = str(val)

        cursor.execute("""
            INSERT INTO ba_record_versions
            (operation_id, record_id, version_type, record_data)
            VALUES (?, ?, ?, ?)
        """, (
            operation_id,
            golden_record.get('record_id', ''),
            'golden',
            json.dumps(golden_snapshot)
        ))

        # 4. Record merge relationships (batch insert)
        relationship_data = []
        for idx, record in records_df.iterrows():
            relationship_data.append((
                operation_id,
                record.get('record_id', ''),
                golden_record.get('record_id', ''),
                record.get('similarity_score', 1.0)
            ))

        # Batch insert all merge relationships
        if relationship_data:
            cursor.executemany("""
                INSERT INTO ba_merge_relationships
                (operation_id, source_record_id, target_record_id, similarity_score)
                VALUES (?, ?, ?, ?)
            """, relationship_data)

        self.db.commit()

        logger.info(f"Recorded merge operation {operation_id}: cluster {cluster_id}, {len(records_df)} records")

        return operation_id

    def undo_merge(self, operation_id: int, user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Undo a specific merge operation.

        Restores all records to their pre-merge state and marks the operation as undone.

        Args:
            operation_id: The operation ID to undo
            user_id: User performing the undo

        Returns:
            Dict with undo results and restored records
        """
        cursor = self.db.cursor()

        # 1. Check if operation exists and is not already undone
        cursor.execute("""
            SELECT operation_id, cluster_id, record_count, is_undone, golden_record_id
            FROM ba_merge_operations
            WHERE operation_id = ?
        """, (operation_id,))

        operation = cursor.fetchone()

        if not operation:
            logger.error(f"Operation {operation_id} not found")
            return {'success': False, 'error': 'Operation not found'}

        if operation[3]:  # is_undone
            logger.warning(f"Operation {operation_id} is already undone")
            return {'success': False, 'error': 'Operation already undone'}

        cluster_id = operation[1]
        record_count = operation[2]
        golden_record_id = operation[4]

        # 2. Retrieve before-merge snapshots
        cursor.execute("""
            SELECT record_id, record_data
            FROM ba_record_versions
            WHERE operation_id = ? AND version_type = 'before_merge'
        """, (operation_id,))

        before_snapshots = cursor.fetchall()

        if not before_snapshots:
            logger.error(f"No before-merge snapshots found for operation {operation_id}")
            return {'success': False, 'error': 'No snapshots found'}

        # 3. Restore records to before-merge state
        restored_records = []

        for record_id, record_data_json in before_snapshots:
            record_data = json.loads(record_data_json)
            restored_records.append(record_data)

            # Update ba_source_records table with restored data
            # Build UPDATE query dynamically based on fields
            set_clauses = []
            values = []
            for key, val in record_data.items():
                if key not in ['record_id']:  # Don't update primary key
                    set_clauses.append(f"{key} = ?")
                    values.append(val)

            if set_clauses:
                values.append(record_id)
                update_query = f"""
                    UPDATE ba_source_records
                    SET {', '.join(set_clauses)}
                    WHERE record_id = ?
                """
                cursor.execute(update_query, values)

        # 4. Mark operation as undone
        cursor.execute("""
            UPDATE ba_merge_operations
            SET is_undone = 1
            WHERE operation_id = ?
        """, (operation_id,))

        # 5. Create undo operation record
        cursor.execute("""
            INSERT INTO ba_merge_operations
            (operation_type, user_id, cluster_id, record_count, notes)
            VALUES (?, ?, ?, ?, ?)
        """, (
            'undo',
            user_id or 'system',
            cluster_id,
            record_count,
            f"Undo of operation {operation_id}"
        ))

        undo_operation_id = cursor.lastrowid

        # Link undo to original operation
        cursor.execute("""
            UPDATE ba_merge_operations
            SET undone_by_operation_id = ?
            WHERE operation_id = ?
        """, (undo_operation_id, operation_id))

        self.db.commit()

        logger.info(f"Undid merge operation {operation_id}, restored {len(restored_records)} records")

        return {
            'success': True,
            'operation_id': operation_id,
            'undo_operation_id': undo_operation_id,
            'cluster_id': cluster_id,
            'restored_count': len(restored_records),
            'restored_records': restored_records
        }

    def get_merge_history(self,
                         record_id: Optional[str] = None,
                         cluster_id: Optional[int] = None,
                         limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get merge operation history.

        Args:
            record_id: Optional filter by record ID
            cluster_id: Optional filter by cluster ID
            limit: Maximum number of operations to return

        Returns:
            List of merge operations with details
        """
        cursor = self.db.cursor()

        if record_id:
            # Find operations involving this record
            cursor.execute("""
                SELECT DISTINCT mo.operation_id, mo.operation_timestamp, mo.operation_type,
                       mo.user_id, mo.cluster_id, mo.record_count, mo.golden_record_id,
                       mo.is_undone, mo.notes
                FROM ba_merge_operations mo
                JOIN ba_merge_relationships mr ON mo.operation_id = mr.operation_id
                WHERE mr.source_record_id = ? OR mo.golden_record_id = ?
                ORDER BY mo.operation_timestamp DESC
                LIMIT ?
            """, (record_id, record_id, limit))
        elif cluster_id is not None:
            cursor.execute("""
                SELECT operation_id, operation_timestamp, operation_type,
                       user_id, cluster_id, record_count, golden_record_id,
                       is_undone, notes
                FROM ba_merge_operations
                WHERE cluster_id = ?
                ORDER BY operation_timestamp DESC
                LIMIT ?
            """, (cluster_id, limit))
        else:
            cursor.execute("""
                SELECT operation_id, operation_timestamp, operation_type,
                       user_id, cluster_id, record_count, golden_record_id,
                       is_undone, notes
                FROM ba_merge_operations
                ORDER BY operation_timestamp DESC
                LIMIT ?
            """, (limit,))

        operations = []
        for row in cursor.fetchall():
            operations.append({
                'operation_id': row[0],
                'timestamp': row[1],
                'type': row[2],
                'user_id': row[3],
                'cluster_id': row[4],
                'record_count': row[5],
                'golden_record_id': row[6],
                'is_undone': bool(row[7]),
                'notes': row[8]
            })

        return operations

    def rollback_to_timestamp(self,
                             target_timestamp: datetime,
                             user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Rollback all merges that occurred after a specific timestamp.

        This is a more aggressive operation that undoes multiple merges.

        Args:
            target_timestamp: Undo all operations after this time
            user_id: User performing the rollback

        Returns:
            Dict with rollback results
        """
        cursor = self.db.cursor()

        # Find all operations after target timestamp that aren't undone
        cursor.execute("""
            SELECT operation_id, operation_timestamp
            FROM ba_merge_operations
            WHERE operation_timestamp > ?
            AND is_undone = 0
            AND operation_type != 'undo'
            ORDER BY operation_timestamp DESC
        """, (target_timestamp,))

        operations_to_undo = cursor.fetchall()

        if not operations_to_undo:
            logger.info(f"No operations found after {target_timestamp}")
            return {
                'success': True,
                'operations_undone': 0,
                'message': 'No operations to rollback'
            }

        # Undo operations in reverse chronological order (most recent first)
        undone_operations = []
        for operation_id, timestamp in operations_to_undo:
            result = self.undo_merge(operation_id, user_id)
            if result['success']:
                undone_operations.append({
                    'operation_id': operation_id,
                    'timestamp': timestamp
                })

        logger.info(f"Rolled back {len(undone_operations)} operations to {target_timestamp}")

        return {
            'success': True,
            'operations_undone': len(undone_operations),
            'target_timestamp': str(target_timestamp),
            'undone_operations': undone_operations
        }

    def compare_versions(self, record_id: str, version_id_1: int, version_id_2: int) -> Dict[str, Any]:
        """
        Compare two versions of the same record.

        Args:
            record_id: Record ID to compare
            version_id_1: First version ID
            version_id_2: Second version ID

        Returns:
            Dict with comparison results showing field differences
        """
        cursor = self.db.cursor()

        # Get both versions
        cursor.execute("""
            SELECT version_id, record_data, version_type, version_timestamp
            FROM ba_record_versions
            WHERE version_id IN (?, ?) AND record_id = ?
        """, (version_id_1, version_id_2, record_id))

        versions = cursor.fetchall()

        if len(versions) != 2:
            return {'success': False, 'error': 'Could not find both versions'}

        version_1_data = json.loads(versions[0][1])
        version_2_data = json.loads(versions[1][1])

        # Compare field by field
        differences = {}
        all_fields = set(version_1_data.keys()) | set(version_2_data.keys())

        for field in all_fields:
            val1 = version_1_data.get(field)
            val2 = version_2_data.get(field)

            if val1 != val2:
                differences[field] = {
                    'version_1': val1,
                    'version_2': val2,
                    'changed': True
                }

        return {
            'success': True,
            'record_id': record_id,
            'version_1': {
                'version_id': versions[0][0],
                'type': versions[0][2],
                'timestamp': versions[0][3]
            },
            'version_2': {
                'version_id': versions[1][0],
                'type': versions[1][2],
                'timestamp': versions[1][3]
            },
            'differences': differences,
            'changed_fields': list(differences.keys())
        }

    def get_audit_trail(self,
                       start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None,
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
        cursor = self.db.cursor()

        query = """
            SELECT
                mo.operation_id,
                mo.operation_timestamp,
                mo.operation_type,
                mo.user_id,
                mo.cluster_id,
                mo.record_count,
                mo.golden_record_id,
                mo.is_undone,
                mo.undone_by_operation_id,
                mo.notes,
                COUNT(DISTINCT mr.source_record_id) as affected_records
            FROM ba_merge_operations mo
            LEFT JOIN ba_merge_relationships mr ON mo.operation_id = mr.operation_id
            WHERE 1=1
        """

        params = []

        if start_date:
            query += " AND mo.operation_timestamp >= ?"
            params.append(start_date)

        if end_date:
            query += " AND mo.operation_timestamp <= ?"
            params.append(end_date)

        if user_id:
            query += " AND mo.user_id = ?"
            params.append(user_id)

        query += " GROUP BY mo.operation_id ORDER BY mo.operation_timestamp DESC"

        df = pd.read_sql_query(query, self.db, params=params)

        logger.info(f"Generated audit trail with {len(df)} operations")

        return df
