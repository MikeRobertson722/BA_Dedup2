"""
Import Tracker - Tracks source CSV imports with unique IDs.
Maintains audit trail linking records to their source imports.
"""
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any
import hashlib
import json
from pathlib import Path
from data.db_connector import DatabaseConnector
from data.table_writer import TableWriter
from utils.logger import get_logger

logger = get_logger(__name__)


class ImportTracker:
    """
    Tracks CSV imports and maintains source data lineage.
    Each import gets a unique ID and records are linked back to their source.
    """

    def __init__(self):
        """Initialize import tracker."""
        self.db = DatabaseConnector()
        self.writer = TableWriter()
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """Create import tracking tables if they don't exist."""
        try:
            # Import metadata table
            create_imports_table = """
            CREATE TABLE IF NOT EXISTS ba_imports (
                import_id TEXT PRIMARY KEY,
                import_date TIMESTAMP,
                source_file TEXT,
                source_hash TEXT,
                record_count INTEGER,
                status TEXT,
                metadata TEXT
            )
            """

            # Source records table (original CSV data)
            create_source_table = """
            CREATE TABLE IF NOT EXISTS ba_source_records (
                source_record_id TEXT PRIMARY KEY,
                import_id TEXT,
                row_number INTEGER,
                name TEXT,
                address TEXT,
                city TEXT,
                state TEXT,
                zip TEXT,
                phone TEXT,
                email TEXT,
                contact_person TEXT,
                notes TEXT,
                raw_data TEXT,
                cluster_id INTEGER,
                similarity_score REAL,
                FOREIGN KEY (import_id) REFERENCES ba_imports(import_id)
            )
            """

            # Audit trail table (merge history)
            create_audit_table = """
            CREATE TABLE IF NOT EXISTS ba_merge_audit (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                merge_date TIMESTAMP,
                cluster_id INTEGER,
                golden_record_id TEXT,
                source_record_ids TEXT,
                merge_strategy TEXT,
                similarity_score REAL,
                match_method TEXT,
                ai_reasoning TEXT,
                field_selections TEXT,
                FOREIGN KEY (golden_record_id) REFERENCES business_associates_deduplicated(id)
            )
            """

            # Execute table creation
            engine = self.db.db.get_engine()
            conn = engine.raw_connection()
            cursor = conn.cursor()

            cursor.execute(create_imports_table)
            cursor.execute(create_source_table)
            cursor.execute(create_audit_table)

            conn.commit()
            conn.close()

            logger.info("Import tracking tables initialized")

        except Exception as e:
            logger.error(f"Failed to create tracking tables: {e}")
            raise

    def generate_import_id(self, source_file: str) -> str:
        """
        Generate unique import ID based on file and timestamp.

        Args:
            source_file: Path to source file

        Returns:
            Unique import ID
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = Path(source_file).stem
        return f"IMP_{timestamp}_{file_name}"

    def calculate_file_hash(self, file_path: str) -> str:
        """
        Calculate MD5 hash of file for change detection.

        Args:
            file_path: Path to file

        Returns:
            MD5 hash string
        """
        md5_hash = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()

    def import_csv_to_database(self, csv_path: str,
                               metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Import CSV file to source records table with tracking.

        Args:
            csv_path: Path to CSV file
            metadata: Optional metadata about the import

        Returns:
            Import ID
        """
        logger.info(f"Importing CSV: {csv_path}")

        # Read CSV
        df = pd.read_csv(csv_path)
        record_count = len(df)

        # Generate import ID and hash
        import_id = self.generate_import_id(csv_path)
        file_hash = self.calculate_file_hash(csv_path)

        logger.info(f"Import ID: {import_id}")
        logger.info(f"Records: {record_count}")

        # Create import metadata record
        import_record = {
            'import_id': import_id,
            'import_date': datetime.now(),
            'source_file': str(csv_path),
            'source_hash': file_hash,
            'record_count': record_count,
            'status': 'imported',
            'metadata': json.dumps(metadata or {})
        }

        # Save import metadata
        import_df = pd.DataFrame([import_record])
        self.writer.write_table(import_df, 'ba_imports', if_exists='append')

        # Prepare source records
        source_records = []
        for idx, row in df.iterrows():
            source_record = {
                'source_record_id': f"{import_id}_R{idx+1:04d}",
                'import_id': import_id,
                'row_number': idx + 1,
                'name': row.get('name', ''),
                'address': row.get('address', ''),
                'city': row.get('city', ''),
                'state': row.get('state', ''),
                'zip': row.get('zip', ''),
                'phone': row.get('phone', ''),
                'email': row.get('email', ''),
                'contact_person': row.get('contact_person', ''),
                'notes': row.get('notes', ''),
                'raw_data': row.to_json(),
                'cluster_id': None,
                'similarity_score': None
            }
            source_records.append(source_record)

        # Save source records
        source_df = pd.DataFrame(source_records)
        self.writer.write_table(source_df, 'ba_source_records', if_exists='append')

        logger.info(f"Import complete: {import_id}")

        # Add import_id to the DataFrame for tracking
        df['import_id'] = import_id
        df['source_record_id'] = [f"{import_id}_R{idx+1:04d}" for idx in range(len(df))]

        return import_id

    def record_merge(self, merge_info: Dict[str, Any]):
        """
        Record a merge operation in the audit trail.

        Args:
            merge_info: Dictionary with merge information:
                - cluster_id: Cluster ID
                - golden_record_id: ID of resulting golden record
                - source_record_ids: List of source record IDs that were merged
                - merge_strategy: Strategy used
                - similarity_score: Match confidence
                - match_method: 'fuzzy', 'ai', or 'hybrid'
                - ai_reasoning: AI explanation (if applicable)
                - field_selections: Which fields came from which source
        """
        audit_record = {
            'merge_date': datetime.now(),
            'cluster_id': merge_info.get('cluster_id'),
            'golden_record_id': merge_info.get('golden_record_id'),
            'source_record_ids': json.dumps(merge_info.get('source_record_ids', [])),
            'merge_strategy': merge_info.get('merge_strategy'),
            'similarity_score': merge_info.get('similarity_score'),
            'match_method': merge_info.get('match_method'),
            'ai_reasoning': merge_info.get('ai_reasoning', ''),
            'field_selections': json.dumps(merge_info.get('field_selections', {}))
        }

        audit_df = pd.DataFrame([audit_record])
        self.writer.write_table(audit_df, 'ba_merge_audit', if_exists='append')

        logger.debug(f"Recorded merge for cluster {merge_info.get('cluster_id')}")

    def get_import_history(self) -> pd.DataFrame:
        """
        Get history of all imports.

        Returns:
            DataFrame with import history
        """
        return self.db.read_table('ba_imports')

    def get_source_records(self, import_id: Optional[str] = None) -> pd.DataFrame:
        """
        Get source records, optionally filtered by import ID.

        Args:
            import_id: Optional import ID to filter by

        Returns:
            DataFrame with source records
        """
        if import_id:
            query = "SELECT * FROM ba_source_records WHERE import_id = ?"
            return pd.read_sql(query, self.db.db.get_engine(), params=[import_id])
        else:
            return self.db.read_table('ba_source_records')

    def get_merge_audit(self, golden_record_id: Optional[str] = None) -> pd.DataFrame:
        """
        Get merge audit trail, optionally for a specific record.

        Args:
            golden_record_id: Optional golden record ID to filter by

        Returns:
            DataFrame with merge audit
        """
        if golden_record_id:
            query = "SELECT * FROM ba_merge_audit WHERE golden_record_id = ?"
            return pd.read_sql(query, self.db.db.get_engine(), params=[golden_record_id])
        else:
            return self.db.read_table('ba_merge_audit')

    def update_source_records_with_clusters(self, df: pd.DataFrame):
        """
        Update source records with cluster_id and similarity_score from processed DataFrame.

        Args:
            df: DataFrame with source_record_id, cluster_id, and similarity_score columns
        """
        logger.info("Updating source records with cluster assignments and similarity scores...")

        # Filter to records that have source_record_id
        if 'source_record_id' not in df.columns:
            logger.warning("DataFrame missing source_record_id column, cannot update source records")
            return

        engine = self.db.db.get_engine()
        conn = engine.raw_connection()
        cursor = conn.cursor()

        # Prepare batch update data
        update_data = []
        for idx, row in df.iterrows():
            source_record_id = row.get('source_record_id')
            cluster_id = row.get('cluster_id', -1)
            similarity_score = row.get('similarity_score')

            if source_record_id:
                update_data.append((
                    int(cluster_id) if cluster_id is not None else None,
                    float(similarity_score) if similarity_score is not None else None,
                    source_record_id
                ))

        # Batch update using executemany (much faster than individual updates)
        if update_data:
            cursor.executemany(
                """
                UPDATE ba_source_records
                SET cluster_id = ?, similarity_score = ?
                WHERE source_record_id = ?
                """,
                update_data
            )

        conn.commit()
        conn.close()

        logger.info(f"Updated {len(update_data)} source records with cluster assignments (batch operation)")

    def trace_record_lineage(self, golden_record_id: str) -> Dict[str, Any]:
        """
        Trace the complete lineage of a golden record back to source.

        Args:
            golden_record_id: ID of golden record

        Returns:
            Dictionary with complete lineage information
        """
        # Get merge audit
        merge_audit = self.get_merge_audit(golden_record_id)

        if merge_audit.empty:
            return {'error': 'No merge audit found for this record'}

        audit_record = merge_audit.iloc[0]

        # Parse source record IDs
        source_ids = json.loads(audit_record['source_record_ids'])

        # Get source records
        source_records = []
        engine = self.db.db.get_engine()
        for source_id in source_ids:
            query = "SELECT * FROM ba_source_records WHERE source_record_id = ?"
            source_rec = pd.read_sql(query, engine, params=[source_id])
            if not source_rec.empty:
                source_records.append(source_rec.iloc[0].to_dict())

        # Get import info
        if source_records:
            import_id = source_records[0]['import_id']
            query = "SELECT * FROM ba_imports WHERE import_id = ?"
            import_info = pd.read_sql(query, engine, params=[import_id])
            import_dict = import_info.iloc[0].to_dict() if not import_info.empty else {}
        else:
            import_dict = {}

        return {
            'golden_record_id': golden_record_id,
            'merge_date': audit_record['merge_date'],
            'merge_strategy': audit_record['merge_strategy'],
            'similarity_score': audit_record['similarity_score'],
            'match_method': audit_record['match_method'],
            'ai_reasoning': audit_record['ai_reasoning'],
            'source_records': source_records,
            'import_info': import_dict
        }
