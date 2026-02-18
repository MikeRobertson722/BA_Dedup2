"""
Backup and restore manager for BA deduplication system.
Enables iterative development workflow with rollback capability.
"""
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime
import os

class BackupManager:
    """Manages database backups and restores for deduplication runs."""

    def __init__(self, db_path='ba_dedup.db', backup_dir='backups'):
        self.db_path = Path(db_path)
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)

    def get_current_stats(self):
        """Get current database statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        stats = {}

        # Source records count
        try:
            cursor.execute("SELECT COUNT(*) FROM ba_source_records")
            stats['source_records'] = cursor.fetchone()[0]
        except sqlite3.OperationalError:
            stats['source_records'] = 0

        # Review queue total
        try:
            cursor.execute("SELECT COUNT(*) FROM human_review_queue")
            stats['review_queue_total'] = cursor.fetchone()[0]
        except sqlite3.OperationalError:
            stats['review_queue_total'] = 0

        # Pending reviews
        try:
            cursor.execute("SELECT COUNT(*) FROM human_review_queue WHERE review_status = 'pending'")
            stats['pending_reviews'] = cursor.fetchone()[0]
        except sqlite3.OperationalError:
            stats['pending_reviews'] = 0

        conn.close()
        return stats

    def create_backup(self, description='', version_name=None, run_by='system'):
        """
        Create full database backup before deduplication run.

        Args:
            description: What is being tested/changed in this run
            version_name: Optional user-friendly name for this version
            run_by: Username or identifier

        Returns:
            Tuple of (backup_path, version_id)
        """
        # Generate timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Create backup file path
        backup_filename = f'ba_dedup_backup_{timestamp}.db'
        backup_path = self.backup_dir / backup_filename

        print('='*80)
        print('CREATING BACKUP')
        print('='*80)

        # Get stats before backup
        stats = self.get_current_stats()
        print(f'Current state:')
        print(f'  Source records: {stats["source_records"]:,}')
        print(f'  Review queue total: {stats["review_queue_total"]:,}')
        print(f'  Pending reviews: {stats["pending_reviews"]:,}')

        # Create backup using SQLite backup API
        print(f'\nBacking up to: {backup_path}')
        conn = sqlite3.connect(self.db_path)
        backup_conn = sqlite3.connect(backup_path)

        # Perform backup
        conn.backup(backup_conn)

        backup_conn.close()
        conn.close()

        # Get backup file size
        backup_size_mb = backup_path.stat().st_size / (1024 * 1024)

        # Record backup metadata in version tracking table
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO ba_run_versions (
                version_timestamp,
                version_name,
                backup_file,
                backup_size_mb,
                description,
                run_by,
                run_status,
                source_records_before,
                review_queue_before,
                pending_reviews_before
            ) VALUES (?, ?, ?, ?, ?, ?, 'backup_created', ?, ?, ?)
        """, (
            timestamp,
            version_name,
            str(backup_path),
            backup_size_mb,
            description,
            run_by,
            stats['source_records'],
            stats['review_queue_total'],
            stats['pending_reviews']
        ))

        version_id = cursor.lastrowid
        conn.commit()
        conn.close()

        print(f'Backup created: {backup_filename}')
        print(f'Size: {backup_size_mb:.2f} MB')
        print(f'Version ID: {version_id}')
        print('='*80)

        return str(backup_path), version_id

    def update_run_completion(self, version_id, notes=''):
        """Update version record after run completes."""
        stats = self.get_current_stats()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get stats from before run
        cursor.execute("""
            SELECT source_records_before, review_queue_before, pending_reviews_before
            FROM ba_run_versions
            WHERE id = ?
        """, (version_id,))
        before_stats = cursor.fetchone()

        if before_stats:
            source_before, queue_before, pending_before = before_stats
            auto_merged = (source_before - stats['source_records']) if source_before else 0
            flagged = stats['pending_reviews'] - pending_before if pending_before else stats['pending_reviews']
        else:
            auto_merged = 0
            flagged = stats['pending_reviews']

        cursor.execute("""
            UPDATE ba_run_versions
            SET run_status = 'run_completed',
                run_completed_date = CURRENT_TIMESTAMP,
                source_records_after = ?,
                review_queue_after = ?,
                pending_reviews_after = ?,
                auto_merged_count = ?,
                flagged_for_review_count = ?,
                notes = ?
            WHERE id = ?
        """, (
            stats['source_records'],
            stats['review_queue_total'],
            stats['pending_reviews'],
            auto_merged,
            flagged,
            notes,
            version_id
        ))

        conn.commit()
        conn.close()

    def restore_backup(self, backup_file):
        """
        Restore database from backup file.

        Args:
            backup_file: Path to backup file to restore

        Returns:
            True if successful, False otherwise
        """
        backup_path = Path(backup_file)

        if not backup_path.exists():
            print(f'ERROR: Backup file not found: {backup_path}')
            return False

        print('='*80)
        print('RESTORING BACKUP')
        print('='*80)
        print(f'Backup file: {backup_path}')
        print(f'Backup size: {backup_path.stat().st_size / (1024*1024):.2f} MB')

        # Get current stats before restore
        current_stats = self.get_current_stats()
        print(f'\nCurrent state (will be overwritten):')
        print(f'  Source records: {current_stats["source_records"]:,}')
        print(f'  Review queue: {current_stats["review_queue_total"]:,}')
        print(f'  Pending reviews: {current_stats["pending_reviews"]:,}')

        # Create safety backup of current state
        safety_backup = self.backup_dir / f'safety_backup_before_restore_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
        print(f'\nCreating safety backup: {safety_backup.name}')
        shutil.copy(self.db_path, safety_backup)

        # Restore backup
        print(f'\nRestoring {backup_path.name}...')
        shutil.copy(backup_path, self.db_path)

        # Get stats after restore
        restored_stats = self.get_current_stats()
        print(f'\nRestored state:')
        print(f'  Source records: {restored_stats["source_records"]:,}')
        print(f'  Review queue: {restored_stats["review_queue_total"]:,}')
        print(f'  Pending reviews: {restored_stats["pending_reviews"]:,}')

        # Record restore in version tracking
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Find the version ID for this backup
        cursor.execute("SELECT id FROM ba_run_versions WHERE backup_file = ?", (str(backup_path),))
        result = cursor.fetchone()
        if result:
            version_id = result[0]
            cursor.execute("""
                UPDATE ba_run_versions
                SET run_status = 'restored',
                    restored_date = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (version_id,))
            conn.commit()

        conn.close()

        print(f'\nBackup restored successfully!')
        print(f'Safety backup saved: {safety_backup.name}')
        print('='*80)

        return True

    def list_backups(self, limit=20):
        """List available backups with metadata."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                id,
                version_timestamp,
                version_name,
                backup_file,
                backup_size_mb,
                description,
                run_status,
                pending_reviews_before,
                flagged_for_review_count,
                backup_created_date,
                issues_found
            FROM ba_run_versions
            ORDER BY backup_created_date DESC
            LIMIT ?
        """, (limit,))

        backups = cursor.fetchall()
        conn.close()

        return backups

    def add_issue_notes(self, version_id, issues_found, code_changes=''):
        """Add notes about issues found during manual review."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE ba_run_versions
            SET issues_found = ?,
                code_changes = ?
            WHERE id = ?
        """, (issues_found, code_changes, version_id))

        conn.commit()
        conn.close()

        print(f'Notes added to version {version_id}')


# Standalone utility functions
def create_backup(description='', version_name=None):
    """Create backup (standalone function)."""
    manager = BackupManager()
    return manager.create_backup(description, version_name)

def restore_backup(backup_file):
    """Restore backup (standalone function)."""
    manager = BackupManager()
    return manager.restore_backup(backup_file)

def list_backups():
    """List backups (standalone function)."""
    manager = BackupManager()
    return manager.list_backups()


if __name__ == '__main__':
    # Example usage
    print('Backup Manager - Example Usage\n')

    manager = BackupManager()

    # List existing backups
    print('='*80)
    print('AVAILABLE BACKUPS')
    print('='*80)

    backups = manager.list_backups()
    if backups:
        print(f'{"ID":<5} {"Timestamp":<16} {"Status":<15} {"Size (MB)":<10} {"Description":<30}')
        print('-'*80)
        for backup in backups:
            id_, timestamp, name, file, size, desc, status, *_ = backup
            desc_short = (desc or '')[:27] + '...' if desc and len(desc) > 30 else (desc or '')
            print(f'{id_:<5} {timestamp:<16} {status:<15} {size:<10.2f} {desc_short:<30}')
    else:
        print('No backups found.')

    print('\n' + '='*80)
    print('To create backup: manager.create_backup(description="Testing new fuzzy threshold")')
    print('To restore: manager.restore_backup("backups/ba_dedup_backup_20260218_143022.db")')
    print('='*80)
