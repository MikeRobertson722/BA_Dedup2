"""
Interactive backup restore script.
Lists available backups and allows user to select one to restore.

Usage:
    python restore_backup.py                    # Interactive mode
    python restore_backup.py <backup_file>      # Direct restore
"""
import sys
from pathlib import Path
from utils.backup_manager import BackupManager

def interactive_restore():
    """Interactive backup selection and restore."""
    manager = BackupManager()

    print('='*80)
    print('BACKUP RESTORE UTILITY')
    print('='*80)

    # List available backups
    backups = manager.list_backups()

    if not backups:
        print('No backups found.')
        print('Backups are created automatically when running deduplication.')
        return

    print('\nAvailable backups:\n')
    print(f'{"#":<4} {"ID":<5} {"Timestamp":<16} {"Status":<15} {"Size (MB)":<10} {"Pending":<8} {"Description":<30}')
    print('-'*100)

    for i, backup in enumerate(backups, 1):
        id_, timestamp, name, file, size, desc, status, pending_before, flagged, *_ = backup
        desc_short = (desc or '')[:27] + '...' if desc and len(desc) > 30 else (desc or '')
        print(f'{i:<4} {id_:<5} {timestamp:<16} {status:<15} {size:<10.2f} {pending_before or 0:<8} {desc_short:<30}')

    print()
    print('='*100)

    # Get user selection
    try:
        selection = input('\nEnter backup number to restore (or "q" to quit): ').strip()

        if selection.lower() == 'q':
            print('Cancelled.')
            return

        backup_num = int(selection)
        if backup_num < 1 or backup_num > len(backups):
            print(f'Invalid selection. Please enter 1-{len(backups)}')
            return

        selected_backup = backups[backup_num - 1]
        backup_file = selected_backup[3]  # backup_file column

        # Show details
        print('\n' + '='*80)
        print('SELECTED BACKUP')
        print('='*80)
        print(f'ID: {selected_backup[0]}')
        print(f'Timestamp: {selected_backup[1]}')
        print(f'File: {backup_file}')
        print(f'Size: {selected_backup[4]:.2f} MB')
        print(f'Description: {selected_backup[5] or "(none)"}')
        print(f'Status: {selected_backup[6]}')
        print(f'Pending reviews before: {selected_backup[7] or 0}')

        if selected_backup[10]:  # issues_found
            print(f'\nIssues found: {selected_backup[10]}')

        # Confirm
        confirm = input('\nAre you sure you want to restore this backup? (yes/no): ').strip().lower()

        if confirm == 'yes':
            # Restore
            success = manager.restore_backup(backup_file)
            if success:
                print('\nRestore completed successfully!')
                print('\nNext steps:')
                print('1. Fix any code issues')
                print('2. Re-run: python run_dedup_with_db_review.py')
                print('3. Review results')
            else:
                print('\nRestore failed.')
        else:
            print('Restore cancelled.')

    except ValueError:
        print('Invalid input. Please enter a number.')
    except KeyboardInterrupt:
        print('\n\nCancelled.')

def direct_restore(backup_file):
    """Restore specific backup file directly."""
    manager = BackupManager()

    if not Path(backup_file).exists():
        print(f'ERROR: Backup file not found: {backup_file}')
        return

    print(f'Restoring backup: {backup_file}\n')
    success = manager.restore_backup(backup_file)

    if success:
        print('\nRestore completed successfully!')
    else:
        print('\nRestore failed.')

if __name__ == '__main__':
    if len(sys.argv) > 1:
        # Direct restore mode
        backup_file = sys.argv[1]
        direct_restore(backup_file)
    else:
        # Interactive mode
        interactive_restore()
