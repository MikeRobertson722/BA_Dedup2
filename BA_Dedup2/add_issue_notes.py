"""
Add issue notes to a specific run version after manual review.
Helps track what problems were found and what needs to be fixed.

Usage:
    python add_issue_notes.py
    python add_issue_notes.py <version_id>
"""
import sys
from utils.backup_manager import BackupManager

def add_notes_interactive():
    """Interactive mode for adding issue notes."""
    manager = BackupManager()

    print('='*80)
    print('ADD ISSUE NOTES TO RUN VERSION')
    print('='*80)

    # List recent runs
    backups = manager.list_backups(limit=10)

    if not backups:
        print('No run versions found.')
        return

    print('\nRecent runs:\n')
    print(f'{"#":<4} {"ID":<5} {"Timestamp":<16} {"Status":<15} {"Flagged":<8} {"Description":<30}')
    print('-'*90)

    for i, backup in enumerate(backups, 1):
        id_, timestamp, name, file, size, desc, status, pending_before, flagged, created, issues = backup
        desc_short = (desc or '')[:27] + '...' if desc and len(desc) > 30 else (desc or '')
        flagged_str = str(flagged) if flagged else '-'
        print(f'{i:<4} {id_:<5} {timestamp:<16} {status:<15} {flagged_str:<8} {desc_short:<30}')

    print()
    print('='*90)

    # Get version selection
    try:
        selection = input('\nEnter run number to add notes (or "q" to quit): ').strip()

        if selection.lower() == 'q':
            print('Cancelled.')
            return

        run_num = int(selection)
        if run_num < 1 or run_num > len(backups):
            print(f'Invalid selection. Please enter 1-{len(backups)}')
            return

        selected = backups[run_num - 1]
        version_id = selected[0]

        print(f'\nSelected: Version {version_id} ({selected[1]})')
        print(f'Description: {selected[5]}')

        if selected[10]:  # existing issues
            print(f'Existing issues: {selected[10]}')

        print('\n' + '='*80)
        print('Enter issue notes (problems found during manual review):')
        print("Press Ctrl+Z (Windows) or Ctrl+D (Unix) and Enter when done, or just Enter for single line\n")

        lines = []
        try:
            while True:
                line = input()
                if not line and len(lines) == 0:
                    # Single line mode - just got empty input on first line
                    issues_found = input('Issues found: ').strip()
                    break
                lines.append(line)
        except EOFError:
            issues_found = '\n'.join(lines)

        if not issues_found:
            print('No issues entered.')
            return

        print('\n' + '='*80)
        print('Enter code changes needed to fix these issues:')
        print("Press Ctrl+Z (Windows) or Ctrl+D (Unix) and Enter when done, or just Enter for single line\n")

        lines = []
        try:
            while True:
                line = input()
                if not line and len(lines) == 0:
                    # Single line mode
                    code_changes = input('Code changes: ').strip()
                    break
                lines.append(line)
        except EOFError:
            code_changes = '\n'.join(lines)

        # Save notes
        manager.add_issue_notes(version_id, issues_found, code_changes)

        print('\n' + '='*80)
        print('Notes saved successfully!')
        print('\nNext steps:')
        print('1. Restore this backup: python restore_backup.py')
        print('2. Fix the code issues')
        print('3. Re-run: python run_dedup_with_db_review.py')
        print('='*80)

    except ValueError:
        print('Invalid input. Please enter a number.')
    except KeyboardInterrupt:
        print('\n\nCancelled.')

def add_notes_direct(version_id, issues_found, code_changes=''):
    """Direct mode for adding issue notes."""
    manager = BackupManager()
    manager.add_issue_notes(int(version_id), issues_found, code_changes)
    print(f'Notes added to version {version_id}')

if __name__ == '__main__':
    if len(sys.argv) > 2:
        # Direct mode: python add_issue_notes.py <version_id> <issues>
        version_id = sys.argv[1]
        issues = sys.argv[2]
        code_changes = sys.argv[3] if len(sys.argv) > 3 else ''
        add_notes_direct(version_id, issues, code_changes)
    else:
        # Interactive mode
        add_notes_interactive()
