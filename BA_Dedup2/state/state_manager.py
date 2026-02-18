"""
State management for pipeline execution.
Tracks progress, enables resume capability, and maintains execution history.
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class StateManager:
    """
    Manages pipeline execution state.
    Tracks completed steps, errors, and allows resuming from last successful step.
    """

    def __init__(self, state_file: Optional[Path] = None):
        """
        Initialize state manager.

        Args:
            state_file: Path to state file. If None, uses settings.STATE_FILE
        """
        self.state_file = state_file or settings.STATE_FILE
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        """Load state from file or create new state."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                logger.info(f"Loaded pipeline state from {self.state_file}")
                return state
            except Exception as e:
                logger.warning(f"Failed to load state file: {e}. Starting with fresh state.")

        return self._create_initial_state()

    def _create_initial_state(self) -> Dict[str, Any]:
        """Create initial state structure."""
        return {
            'pipeline_id': None,
            'started_at': None,
            'updated_at': None,
            'completed_at': None,
            'status': 'not_started',  # not_started, in_progress, completed, failed
            'current_step': None,
            'completed_steps': [],
            'failed_steps': [],
            'step_results': {},
            'errors': [],
            'metadata': {}
        }

    def _save_state(self):
        """Save current state to file."""
        try:
            self.state['updated_at'] = datetime.now().isoformat()
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2, default=str)
            logger.debug(f"Saved pipeline state to {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def start_pipeline(self, pipeline_id: str):
        """Mark pipeline as started."""
        self.state['pipeline_id'] = pipeline_id
        self.state['started_at'] = datetime.now().isoformat()
        self.state['status'] = 'in_progress'
        self._save_state()
        logger.info(f"Pipeline started: {pipeline_id}")

    def complete_pipeline(self):
        """Mark pipeline as completed."""
        self.state['completed_at'] = datetime.now().isoformat()
        self.state['status'] = 'completed'
        self.state['current_step'] = None
        self._save_state()
        logger.info("Pipeline completed successfully")

    def fail_pipeline(self, error: str):
        """Mark pipeline as failed."""
        self.state['status'] = 'failed'
        self.state['errors'].append({
            'timestamp': datetime.now().isoformat(),
            'error': str(error),
            'step': self.state.get('current_step')
        })
        self._save_state()
        logger.error(f"Pipeline failed: {error}")

    def start_step(self, step_name: str):
        """Mark a step as started."""
        self.state['current_step'] = step_name
        self.state['step_results'][step_name] = {
            'status': 'in_progress',
            'started_at': datetime.now().isoformat(),
            'completed_at': None,
            'record_count': None,
            'error': None
        }
        self._save_state()
        logger.info(f"Step started: {step_name}")

    def complete_step(self, step_name: str, record_count: Optional[int] = None, metadata: Optional[Dict] = None):
        """Mark a step as completed."""
        if step_name not in self.state['completed_steps']:
            self.state['completed_steps'].append(step_name)

        self.state['step_results'][step_name].update({
            'status': 'completed',
            'completed_at': datetime.now().isoformat(),
            'record_count': record_count,
            'metadata': metadata or {}
        })

        self._save_state()
        logger.info(f"Step completed: {step_name}")

    def fail_step(self, step_name: str, error: str):
        """Mark a step as failed."""
        if step_name not in self.state['failed_steps']:
            self.state['failed_steps'].append(step_name)

        self.state['step_results'][step_name].update({
            'status': 'failed',
            'completed_at': datetime.now().isoformat(),
            'error': str(error)
        })

        self.state['errors'].append({
            'timestamp': datetime.now().isoformat(),
            'step': step_name,
            'error': str(error)
        })

        self._save_state()
        logger.error(f"Step failed: {step_name} - {error}")

    def should_skip_step(self, step_name: str) -> bool:
        """Check if a step should be skipped (already completed)."""
        return step_name in self.state['completed_steps']

    def get_last_completed_step(self) -> Optional[str]:
        """Get the name of the last completed step."""
        if self.state['completed_steps']:
            return self.state['completed_steps'][-1]
        return None

    def get_step_result(self, step_name: str) -> Optional[Dict]:
        """Get the result of a specific step."""
        return self.state['step_results'].get(step_name)

    def get_status(self) -> str:
        """Get current pipeline status."""
        return self.state['status']

    def reset(self):
        """Reset state to initial values."""
        self.state = self._create_initial_state()
        self._save_state()
        logger.info("Pipeline state reset")

    def set_metadata(self, key: str, value: Any):
        """Set a metadata value."""
        self.state['metadata'][key] = value
        self._save_state()

    def get_metadata(self, key: str, default=None) -> Any:
        """Get a metadata value."""
        return self.state['metadata'].get(key, default)

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of pipeline execution."""
        return {
            'pipeline_id': self.state['pipeline_id'],
            'status': self.state['status'],
            'started_at': self.state['started_at'],
            'completed_at': self.state['completed_at'],
            'current_step': self.state['current_step'],
            'completed_steps': len(self.state['completed_steps']),
            'failed_steps': len(self.state['failed_steps']),
            'total_errors': len(self.state['errors'])
        }
