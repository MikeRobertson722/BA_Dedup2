"""
Workflow Engine - Orchestrates agent execution in sequence.
Manages data handoff, error handling, and state persistence.
"""
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd

from agents import (
    IngestionAgent,
    ValidationAgent,
    MatchingAgent,
    AIMatchingAgent,
    HybridMatchingAgent,
    MergeAgent,
    OutputAgent
)
from state.state_manager import StateManager
from utils.logger import PipelineLogger
from config import settings


class WorkflowEngine:
    """
    Orchestrates the execution of agents in a defined workflow.
    Manages data flow, error handling, retries, and state persistence.
    """

    # Agent registry maps agent names to classes
    AGENT_REGISTRY = {
        'ingestion': IngestionAgent,
        'validation': ValidationAgent,
        'matching': MatchingAgent,
        'ai_matching': AIMatchingAgent,
        'hybrid_matching': HybridMatchingAgent,
        'merge': MergeAgent,
        'output': OutputAgent
    }

    def __init__(self,
                 workflow_definition: Optional[Dict[str, Any]] = None,
                 workflow_file: Optional[str] = None,
                 state_manager: Optional[StateManager] = None):
        """
        Initialize workflow engine.

        Args:
            workflow_definition: Workflow definition dictionary
            workflow_file: Path to workflow definition JSON file
            state_manager: Optional state manager instance
        """
        self.logger = PipelineLogger('ba_dedup_workflow')

        # Load workflow definition
        if workflow_definition:
            self.workflow_def = workflow_definition
        elif workflow_file:
            self.workflow_def = self._load_workflow_file(workflow_file)
        else:
            # Use default workflow
            default_file = Path(__file__).parent / 'definitions' / 'data_pipeline.json'
            self.workflow_def = self._load_workflow_file(str(default_file))

        # Initialize state manager
        self.state_manager = state_manager or StateManager()

        # Initialize agents
        self.agents = self._initialize_agents()

        # Workflow execution data
        self.current_data = None
        self.step_results = {}

    def _load_workflow_file(self, file_path: str) -> Dict[str, Any]:
        """Load workflow definition from JSON file."""
        try:
            with open(file_path, 'r') as f:
                definition = json.load(f)

            self.logger.logger.info(f"Loaded workflow definition from {file_path}")
            return definition

        except Exception as e:
            self.logger.log_error(f"Failed to load workflow file: {e}")
            raise

    def _initialize_agents(self) -> Dict[str, Any]:
        """Initialize all agents defined in the workflow."""
        agents = {}

        for step in self.workflow_def.get('steps', []):
            agent_name = step['agent']
            agent_config = step.get('config', {})

            if agent_name not in self.AGENT_REGISTRY:
                raise ValueError(f"Unknown agent type: {agent_name}")

            # Create agent instance
            agent_class = self.AGENT_REGISTRY[agent_name]
            agent = agent_class(config=agent_config)

            agents[step['name']] = {
                'agent': agent,
                'config': step
            }

            self.logger.logger.debug(f"Initialized agent: {step['name']}")

        return agents

    def run(self, initial_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Execute the complete workflow.

        Args:
            initial_data: Optional initial data (if not using ingestion agent)

        Returns:
            Final processed DataFrame

        Raises:
            Exception: If workflow execution fails
        """
        try:
            # Start pipeline
            pipeline_id = self.workflow_def.get('name', 'ba_dedup')
            self.logger.start_pipeline()
            self.state_manager.start_pipeline(pipeline_id)

            self.current_data = initial_data

            # Execute each step in sequence
            for step in self.workflow_def.get('steps', []):
                step_name = step['name']

                # Check if step should be skipped (already completed)
                if self.state_manager.should_skip_step(step_name):
                    self.logger.log_agent_execution(
                        step_name,
                        "Skipped (already completed)"
                    )
                    continue

                # Execute step
                try:
                    self.current_data = self._execute_step(step_name)
                except Exception as e:
                    self.state_manager.fail_step(step_name, str(e))
                    raise

            # Complete pipeline
            self.state_manager.complete_pipeline()
            self.logger.end_pipeline(success=True)

            return self.current_data

        except Exception as e:
            self.logger.log_error(e)
            self.state_manager.fail_pipeline(str(e))
            self.logger.end_pipeline(success=False)
            raise

    def _execute_step(self, step_name: str) -> pd.DataFrame:
        """
        Execute a single workflow step.

        Args:
            step_name: Name of the step to execute

        Returns:
            Processed DataFrame

        Raises:
            Exception: If step execution fails
        """
        if step_name not in self.agents:
            raise ValueError(f"Unknown step: {step_name}")

        agent_info = self.agents[step_name]
        agent = agent_info['agent']
        step_config = agent_info['config']

        # Start step
        self.logger.start_step(step_name)
        self.state_manager.start_step(step_name)

        try:
            # Execute agent
            self.logger.log_agent_execution(
                agent.name,
                "Starting execution"
            )

            result = agent.run(self.current_data)

            # Store step result
            self.step_results[step_name] = result
            record_count = len(result) if isinstance(result, pd.DataFrame) else None

            # Log statistics
            if record_count is not None:
                self.logger.log_data_stats(step_name, {
                    'records': record_count,
                    'columns': len(result.columns) if hasattr(result, 'columns') else 0
                })

            # Complete step
            self.state_manager.complete_step(step_name, record_count)
            self.logger.end_step(step_name, record_count, success=True)

            return result

        except Exception as e:
            self.logger.log_error(e, context=f"Step: {step_name}")
            self.logger.end_step(step_name, success=False)
            raise

    def get_step_result(self, step_name: str) -> Optional[pd.DataFrame]:
        """
        Get the result of a specific step.

        Args:
            step_name: Name of the step

        Returns:
            DataFrame result or None if step not executed
        """
        return self.step_results.get(step_name)

    def get_agent(self, step_name: str):
        """
        Get an agent instance by step name.

        Args:
            step_name: Name of the step

        Returns:
            Agent instance
        """
        if step_name in self.agents:
            return self.agents[step_name]['agent']
        return None

    def reset(self):
        """Reset workflow state and data."""
        self.current_data = None
        self.step_results = {}
        self.state_manager.reset()
        self.logger.logger.info("Workflow reset")

    def get_summary(self) -> Dict[str, Any]:
        """
        Get workflow execution summary.

        Returns:
            Dictionary with execution summary
        """
        return {
            'workflow': self.workflow_def.get('name'),
            'state': self.state_manager.get_summary(),
            'steps_executed': len(self.step_results),
            'agent_stats': {
                step_name: agent_info['agent'].get_stats()
                for step_name, agent_info in self.agents.items()
            }
        }
