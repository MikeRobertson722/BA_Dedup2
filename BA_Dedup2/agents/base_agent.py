"""
Base agent class for the BA Deduplication pipeline.
All agents inherit from this class and implement the execute method.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd
from utils.logger import get_logger


class BaseAgent(ABC):
    """
    Abstract base class for all pipeline agents.
    Defines the common interface and lifecycle methods.
    """

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the agent.

        Args:
            name: Agent name for logging and identification
            config: Optional configuration dictionary
        """
        self.name = name
        self.config = config or {}
        self.logger = get_logger(f'agent.{name}')
        self.execution_count = 0
        self.error_count = 0

    @abstractmethod
    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Execute the agent's main logic.
        Must be implemented by all subclasses.

        Args:
            data: Input DataFrame to process

        Returns:
            Processed DataFrame

        Raises:
            Exception: If execution fails
        """
        pass

    def validate(self, result: pd.DataFrame) -> bool:
        """
        Validate the result of execution.
        Can be overridden by subclasses for custom validation.

        Args:
            result: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        if result is None:
            self.logger.error("Validation failed: result is None")
            return False

        if not isinstance(result, pd.DataFrame):
            self.logger.error(f"Validation failed: result is not a DataFrame (type: {type(result)})")
            return False

        if result.empty:
            self.logger.warning("Validation warning: result DataFrame is empty")
            # Empty is not necessarily invalid, so return True
            return True

        return True

    def on_error(self, error: Exception) -> None:
        """
        Handle errors during execution.
        Can be overridden by subclasses for custom error handling.

        Args:
            error: The exception that occurred
        """
        self.error_count += 1
        self.logger.error(f"Agent {self.name} encountered error: {error}", exc_info=True)

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Run the agent with error handling and validation.
        This is the main entry point that wraps execute().

        Args:
            data: Input DataFrame (can be None for agents like IngestionAgent)

        Returns:
            Processed DataFrame

        Raises:
            Exception: If execution or validation fails
        """
        try:
            self.logger.info(f"Executing agent: {self.name}")
            if data is not None:
                self.logger.debug(f"Input shape: {data.shape}")
            else:
                self.logger.debug("Input: None (will be loaded by agent)")

            # Execute main logic
            result = self.execute(data)

            # Validate result
            if not self.validate(result):
                raise ValueError(f"Agent {self.name} produced invalid result")

            self.execution_count += 1
            self.logger.info(f"Agent {self.name} completed successfully")
            self.logger.debug(f"Output shape: {result.shape}")

            return result

        except Exception as e:
            self.on_error(e)
            raise

    def get_stats(self) -> Dict[str, Any]:
        """
        Get agent execution statistics.

        Returns:
            Dictionary with execution stats
        """
        return {
            'name': self.name,
            'execution_count': self.execution_count,
            'error_count': self.error_count
        }

    def reset_stats(self):
        """Reset execution statistics."""
        self.execution_count = 0
        self.error_count = 0

    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}')"
