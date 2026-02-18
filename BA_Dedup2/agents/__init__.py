"""
Agents package for BA Deduplication pipeline.
Contains all agent implementations for the workflow.
"""
from agents.base_agent import BaseAgent
from agents.ingestion_agent import IngestionAgent
from agents.validation_agent import ValidationAgent
from agents.matching_agent import MatchingAgent
from agents.ai_matching_agent import AIMatchingAgent
from agents.hybrid_matching_agent import HybridMatchingAgent
from agents.merge_agent import MergeAgent
from agents.output_agent import OutputAgent

__all__ = [
    'BaseAgent',
    'IngestionAgent',
    'ValidationAgent',
    'MatchingAgent',
    'AIMatchingAgent',
    'HybridMatchingAgent',
    'MergeAgent',
    'OutputAgent'
]
