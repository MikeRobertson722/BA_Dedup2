"""
Data access layer for BA Deduplication pipeline.
Handles reading from and writing to various data sources.
"""
from data.db_connector import DatabaseConnector
from data.file_reader import FileReader
from data.table_writer import TableWriter

__all__ = ['DatabaseConnector', 'FileReader', 'TableWriter']
