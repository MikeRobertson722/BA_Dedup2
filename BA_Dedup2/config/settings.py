"""
Configuration settings for BA Deduplication pipeline.
Loads from environment variables with sensible defaults.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Database Configuration
DB_TYPE = os.getenv('DB_TYPE', 'sqlite')
DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING', 'sqlite:///ba_dedup.db')

# Input Configuration
INPUT_TYPE = os.getenv('INPUT_TYPE', 'csv')  # csv, excel, database
INPUT_PATH = os.getenv('INPUT_PATH', 'input/sample_data.csv')
INPUT_TABLE = os.getenv('INPUT_TABLE', 'business_associates_raw')

# Output Configuration
OUTPUT_TABLE = os.getenv('OUTPUT_TABLE', 'business_associates_deduplicated')
OUTPUT_AUDIT_TABLE = os.getenv('OUTPUT_AUDIT_TABLE', 'ba_dedup_audit')

# Deduplication Settings
SIMILARITY_THRESHOLD = float(os.getenv('SIMILARITY_THRESHOLD', '0.85'))
MATCH_FIELDS = os.getenv('MATCH_FIELDS', 'name,address,city,state,zip').split(',')
MERGE_STRATEGY = os.getenv('MERGE_STRATEGY', 'most_complete')  # most_complete, most_recent, manual

# Processing Settings
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# AI-Powered Matching Settings
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY', '')
AI_MATCHING_ENABLED = os.getenv('AI_MATCHING_ENABLED', 'false').lower() == 'true'
AI_MODEL = os.getenv('AI_MODEL', 'claude-sonnet-4-20250514')

# Field Mapping (optional - for source data with different column names)
FIELD_MAP = {}
field_map_str = os.getenv('FIELD_MAP')
if field_map_str:
    import json
    FIELD_MAP = json.loads(field_map_str)

# Required fields for BA records
REQUIRED_FIELDS = ['name', 'address']

# Optional fields with defaults
OPTIONAL_FIELDS = {
    'city': '',
    'state': '',
    'zip': '',
    'phone': '',
    'email': '',
    'contact_person': '',
    'notes': ''
}

# State management
STATE_FILE = PROJECT_ROOT / 'state' / 'pipeline_state.json'

# Logging configuration
LOG_DIR = PROJECT_ROOT / 'logs'
LOG_FILE = LOG_DIR / 'ba_dedup.log'

# Performance & Optimization Settings (Priority 4)
# Caching
ENABLE_CACHING = os.getenv('ENABLE_CACHING', 'true').lower() == 'true'
NORMALIZATION_CACHE_SIZE = int(os.getenv('NORMALIZATION_CACHE_SIZE', '10000'))
FUZZY_MATCH_CACHE_SIZE = int(os.getenv('FUZZY_MATCH_CACHE_SIZE', '50000'))

# Parallel Processing
ENABLE_PARALLEL = os.getenv('ENABLE_PARALLEL', 'false').lower() == 'true'
N_JOBS = int(os.getenv('N_JOBS', '-1'))  # -1 = use all cores

# Chunking for large datasets
ENABLE_CHUNKING = os.getenv('ENABLE_CHUNKING', 'true').lower() == 'true'
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '10000'))

# Data type optimization
OPTIMIZE_DTYPES = os.getenv('OPTIMIZE_DTYPES', 'true').lower() == 'true'

# Query profiling (debugging only - adds overhead)
ENABLE_QUERY_PROFILING = os.getenv('ENABLE_QUERY_PROFILING', 'false').lower() == 'true'
SLOW_QUERY_THRESHOLD = float(os.getenv('SLOW_QUERY_THRESHOLD', '1.0'))

# Database path for migrations
DATABASE_PATH = os.getenv('DATABASE_PATH', str(PROJECT_ROOT / 'ba_dedup.db'))

# Smart Blocking Settings (Priority 4 optimization)
MAX_MISSING_DATA_PAIRS = int(os.getenv('MAX_MISSING_DATA_PAIRS', '50000'))  # Cap for missing data fallback
