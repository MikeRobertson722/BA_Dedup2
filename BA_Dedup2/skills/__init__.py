"""
Reusable skills extracted from BA deduplication agents.
Each skill is a standalone, callable function or class that can be used independently.
"""

# Ingestion skills
from .ingestion_skills import (
    read_csv_file,
    read_excel_file,
    read_database_table,
    apply_field_mappings,
    normalize_column_names
)

# Validation skills
from .validation_skills import (
    check_required_fields,
    add_optional_fields,
    standardize_name,
    standardize_address,
    standardize_phone,
    standardize_zip,
    standardize_email,
    parse_name_components,
    extract_entity_type,
    validate_data_quality,
    remove_exact_duplicates
)

# Matching skills
from .matching_skills import (
    generate_candidate_pairs,
    calculate_similarity_scores,
    cluster_duplicates,
    fuzzy_match_names,
    fuzzy_match_addresses,
    apply_blocking_strategy
)

# Merge skills
from .merge_skills import (
    create_golden_record,
    merge_cluster,
    select_best_values,
    calculate_completeness_score,
    merge_all_clusters
)

# Output skills
from .output_skills import (
    generate_summary_report,
    export_golden_records,
    export_duplicate_report,
    create_excel_report,
    generate_statistics
)

__all__ = [
    # Ingestion
    'read_csv_file',
    'read_excel_file',
    'read_database_table',
    'apply_field_mappings',
    'normalize_column_names',

    # Validation
    'check_required_fields',
    'add_optional_fields',
    'standardize_name',
    'standardize_address',
    'standardize_phone',
    'standardize_zip',
    'standardize_email',
    'parse_name_components',
    'extract_entity_type',
    'validate_data_quality',
    'remove_exact_duplicates',

    # Matching
    'generate_candidate_pairs',
    'calculate_similarity_scores',
    'cluster_duplicates',
    'fuzzy_match_names',
    'fuzzy_match_addresses',
    'apply_blocking_strategy',

    # Merge
    'create_golden_record',
    'merge_cluster',
    'select_best_values',
    'calculate_completeness_score',
    'merge_all_clusters',

    # Output
    'generate_summary_report',
    'export_golden_records',
    'export_duplicate_report',
    'create_excel_report',
    'generate_statistics',
]
