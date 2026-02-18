"""
Keywords that trigger human review requirement.
Records containing these keywords should be manually reviewed before merging.
"""

# Keywords that require human review (case-insensitive)
HUMAN_REVIEW_KEYWORDS = [
    # Legal entity types that need careful handling
    'TRUST',
    'TRUSTEE',
    'TR ',  # abbreviation for trust
    'ESTATE',

    # Organizational units that might be duplicated across companies
    'DEPARTMENT',
    'DEPT',
    'DIVISION',
    'DIV',

    # Add more keywords here as needed
    # Examples:
    # 'FOUNDATION',
    # 'CHARITY',
    # 'EXECUTOR',
    # 'ADMINISTRATOR',
]

# Reason codes for why review is needed
REVIEW_REASONS = {
    'TRUST': 'Trust entities require verification - similar trust names may belong to different beneficiaries',
    'TRUSTEE': 'Trustee roles require verification - multiple trustees may exist for same entity',
    'ESTATE': 'Estate entities require verification - similar estate names may belong to different individuals',
    'DEPARTMENT': 'Department names may be duplicated across different organizations',
    'DIVISION': 'Division names may be duplicated across different organizations',
}

def get_review_reason(keyword):
    """Get the reason why a keyword requires human review."""
    keyword_upper = keyword.upper().strip()
    return REVIEW_REASONS.get(keyword_upper, f'Contains keyword "{keyword}" which requires human review')
