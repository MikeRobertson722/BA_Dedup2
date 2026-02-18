"""
Security utilities for PII data handling.
Includes SSN tokenization, hashing, and other security functions.
"""
import hashlib
import os
import re
from typing import Optional
import pandas as pd

# Salt for hashing - CHANGE THIS IN PRODUCTION!
# In production, store in environment variable or secure key vault
SSN_SALT = os.getenv('SSN_SALT', 'CHANGE_THIS_SECRET_SALT_IN_PRODUCTION_123456')


def tokenize_ssn(ssn: Optional[str]) -> str:
    """
    Hash SSN with salt for privacy while maintaining matching capability.

    Uses SHA256 with salt to create a one-way hash of SSN.
    Same SSN always produces same hash, enabling matching.
    Original SSN cannot be recovered from hash.

    Args:
        ssn: Raw SSN (xxx-xx-xxxx or xxxxxxxxx)

    Returns:
        Hex-encoded SHA256 hash (empty string if invalid)
    """
    if pd.isna(ssn) or not ssn:
        return ''

    # Normalize to 9 digits only
    ssn_norm = clean_ssn(ssn)

    if not ssn_norm or len(ssn_norm) != 9:
        return ''

    # Hash with salt
    salted = f"{ssn_norm}|{SSN_SALT}"
    hashed = hashlib.sha256(salted.encode()).hexdigest()

    return hashed


def clean_ssn(ssn: Optional[str]) -> str:
    """
    Clean and normalize SSN to 9 digits.

    Removes all non-digit characters.
    Validates that result is exactly 9 digits.

    Args:
        ssn: Raw SSN string (any format)

    Returns:
        9-digit SSN string, or empty string if invalid
    """
    if pd.isna(ssn) or not ssn:
        return ''

    # Extract only digits
    digits = re.sub(r'\D', '', str(ssn))

    # Validate length
    if len(digits) != 9:
        return ''

    # Basic validation: SSN can't be all zeros, all same digit, or certain ranges
    if digits == '000000000':
        return ''
    if len(set(digits)) == 1:  # All same digit
        return ''
    if digits.startswith('000') or digits.startswith('666') or digits.startswith('9'):
        return ''  # Invalid SSN area numbers

    return digits


def validate_ssn(ssn: Optional[str]) -> bool:
    """
    Validate if SSN is in valid format.

    Args:
        ssn: SSN string to validate

    Returns:
        True if valid SSN format, False otherwise
    """
    cleaned = clean_ssn(ssn)
    return len(cleaned) == 9


def tokenize_ein(ein: Optional[str]) -> str:
    """
    Hash EIN (Employer Identification Number) with salt.

    Similar to SSN tokenization but for business tax IDs.

    Args:
        ein: Raw EIN (xx-xxxxxxx or xxxxxxxxx)

    Returns:
        Hex-encoded SHA256 hash
    """
    if pd.isna(ein) or not ein:
        return ''

    # Normalize to 9 digits only
    ein_digits = re.sub(r'\D', '', str(ein))

    if len(ein_digits) != 9:
        return ''

    # Hash with salt
    salted = f"{ein_digits}|{SSN_SALT}"
    hashed = hashlib.sha256(salted.encode()).hexdigest()

    return hashed


def mask_ssn(ssn: Optional[str]) -> str:
    """
    Mask SSN for display purposes (e.g., "XXX-XX-1234").

    Shows only last 4 digits, masks the rest.
    Use this for logging, UI display, etc.

    Args:
        ssn: Raw SSN

    Returns:
        Masked SSN string
    """
    cleaned = clean_ssn(ssn)

    if not cleaned:
        return ''

    # Show last 4 digits only
    return f"XXX-XX-{cleaned[-4:]}"


def tokenize_pii_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tokenize all PII fields in DataFrame.

    Adds tokenized columns (ssn_token, ein_token) while preserving
    masked versions for display.

    Args:
        df: DataFrame with PII fields

    Returns:
        DataFrame with tokenized columns added
    """
    df = df.copy()

    # Tokenize SSN if present
    if 'ssn' in df.columns:
        df['ssn_token'] = df['ssn'].apply(tokenize_ssn)
        df['ssn_masked'] = df['ssn'].apply(mask_ssn)
        # Drop raw SSN for security (optional - uncomment if desired)
        # df = df.drop(columns=['ssn'])

    # Tokenize EIN if present
    if 'ein' in df.columns:
        df['ein_token'] = df['ein'].apply(tokenize_ein)

    return df
