"""
Validation Skills - Reusable data validation and standardization functions.
Extracted from ValidationAgent for standalone use.
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
from utils.logger import get_logger
from utils.helpers import (
    normalize_string,
    normalize_address,
    normalize_phone,
    normalize_zip,
    remove_title,
    parse_name,
    extract_entity_type
)
from utils.security import tokenize_pii_fields

logger = get_logger(__name__)


def check_required_fields(
    df: pd.DataFrame,
    required_fields: List[str],
    drop_invalid: bool = False
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Check that all required fields are present and have values.

    Args:
        df: Input DataFrame
        required_fields: List of required field names
        drop_invalid: Whether to drop records with missing values

    Returns:
        Tuple of (DataFrame, list of validation errors)

    Example:
        df, errors = check_required_fields(df, ['name', 'address'], drop_invalid=True)
        if errors:
            print(f"Found {len(errors)} validation issues")
    """
    validation_errors = []

    # Check for missing columns
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        error_msg = f"Missing required fields: {missing_fields}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Check for records with missing required values
    for field in required_fields:
        null_mask = df[field].isna() | (df[field].astype(str).str.strip() == '')
        null_count = null_mask.sum()

        if null_count > 0:
            validation_errors.append(f"{null_count} records missing required field '{field}'")

            if drop_invalid:
                df = df[~null_mask]
                logger.warning(f"Dropped {null_count} records with missing {field}")

    return df, validation_errors


def add_optional_fields(df: pd.DataFrame, optional_fields: Dict[str, any]) -> pd.DataFrame:
    """
    Add optional fields with default values if not present.

    Args:
        df: Input DataFrame
        optional_fields: Dictionary of field names and default values
                        Example: {'status': 'active', 'priority': 0}

    Returns:
        DataFrame with optional fields added

    Example:
        optional_fields = {'status': 'active', 'notes': '', 'score': 0.0}
        df = add_optional_fields(df, optional_fields)
    """
    for field, default_value in optional_fields.items():
        if field not in df.columns:
            df[field] = default_value
            logger.debug(f"Added optional field '{field}' with default value: {default_value}")

    return df


def standardize_name(df: pd.DataFrame, name_column: str = 'name') -> pd.DataFrame:
    """
    Standardize name field - remove titles, normalize, parse components.

    Args:
        df: Input DataFrame
        name_column: Name of the column containing names

    Returns:
        DataFrame with additional name columns:
        - {name}_no_title: Name without title
        - {name}_title: Extracted title
        - {name}_normalized: Normalized name
        - {name}_first, {name}_middle, {name}_last, {name}_suffix: Name components
        - entity_type: Type of entity (individual, trust, business, etc.)

    Example:
        df = standardize_name(df, name_column='name')
        # Creates: name_no_title, name_title, name_normalized, name_first, etc.
    """
    if name_column not in df.columns:
        logger.warning(f"Column '{name_column}' not found, skipping name standardization")
        return df

    logger.info(f"Standardizing name field: {name_column}")

    # Remove titles (Dr, Mr, Mrs, etc.)
    df[f'{name_column}_no_title'] = df[name_column].apply(
        lambda x: remove_title(x)[0] if pd.notna(x) else ''
    )
    df[f'{name_column}_title'] = df[name_column].apply(
        lambda x: remove_title(x)[1] if pd.notna(x) else ''
    )

    # Normalize name (without title)
    df[f'{name_column}_normalized'] = df[f'{name_column}_no_title'].apply(
        lambda x: normalize_string(x, lowercase=False)
    )

    # Parse name components
    parsed_names = df[name_column].apply(parse_name)
    df[f'{name_column}_first'] = parsed_names.apply(
        lambda x: x.get('first', '') if isinstance(x, dict) else ''
    )
    df[f'{name_column}_middle'] = parsed_names.apply(
        lambda x: x.get('middle', '') if isinstance(x, dict) else ''
    )
    df[f'{name_column}_last'] = parsed_names.apply(
        lambda x: x.get('last', '') if isinstance(x, dict) else ''
    )
    df[f'{name_column}_suffix'] = parsed_names.apply(
        lambda x: x.get('suffix', '') if isinstance(x, dict) else ''
    )

    # Identify entity type
    df['entity_type'] = df[name_column].apply(extract_entity_type)

    logger.debug(f"Created {7} name-related columns")

    return df


def standardize_address(df: pd.DataFrame, address_column: str = 'address') -> pd.DataFrame:
    """
    Standardize address field.

    Args:
        df: Input DataFrame
        address_column: Name of the column containing addresses

    Returns:
        DataFrame with {address}_normalized column

    Example:
        df = standardize_address(df, address_column='address')
        # Creates: address_normalized
    """
    if address_column not in df.columns:
        logger.warning(f"Column '{address_column}' not found, skipping address standardization")
        return df

    logger.info(f"Standardizing address field: {address_column}")
    df[f'{address_column}_normalized'] = df[address_column].apply(normalize_address)

    return df


def standardize_phone(df: pd.DataFrame, phone_column: str = 'phone') -> pd.DataFrame:
    """
    Standardize phone field to digits only.

    Args:
        df: Input DataFrame
        phone_column: Name of the column containing phone numbers

    Returns:
        DataFrame with {phone}_normalized column

    Example:
        df = standardize_phone(df, phone_column='phone')
        # Creates: phone_normalized (digits only)
    """
    if phone_column not in df.columns:
        logger.warning(f"Column '{phone_column}' not found, skipping phone standardization")
        return df

    logger.info(f"Standardizing phone field: {phone_column}")
    df[f'{phone_column}_normalized'] = df[phone_column].apply(normalize_phone)

    return df


def standardize_zip(df: pd.DataFrame, zip_column: str = 'zip') -> pd.DataFrame:
    """
    Standardize ZIP code to 5 digits.

    Args:
        df: Input DataFrame
        zip_column: Name of the column containing ZIP codes

    Returns:
        DataFrame with {zip}_normalized column

    Example:
        df = standardize_zip(df, zip_column='zip')
        # Creates: zip_normalized (5 digits)
    """
    if zip_column not in df.columns:
        logger.warning(f"Column '{zip_column}' not found, skipping ZIP standardization")
        return df

    logger.info(f"Standardizing ZIP field: {zip_column}")
    df[f'{zip_column}_normalized'] = df[zip_column].apply(normalize_zip)

    return df


def standardize_email(df: pd.DataFrame, email_column: str = 'email') -> pd.DataFrame:
    """
    Standardize email to lowercase.

    Args:
        df: Input DataFrame
        email_column: Name of the column containing email addresses

    Returns:
        DataFrame with standardized email (in-place modification)

    Example:
        df = standardize_email(df, email_column='email')
        # Modifies email column to lowercase
    """
    if email_column not in df.columns:
        logger.warning(f"Column '{email_column}' not found, skipping email standardization")
        return df

    logger.info(f"Standardizing email field: {email_column}")
    df[email_column] = df[email_column].apply(
        lambda x: str(x).strip().lower() if pd.notna(x) and '@' in str(x) else ''
    )

    return df


def parse_name_components(df: pd.DataFrame, name_column: str = 'name') -> pd.DataFrame:
    """
    Parse name into components (first, middle, last, suffix).
    Alias for standardize_name for backward compatibility.

    Args:
        df: Input DataFrame
        name_column: Name of the column containing names

    Returns:
        DataFrame with name component columns

    Example:
        df = parse_name_components(df, name_column='name')
    """
    return standardize_name(df, name_column)


def validate_data_quality(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """
    Validate data quality and flag suspicious records.

    Args:
        df: Input DataFrame

    Returns:
        Tuple of (DataFrame, list of validation warnings)

    Example:
        df, warnings = validate_data_quality(df)
        if warnings:
            for warning in warnings:
                print(f"Warning: {warning}")
    """
    validation_errors = []

    # Check for suspiciously short names
    if 'name' in df.columns:
        short_names = df['name'].astype(str).str.len() < 3
        if short_names.any():
            validation_errors.append(f"{short_names.sum()} records with very short names")

    # Check for invalid email formats
    if 'email' in df.columns:
        has_email = df['email'].notna() & (df['email'].astype(str).str.strip() != '')
        invalid_email = has_email & ~df['email'].astype(str).str.contains('@', na=False)
        if invalid_email.any():
            validation_errors.append(f"{invalid_email.sum()} records with invalid email format")

    # Check for invalid ZIP codes
    if 'zip_normalized' in df.columns:
        has_zip = df['zip_normalized'].notna() & (df['zip_normalized'].astype(str).str.strip() != '')
        invalid_zip = has_zip & (df['zip_normalized'].astype(str).str.len() < 5)
        if invalid_zip.any():
            validation_errors.append(f"{invalid_zip.sum()} records with invalid ZIP code")

    # Check for invalid phone numbers
    if 'phone_normalized' in df.columns:
        has_phone = df['phone_normalized'].notna() & (df['phone_normalized'].astype(str).str.strip() != '')
        invalid_phone = has_phone & (df['phone_normalized'].astype(str).str.len() < 10)
        if invalid_phone.any():
            validation_errors.append(f"{invalid_phone.sum()} records with invalid phone number")

    return df, validation_errors


def remove_exact_duplicates(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Remove exact duplicate records.

    Args:
        df: Input DataFrame

    Returns:
        Tuple of (DataFrame without duplicates, number of duplicates removed)

    Example:
        df, removed_count = remove_exact_duplicates(df)
        print(f"Removed {removed_count} exact duplicates")
    """
    initial_count = len(df)
    df = df.drop_duplicates()
    removed_count = initial_count - len(df)

    if removed_count > 0:
        logger.info(f"Removed {removed_count} exact duplicate records")

    return df, removed_count


def validate_all(
    df: pd.DataFrame,
    required_fields: List[str],
    optional_fields: Dict[str, any] = None,
    drop_invalid: bool = False,
    standardize_all: bool = True
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Comprehensive validation and standardization pipeline.

    Args:
        df: Input DataFrame
        required_fields: List of required field names
        optional_fields: Optional fields with default values
        drop_invalid: Whether to drop invalid records
        standardize_all: Whether to standardize all fields

    Returns:
        Tuple of (validated DataFrame, list of validation errors/warnings)

    Example:
        df, errors = validate_all(
            df,
            required_fields=['name', 'address'],
            optional_fields={'status': 'active'},
            drop_invalid=True,
            standardize_all=True
        )
    """
    all_errors = []

    logger.info(f"Starting validation of {len(df)} records")

    # Check required fields
    df, errors = check_required_fields(df, required_fields, drop_invalid)
    all_errors.extend(errors)

    # Add optional fields
    if optional_fields:
        df = add_optional_fields(df, optional_fields)

    # Standardize all fields if requested
    if standardize_all:
        df = standardize_name(df)
        df = standardize_address(df)
        df = standardize_phone(df)
        df = standardize_zip(df)
        df = standardize_email(df)

        # Tokenize PII fields
        df = tokenize_pii_fields(df)

    # Validate data quality
    df, quality_errors = validate_data_quality(df)
    all_errors.extend(quality_errors)

    # Remove exact duplicates
    df, duplicates_removed = remove_exact_duplicates(df)
    if duplicates_removed > 0:
        all_errors.append(f"Removed {duplicates_removed} exact duplicate records")

    logger.info(f"Validation complete: {len(df)} valid records")
    if all_errors:
        logger.warning(f"Found {len(all_errors)} validation issues")

    return df, all_errors
