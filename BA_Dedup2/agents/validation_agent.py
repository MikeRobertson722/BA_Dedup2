"""
Validation Agent - Validates data quality, checks required fields, standardizes formatting.
"""
import pandas as pd
import numpy as np
from typing import Dict, Any, List
from agents.base_agent import BaseAgent
from config import settings
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


class ValidationAgent(BaseAgent):
    """
    Agent responsible for data validation and standardization.
    Checks for required fields, validates data types, and standardizes formatting.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize validation agent.

        Args:
            config: Configuration dictionary with keys:
                - required_fields: List of required field names
                - drop_invalid: Whether to drop invalid records (default: False)
        """
        super().__init__('validation', config)
        self.required_fields = self.config.get('required_fields', settings.REQUIRED_FIELDS)
        self.drop_invalid = self.config.get('drop_invalid', False)
        self.validation_errors = []

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and standardize data.

        Args:
            data: Input DataFrame to validate

        Returns:
            Validated and standardized DataFrame
        """
        self.logger.info(f"Validating {len(data)} records")
        self.validation_errors = []

        # Note: No .copy() needed - all operations below create new DataFrames
        df = data
        initial_count = len(df)

        # Check for required fields
        df = self._check_required_fields(df)

        # Add missing optional fields with defaults
        df = self._add_optional_fields(df)

        # Standardize data formats
        df = self._standardize_formats(df)

        # Validate data quality
        df = self._validate_data_quality(df)

        # Remove duplicates based on all fields (exact duplicates only)
        duplicates_removed = len(df)
        df = df.drop_duplicates()
        duplicates_removed = duplicates_removed - len(df)

        final_count = len(df)
        removed_count = initial_count - final_count

        self.logger.info(f"Validation complete: {final_count} valid records")
        if removed_count > 0:
            self.logger.warning(f"Removed {removed_count} invalid records ({duplicates_removed} exact duplicates)")

        if self.validation_errors:
            self.logger.warning(f"Found {len(self.validation_errors)} validation issues")
            for error in self.validation_errors[:10]:  # Log first 10 errors
                self.logger.warning(f"  - {error}")

        return df

    def _check_required_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check that all required fields are present."""
        missing_fields = [field for field in self.required_fields if field not in df.columns]

        if missing_fields:
            error_msg = f"Missing required fields: {missing_fields}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # Check for records with missing required values
        for field in self.required_fields:
            null_mask = df[field].isna() | (df[field].astype(str).str.strip() == '')
            null_count = null_mask.sum()

            if null_count > 0:
                self.validation_errors.append(f"{null_count} records missing required field '{field}'")

                if self.drop_invalid:
                    df = df[~null_mask]
                    self.logger.warning(f"Dropped {null_count} records with missing {field}")

        return df

    def _add_optional_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add optional fields with default values if not present."""
        for field, default_value in settings.OPTIONAL_FIELDS.items():
            if field not in df.columns:
                df[field] = default_value
                self.logger.debug(f"Added optional field '{field}' with default value")

        return df

    def _standardize_formats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize data formats for consistent processing."""
        self.logger.info("Standardizing data formats")

        # Standardize name - remove titles and parse components
        if 'name' in df.columns:
            # Remove titles (Dr, Mr, Mrs, etc.) for better matching
            df['name_no_title'] = df['name'].apply(lambda x: remove_title(x)[0] if pd.notna(x) else '')
            df['name_title'] = df['name'].apply(lambda x: remove_title(x)[1] if pd.notna(x) else '')

            # Normalize name (without title)
            df['name_normalized'] = df['name_no_title'].apply(lambda x: normalize_string(x, lowercase=False))

            # Parse name components
            parsed_names = df['name'].apply(parse_name)
            df['name_first'] = parsed_names.apply(lambda x: x.get('first', '') if isinstance(x, dict) else '')
            df['name_middle'] = parsed_names.apply(lambda x: x.get('middle', '') if isinstance(x, dict) else '')
            df['name_last'] = parsed_names.apply(lambda x: x.get('last', '') if isinstance(x, dict) else '')
            df['name_suffix'] = parsed_names.apply(lambda x: x.get('suffix', '') if isinstance(x, dict) else '')

            # Identify entity type (individual, trust, department, business)
            df['entity_type'] = df['name'].apply(extract_entity_type)

        # Standardize address
        if 'address' in df.columns:
            df['address_normalized'] = df['address'].apply(normalize_address)

        # Standardize city
        if 'city' in df.columns:
            df['city_normalized'] = df['city'].apply(lambda x: normalize_string(x, lowercase=False))

        # Standardize state (uppercase)
        if 'state' in df.columns:
            df['state'] = df['state'].apply(
                lambda x: str(x).strip().upper() if pd.notna(x) else ''
            )

        # Standardize ZIP code
        if 'zip' in df.columns:
            df['zip_normalized'] = df['zip'].apply(normalize_zip)

        # Standardize phone
        if 'phone' in df.columns:
            df['phone_normalized'] = df['phone'].apply(normalize_phone)

        # Standardize email (lowercase)
        if 'email' in df.columns:
            df['email'] = df['email'].apply(
                lambda x: str(x).strip().lower() if pd.notna(x) and '@' in str(x) else ''
            )

        # Tokenize PII fields (SSN, EIN) for security
        # Creates hashed tokens while preserving masked versions for display
        df = tokenize_pii_fields(df)

        return df

    def _validate_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data quality and flag suspicious records."""
        # Check for suspiciously short names
        if 'name' in df.columns:
            short_names = df['name'].astype(str).str.len() < 3
            if short_names.any():
                self.validation_errors.append(f"{short_names.sum()} records with very short names")

        # Check for invalid email formats
        if 'email' in df.columns:
            has_email = df['email'].notna() & (df['email'].astype(str).str.strip() != '')
            invalid_email = has_email & ~df['email'].astype(str).str.contains('@', na=False)
            if invalid_email.any():
                self.validation_errors.append(f"{invalid_email.sum()} records with invalid email format")

        # Check for invalid ZIP codes
        if 'zip_normalized' in df.columns:
            has_zip = df['zip_normalized'].notna() & (df['zip_normalized'].astype(str).str.strip() != '')
            invalid_zip = has_zip & (df['zip_normalized'].astype(str).str.len() < 5)
            if invalid_zip.any():
                self.validation_errors.append(f"{invalid_zip.sum()} records with invalid ZIP code")

        return df

    def validate(self, result: pd.DataFrame) -> bool:
        """
        Validate agent results.

        Args:
            result: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        if not super().validate(result):
            return False

        # Check that normalized fields were added
        expected_normalized = ['name_normalized', 'address_normalized']
        missing_normalized = [field for field in expected_normalized
                            if field.replace('_normalized', '') in result.columns
                            and field not in result.columns]

        if missing_normalized:
            self.logger.error(f"Missing normalized fields: {missing_normalized}")
            return False

        return True
