"""Snowflake connection helper for BA Dedup2 pipeline."""
import os


def get_snowflake_connection():
    """Connect to Snowflake using browser-based SSO.

    Reads connection params from environment variables (loaded from .env).
    Opens a browser window for Microsoft SSO authentication.
    Returns an open snowflake.connector connection.
    """
    import snowflake.connector

    params = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        'role': os.getenv('SNOWFLAKE_ROLE'),
        'authenticator': 'externalbrowser',
        'client_store_temporary_credential': True,
    }
    # Remove empty/None values
    params = {k: v for k, v in params.items() if v}

    return snowflake.connector.connect(**params)
