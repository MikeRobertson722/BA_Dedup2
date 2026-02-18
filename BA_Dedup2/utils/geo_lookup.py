"""
Geographic Lookup Utilities - ZIP/City/State lookups and enrichment.
Fills in missing geographic data using known relationships.
"""
import pandas as pd
from typing import Optional, Dict, Tuple
from utils.logger import get_logger

logger = get_logger(__name__)


class GeoLookup:
    """
    Handles geographic data lookups and enrichment.
    Fills in missing ZIP codes, cities, or states based on available data.
    """

    def __init__(self):
        """Initialize geo lookup with US ZIP code database."""
        self.zip_db = self._load_zip_database()
        logger.info(f"Loaded {len(self.zip_db)} ZIP code records")

    def _load_zip_database(self) -> pd.DataFrame:
        """
        Load US ZIP code database.
        In production, this would load from a comprehensive ZIP database.
        For now, creates a basic lookup from common patterns.
        """
        # Basic ZIP database - in production, load from uszipcode or similar
        # Format: zip, city, state, county
        zip_data = [
            # Illinois
            ('62701', 'Springfield', 'IL', 'Sangamon'),
            ('62702', 'Springfield', 'IL', 'Sangamon'),
            ('60601', 'Chicago', 'IL', 'Cook'),
            ('60602', 'Chicago', 'IL', 'Cook'),

            # Texas
            ('77459', 'Missouri City', 'TX', 'Fort Bend'),
            ('77489', 'Missouri City', 'TX', 'Fort Bend'),
            ('77001', 'Houston', 'TX', 'Harris'),
            ('75001', 'Dallas', 'TX', 'Dallas'),

            # California
            ('90001', 'Los Angeles', 'CA', 'Los Angeles'),
            ('94102', 'San Francisco', 'CA', 'San Francisco'),

            # New York
            ('10001', 'New York', 'NY', 'New York'),
            ('10002', 'New York', 'NY', 'New York'),

            # Common cities
            ('30301', 'Atlanta', 'GA', 'Fulton'),
            ('33101', 'Miami', 'FL', 'Miami-Dade'),
            ('02101', 'Boston', 'MA', 'Suffolk'),
            ('98101', 'Seattle', 'WA', 'King'),
        ]

        df = pd.DataFrame(zip_data, columns=['zip', 'city', 'state', 'county'])

        # Normalize for matching
        df['city_normalized'] = df['city'].str.lower().str.strip()
        df['state_normalized'] = df['state'].str.upper().str.strip()

        return df

    def lookup_zip_from_city_state(self, city: Optional[str],
                                   state: Optional[str]) -> Optional[str]:
        """
        Lookup ZIP code given city and state.

        Args:
            city: City name
            state: State code (2-letter)

        Returns:
            ZIP code if found, None otherwise
        """
        if not city or not state:
            return None

        # Normalize inputs
        city_norm = str(city).lower().strip()
        state_norm = str(state).upper().strip()

        # Query database
        matches = self.zip_db[
            (self.zip_db['city_normalized'] == city_norm) &
            (self.zip_db['state_normalized'] == state_norm)
        ]

        if len(matches) > 0:
            # Return first matching ZIP (most common for that city)
            zip_code = matches.iloc[0]['zip']
            logger.debug(f"Found ZIP {zip_code} for {city}, {state}")
            return zip_code

        logger.debug(f"No ZIP found for {city}, {state}")
        return None

    def lookup_city_state_from_zip(self, zip_code: Optional[str]) -> Optional[Tuple[str, str]]:
        """
        Lookup city and state given ZIP code.

        Args:
            zip_code: 5-digit ZIP code

        Returns:
            Tuple of (city, state) if found, None otherwise
        """
        if not zip_code:
            return None

        # Normalize ZIP (take first 5 digits)
        zip_norm = str(zip_code).strip()[:5]

        # Query database
        matches = self.zip_db[self.zip_db['zip'] == zip_norm]

        if len(matches) > 0:
            city = matches.iloc[0]['city']
            state = matches.iloc[0]['state']
            logger.debug(f"Found {city}, {state} for ZIP {zip_norm}")
            return (city, state)

        logger.debug(f"No city/state found for ZIP {zip_norm}")
        return None

    def enrich_record(self, record: Dict[str, any]) -> Dict[str, any]:
        """
        Enrich a record by filling in missing geographic data.

        Args:
            record: Dictionary with name, address, city, state, zip fields

        Returns:
            Enriched record with filled-in fields
        """
        enriched = record.copy()
        changed = []

        # If ZIP is missing but we have city/state
        if not record.get('zip') and record.get('city') and record.get('state'):
            zip_code = self.lookup_zip_from_city_state(
                record.get('city'),
                record.get('state')
            )
            if zip_code:
                enriched['zip'] = zip_code
                enriched['zip_source'] = 'lookup'
                changed.append('zip')

        # If city or state is missing but we have ZIP
        if record.get('zip'):
            needs_city = not record.get('city')
            needs_state = not record.get('state')

            if needs_city or needs_state:
                result = self.lookup_city_state_from_zip(record.get('zip'))
                if result:
                    city, state = result
                    if needs_city:
                        enriched['city'] = city
                        enriched['city_source'] = 'lookup'
                        changed.append('city')
                    if needs_state:
                        enriched['state'] = state
                        enriched['state_source'] = 'lookup'
                        changed.append('state')

        if changed:
            logger.debug(f"Enriched record: filled in {', '.join(changed)}")

        return enriched

    def enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich a DataFrame by filling in missing geographic data.

        Args:
            df: DataFrame with city, state, zip columns

        Returns:
            Enriched DataFrame
        """
        logger.info(f"Enriching {len(df)} records with geographic lookups...")

        enriched_df = df.copy()
        enriched_count = 0

        for idx in enriched_df.index:
            record = enriched_df.loc[idx].to_dict()
            enriched_record = self.enrich_record(record)

            # Check if anything changed
            if any(k.endswith('_source') for k in enriched_record.keys()):
                for key, value in enriched_record.items():
                    enriched_df.at[idx, key] = value
                enriched_count += 1

        logger.info(f"Enriched {enriched_count} records with geographic data")

        return enriched_df


def load_comprehensive_zip_database() -> pd.DataFrame:
    """
    Load comprehensive US ZIP code database.

    In production, use one of these approaches:
    1. uszipcode library: pip install uszipcode
    2. Download from https://www.unitedstateszipcodes.org/
    3. Use US Census Bureau data
    4. Use commercial ZIP database

    Returns:
        DataFrame with ZIP code data
    """
    try:
        # Try to use uszipcode if available
        from uszipcode import SearchEngine

        search = SearchEngine()
        all_zips = search.by_state('*', returns=0)  # Get all

        zip_data = []
        for zipcode in all_zips:
            zip_data.append({
                'zip': zipcode.zipcode,
                'city': zipcode.major_city,
                'state': zipcode.state,
                'county': zipcode.county,
                'lat': zipcode.lat,
                'lng': zipcode.lng
            })

        return pd.DataFrame(zip_data)

    except ImportError:
        logger.warning("uszipcode not installed, using basic ZIP database")
        logger.info("For comprehensive ZIP lookups, install: pip install uszipcode")
        return None
