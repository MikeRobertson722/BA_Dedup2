"""Load pipeline configuration from Snowflake BA_CONFIG/BA_LOOKUP tables.

Falls back to hardcoded defaults if Snowflake is unavailable.
"""

# ── Scalar config defaults (frozen copy of current hardcoded values) ──────────

DEFAULTS = {
    # API override
    'USE_API_OVERRIDE': True,
    'API_MODEL': 'claude-sonnet-4-5-20250929',
    'API_SCORE_MIN': 15.0,
    'API_SCORE_MAX': 70.0,
    'API_BATCH_SIZE': 20,
    # Google address API
    'GOOGLE_ADDR_SCORE_MIN': 15.0,
    'GOOGLE_ADDR_SCORE_MAX': 65.0,
    # Bucket classification ranges (0-100 scale)
    # NEW BA AND NEW ADDRESS
    'NEW_BA_NEW_ADDR_MIN_NAME_SCORE': 0,
    'NEW_BA_NEW_ADDR_MAX_NAME_SCORE': 0,
    'NEW_BA_NEW_ADDR_MIN_ADDR_SCORE': 0,
    'NEW_BA_NEW_ADDR_MAX_ADDR_SCORE': 0,
    # EXISTING BA ADD NEW ADDRESS
    'EXISTING_BA_NEW_ADDR_MIN_NAME_SCORE': 100,
    'EXISTING_BA_NEW_ADDR_MAX_NAME_SCORE': 100,
    'EXISTING_BA_NEW_ADDR_MIN_ADDR_SCORE': 0,
    'EXISTING_BA_NEW_ADDR_MAX_ADDR_SCORE': 0,
    # EXISTING BA AND EXISTING ADDRESS
    'EXISTING_BA_EXISTING_ADDR_MIN_NAME_SCORE': 100,
    'EXISTING_BA_EXISTING_ADDR_MAX_NAME_SCORE': 100,
    'EXISTING_BA_EXISTING_ADDR_MIN_ADDR_SCORE': 100,
    'EXISTING_BA_EXISTING_ADDR_MAX_ADDR_SCORE': 100,
    # Address scoring weights
    'STREET_WEIGHT': 0.60,
    'CITY_WEIGHT': 0.40,
    # Same-address detection
    'SAME_ADDR_STREET_SIM': 0.90,
    'SAME_ADDR_CITY_SIM': 0.85,
    # Name matching
    'NAME_MATCH_THRESHOLD': 0.85,
    'ACRONYM_MATCH_SCORE': 0.95,
    'FUZZY_TOKEN_MIN_LENGTH': 4,
    'FUZZY_TOKEN_JW_THRESHOLD': 0.92,
    'SUPP_NAME_BOOST_CAP': 0.95,
}

# ── Lookup table defaults (frozen copy of current hardcoded dicts/sets) ───────

LOOKUP_DEFAULTS = {
    'NICKNAME': {
        # Male
        'BILL': 'WILLIAM', 'BILLY': 'WILLIAM', 'WILL': 'WILLIAM', 'WILLY': 'WILLIAM',
        'WILLIE': 'WILLIAM', 'LIAM': 'WILLIAM',
        'BOB': 'ROBERT', 'BOBBY': 'ROBERT', 'ROB': 'ROBERT', 'ROBBIE': 'ROBERT',
        'CHUCK': 'CHARLES', 'CHARLIE': 'CHARLES', 'CHAS': 'CHARLES',
        'DICK': 'RICHARD', 'RICK': 'RICHARD', 'RICH': 'RICHARD', 'RICKY': 'RICHARD',
        'MIKE': 'MICHAEL', 'MIKEY': 'MICHAEL',
        'JIM': 'JAMES', 'JIMMY': 'JAMES', 'JIMMIE': 'JAMES', 'JAMIE': 'JAMES',
        'TOM': 'THOMAS', 'TOMMY': 'THOMAS',
        'JACK': 'JOHN', 'JOHNNY': 'JOHN', 'JON': 'JOHN',
        'JOE': 'JOSEPH', 'JOEY': 'JOSEPH',
        'ED': 'EDWARD', 'EDDIE': 'EDWARD', 'TED': 'EDWARD', 'TEDDY': 'EDWARD',
        'DAN': 'DANIEL', 'DANNY': 'DANIEL',
        'DAVE': 'DAVID', 'DAVY': 'DAVID',
        'STEVE': 'STEVEN', 'STEVIE': 'STEVEN', 'STEPHEN': 'STEVEN',
        'PAT': 'PATRICK',
        'TONY': 'ANTHONY',
        'LARRY': 'LAWRENCE', 'LARS': 'LAWRENCE',
        'HARRY': 'HAROLD', 'HAL': 'HAROLD',
        'JERRY': 'GERALD', 'GERRY': 'GERALD',
        'TERRY': 'TERRENCE',
        'MATT': 'MATTHEW',
        'ANDY': 'ANDREW', 'DREW': 'ANDREW',
        'FRED': 'FREDERICK', 'FREDDY': 'FREDERICK',
        'SAM': 'SAMUEL', 'SAMMY': 'SAMUEL',
        'BEN': 'BENJAMIN', 'BENNY': 'BENJAMIN',
        'KEN': 'KENNETH', 'KENNY': 'KENNETH',
        'RON': 'RONALD', 'RONNIE': 'RONALD',
        'DON': 'DONALD', 'DONNIE': 'DONALD',
        'PETE': 'PETER',
        'NICK': 'NICHOLAS',
        'WALT': 'WALTER', 'WALLY': 'WALTER',
        'HANK': 'HENRY',
        'RAY': 'RAYMOND',
        'PHIL': 'PHILIP', 'PHILLIP': 'PHILIP',
        'DOUG': 'DOUGLAS',
        'GREG': 'GREGORY',
        'JEFF': 'JEFFREY', 'GEOFF': 'JEFFREY',
        'GENE': 'EUGENE',
        'ART': 'ARTHUR',
        'VIC': 'VICTOR',
        'VINCE': 'VINCENT', 'VINNY': 'VINCENT',
        'STAN': 'STANLEY',
        'HERB': 'HERBERT',
        'LOU': 'LOUIS',
        'BERNIE': 'BERNARD',
        'MARTY': 'MARTIN',
        'NORM': 'NORMAN',
        'LENNY': 'LEONARD', 'LEO': 'LEONARD',
        'AL': 'ALBERT',
        'ALEX': 'ALEXANDER',
        'CHRIS': 'CHRISTOPHER',
        # Female
        'SANDY': 'SANDRA', 'SONDRA': 'SANDRA',
        'DOLORES': 'DELORES',
        'CATHY': 'CATHERINE', 'KATHY': 'CATHERINE', 'KATE': 'CATHERINE',
        'KATIE': 'CATHERINE', 'KATHERINE': 'CATHERINE', 'KATHRYN': 'CATHERINE',
        'BETTY': 'ELIZABETH', 'BETH': 'ELIZABETH', 'LIZ': 'ELIZABETH',
        'LIZZY': 'ELIZABETH', 'ELIZA': 'ELIZABETH',
        'PEGGY': 'MARGARET', 'PEG': 'MARGARET', 'MAGGIE': 'MARGARET',
        'MARGE': 'MARGARET', 'MARGIE': 'MARGARET',
        'SUE': 'SUSAN', 'SUZY': 'SUSAN', 'SUSIE': 'SUSAN',
        'BARB': 'BARBARA', 'BARBIE': 'BARBARA',
        'DEBBIE': 'DEBORAH', 'DEB': 'DEBORAH', 'DEBRA': 'DEBORAH',
        'JENNY': 'JENNIFER', 'JEN': 'JENNIFER',
        'SALLY': 'SARAH', 'SAL': 'SARAH',
        'TINA': 'CHRISTINA',
        'VICKY': 'VICTORIA', 'VIKKI': 'VICTORIA',
        'CONNIE': 'CONSTANCE',
        'DOTTY': 'DOROTHY', 'DOT': 'DOROTHY',
        'FRAN': 'FRANCES',
        'GINNY': 'VIRGINIA',
        'JUDY': 'JUDITH',
        'MILLIE': 'MILDRED',
        'NELL': 'HELEN', 'NELLIE': 'HELEN',
        'PATTY': 'PATRICIA', 'PATSY': 'PATRICIA', 'TRISH': 'PATRICIA',
    },
    'BUSINESS_SUFFIX': {
        'LLC', 'INC', 'CORP', 'CO', 'LTD', 'LP', 'LC', 'LLP',
        'PARTNERSHIP', 'PARTNE', 'PARTNERSHP', 'COMPANY',
    },
    'BUSINESS_DESCRIPTOR': {
        'SERVICES', 'HOLDINGS', 'INVESTMENT', 'INVESTMENTS',
    },
    'INDUSTRY_TERM': {
        'EXPLORATION', 'DRILLING', 'PRODUCTION', 'PETROLEUM',
        'ENERGY', 'RESOURCES', 'OPERATING', 'PIPELINE',
        'MIDSTREAM', 'UPSTREAM', 'DOWNSTREAM',
        'MINERALS', 'ROYALTIES', 'PROPERTIES',
        'OIL', 'GAS', 'NATURAL',
    },
    'NAME_SUFFIX': {'JR', 'SR', 'II', 'III', 'IV', 'V'},
    'STATE_ABBREV': {
        'AL': 'ALABAMA', 'AK': 'ALASKA', 'AZ': 'ARIZONA', 'AR': 'ARKANSAS',
        'CA': 'CALIFORNIA', 'CT': 'CONNECTICUT',
        'DE': 'DELAWARE', 'FL': 'FLORIDA', 'GA': 'GEORGIA', 'HI': 'HAWAII',
        'ID': 'IDAHO', 'IL': 'ILLINOIS', 'IA': 'IOWA',
        'KS': 'KANSAS', 'KY': 'KENTUCKY', 'LA': 'LOUISIANA', 'ME': 'MAINE',
        'MD': 'MARYLAND', 'MA': 'MASSACHUSETTS', 'MI': 'MICHIGAN',
        'MN': 'MINNESOTA', 'MS': 'MISSISSIPPI', 'MO': 'MISSOURI',
        'MT': 'MONTANA', 'NE': 'NEBRASKA', 'NV': 'NEVADA',
        'NH': 'NEWHAMPSHIRE', 'NJ': 'NEWJERSEY', 'NM': 'NEWMEXICO',
        'NY': 'NEWYORK', 'NC': 'NORTHCAROLINA', 'ND': 'NORTHDAKOTA',
        'OH': 'OHIO', 'OK': 'OKLAHOMA', 'OR': 'OREGON', 'PA': 'PENNSYLVANIA',
        'RI': 'RHODEISLAND', 'SC': 'SOUTHCAROLINA', 'SD': 'SOUTHDAKOTA',
        'TN': 'TENNESSEE', 'TX': 'TEXAS', 'UT': 'UTAH', 'VT': 'VERMONT',
        'VA': 'VIRGINIA', 'WA': 'WASHINGTON', 'WV': 'WESTVIRGINIA',
        'WI': 'WISCONSIN', 'WY': 'WYOMING', 'DC': 'DISTRICTOFCOLUMBIA',
    },
    'BAD_SSN': {
        '000000000', '111111111', '222222222', '333333333',
        '444444444', '555555555', '666666666', '777777777',
        '888888888', '999999999',
    },
    'TRUST_KEYWORD': {
        'TRUST', 'TRUSTEE', 'TRUSTEES', 'TRUSTEESHIP', 'TTEE',
        'LIVING TRUST', 'FAMILY TRUST', 'REVOCABLE TRUST', 'IRREVOCABLE TRUST',
        'TESTAMENTARY', 'ESTATE OF', 'DECEDENT',
    },
}


def _cast(val: str, dtype: str):
    """Cast a string config value to its declared type."""
    if dtype == 'BOOL':
        return val.lower() in ('true', '1', 'yes')
    if dtype == 'INT':
        return int(val)
    if dtype == 'FLOAT':
        return float(val)
    return val


def load_config(sf_conn=None) -> dict:
    """Load scalar config from Snowflake BA_CONFIG table.

    Falls back to DEFAULTS if sf_conn is None or query fails.
    """
    config = dict(DEFAULTS)
    if sf_conn is None:
        return config
    try:
        cur = sf_conn.cursor()
        cur.execute("SELECT CONFIG_KEY, CONFIG_VALUE, DATA_TYPE FROM BA_CONFIG")
        for key, val, dtype in cur:
            config[key] = _cast(val, dtype)
        cur.close()
        print(f'  Loaded {len(config)} config values from Snowflake BA_CONFIG')
    except Exception as e:
        print(f'  Warning: Could not load config from Snowflake: {e}')
        print(f'  Using hardcoded defaults.')
    return config


def load_lookups(sf_conn=None) -> dict:
    """Load lookup tables from Snowflake BA_LOOKUP table.

    Falls back to LOOKUP_DEFAULTS if sf_conn is None or query fails.
    Returns dict keyed by lookup_type. Dict-type lookups (NICKNAME, STATE_ABBREV)
    have {key: value}. Set-type lookups (BUSINESS_SUFFIX, etc.) have {key, ...}.
    """
    # Start with deep copy of defaults
    lookups = {}
    for k, v in LOOKUP_DEFAULTS.items():
        if isinstance(v, dict):
            lookups[k] = dict(v)
        else:
            lookups[k] = set(v)

    if sf_conn is None:
        return lookups

    try:
        cur = sf_conn.cursor()
        cur.execute("SELECT LOOKUP_TYPE, LOOKUP_KEY, LOOKUP_VALUE FROM BA_LOOKUP ORDER BY LOOKUP_TYPE")

        # Rebuild from DB rows (replaces defaults entirely if any rows exist)
        db_lookups = {}
        for ltype, lkey, lval in cur:
            if ltype not in db_lookups:
                db_lookups[ltype] = {}
            db_lookups[ltype][lkey] = lval
        cur.close()

        if db_lookups:
            for ltype, entries in db_lookups.items():
                # If all values are None, this is a set-type lookup
                if all(v is None for v in entries.values()):
                    lookups[ltype] = set(entries.keys())
                else:
                    lookups[ltype] = {k: v for k, v in entries.items() if v is not None}
            print(f'  Loaded {sum(len(v) for v in lookups.values())} lookup entries from Snowflake BA_LOOKUP')
    except Exception as e:
        print(f'  Warning: Could not load lookups from Snowflake: {e}')
        print(f'  Using hardcoded defaults.')

    return lookups
