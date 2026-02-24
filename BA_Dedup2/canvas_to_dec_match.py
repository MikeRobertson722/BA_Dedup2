"""
Canvas-to-DEC BA Matching Script
Compares Canvas BA records against DEC BA Master in database.
Scores BA match % (by SSN blocking) and Address match % separately.
Outputs results to canvas_dec_matches table.
"""
import re
import json
import os
import pandas as pd
import sqlite3
import time
from openpyxl.styles import PatternFill, Font, Alignment

# Load .env file if present (no external dependency needed)
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _key, _val = _line.split('=', 1)
                os.environ[_key.strip()] = _val.strip()

# Paths
DB_PATH = 'ba_dedup.db'
CANVAS_FILE = 'input/CANVAS_BA_MASTER_.xlsx'

# API override: send ambiguous name matches (score 15-70) to Claude for evaluation
USE_API_OVERRIDE = True
API_MODEL = 'claude-sonnet-4-5-20250929'
API_SCORE_MIN = 15
API_SCORE_MAX = 70
API_BATCH_SIZE = 20

# Google Address Validation API: override ambiguous address scores (15-65)
USE_GOOGLE_ADDRESS_API = os.environ.get('GOOGLE_ADDRESS_API_ENABLED', 'false').lower() == 'true'
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY', '')
GOOGLE_ADDR_SCORE_MIN = 15
GOOGLE_ADDR_SCORE_MAX = 65

# ===== String matching functions =====

WORD_NUM = {
    "ONE": "1", "TWO": "2", "THREE": "3", "FOUR": "4", "FIVE": "5",
    "SIX": "6", "SEVEN": "7", "EIGHT": "8", "NINE": "9", "TEN": "10"
}


def safe_str(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val).strip()


def jaro(s1: str, s2: str) -> float:
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0
    match_dist = max(0, max(len1, len2) // 2 - 1)
    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    transpositions = 0
    for i in range(len1):
        start = max(0, i - match_dist)
        end = min(i + match_dist + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    return (matches / len1 + matches / len2 +
            (matches - transpositions / 2) / matches) / 3.0


def jaro_winkler(s1: str, s2: str, p: float = 0.1) -> float:
    j = jaro(s1, s2)
    prefix = 0
    for c1, c2 in zip(s1, s2):
        if c1 == c2:
            prefix += 1
        else:
            break
        if prefix == 4:
            break
    return j + prefix * p * (1 - j)


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return jaro_winkler(a, b)


def compact_alnum(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())


# ===== Nickname / business-suffix / number-word lookups =====

NICKNAMES = {
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
}

BUSINESS_SUFFIXES = {
    'LLC', 'INC', 'CORP', 'CO', 'LTD', 'LP', 'LC', 'LLP',
    'PARTNERSHIP', 'PARTNE', 'PARTNERSHP', 'COMPANY',
}

# Business descriptors: interchangeable fluff when SSN already matches
BUSINESS_DESCRIPTORS = {
    'SERVICES', 'HOLDINGS', 'INVESTMENT', 'INVESTMENTS',
}

# Oil & gas industry terms: generic words that inflate scores between
# unrelated companies (e.g. OVINTIV EXPLORATION vs NEWFIELD EXPLORATION)
INDUSTRY_TERMS = {
    'EXPLORATION', 'DRILLING', 'PRODUCTION', 'PETROLEUM',
    'ENERGY', 'RESOURCES', 'OPERATING', 'PIPELINE',
    'MIDSTREAM', 'UPSTREAM', 'DOWNSTREAM',
    'MINERALS', 'ROYALTIES', 'PROPERTIES',
    'OIL', 'GAS', 'NATURAL',
}

NAME_SUFFIXES = {'JR', 'SR', 'II', 'III', 'IV', 'V'}

NUMBER_WORDS = {
    'ONE': '1', 'TWO': '2', 'THREE': '3', 'FOUR': '4', 'FIVE': '5',
    'SIX': '6', 'SEVEN': '7', 'EIGHT': '8', 'NINE': '9', 'TEN': '10',
}

# State abbreviation → full name (map abbrev TO full so both forms match;
# avoids conflict with CO in BUSINESS_SUFFIXES which strips before this runs)
STATE_ABBREVS = {
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
    # Skip IN (Indiana) and CO (Colorado) — too ambiguous as common words/suffixes
}


def canonicalize_token(token):
    """Normalize nicknames, number words, and state names to canonical form."""
    if token in NICKNAMES:
        return NICKNAMES[token]
    if token in NUMBER_WORDS:
        return NUMBER_WORDS[token]
    if token in STATE_ABBREVS:
        return STATE_ABBREVS[token]
    return token


def enhanced_token_overlap(tokens1, tokens2):
    """Token overlap counting exact, initial, and fuzzy (typo) matches."""
    if not tokens1 or not tokens2:
        return 0.0
    # Iterate over the smaller set, match against the larger
    if len(tokens1) > len(tokens2):
        small, big = list(tokens2), list(tokens1)
    else:
        small, big = list(tokens1), list(tokens2)

    matched = 0
    used = [False] * len(big)

    for t1 in small:
        found = False
        # 1) Exact match
        for j, t2 in enumerate(big):
            if not used[j] and t1 == t2:
                matched += 1
                used[j] = True
                found = True
                break
        if found:
            continue
        # 2) Initial match (single letter ↔ full name starting with that letter)
        if len(t1) == 1:
            for j, t2 in enumerate(big):
                if not used[j] and len(t2) > 1 and t2[0] == t1:
                    matched += 1
                    used[j] = True
                    found = True
                    break
        else:
            for j, t2 in enumerate(big):
                if not used[j] and len(t2) == 1 and t1[0] == t2:
                    matched += 1
                    used[j] = True
                    found = True
                    break
        if found:
            continue
        # 3) Fuzzy match for typos (both tokens > 4 chars, JW > 0.92)
        if len(t1) > 4:
            best_j, best_sim = -1, 0.0
            for j, t2 in enumerate(big):
                if not used[j] and len(t2) > 4:
                    sim = jaro_winkler(t1, t2)
                    if sim > best_sim:
                        best_sim = sim
                        best_j = j
            if best_sim > 0.88:
                matched += 1
                used[best_j] = True

    return matched / len(small)


# ===== Normalization functions =====

def normalize_name(name, trust: bool = False) -> str:
    name = safe_str(name)
    if not name:
        return ""
    if trust:
        return re.sub(r"\s+", " ", name.upper()).strip()
    # Replace word-separating chars with space, strip all other specials
    name = re.sub(r"[/&]", " ", name)
    name = re.sub(r"[^A-Za-z0-9\s]", "", name)
    suffix_map = {
        # Business name aliases (multi-word first)
        r"FEDERAL\s+EXPRESS": "FEDEX",
        r"FEDEX\s+OFFICE": "FEDEX",
        # Multi-word state names → single token (matches STATE_ABBREVS expansion)
        r"NEW\s+HAMPSHIRE": "NEWHAMPSHIRE",
        r"NEW\s+JERSEY": "NEWJERSEY",
        r"NEW\s+MEXICO": "NEWMEXICO",
        r"NEW\s+YORK": "NEWYORK",
        r"NORTH\s+CAROLINA": "NORTHCAROLINA",
        r"NORTH\s+DAKOTA": "NORTHDAKOTA",
        r"SOUTH\s+CAROLINA": "SOUTHCAROLINA",
        r"SOUTH\s+DAKOTA": "SOUTHDAKOTA",
        r"WEST\s+VIRGINIA": "WESTVIRGINIA",
        r"RHODE\s+ISLAND": "RHODEISLAND",
        r"DISTRICT\s+OF\s+COLUMBIA": "DISTRICTOFCOLUMBIA",
        # Trust abbreviation expansions (multi-word first)
        r"LIV\s+TR": "LIVING TRUST",
        "REVOC": "REVOCABLE",
        "REV": "REVOCABLE",
        "TR": "TRUST",
        # Name suffixes
        "JUNIOR": "JR", "SENIOR": "SR",
        "THIRD": "III", "FOURTH": "IV", "SECOND": "II",
        "CORPORATION": "CORP", "INCORPORATED": "INC",
        "COMPANY": "CO", "LIMITED": "LTD",
        r"L\s*L\s*C": "LLC", r"L\s*C": "LC", r"L\s*P": "LP",
    }
    for pat, repl in suffix_map.items():
        name = re.sub(r"\b" + pat + r"\b", repl, name, flags=re.IGNORECASE)
    # Split concatenated Roman numerals: PDIII → PD III
    name = re.sub(r"([A-Za-z]{2,})(III|IV|II)", r"\1 \2", name)
    name = re.sub(r"\s+", " ", name.upper()).strip()
    # Collapse runs of consecutive single letters: "J V S LLC" → "JVS LLC"
    def _collapse_singles(m):
        return m.group(0).replace(" ", "")
    name = re.sub(r"\b[A-Z](?:\s[A-Z]){1,}\b", _collapse_singles, name)
    return name


# Date-like pattern: strips dates from addresses before comparison and number extraction
_MONTH_NAMES = (r'(?:JAN(?:UARY)?|FEB(?:RUARY)?|MAR(?:CH)?|APR(?:IL)?|MAY'
                r'|JUNE?|JULY?|AUG(?:UST)?|SEP(?:T(?:EMBER)?)?'
                r'|OCT(?:OBER)?|NOV(?:EMBER)?|DEC(?:EMBER)?)')
_DATE_RE = re.compile(
    r'\b' + _MONTH_NAMES + r'[\s,]+\d{1,2}[\s,]+\d{2,4}\b'  # JUNE 14, 2012
    r'|\b\d{1,2}[\s,]+' + _MONTH_NAMES + r'[\s,]+\d{2,4}\b' # 14 JUNE 2012
    r'|\b' + _MONTH_NAMES + r'[\s,]+\d{4}\b'                 # MARCH 2012 (no day)
    r'|\b\d{1,2}[/\-\s]\d{1,2}[/\-\s]\d{2,4}\b'             # 1-23-14, MM/DD/YYYY
    r'|\b\d{8}\b'                                              # MMDDYYYY (8 digits)
    r'|\b(?:19|20)\d{2}\b'                                     # 4-digit year 19xx/20xx
    r'|\b\d{1,2}[/\-]\d{2,4}\b',                              # MM/YYYY or MM-YY
    re.IGNORECASE
)


def normalize_address(addr, trust: bool = False) -> str:
    addr = safe_str(addr)
    if not addr:
        return ""
    if trust:
        return re.sub(r"\s+", " ", addr.upper()).strip()
    addr = addr.upper()

    # Strip "DATED <date>" patterns from address text (conservative — month names only,
    # NOT standalone 4-digit numbers which could be house numbers like 1950, 2000)
    addr = re.sub(r'\bDATED\s+', '', addr)
    addr = re.sub(
        r'\b' + _MONTH_NAMES + r'[\s,]+\d{1,2}[\s,]+\d{2,4}\b'
        r'|\b\d{1,2}[\s,]+' + _MONTH_NAMES + r'[\s,]+\d{2,4}\b'
        r'|\b' + _MONTH_NAMES + r'[\s,]+\d{4}\b',
        ' ', addr, flags=re.IGNORECASE)
    addr = re.sub(r'\s+', ' ', addr).strip()

    # STRICT: Treat as PO Box ONLY if entire line is exactly "BOX <num/wordnum>"
    m = re.fullmatch(
        r"\s*BOX\s*#?\s*(\d+|ONE|TWO|THREE|FOUR|FIVE|SIX|SEVEN|EIGHT|NINE|TEN)\s*", addr)
    if m:
        tok = m.group(1).upper()
        tok = WORD_NUM.get(tok, tok)
        addr = f"PO BOX {tok}"

    # Explicit PO patterns only
    addr = re.sub(r"\bPOST\s*OFFICE\s*BOX\b", "PO BOX", addr)
    addr = re.sub(r"\bP\.?\s*O\.?\s*BOX\b", "PO BOX", addr)
    addr = re.sub(r"\bP\.?\s*O\.?\s*B\b", "PO BOX", addr)
    addr = re.sub(r"\bPOB\b", "PO BOX", addr)
    addr = re.sub(r"\bPO\s+BOX\s*#", "PO BOX ", addr)

    # Normalize word numbers in PO BOX addresses
    po_match = re.search(
        r"\bPO\s*BOX\s+(ONE|TWO|THREE|FOUR|FIVE|SIX|SEVEN|EIGHT|NINE|TEN)\b", addr)
    if po_match:
        word = po_match.group(1)
        addr = addr.replace(f"PO BOX {word}", f"PO BOX {WORD_NUM[word]}")

    # Normalize COUNTY ROAD -> CR before street types (so ROAD doesn't get hit first)
    addr = re.sub(r"\bCOUNTY\s+ROAD\b", "CR", addr)
    addr = re.sub(r"\bCOUNTY\s+RD\b", "CR", addr)

    for full, abbr in [
        ("NORTHEAST", "NE"), ("NORTHWEST", "NW"),
        ("SOUTHEAST", "SE"), ("SOUTHWEST", "SW"),
        ("NORTH", "N"), ("SOUTH", "S"), ("EAST", "E"), ("WEST", "W"),
    ]:
        addr = re.sub(r"\b" + full + r"\b", abbr, addr)

    for full, abbr in [
        ("STREET", "ST"), ("AVENUE", "AVE"), ("BOULEVARD", "BLVD"),
        ("DRIVE", "DR"), ("ROAD", "RD"), ("LANE", "LN"),
        ("COURT", "CT"), ("CIRCLE", "CIR"), ("PLACE", "PL"),
        ("HIGHWAY", "HWY"), ("PARKWAY", "PKWY"),
        ("TERRACE", "TER"), ("TRAIL", "TRL"),
    ]:
        addr = re.sub(r"\b" + full + r"\b", abbr, addr)

    for full, abbr in [
        ("APARTMENT", "APT"), ("SUITE", "STE"),
        ("BUILDING", "BLDG"), ("FLOOR", "FL"), ("ROOM", "RM"),
        (r"#\s*(\d+)", r"APT \1"),
    ]:
        addr = re.sub(r"\b" + full + r"\b", abbr, addr)

    addr = re.sub(r"[.,\-]", "", addr)
    return re.sub(r"\s+", " ", addr).strip()


def normalize_city(val: str) -> str:
    s = safe_str(val).upper().strip()
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    # FT -> FORT (FT WORTH, FT SMITH, FT COLLINS, etc.)
    s = re.sub(r"\bFT\b", "FORT", s)
    return re.sub(r"\s+", " ", s).strip()


def normalize_zip(z: str) -> str:
    """Normalize zip code: strip to digits, left-pad to 5 if 4 digits."""
    digits = re.sub(r'[^0-9]', '', str(z)) if z else ''
    if len(digits) >= 4:
        return digits[:5].zfill(5)
    return digits


# ===== Parsing helpers =====

POBOX_CANON_RE = re.compile(r"\bPO\s*BOX\b", re.I)


def parse_house_number(addr_norm: str) -> str:
    m = re.match(r"^\s*(\d+)\b", addr_norm or "")
    return m.group(1) if m else ""


def parse_po_box_number(addr_norm: str) -> str:
    if not addr_norm or not POBOX_CANON_RE.search(addr_norm):
        return ""
    m = re.search(r"\bPO\s*BOX\s*([A-Z]+|\d+)\b", addr_norm)
    if not m:
        return ""
    val = m.group(1).upper()
    return WORD_NUM.get(val, val) if val.isalpha() else val


def street_core_for_match(addr_norm: str) -> str:
    STREET_TYPE_TOKENS = {"ST", "AVE", "BLVD", "DR", "RD", "LN", "CT",
                          "CIR", "PL", "HWY", "PKWY", "TER", "TRL"}
    DIR_TOKENS = {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}
    UNIT_TOKENS = {"APT", "STE", "UNIT", "BLDG", "FL", "RM"}
    s = (addr_norm or "").upper()
    s = re.sub(r"^\s*\d+\s+", "", s)
    s = re.sub(r"\b(APT|STE|UNIT|BLDG|FL|RM)\b\s*\w+", " ", s)
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    tokens = [t for t in s.split()
              if t not in STREET_TYPE_TOKENS
              and t not in DIR_TOKENS
              and t not in UNIT_TOKENS]
    return " ".join(tokens).strip()



def extract_addr_numbers(addr_norm: str) -> set:
    """Extract embedded numbers from address, ignoring date-like patterns."""
    cleaned = _DATE_RE.sub(' ', addr_norm or '')
    return set(re.findall(r'\d{2,}', cleaned))


# ===== Comparison functions =====

def name_compare(name1: str, name2: str) -> dict:
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    if not n1 or not n2:
        return {"name_score": 0.0, "name_match": False}
    if n1 == n2:
        return {"name_score": 1.0, "name_match": True}

    # Strip personal name suffixes (safe: SSN already matched)
    t1_raw = [t for t in n1.split() if t not in NAME_SUFFIXES]
    t2_raw = [t for t in n2.split() if t not in NAME_SUFFIXES]
    if not t1_raw or not t2_raw:
        t1_raw, t2_raw = n1.split(), n2.split()

    # Acronym detection BEFORE business suffix stripping
    # e.g. CSC = CORP SERVICE CO (C-S-C)
    if len(t1_raw) == 1 and len(t1_raw[0]) > 1 and len(t2_raw) >= 2:
        acr = t1_raw[0]
        if len(acr) == len(t2_raw) and all(a == w[0] for a, w in zip(acr, t2_raw)):
            return {"name_score": 0.95, "name_match": True}
    if len(t2_raw) == 1 and len(t2_raw[0]) > 1 and len(t1_raw) >= 2:
        acr = t2_raw[0]
        if len(acr) == len(t1_raw) and all(a == w[0] for a, w in zip(acr, t1_raw)):
            return {"name_score": 0.95, "name_match": True}

    # Strip business suffixes/descriptors, remove AND, canonicalize nicknames/numbers
    STRIP = BUSINESS_SUFFIXES | BUSINESS_DESCRIPTORS | INDUSTRY_TERMS | {'AND', 'THE'}
    t1 = [canonicalize_token(t) for t in t1_raw if t not in STRIP]
    t2 = [canonicalize_token(t) for t in t2_raw if t not in STRIP]
    if not t1 or not t2:
        t1 = [canonicalize_token(t) for t in t1_raw]
        t2 = [canonicalize_token(t) for t in t2_raw]

    c1 = " ".join(t1)
    c2 = " ".join(t2)

    if c1 == c2:
        return {"name_score": 1.0, "name_match": True}

    # Initial-match check (J SMITH vs JOHN SMITH)
    if len(t1) >= 2 and len(t2) >= 2 and t1[-1] == t2[-1]:
        if len(t1[0]) == 1 and len(t2[0]) > 1 and t1[0] == t2[0][0]:
            return {"name_score": 0.95, "name_match": True}
        if len(t2[0]) == 1 and len(t1[0]) > 1 and t2[0] == t1[0][0]:
            return {"name_score": 0.95, "name_match": True}

    # Character-level similarities (multiple strategies)
    direct_jw = jaro_winkler(c1, c2)
    sorted_jw = jaro_winkler(" ".join(sorted(t1)), " ".join(sorted(t2)))
    compact_jw = jaro_winkler(compact_alnum(c1), compact_alnum(c2))
    best_jw = max(direct_jw, sorted_jw, compact_jw)

    # Enhanced token overlap (counts initial matches and fuzzy/typo matches)
    if min(len(t1), len(t2)) > 1:
        overlap_coeff = enhanced_token_overlap(t1, t2)

        # Bookend bonus: if first AND last tokens match, middle name
        # differences shouldn't tank the score (SSN already matched)
        if len(t1) >= 2 and len(t2) >= 2:
            first_match = (t1[0] == t2[0]
                           or (len(t1[0]) == 1 and len(t2[0]) > 1 and t1[0] == t2[0][0])
                           or (len(t2[0]) == 1 and len(t1[0]) > 1 and t2[0] == t1[0][0])
                           or (len(t1[0]) > 1 and len(t2[0]) > 1 and jaro_winkler(t1[0], t2[0]) >= 0.85))
            last_match = t1[-1] == t2[-1]
            if first_match and last_match:
                overlap_coeff = max(overlap_coeff, 0.9)

        score = best_jw * (0.3 + 0.7 * overlap_coeff)
    else:
        score = best_jw

    # Compact similarity floor: handles compound words and concatenated initials
    if compact_jw >= 0.95:
        score = max(score, compact_jw)

    return {"name_score": score, "name_match": score >= 0.85}


# Regex for extracting names from address fields (C/O, ATTN patterns)
_CO_RE = re.compile(
    r'\bC\s*/?\s*O\s+(.+?)(?:\s{2,}|\s+\d|\s+P\s*\.?\s*O\s|\s+BOX\s|$)',
    re.IGNORECASE)
_ATTN_RE = re.compile(
    r'\bATTN:?\s+(.+?)(?:\s{2,}|\s+\d|\s+P\s*\.?\s*O\s|\s+BOX\s|$)',
    re.IGNORECASE)
_ROLE_SUFFIX_RE = re.compile(
    r',?\s*(MANAGER|AGENT|DIRECTOR|TREASURER|SECRETARY|OFFICER|'
    r'PRESIDENT|VP|VICE PRESIDENT|ADMINISTRATOR|SUPERVISOR)\s*$',
    re.IGNORECASE)


def extract_names_from_address(addr: str) -> list:
    """Extract embedded person/entity names from C/O and ATTN patterns."""
    if not addr:
        return []
    text = safe_str(addr).upper()
    names = []
    for pattern in (_CO_RE, _ATTN_RE):
        m = pattern.search(text)
        if m:
            name = m.group(1).strip().rstrip(',')
            name = _ROLE_SUFFIX_RE.sub('', name).strip()
            if len(name) >= 3:
                names.append(name)
    return names


def address_compare(addr1, city1, zip1, addr2, city2, zip2) -> dict:
    addr1_norm = normalize_address(addr1)
    addr2_norm = normalize_address(addr2)
    zip1 = normalize_zip(zip1)
    zip2 = normalize_zip(zip2)

    # Both empty
    if not addr1_norm and not addr2_norm:
        return {"same_address": False, "score": 0.0, "reason": "BOTH_EMPTY"}
    if not addr1_norm or not addr2_norm:
        return {"same_address": False, "score": 0.0, "reason": "ONE_EMPTY"}

    is_pobox1 = bool(POBOX_CANON_RE.search(addr1_norm))
    is_pobox2 = bool(POBOX_CANON_RE.search(addr2_norm))

    if is_pobox1 or is_pobox2:
        if not (is_pobox1 and is_pobox2):
            return {"same_address": False, "score": 0.0, "reason": "POBOX_VS_STREET"}
        box1 = parse_po_box_number(addr1_norm)
        box2 = parse_po_box_number(addr2_norm)
        if not box1 or not box2:
            return {"same_address": False, "score": 0.0, "reason": "POBOX_MISSING_NUM"}
        if box1 != box2:
            return {"same_address": False, "score": 0.0,
                    "reason": f"POBOX_NUM_MISMATCH {box1}!={box2}"}

        city1_norm = normalize_city(city1)
        city2_norm = normalize_city(city2)
        if city1_norm and city2_norm:
            city_sim = max(similarity(city1_norm, city2_norm),
                          similarity(compact_alnum(city1_norm),
                                     compact_alnum(city2_norm)))
        else:
            city_sim = 0.90

        zip_bonus = 0.05 if (zip1 and zip2 and
                             zip1 == zip2) else 0.0
        score = min(1.0, 0.92 * city_sim + zip_bonus)
        same = score >= 0.90 and city_sim >= 0.85
        return {"same_address": same, "score": score,
                "reason": f"POBOX_MATCH box={box1} city_sim={city_sim:.2f}"}

    # Street address comparison
    num1 = parse_house_number(addr1_norm)
    num2 = parse_house_number(addr2_norm)

    # If neither has a house number (military: CMR/PSC/UNIT, or other
    # non-standard formats), fall back to full normalized string comparison
    if not num1 and not num2:
        addr_sim = max(similarity(addr1_norm, addr2_norm),
                       similarity(compact_alnum(addr1_norm),
                                  compact_alnum(addr2_norm)))
        city1_norm = normalize_city(city1)
        city2_norm = normalize_city(city2)
        if city1_norm and city2_norm:
            city_sim = max(similarity(city1_norm, city2_norm),
                          similarity(compact_alnum(city1_norm),
                                     compact_alnum(city2_norm)))
        else:
            city_sim = 0.90
        zip_match = (zip1 and zip2 and zip1 == zip2)
        score = addr_sim * (0.6 + 0.4 * city_sim)
        if zip_match and score >= 0.85:
            score = min(1.0, score + 0.05)

        # Penalty: embedded numbers (2+ digits) that don't overlap (ignore dates)
        nums1 = extract_addr_numbers(addr1_norm)
        nums2 = extract_addr_numbers(addr2_norm)
        nums_mismatch = (nums1 and nums2 and not nums1 & nums2)
        if nums_mismatch:
            score = max(0.0, score - 0.10)

        # Penalty: both have zip codes but they don't match
        zip_penalty = (zip1 and zip2 and zip1 != zip2)
        if zip_penalty:
            score = max(0.0, score - 0.20)

        # Bonus: all embedded numbers match AND zip codes match
        nums_all_match = (nums1 and nums2 and nums1 == nums2)
        if nums_all_match and zip_match:
            score = min(1.0, score + 0.50)

        same = (addr_sim >= 0.90 and city_sim >= 0.85 and not nums_mismatch) or \
               (nums_all_match and zip_match and city_sim >= 0.85)
        return {
            "same_address": same, "score": score,
            "reason": f"NON_STANDARD addr_sim={addr_sim:.2f} "
                      f"city_sim={city_sim:.2f} zip_match={zip_match}"
                      f" nums_mismatch={nums_mismatch}"
                      f" zip_penalty={zip_penalty}"
        }

    if not num1 or not num2:
        return {"same_address": False, "score": 0.0,
                "reason": "MISSING_HOUSE_NUMBER"}
    if num1 != num2:
        return {"same_address": False, "score": 0.0,
                "reason": f"HOUSE_NUM_MISMATCH {num1}!={num2}"}

    core1 = street_core_for_match(addr1_norm)
    core2 = street_core_for_match(addr2_norm)
    street_sim = max(similarity(core1, core2),
                     similarity(compact_alnum(core1), compact_alnum(core2)))

    city1_norm = normalize_city(city1)
    city2_norm = normalize_city(city2)
    if city1_norm and city2_norm:
        city_sim = max(similarity(city1_norm, city2_norm),
                       similarity(compact_alnum(city1_norm),
                                  compact_alnum(city2_norm)))
    else:
        city_sim = 0.90

    zip_match = (zip1 and zip2 and zip1 == zip2)
    zip_ok = True
    if zip1 and zip2 and zip1 != zip2:
        zip_ok = city_sim >= 0.92

    same = ((street_sim >= 0.90) and zip_ok and
            (city_sim >= 0.85 or not city1_norm or not city2_norm))
    score = street_sim * (0.6 + 0.4 * city_sim)
    if zip_match and score >= 0.85:
        score = min(1.0, score + 0.05)
    if not zip_ok:
        score *= 0.75

    return {
        "same_address": same,
        "score": score,
        "reason": f"STREET num={num1} street_sim={street_sim:.2f} "
                  f"city_sim={city_sim:.2f} zip_ok={zip_ok}"
    }


# ===== SSN cleaning =====

BAD_SSNS = {'000000000', '111111111', '222222222', '333333333',
            '444444444', '555555555', '666666666', '777777777',
            '888888888', '999999999'}


def round5(x: float) -> float:
    """Round to nearest 5 (e.g. 3->5, 12->10, 34->35, 89->90)."""
    return round(x / 5) * 5


def mask_ssn(ssn: str) -> str:
    """Mask SSN to show only last 4 digits: 123456789 -> *****6789."""
    digits = re.sub(r'[^0-9]', '', str(ssn)) if ssn else ''
    if len(digits) <= 4:
        return digits
    return '*' * (len(digits) - 4) + digits[-4:]


def clean_ssn(ssn_raw) -> str:
    if not ssn_raw:
        return ""
    digits = re.sub(r'[^0-9]', '', str(ssn_raw))
    if len(digits) < 4:
        return ""
    if digits.startswith('000') or digits.startswith('666'):
        return ""
    if digits in BAD_SSNS:
        return ""
    return digits


# ===== API Override =====

def api_override_name_scores(pairs):
    """Send ambiguous name pairs to Claude API for semantic evaluation.

    Args:
        pairs: list of (index_into_results, canvas_name, dec_name)
    Returns:
        dict mapping result index -> new score (0-100)
    """
    try:
        import anthropic
    except ImportError:
        print('  anthropic package not installed, skipping API override')
        return {}

    client = anthropic.Anthropic()
    overrides = {}

    for batch_start in range(0, len(pairs), API_BATCH_SIZE):
        batch = pairs[batch_start:batch_start + API_BATCH_SIZE]
        lines = []
        for i, (idx, name1, name2) in enumerate(batch, 1):
            lines.append(f'{i}. "{name1}" vs "{name2}"')

        prompt = (
            "You are evaluating whether business/person name pairs refer to the "
            "same entity. These pairs already share the same SSN, so they are "
            "likely the same person or company.\n\n"
            "For each pair, score 0-100:\n"
            "  100 = definitely same entity (exact or trivial variation)\n"
            "  85-95 = very likely same (abbreviations, nicknames, minor differences)\n"
            "  60-80 = probably same but significant differences\n"
            "  30-55 = uncertain, could be different entities sharing SSN\n"
            "  0-25 = likely different entities\n\n"
            "Name pairs:\n" + "\n".join(lines) + "\n\n"
            "Respond with ONLY a JSON object: {\"scores\": [score1, score2, ...]}"
        )

        try:
            response = client.messages.create(
                model=API_MODEL,
                max_tokens=256,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text.strip()
            # Parse JSON from response (handle markdown code blocks)
            if text.startswith('```'):
                text = text.split('\n', 1)[1].rsplit('```', 1)[0].strip()
            data = json.loads(text)
            scores = data['scores']

            for i, (idx, _, _) in enumerate(batch):
                if i < len(scores):
                    overrides[idx] = round5(max(0, min(100, scores[i])))
        except Exception as e:
            print(f'  API batch {batch_start // API_BATCH_SIZE + 1} failed: {e}')
            err_str = str(e).lower()
            if 'authentication_error' in err_str or '401' in err_str \
               or 'credit balance' in err_str or 'billing' in err_str:
                print('  Fatal API error — skipping remaining batches.')
                print('  Check your API key and account billing at console.anthropic.com.')
                break
            continue

    return overrides


# ===== Google Address Validation API =====

_google_addr_cache = {}  # keyed on (address, city, state, zip) → dict
_google_api_calls = 0


def _init_google_cache(conn):
    """Create persistent cache table and pre-load into memory."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS google_address_lookups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            input_address TEXT NOT NULL,
            input_city TEXT NOT NULL,
            input_state TEXT NOT NULL,
            input_zip TEXT NOT NULL,
            std_address TEXT,
            std_city TEXT,
            std_state TEXT,
            std_zip TEXT,
            verdict_json TEXT,
            lookup_status TEXT NOT NULL DEFAULT 'success',
            http_status INTEGER,
            first_source_type TEXT,
            first_source_id TEXT,
            first_source_sub_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(input_address, input_city, input_state, input_zip)
        )
    """)
    conn.commit()

    # Pre-load success and failed rows (skip quota_error so they get retried)
    cursor.execute("""
        SELECT input_address, input_city, input_state, input_zip,
               std_address, std_city, std_state, std_zip,
               verdict_json, lookup_status
        FROM google_address_lookups
        WHERE lookup_status IN ('success', 'failed')
    """)
    for row in cursor:
        key = (row[0], row[1], row[2], row[3])
        if row[9] == 'success':
            verdict = json.loads(row[8]) if row[8] else None
            _google_addr_cache[key] = {
                'standardized': (row[4], row[5], row[6], row[7]),
                'verdict': verdict,
                'status': 'success',
            }
        else:  # failed
            _google_addr_cache[key] = {
                'standardized': None,
                'verdict': None,
                'status': 'failed',
            }
    if _google_addr_cache:
        print(f'Loaded {len(_google_addr_cache):,} cached Google address lookups')


def google_validate_address(address, city, state, zip_code, conn=None,
                            source_info=None):
    """Validate/standardize a single address via Google Address Validation API.

    Returns dict with keys: standardized, verdict, status.
    Uses in-memory cache (populated from persistent DB table) first.

    Args:
        address, city, state, zip_code: Address components to validate
        conn: SQLite connection for persistent cache writes
        source_info: Optional (source_type, source_id, source_sub_id) for traceability
    """
    global _google_api_calls

    key = (address, city, state, zip_code)
    if key in _google_addr_cache:
        return _google_addr_cache[key]

    try:
        import requests as req
    except ImportError:
        return {'standardized': None, 'verdict': None, 'status': 'failed'}

    url = f'https://addressvalidation.googleapis.com/v1:validateAddress?key={GOOGLE_API_KEY}'
    body = {
        'address': {
            'regionCode': 'US',
            'addressLines': [address],
            'locality': city,
            'administrativeArea': state,
            'postalCode': zip_code,
        }
    }

    try:
        resp = req.post(url, json=body, timeout=10)
        _google_api_calls += 1

        if resp.status_code in (403, 429):
            # Don't cache quota errors in memory — they should be retried next run
            _persist_lookup(conn, key, None, None, 'quota_error',
                            resp.status_code, source_info)
            return {'standardized': None, 'verdict': None, 'status': 'quota_error'}

        if resp.status_code != 200:
            result = {'standardized': None, 'verdict': None, 'status': 'failed'}
            _google_addr_cache[key] = result
            _persist_lookup(conn, key, None, None, 'failed',
                            resp.status_code, source_info)
            return result

        data = resp.json()
        api_result = data.get('result', {})
        verdict = api_result.get('verdict')
        postal = api_result.get('address', {}).get('postalAddress', {})

        # Build address from components to exclude point_of_interest (name)
        # that Google bakes into addressLines
        components = api_result.get('address', {}).get('addressComponents', [])
        comp_map = {}
        for comp in components:
            ctype = comp.get('componentType', '')
            ctext = comp.get('componentName', {}).get('text', '')
            if ctext:
                comp_map[ctype] = ctext

        # Reconstruct street address from components (skip point_of_interest)
        parts = []
        if 'street_number' in comp_map:
            parts.append(comp_map['street_number'])
        if 'route' in comp_map:
            parts.append(comp_map['route'])
        if 'subpremise' in comp_map:
            parts.append(comp_map['subpremise'])
        if parts:
            std_addr = ' '.join(parts)
        else:
            # Fallback to addressLines if no components
            std_lines = postal.get('addressLines', [])
            std_addr = std_lines[0] if std_lines else address

        std_city = postal.get('locality', city)
        std_state = postal.get('administrativeArea', state)
        std_zip = postal.get('postalCode', zip_code)
        if std_zip and len(std_zip) > 5:
            std_zip = std_zip[:5]

        standardized = (std_addr.upper(), std_city.upper(),
                        std_state.upper(), std_zip)
        result = {
            'standardized': standardized,
            'verdict': verdict,
            'status': 'success',
        }
        _google_addr_cache[key] = result
        _persist_lookup(conn, key, standardized, verdict, 'success',
                        resp.status_code, source_info)
        return result

    except Exception:
        result = {'standardized': None, 'verdict': None, 'status': 'failed'}
        _google_addr_cache[key] = result
        _persist_lookup(conn, key, None, None, 'failed', None, source_info)
        return result


def _persist_lookup(conn, key, standardized, verdict, status, http_status,
                    source_info):
    """Write a lookup result to the persistent google_address_lookups table."""
    if not conn:
        return
    try:
        verdict_str = json.dumps(verdict) if verdict else None
        src_type = source_info[0] if source_info else None
        src_id = source_info[1] if source_info else None
        src_sub = source_info[2] if source_info else None
        conn.execute("""
            INSERT OR REPLACE INTO google_address_lookups
            (input_address, input_city, input_state, input_zip,
             std_address, std_city, std_state, std_zip,
             verdict_json, lookup_status, http_status,
             first_source_type, first_source_id, first_source_sub_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (key[0], key[1], key[2], key[3],
              standardized[0] if standardized else None,
              standardized[1] if standardized else None,
              standardized[2] if standardized else None,
              standardized[3] if standardized else None,
              verdict_str, status, http_status,
              src_type, src_id, src_sub))
        conn.commit()
    except Exception:
        pass  # Don't let cache write failures break the pipeline


def google_override_address_scores(pairs, conn):
    """Send ambiguous address pairs to Google Address Validation API.

    For each pair, validates both Canvas and DEC addresses, then re-runs
    address_compare() on the standardized forms.

    Args:
        pairs: list of (index, canvas_addr, canvas_city, canvas_state, canvas_zip,
                        canvas_id, canvas_addrseq,
                        dec_addr, dec_city, dec_state, dec_zip,
                        dec_hdrcode, dec_addrsubcode, original_score)
        conn: SQLite connection for persistent cache
    Returns:
        dict mapping result index -> override info dict with keys:
        new_score, same_address, canvas_std, canvas_verdict,
        dec_std, dec_verdict, score_changed
    """
    global _google_api_calls
    _google_api_calls = 0
    overrides = {}
    cache_hits_before = len(_google_addr_cache)
    quota_error = False

    for count, pair in enumerate(pairs, 1):
        if quota_error:
            break

        (idx, c_addr, c_city, c_state, c_zip, c_id, c_addrseq,
         d_addr, d_city, d_state, d_zip, d_hdrcode, d_addrsubcode,
         orig_score) = pair

        # Validate Canvas address
        c_result = google_validate_address(
            c_addr, c_city, c_state, c_zip, conn=conn,
            source_info=('canvas', c_id, c_addrseq))
        if c_result['status'] == 'quota_error':
            print('  Google API quota/auth error — stopping remaining calls.')
            print('  Check your API key and billing at console.cloud.google.com.')
            quota_error = True
            break

        # Validate DEC address
        d_result = google_validate_address(
            d_addr, d_city, d_state, d_zip, conn=conn,
            source_info=('dec', d_hdrcode, d_addrsubcode))
        if d_result['status'] == 'quota_error':
            print('  Google API quota/auth error — stopping remaining calls.')
            quota_error = True
            break

        # Build override entry for this pair (even if score doesn't change)
        entry = {
            'canvas_std': c_result['standardized'],
            'canvas_verdict': c_result['verdict'],
            'dec_std': d_result['standardized'],
            'dec_verdict': d_result['verdict'],
        }

        # Re-compare if both sides succeeded
        if c_result['standardized'] and d_result['standardized']:
            c_std = c_result['standardized']
            d_std = d_result['standardized']
            new_compare = address_compare(c_std[0], c_std[1], c_std[3],
                                          d_std[0], d_std[1], d_std[3])
            new_score = round5(new_compare['score'] * 100)
            entry['new_score'] = new_score
            entry['same_address'] = new_compare['same_address']
            entry['score_changed'] = abs(new_score - orig_score) >= 10
        else:
            entry['new_score'] = orig_score
            entry['same_address'] = None
            entry['score_changed'] = False

        overrides[idx] = entry

        if count % 100 == 0:
            cache_hits = len(_google_addr_cache) - cache_hits_before
            print(f'  Progress: {count}/{len(pairs)} pairs '
                  f'({_google_api_calls} API calls, ~{cache_hits} cache hits)')

    cache_hits = len(_google_addr_cache) - cache_hits_before
    print(f'  Google API: {_google_api_calls} calls, '
          f'{len(_google_addr_cache)} cached addresses')

    return overrides


# ===== Main =====

def main():
    print('=' * 80)
    print('CANVAS-TO-DEC BA MATCHING')
    print('=' * 80)

    # 1. Read Canvas file
    print(f'\nReading Canvas file: {CANVAS_FILE}')
    canvas_df = pd.read_excel(CANVAS_FILE, dtype=str)
    canvas_df = canvas_df.fillna('')
    print(f'Loaded {len(canvas_df):,} Canvas records')

    # 2. Load DEC records from database into memory dict keyed by clean SSN
    print(f'\nLoading DEC records from database...')
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        'SELECT ssn, hdrcode, hdrname, addraddress, addrcity, '
        'addrstate, addrzipcode, addrcontact, addrsubcode FROM dec_ba_master')

    dec_by_ssn = {}
    dec_total = 0
    for row in cursor:
        dec_total += 1
        ssn_clean = clean_ssn(row[0])
        if ssn_clean:
            if ssn_clean not in dec_by_ssn:
                dec_by_ssn[ssn_clean] = []
            dec_by_ssn[ssn_clean].append({
                'ssn': row[0],
                'hdrcode': row[1],
                'hdrname': row[2],
                'addraddress': row[3],
                'addrcity': row[4],
                'addrstate': row[5],
                'addrzipcode': row[6],
                'addrcontact': row[7],
                'addrsubcode': row[8],
            })

    print(f'Loaded {dec_total:,} DEC records')
    dec_with_ssn = sum(len(v) for v in dec_by_ssn.values())
    print(f'DEC records with valid SSN: {dec_with_ssn:,}')
    print(f'Unique DEC SSNs: {len(dec_by_ssn):,}')

    # 2b. Init persistent Google address cache (if enabled)
    if USE_GOOGLE_ADDRESS_API and GOOGLE_API_KEY:
        _init_google_cache(conn)

    # 3. Create output table (MERGE_OUTPUT.csv format + match scores)
    cursor.execute('DROP TABLE IF EXISTS canvas_dec_matches')
    cursor.execute("""
        CREATE TABLE canvas_dec_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            -- Canvas source
            canvas_name TEXT,
            canvas_address TEXT,
            canvas_city TEXT,
            canvas_state TEXT,
            canvas_zip TEXT,
            canvas_addrseq TEXT,
            -- Canvas identifiers
            canvas_id TEXT,
            canvas_ssn TEXT,
            -- DEC best match
            dec_hdrcode TEXT,
            dec_name TEXT,
            dec_address TEXT,
            dec_city TEXT,
            dec_state TEXT,
            dec_zip TEXT,
            dec_contact TEXT,
            dec_addrsubcode TEXT,
            -- Scores
            ssn_match REAL,
            name_score REAL,
            name_boost_source TEXT,
            address_score REAL,
            address_reason TEXT,
            recommendation TEXT,
            dec_match_count INTEGER,
            Number_possible_address_matches INTEGER,
            is_trust INTEGER DEFAULT 0,
            -- Google Address Lookup: Canvas
            before_lookup_address TEXT,
            before_lookup_city TEXT,
            before_lookup_state TEXT,
            before_lookup_zip TEXT,
            address_looked_up INTEGER DEFAULT 0,
            canvas_lookup_verdict TEXT,
            -- Google Address Lookup: DEC
            before_lookup_dec_address TEXT,
            before_lookup_dec_city TEXT,
            before_lookup_dec_state TEXT,
            before_lookup_dec_zip TEXT,
            dec_address_looked_up INTEGER DEFAULT 0,
            dec_lookup_verdict TEXT,
            --
            jib INTEGER,
            rev INTEGER,
            vendor INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 4. Process each Canvas record
    results = []
    stats = {
        'existing_ba_existing_addr': 0,
        'existing_ba_likely_addr': 0,
        'existing_ba_new_addr': 0,
        'likely_new_ba': 0,
        'new_ba': 0,
        'no_ssn': 0,
        'bad_ssn': 0,
    }

    _trust_re = re.compile(
        r'\b(TRUST|TRUSTEE|TRUSTEES|TRUSTEESHIP|TTEE|LIVING TRUST|'
        r'FAMILY TRUST|REVOCABLE TRUST|IRREVOCABLE TRUST|'
        r'TESTAMENTARY|ESTATE OF|DECEDENT)\b', re.IGNORECASE)

    start = time.time()
    total = len(canvas_df)

    for idx, row in canvas_df.iterrows():
        canvas_ssn_raw = row.get('SSN', '')
        canvas_ssn = clean_ssn(canvas_ssn_raw)
        canvas_name = row.get('ENTITY_LIST_NAME', '') or row.get('HDRNAME', '')
        canvas_addr = row.get('ADDRADDRESS', '')
        canvas_city = row.get('ADDRCITY', '')
        canvas_state = row.get('ADDRSTATE', '')
        canvas_zip = row.get('ADDRZIPCODE', '')
        canvas_id = row.get('ID', '')
        canvas_addrseq = row.get('ADDRSEQ', '')

        # Store digits-only SSN (cleaned of dashes, spaces, special chars)
        canvas_ssn_digits = re.sub(r'[^0-9]', '', str(canvas_ssn_raw)) if canvas_ssn_raw else ''

        is_trust = 1 if _trust_re.search(str(canvas_name)) else 0

        base_record = {
            'canvas_name': canvas_name,
            'canvas_address': canvas_addr,
            'canvas_city': canvas_city,
            'canvas_state': canvas_state,
            'canvas_zip': canvas_zip,
            'canvas_addrseq': canvas_addrseq,
            'canvas_id': canvas_id,
            'canvas_ssn': canvas_ssn_digits,
            'is_trust': is_trust,
            # Google Address Lookup defaults
            'before_lookup_address': None,
            'before_lookup_city': None,
            'before_lookup_state': None,
            'before_lookup_zip': None,
            'address_looked_up': 0,
            'canvas_lookup_verdict': None,
            'before_lookup_dec_address': None,
            'before_lookup_dec_city': None,
            'before_lookup_dec_state': None,
            'before_lookup_dec_zip': None,
            'dec_address_looked_up': 0,
            'dec_lookup_verdict': None,
        }

        if not canvas_ssn:
            if canvas_ssn_raw:
                stats['bad_ssn'] += 1
                rec = 'NEW BA - INVALID SSN'
            else:
                stats['no_ssn'] += 1
                rec = 'NEW BA - NO SSN'
            results.append({
                **base_record,
                'dec_hdrcode': '', 'dec_name': '', 'dec_address': '',
                'dec_city': '', 'dec_state': '', 'dec_zip': '',
                'dec_contact': '', 'dec_addrsubcode': '',
                'ssn_match': 0.0, 'name_score': 0.0, 'address_score': 0.0,
                'address_reason': 'NO SSN TO MATCH',
                'recommendation': rec, 'dec_match_count': 0,
            'Number_possible_address_matches': 0, 'name_boost_source': None,
            })
            continue

        # Look up in DEC by cleaned SSN
        dec_matches = dec_by_ssn.get(canvas_ssn, [])

        if not dec_matches:
            stats['new_ba'] += 1
            results.append({
                **base_record,
                'dec_hdrcode': '', 'dec_name': '', 'dec_address': '',
                'dec_city': '', 'dec_state': '', 'dec_zip': '',
                'dec_contact': '', 'dec_addrsubcode': '',
                'ssn_match': 0.0, 'name_score': 0.0, 'address_score': 0.0,
                'address_reason': 'SSN NOT FOUND IN DEC',
                'recommendation': 'NEW BA - NO DEC MATCH',
                'dec_match_count': 0,
                'Number_possible_address_matches': 0, 'name_boost_source': None,
            })
            continue

        # SSN match found - BA is same (100% BA score)
        # Find best address match among all DEC records with this SSN
        best_addr_score = -1
        best_dec = None
        best_addr_result = None
        best_name_score = 0.0
        best_boost_source = None
        possible_addr_matches = 0

        # Pre-extract names from Canvas address (once per Canvas record)
        canvas_addr_names = extract_names_from_address(canvas_addr)

        for dec in dec_matches:
            addr_result = address_compare(
                canvas_addr, canvas_city, canvas_zip,
                dec['addraddress'], dec['addrcity'], dec['addrzipcode'])
            name_result = name_compare(canvas_name, dec['hdrname'])
            primary_score = name_result['name_score']

            # --- Supplementary name comparisons (boost only) ---
            effective_score = primary_score
            boost_source = None

            if primary_score < 0.95:
                supp_candidates = []
                dec_contact = safe_str(dec.get('addrcontact', ''))
                dec_addr_names = extract_names_from_address(
                    dec.get('addraddress', ''))

                # 1) Canvas name vs DEC addrcontact
                if dec_contact:
                    s = name_compare(canvas_name, dec_contact)['name_score']
                    if s > effective_score:
                        supp_candidates.append(('DEC_CONTACT', s))

                # 2) Canvas addr name vs DEC hdrname
                for aname in canvas_addr_names:
                    s = name_compare(aname, dec['hdrname'])['name_score']
                    if s > effective_score:
                        supp_candidates.append(('CANVAS_ADDR_NAME', s))

                # 3) Canvas addr name vs DEC addrcontact
                if dec_contact:
                    for aname in canvas_addr_names:
                        s = name_compare(aname, dec_contact)['name_score']
                        if s > effective_score:
                            supp_candidates.append(
                                ('CANVAS_ADDR+DEC_CONTACT', s))

                # 4) Canvas name vs DEC addr name
                for dname in dec_addr_names:
                    s = name_compare(canvas_name, dname)['name_score']
                    if s > effective_score:
                        supp_candidates.append(('DEC_ADDR_NAME', s))

                # 5) Canvas addr name vs DEC addr name
                for aname in canvas_addr_names:
                    for dname in dec_addr_names:
                        s = name_compare(aname, dname)['name_score']
                        if s > effective_score:
                            supp_candidates.append(
                                ('CANVAS_ADDR+DEC_ADDR', s))

                # Pick best supplementary score, cap at 0.95
                for src, score in supp_candidates:
                    capped = min(score, 0.95)
                    if capped > effective_score:
                        effective_score = capped
                        boost_source = src

            # Count DEC records exceeding both thresholds (using rounded scores)
            if round5(effective_score * 100) > 94 and round5(addr_result['score'] * 100) > 84:
                possible_addr_matches += 1

            if addr_result['score'] > best_addr_score:
                best_addr_score = addr_result['score']
                best_dec = dec
                best_addr_result = addr_result
                best_name_score = effective_score
                best_boost_source = boost_source

        # Determine recommendation
        avg_name_addr = (best_name_score + best_addr_score) / 2
        if avg_name_addr < 0.40:
            # Name score 45+ with low address = same BA, new address
            if best_name_score >= 0.45 and best_addr_score <= 0.30:
                rec = 'EXISTING BA - NEW ADDRESS'
                stats['existing_ba_new_addr'] += 1
            else:
                rec = 'LIKELY BA MATCH - SSN MATCH NAME MISMATCH'
                stats['likely_new_ba'] += 1
        elif best_addr_result['same_address']:
            rec = 'EXISTING BA - EXISTING ADDRESS'
            stats['existing_ba_existing_addr'] += 1
        elif best_addr_score >= 0.75:
            rec = 'EXISTING BA - LIKELY SAME ADDRESS'
            stats['existing_ba_likely_addr'] += 1
        else:
            rec = 'EXISTING BA - NEW ADDRESS'
            stats['existing_ba_new_addr'] += 1

        results.append({
            **base_record,
            'dec_hdrcode': best_dec['hdrcode'],
            'dec_name': best_dec['hdrname'],
            'dec_address': best_dec['addraddress'],
            'dec_city': best_dec['addrcity'],
            'dec_state': best_dec['addrstate'],
            'dec_zip': best_dec['addrzipcode'],
            'dec_contact': best_dec['addrcontact'],
            'dec_addrsubcode': best_dec.get('addrsubcode', ''),
            'ssn_match': 100.0,
            'name_score': round5(best_name_score * 100),
            'address_score': round5(best_addr_score * 100),
            'address_reason': best_addr_result['reason'],
            'recommendation': rec,
            'name_boost_source': best_boost_source,
            'dec_match_count': len(dec_matches),
            'Number_possible_address_matches': possible_addr_matches,
        })
        # Also flag as trust if DEC name matches
        if not is_trust and _trust_re.search(str(best_dec['hdrname'])):
            results[-1]['is_trust'] = 1

        if (idx + 1) % 5000 == 0:
            elapsed = time.time() - start
            rate = (idx + 1) / elapsed
            remaining = (total - idx - 1) / rate
            print(f'  Processed {idx + 1:,}/{total:,} '
                  f'({elapsed:.1f}s elapsed, ~{remaining:.0f}s remaining)')

    elapsed = time.time() - start
    print(f'\nProcessed {total:,} records in {elapsed:.1f}s')

    # 4b. API override for ambiguous name scores
    if USE_API_OVERRIDE:
        ambiguous = [
            (i, r['canvas_name'], r['dec_name'])
            for i, r in enumerate(results)
            if r['ssn_match'] == 100.0
            and API_SCORE_MIN <= r['name_score'] <= API_SCORE_MAX
        ]
        if ambiguous:
            batches = (len(ambiguous) + API_BATCH_SIZE - 1) // API_BATCH_SIZE
            print(f'\nAPI override: {len(ambiguous)} ambiguous name pairs '
                  f'({batches} batch{"es" if batches != 1 else ""})...')
            overrides = api_override_name_scores(ambiguous)
            reclassified = 0
            for idx, new_score in overrides.items():
                r = results[idx]
                old_rec = r['recommendation']
                r['name_score'] = float(new_score)
                # Re-classify recommendation with new score
                addr_score = float(r['address_score'])
                avg = (float(new_score) + addr_score) / 2
                if avg < 40:
                    if float(new_score) >= 45 and addr_score <= 30:
                        r['recommendation'] = 'EXISTING BA - NEW ADDRESS'
                    else:
                        r['recommendation'] = 'LIKELY BA MATCH - SSN MATCH NAME MISMATCH'
                elif addr_score >= 90:
                    r['recommendation'] = 'EXISTING BA - EXISTING ADDRESS'
                elif addr_score >= 75:
                    r['recommendation'] = 'EXISTING BA - LIKELY SAME ADDRESS'
                else:
                    r['recommendation'] = 'EXISTING BA - NEW ADDRESS'
                if r['recommendation'] != old_rec:
                    reclassified += 1
            print(f'  Overrode {len(overrides)} scores, '
                  f'{reclassified} reclassified')

    # 4c. Google Address Validation API override for ambiguous address scores
    if USE_GOOGLE_ADDRESS_API and GOOGLE_API_KEY:
        ambiguous_addr = [
            (i, r['canvas_address'], r['canvas_city'], r['canvas_state'],
             r['canvas_zip'], r['canvas_id'], r['canvas_addrseq'],
             r['dec_address'], r['dec_city'], r['dec_state'],
             r['dec_zip'], r['dec_hdrcode'], r.get('dec_addrsubcode', ''),
             r['address_score'])
            for i, r in enumerate(results)
            if r['ssn_match'] == 100.0
            and GOOGLE_ADDR_SCORE_MIN <= r['address_score'] <= GOOGLE_ADDR_SCORE_MAX
        ]
        if ambiguous_addr:
            print(f'\nGoogle Address API: {len(ambiguous_addr)} ambiguous address pairs...')
            addr_overrides = google_override_address_scores(ambiguous_addr, conn)
            reclassified = 0
            score_overrides = 0
            for idx, ov in addr_overrides.items():
                r = results[idx]

                # Save originals before overwriting (Canvas)
                r['before_lookup_address'] = r['canvas_address']
                r['before_lookup_city'] = r['canvas_city']
                r['before_lookup_state'] = r['canvas_state']
                r['before_lookup_zip'] = r['canvas_zip']
                r['address_looked_up'] = 1
                r['canvas_lookup_verdict'] = (
                    json.dumps(ov['canvas_verdict'])
                    if ov['canvas_verdict'] else None)

                # Save originals before overwriting (DEC)
                r['before_lookup_dec_address'] = r['dec_address']
                r['before_lookup_dec_city'] = r['dec_city']
                r['before_lookup_dec_state'] = r['dec_state']
                r['before_lookup_dec_zip'] = r['dec_zip']
                r['dec_address_looked_up'] = 1
                r['dec_lookup_verdict'] = (
                    json.dumps(ov['dec_verdict'])
                    if ov['dec_verdict'] else None)

                # Replace Canvas address with standardized
                if ov['canvas_std']:
                    r['canvas_address'] = ov['canvas_std'][0]
                    r['canvas_city'] = ov['canvas_std'][1]
                    r['canvas_state'] = ov['canvas_std'][2]
                    r['canvas_zip'] = ov['canvas_std'][3]

                # Replace DEC address with standardized
                if ov['dec_std']:
                    r['dec_address'] = ov['dec_std'][0]
                    r['dec_city'] = ov['dec_std'][1]
                    r['dec_state'] = ov['dec_std'][2]
                    r['dec_zip'] = ov['dec_std'][3]

                # Update score and re-classify if meaningfully changed
                if ov['score_changed']:
                    score_overrides += 1
                    old_rec = r['recommendation']
                    new_addr_score = float(ov['new_score'])
                    r['address_score'] = new_addr_score
                    name_score = float(r['name_score'])
                    avg = (name_score + new_addr_score) / 2
                    if avg < 40:
                        if name_score >= 45 and new_addr_score <= 30:
                            r['recommendation'] = 'EXISTING BA - NEW ADDRESS'
                        else:
                            r['recommendation'] = 'LIKELY BA MATCH - SSN MATCH NAME MISMATCH'
                    elif ov['same_address'] or new_addr_score >= 90:
                        r['recommendation'] = 'EXISTING BA - EXISTING ADDRESS'
                    elif new_addr_score >= 75:
                        r['recommendation'] = 'EXISTING BA - LIKELY SAME ADDRESS'
                    else:
                        r['recommendation'] = 'EXISTING BA - NEW ADDRESS'
                    if r['recommendation'] != old_rec:
                        reclassified += 1

            print(f'  Validated {len(addr_overrides)} pairs, '
                  f'{score_overrides} scores overridden, '
                  f'{reclassified} reclassified')
    elif USE_GOOGLE_ADDRESS_API and not GOOGLE_API_KEY:
        print('\nGoogle Address API enabled but GOOGLE_API_KEY not set — skipping.')

    # 5. Insert results into database
    print(f'\nWriting {len(results):,} results to canvas_dec_matches table...')
    results_df = pd.DataFrame(results)

    # Ensure column order matches table schema
    col_order = [
        'ssn_match', 'name_score', 'name_boost_source', 'address_score', 'address_reason',
        'recommendation',
        'canvas_name', 'canvas_address', 'canvas_city', 'canvas_state',
        'canvas_zip', 'canvas_addrseq', 'canvas_id', 'canvas_ssn',
        # Google lookup - Canvas
        'before_lookup_address', 'before_lookup_city',
        'before_lookup_state', 'before_lookup_zip',
        'address_looked_up', 'canvas_lookup_verdict',
        # DEC
        'dec_hdrcode', 'dec_name', 'dec_address', 'dec_city',
        'dec_state', 'dec_zip', 'dec_contact', 'dec_addrsubcode',
        # Google lookup - DEC
        'before_lookup_dec_address', 'before_lookup_dec_city',
        'before_lookup_dec_state', 'before_lookup_dec_zip',
        'dec_address_looked_up', 'dec_lookup_verdict',
        #
        'dec_match_count', 'Number_possible_address_matches',
        'is_trust',
    ]
    results_df = results_df[col_order]
    results_df = results_df.sort_values('recommendation').reset_index(drop=True)
    results_df.to_sql('canvas_dec_matches', conn, if_exists='append',
                      index=False)

    # Create indexes for fast lookups
    print('Creating indexes...')
    cursor.execute(
        'CREATE INDEX idx_cdm_ssn ON canvas_dec_matches(canvas_ssn)')
    cursor.execute(
        'CREATE INDEX idx_cdm_rec ON canvas_dec_matches(recommendation)')
    cursor.execute(
        'CREATE INDEX idx_cdm_ssn_match ON canvas_dec_matches(ssn_match)')
    cursor.execute(
        'CREATE INDEX idx_cdm_addr_score ON canvas_dec_matches(address_score)')
    conn.commit()

    # 6. Print summary
    print(f'\n{"=" * 80}')
    print('MATCHING RESULTS SUMMARY')
    print('=' * 80)
    print(f'Total Canvas records:              {total:,}')
    print(f'')
    print(f'EXISTING BA - EXISTING ADDRESS:    '
          f'{stats["existing_ba_existing_addr"]:,}')
    print(f'EXISTING BA - LIKELY SAME ADDRESS: '
          f'{stats["existing_ba_likely_addr"]:,}')
    print(f'EXISTING BA - NEW ADDRESS:         '
          f'{stats["existing_ba_new_addr"]:,}')
    print(f'LIKELY NEW BA - NAME MISMATCH:     '
          f'{stats["likely_new_ba"]:,}')
    print(f'NEW BA - NO DEC MATCH:             {stats["new_ba"]:,}')
    print(f'NEW BA - NO SSN:                   {stats["no_ssn"]:,}')
    print(f'NEW BA - INVALID SSN:              {stats["bad_ssn"]:,}')
    print(f'')
    total_existing = (stats['existing_ba_existing_addr'] +
                      stats['existing_ba_likely_addr'] +
                      stats['existing_ba_new_addr'])
    total_new = (stats['new_ba'] + stats['no_ssn'] + stats['bad_ssn'] +
                 stats['likely_new_ba'])
    print(f'Total existing BAs (SSN matched):  {total_existing:,}')
    print(f'Total new BAs (no SSN match):      {total_new:,}')
    print(f'')
    print(f'Results table: canvas_dec_matches')
    print(f'Database: {DB_PATH}')
    print(f'')
    print('Query examples:')
    print('  -- All existing BAs with exact address match')
    print('  SELECT * FROM canvas_dec_matches '
          'WHERE recommendation = "EXISTING BA - EXISTING ADDRESS";')
    print('')
    print('  -- BAs needing address review')
    print('  SELECT * FROM canvas_dec_matches '
          'WHERE recommendation = "EXISTING BA - LIKELY SAME ADDRESS";')
    print('')
    print('  -- All new BAs')
    print('  SELECT * FROM canvas_dec_matches '
          'WHERE ssn_match = 0;')
    print('')
    print('  -- Summary by recommendation')
    print('  SELECT recommendation, COUNT(*) as cnt '
          'FROM canvas_dec_matches GROUP BY recommendation;')
    print('=' * 80)

    # 7. Export to Excel with color-coded headers
    excel_path = 'output/canvas_dec_matches.xlsx'
    print(f'\nExporting to {excel_path}...')
    export_df = pd.read_sql('SELECT * FROM canvas_dec_matches', conn)
    export_df.drop(columns=['id', 'created_at'], inplace=True, errors='ignore')

    # Reorder columns: scores/recommendation first
    export_df = export_df[col_order]

    # Mask SSNs in Excel output (show last 4 only)
    if 'canvas_ssn' in export_df.columns:
        export_df['canvas_ssn'] = export_df['canvas_ssn'].apply(mask_ssn)

    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='Matches')
        ws = writer.sheets['Matches']
        ws.freeze_panes = 'A2'

        blue_fill = PatternFill(start_color='DCE6F1', end_color='DCE6F1',
                                fill_type='solid')
        tan_fill = PatternFill(start_color='FDE9D9', end_color='FDE9D9',
                               fill_type='solid')
        red_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE',
                               fill_type='solid')
        header_font = Font(bold=True)

        # Identify trust-related rows
        trust_words = re.compile(
            r'\b(TRUST|TRUSTEE|TRUSTEES|TRUSTEESHIP|LIVING TRUST|'
            r'FAMILY TRUST|REVOCABLE TRUST|IRREVOCABLE TRUST|'
            r'TESTAMENTARY|ESTATE OF|DECEDENT)\b', re.IGNORECASE)
        trust_rows = set()
        for i, row_data in export_df.iterrows():
            name_text = f"{row_data.get('canvas_name', '')} {row_data.get('dec_name', '')}"
            if trust_words.search(name_text):
                trust_rows.add(i + 2)  # +2: 1-indexed header + 1-indexed data

        num_cols = len(export_df.columns)
        for col_idx, col_name in enumerate(export_df.columns, 1):
            cell = ws.cell(row=1, column=col_idx)
            cell.font = header_font
            cell.alignment = Alignment(horizontal='center')
            if col_name.startswith('canvas_'):
                fill = blue_fill
            elif col_name.startswith('dec_'):
                fill = tan_fill
            else:
                fill = None
            if fill:
                for row_idx in range(1, len(export_df) + 2):
                    if row_idx not in trust_rows:
                        ws.cell(row=row_idx, column=col_idx).fill = fill

        # Apply light red to entire row for trust-related records
        for row_idx in trust_rows:
            for col_idx in range(1, num_cols + 1):
                ws.cell(row=row_idx, column=col_idx).fill = red_fill

        print(f'  Highlighted {len(trust_rows):,} trust-related rows in light red')

        # ===== Stats tab =====
        # Use unmasked export_df (already has all match data)
        df = export_df  # alias for brevity

        # Pre-compute trust flag on df for stats
        df['_is_trust'] = df.apply(
            lambda r: bool(trust_words.search(
                f"{r.get('canvas_name', '')} {r.get('dec_name', '')}")),
            axis=1)

        ssn_matched = df[df['ssn_match'] == 100]
        no_match = df[df['ssn_match'] == 0]

        stats_rows = []

        def add(label, value, section=''):
            stats_rows.append({'Section': section, 'Metric': label,
                               'Value': value})

        def add_blank():
            stats_rows.append({'Section': '', 'Metric': '', 'Value': ''})

        # --- Overall ---
        add('Total Canvas Records', len(df), 'OVERALL')
        add('Unique Canvas SSNs (non-empty)',
            df[df['canvas_ssn'].str.len() > 0]['canvas_ssn'].nunique())
        add('Records with SSN Match (Existing BA)', len(ssn_matched))
        add('Records with No Match (New BA)', len(no_match))
        add_blank()

        # --- Recommendation Breakdown ---
        add('EXISTING BA - EXISTING ADDRESS',
            len(df[df['recommendation'] == 'EXISTING BA - EXISTING ADDRESS']),
            'BY RECOMMENDATION')
        add('EXISTING BA - LIKELY SAME ADDRESS',
            len(df[df['recommendation'] == 'EXISTING BA - LIKELY SAME ADDRESS']))
        add('EXISTING BA - NEW ADDRESS',
            len(df[df['recommendation'] == 'EXISTING BA - NEW ADDRESS']))
        add('LIKELY BA MATCH - SSN MATCH NAME MISMATCH',
            len(df[df['recommendation'] == 'LIKELY BA MATCH - SSN MATCH NAME MISMATCH']))
        add('NEW BA - NO DEC MATCH',
            len(df[df['recommendation'] == 'NEW BA - NO DEC MATCH']))
        add('NEW BA - NO SSN',
            len(df[df['recommendation'] == 'NEW BA - NO SSN']))
        add('NEW BA - INVALID SSN',
            len(df[df['recommendation'] == 'NEW BA - INVALID SSN']))
        add_blank()

        # --- Auto-Add Candidates ---
        auto_add = ssn_matched[ssn_matched['name_score'] >= 90]
        auto_add_high = ssn_matched[ssn_matched['name_score'] >= 95]
        add('SSN Match + Name >= 95% (High Confidence)', len(auto_add_high),
            'AUTO-ADD CANDIDATES')
        add('SSN Match + Name >= 90% (Moderate Confidence)', len(auto_add))
        add('SSN Match + Name < 90% (Needs Review)',
            len(ssn_matched[ssn_matched['name_score'] < 90]))
        add_blank()

        # --- Address Actions ---
        exact_addr = df[df['recommendation'] == 'EXISTING BA - EXISTING ADDRESS']
        likely_addr = df[df['recommendation'] == 'EXISTING BA - LIKELY SAME ADDRESS']
        new_addr = df[df['recommendation'] == 'EXISTING BA - NEW ADDRESS']
        add('Addresses Confirmed (Exact Match)', len(exact_addr),
            'ADDRESS ACTIONS')
        add('Addresses to Review (Likely Same)', len(likely_addr))
        add('New Addresses to Add', len(new_addr))
        add('Pct of Existing BAs Needing New Address',
            f"{len(new_addr) / max(len(ssn_matched), 1) * 100:.1f}%")
        add_blank()

        # --- Name Boost ---
        boosted = ssn_matched[ssn_matched['name_boost_source'].notna()]
        add('Records with Name Score Boosted', len(boosted),
            'NAME BOOST (from address/contact fields)')
        for src in ['DEC_CONTACT', 'CANVAS_ADDR_NAME',
                     'CANVAS_ADDR+DEC_CONTACT', 'DEC_ADDR_NAME',
                     'CANVAS_ADDR+DEC_ADDR']:
            cnt = len(boosted[boosted['name_boost_source'] == src])
            if cnt > 0:
                add(f'  Boost from {src}', cnt)
        add_blank()

        # --- Trust / Estate ---
        trust_df = df[df['_is_trust']]
        add('Total Trust/Estate Records', len(trust_df),
            'TRUST & ESTATE')
        add('Trusts with SSN Match', len(trust_df[trust_df['ssn_match'] == 100]))
        add('Trusts with No Match', len(trust_df[trust_df['ssn_match'] == 0]))
        add('Pct of All Records that are Trust/Estate',
            f"{len(trust_df) / max(len(df), 1) * 100:.1f}%")
        add_blank()

        # --- Multiple DEC Matches ---
        multi = ssn_matched[ssn_matched['dec_match_count'] > 1]
        add('Records with Multiple DEC Matches', len(multi),
            'DUPLICATE RISK')
        add('Records with Possible Address Matches > 0',
            len(df[df['Number_possible_address_matches'] > 0]))
        add('Avg DEC Records per SSN (when matched)',
            f"{ssn_matched['dec_match_count'].mean():.1f}")
        add('Max DEC Records for Single SSN',
            int(ssn_matched['dec_match_count'].max()))
        add_blank()

        # --- Name Score Distribution (SSN-matched only) ---
        add('Name Score = 100', len(ssn_matched[ssn_matched['name_score'] == 100]),
            'NAME SCORE DISTRIBUTION (SSN MATCHED)')
        add('Name Score 90-95',
            len(ssn_matched[ssn_matched['name_score'].between(90, 95)]))
        add('Name Score 80-85',
            len(ssn_matched[ssn_matched['name_score'].between(80, 85)]))
        add('Name Score 70-75',
            len(ssn_matched[ssn_matched['name_score'].between(70, 75)]))
        add('Name Score < 70',
            len(ssn_matched[ssn_matched['name_score'] < 70]))
        add_blank()

        # --- Top States ---
        add('', '', 'TOP 10 STATES')
        top_states = (df[df['canvas_state'] != '']
                      .groupby('canvas_state').size()
                      .sort_values(ascending=False).head(10))
        for state, cnt in top_states.items():
            add(state, cnt)

        # Write stats sheet
        stats_df = pd.DataFrame(stats_rows)
        stats_df.to_excel(writer, index=False, sheet_name='Summary Stats')
        ws2 = writer.sheets['Summary Stats']

        # Format stats sheet
        section_fill = PatternFill(start_color='4472C4', end_color='4472C4',
                                   fill_type='solid')
        section_font = Font(bold=True, color='FFFFFF')
        metric_font = Font(bold=False)
        value_font = Font(bold=True)
        header_fill = PatternFill(start_color='D6DCE4', end_color='D6DCE4',
                                  fill_type='solid')

        # Header row
        for col_idx in range(1, 4):
            cell = ws2.cell(row=1, column=col_idx)
            cell.font = Font(bold=True)
            cell.fill = header_fill

        # Format section headers and values
        for row_idx in range(2, len(stats_df) + 2):
            section_val = ws2.cell(row=row_idx, column=1).value
            metric_val = ws2.cell(row=row_idx, column=2).value
            if section_val and not metric_val:
                # Section header row
                for col_idx in range(1, 4):
                    cell = ws2.cell(row=row_idx, column=col_idx)
                    cell.fill = section_fill
                    cell.font = section_font
            elif metric_val:
                ws2.cell(row=row_idx, column=2).font = metric_font
                ws2.cell(row=row_idx, column=3).font = value_font

        # Column widths
        ws2.column_dimensions['A'].width = 22
        ws2.column_dimensions['B'].width = 50
        ws2.column_dimensions['C'].width = 18

        # Clean up temp column
        df.drop(columns=['_is_trust'], inplace=True, errors='ignore')

        print(f'  Created Summary Stats tab')

        # ===== Data Completeness tab =====
        total_rows = len(df)
        completeness_rows = []
        for col in df.columns:
            filled = df[col].apply(
                lambda v: v is not None and str(v).strip() != ''
                and str(v).strip().lower() != 'nan').sum()
            pct = filled / total_rows * 100 if total_rows > 0 else 0
            completeness_rows.append({
                'Column': col,
                'Non-Empty Count': int(filled),
                'Empty Count': total_rows - int(filled),
                'Pct Filled': round(pct, 1),
            })

        comp_df = pd.DataFrame(completeness_rows)
        comp_df.to_excel(writer, index=False, sheet_name='Data Completeness')
        ws3 = writer.sheets['Data Completeness']

        # Format header
        for col_idx in range(1, 5):
            cell = ws3.cell(row=1, column=col_idx)
            cell.font = Font(bold=True)
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal='center')

        # Color-code Pct Filled column (col 4)
        green_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE',
                                 fill_type='solid')
        yellow_fill = PatternFill(start_color='FFEB9C', end_color='FFEB9C',
                                  fill_type='solid')
        red_comp_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE',
                                    fill_type='solid')
        for row_idx in range(2, len(comp_df) + 2):
            pct_val = ws3.cell(row=row_idx, column=4).value
            if pct_val is not None:
                if pct_val >= 80:
                    ws3.cell(row=row_idx, column=4).fill = green_fill
                elif pct_val >= 40:
                    ws3.cell(row=row_idx, column=4).fill = yellow_fill
                else:
                    ws3.cell(row=row_idx, column=4).fill = red_comp_fill

        ws3.column_dimensions['A'].width = 35
        ws3.column_dimensions['B'].width = 18
        ws3.column_dimensions['C'].width = 15
        ws3.column_dimensions['D'].width = 12

        print(f'  Created Data Completeness tab')

    print(f'Exported {len(export_df):,} rows to {excel_path}')
    print('=' * 80)

    conn.close()


if __name__ == '__main__':
    main()
