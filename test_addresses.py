import re
import pandas as pd

# Copy relevant functions from the script
WORD_NUM = {
    "ONE":"1","TWO":"2","THREE":"3","FOUR":"4","FIVE":"5",
    "SIX":"6","SEVEN":"7","EIGHT":"8","NINE":"9","TEN":"10"
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
    match_dist = max(len1, len2) // 2 - 1
    match_dist = max(0, match_dist)
    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    transpositions = 0
    for i in range(len1):
        start = max(0, i - match_dist)
        end   = min(i + match_dist + 1, len2)
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

def normalize_address(addr, trust: bool = False) -> str:
    addr = safe_str(addr)
    if not addr:
        return ""
    if trust:
        return re.sub(r"\s+", " ", addr.upper()).strip()

    addr = addr.upper()

    # STRICT: Treat as PO Box ONLY if entire line is exactly "BOX <num/wordnum>"
    m = re.fullmatch(r"\s*BOX\s*#?\s*(\d+|ONE|TWO|THREE|FOUR|FIVE|SIX|SEVEN|EIGHT|NINE|TEN)\s*", addr)
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
    po_match = re.search(r"\bPO\s*BOX\s+(ONE|TWO|THREE|FOUR|FIVE|SIX|SEVEN|EIGHT|NINE|TEN)\b", addr)
    if po_match:
        word = po_match.group(1)
        addr = addr.replace(f"PO BOX {word}", f"PO BOX {WORD_NUM[word]}")

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

def compact_alnum(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())

def normalize_city(val: str) -> str:
    s = safe_str(val).upper().strip()
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def normalize_name(name, trust: bool = False) -> str:
    name = safe_str(name)
    if not name:
        return ""
    if trust:
        return re.sub(r"\s+", " ", name.upper()).strip()

    name = re.sub(r"[.,\-']", "", name)

    suffix_map = {
        "JUNIOR": "JR", "SENIOR": "SR",
        "THIRD": "III", "FOURTH": "IV", "SECOND": "II",
        "CORPORATION": "CORP", "INCORPORATED": "INC",
        "COMPANY": "CO", "LIMITED": "LTD",
        r"L\s*L\s*C": "LLC", r"L\s*C": "LC", r"L\s*P": "LP",
    }
    for pat, repl in suffix_map.items():
        name = re.sub(r"\b" + pat + r"\b", repl, name, flags=re.IGNORECASE)

    return re.sub(r"\s+", " ", name.upper()).strip()

def name_compare(name1: str, name2: str) -> dict:
    """Compare two names and return similarity score."""
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)

    if not n1 or not n2:
        return {"name_score": 0.0, "name_match": False}

    if n1 == n2:
        return {"name_score": 1.0, "name_match": True}

    # Check for initials match (e.g., "M ROBERTSON" vs "MIKE ROBERTSON")
    t1 = n1.split()
    t2 = n2.split()

    if len(t1) >= 2 and len(t2) >= 2 and t1[-1] == t2[-1]:
        # Last names match
        if len(t1[0]) == 1 and len(t2[0]) > 1 and t1[0] == t2[0][0]:
            return {"name_score": 0.95, "name_match": True}
        if len(t2[0]) == 1 and len(t1[0]) > 1 and t2[0] == t1[0][0]:
            return {"name_score": 0.95, "name_match": True}

    # Fuzzy match
    score = similarity(n1, n2)
    return {"name_score": score, "name_match": score >= 0.85}

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
    STREET_TYPE_TOKENS = {"ST","AVE","BLVD","DR","RD","LN","CT","CIR","PL","HWY","PKWY","TER","TRL"}
    DIR_TOKENS = {"N","S","E","W","NE","NW","SE","SW"}
    UNIT_TOKENS = {"APT","STE","UNIT","BLDG","FL","RM"}

    s = (addr_norm or "").upper()
    s = re.sub(r"^\s*\d+\s+", "", s)
    s = re.sub(r"\b(APT|STE|UNIT|BLDG|FL|RM)\b\s*\w+", " ", s)
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    tokens = [t for t in s.split() if t not in STREET_TYPE_TOKENS and t not in DIR_TOKENS and t not in UNIT_TOKENS]
    return " ".join(tokens).strip()

def address_compare(addr1, city1, zip1, addr2, city2, zip2) -> dict:
    """Compare two addresses and return match result."""

    addr1_norm = normalize_address(addr1)
    addr2_norm = normalize_address(addr2)

    # Check if PO Box
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
            return {"same_address": False, "score": 0.0, "reason": f"POBOX_NUM_MISMATCH {box1}!={box2}"}

        city1_norm = normalize_city(city1)
        city2_norm = normalize_city(city2)

        if city1_norm and city2_norm:
            city_sim = max(similarity(city1_norm, city2_norm),
                          similarity(compact_alnum(city1_norm), compact_alnum(city2_norm)))
        else:
            city_sim = 0.90

        zip_bonus = 0.05 if (zip1 and zip2 and zip1[:5] == zip2[:5]) else 0.0
        score = min(1.0, 0.92 * city_sim + zip_bonus)
        same = score >= 0.90 and city_sim >= 0.85

        return {"same_address": same, "score": score, "reason": f"POBOX_MATCH box={box1} city_sim={city_sim:.2f}"}

    # Street address comparison
    num1 = parse_house_number(addr1_norm)
    num2 = parse_house_number(addr2_norm)

    if not num1 or not num2:
        return {"same_address": False, "score": 0.0, "reason": "MISSING_HOUSE_NUMBER"}
    if num1 != num2:
        return {"same_address": False, "score": 0.0, "reason": f"HOUSE_NUM_MISMATCH {num1}!={num2}"}

    core1 = street_core_for_match(addr1_norm)
    core2 = street_core_for_match(addr2_norm)
    street_sim = max(similarity(core1, core2), similarity(compact_alnum(core1), compact_alnum(core2)))

    city1_norm = normalize_city(city1)
    city2_norm = normalize_city(city2)

    if city1_norm and city2_norm:
        city_sim = max(similarity(city1_norm, city2_norm),
                      similarity(compact_alnum(city1_norm), compact_alnum(city2_norm)))
    else:
        city_sim = 0.90

    zip_match = (zip1 and zip2 and zip1[:5] == zip2[:5])
    zip_ok = True
    if zip1 and zip2 and zip1[:5] != zip2[:5]:
        zip_ok = city_sim >= 0.92

    same = (street_sim >= 0.90) and zip_ok and (city_sim >= 0.85 or not city1_norm or not city2_norm)
    score = street_sim * (0.6 + 0.4 * city_sim)
    if zip_match and score >= 0.85:
        score = min(1.0, score + 0.05)
    if not zip_ok:
        score *= 0.75

    return {
        "same_address": same,
        "score": score,
        "reason": f"STREET num={num1} street_sim={street_sim:.2f} city_sim={city_sim:.2f} zip_ok={zip_ok}"
    }

import csv

# Test cases from the data
print("=" * 80)
print("CLAUDE-FOCUSED ADDRESS COMPARISON ANALYSIS")
print("=" * 80)

csv_results = []

test_cases = [
    {
        "id": "0027C5TQ7ER8",
        "records": [
            ("Lia Miles", "14435 Jodi Landing Suite 119", "Port Emilyland", "PA", "25212"),
            ("Lisa Miles", "89641 Ortiz Camp", "Mooreville", "NC", "70775"),
        ]
    },
    {
        "id": "0167J952FIE9",
        "records": [
            ("Bianca Johnson", "2172 Jeremiah Points Apt. 955", "New Sara", "CO", "46829"),
            ("Bianca Johnson", "2172 Jeremiah Points", "New Sara", "CO", "46829"),
        ]
    },
    {
        "id": "01T71C728T",
        "records": [
            ("Harris", "35027 John Vista", "Cynthiaton", "IN", "22914"),
            ("Harris Group", "750 Kim Ports Suite 391", "Davidhaven", "NE", "45107"),
        ]
    },
    {
        "id": "020R5Z11Y",
        "records": [
            ("Joseph Poote", "5212 Abbott Land Apt. 103", "Rachelburgh", "WY", "66978"),
            ("Joseph Poote", "5212 Abbott Land", "", "WY", "66978"),
        ]
    },
    {
        "id": "928878833",
        "records": [
            ("mike robertson", "9215 delacorte", "missouricty", "tx", "77459"),
            ("michael robertson", "9215 dela corte", "missouri city", "tx", ""),
            ("m. Robertson", "9215 delacorte", "missouri city", "tx", "77459"),
        ]
    },
    {
        "id": "92887845",
        "records": [
            ("cole robertson", "", "missouri city", "tx", "77459-2733"),
        ]
    },
    {
        "id": "8388499923",
        "records": [
            ("Charles Moak", "po box 1", "HOUSTON", "tx", ""),
            ("Charlie Moke", "P.O. BOX ONE", "", "tx", "77002"),
            ("c. Moke", "BOX 1", "", "", "77002"),
        ]
    },
    {
        "id": "938883992",
        "records": [
            ("Joe smith", "lock box 12", "Houston", "tx", "77005"),
            ("Mr. Joe smith", "12 box springs rd", "Houston", "tx", "77004"),
        ]
    },
    {
        "id": "8399478772",
        "records": [
            ("t. smith", "LOCK BOX 1", "pasadena", "tx", "77503"),
            ("tina smith", "BOX SPRINGS 1", "", "tx", "77503"),
            ("christina smith", "DROP BOX A1", "pasadena", "tx", "77503"),
            ("tina s.", "MAIL BOX 934445", "pasadena", "tx", "77503"),
        ]
    }
]

for case_idx, case in enumerate(test_cases):
    print(f"\n{'=' * 80}")
    print(f"ID: {case['id']}")
    print('=' * 80)

    records = case['records']

    # Within-ID comparisons
    for i, rec1 in enumerate(records):
        for j, rec2 in enumerate(records):
            if i >= j:
                continue

            name1, addr1, city1, state1, zip1 = rec1
            name2, addr2, city2, state2, zip2 = rec2

            print(f"\nComparing:")
            print(f"  [{i+1}] {name1:20s} | {addr1:35s} | {city1:18s} | {state1:2s} | {zip1}")
            print(f"  [{j+1}] {name2:20s} | {addr2:35s} | {city2:18s} | {state2:2s} | {zip2}")

            # ID is authoritative - always block on ID first
            id_match = (case['id'] == case['id'])  # All records in same case have same ID

            if id_match:
                # Same ID = Same BA (no need to check name/address for BA matching)
                ba_score = 1.0
                ba_match = True
                ba_reason = "Same ID (SSN/EIN)"

                # Now check address
                addr_result = address_compare(addr1, city1, zip1, addr2, city2, zip2)
                addr_score = addr_result['score']

                if addr_result['same_address']:
                    recommendation = "EXISTING BA - EXISTING ADDRESS"
                    action = "Same BA, same address - no action needed"
                elif addr_score >= 0.75:
                    recommendation = "EXISTING BA - LIKELY SAME ADDRESS"
                    action = "Same BA, likely same address - review recommended"
                else:
                    recommendation = "EXISTING BA - NEW ADDRESS"
                    action = "Same BA, different address - add new address to BA"
            else:
                # Different IDs should never happen in within-ID loop, but handle it
                ba_score = 0.0
                ba_match = False
                ba_reason = "Different IDs"
                addr_result = {"same_address": False, "score": 0.0, "reason": "N/A - Different IDs"}
                addr_score = 0.0
                recommendation = "ERROR - Different IDs in same group"
                action = "This should not happen"

            print(f"\n  BA MATCH: {ba_match} (Score: {ba_score:.2%}) - {ba_reason}")
            print(f"  ADDRESS MATCH: {addr_result['same_address']} (Score: {addr_score:.2%})")
            print(f"  Recommendation: {recommendation}")
            print(f"  Action: {action}")
            print(f"  Address Reason: {addr_result['reason']}")
            print(f"  Normalized Name 1: {normalize_name(name1)}")
            print(f"  Normalized Name 2: {normalize_name(name2)}")
            print(f"  Normalized Addr 1: {normalize_address(addr1)}")
            print(f"  Normalized Addr 2: {normalize_address(addr2)}")

            # Store for CSV
            csv_results.append({
                "ID": case['id'],
                "Name_1": name1,
                "Name_2": name2,
                "BA_Match": ba_match,
                "BA_Score": f"{ba_score:.2%}",
                "BA_Reason": ba_reason,
                "Address_1": addr1,
                "City_1": city1,
                "State_1": state1,
                "Zip_1": zip1,
                "Address_2": addr2,
                "City_2": city2,
                "State_2": state2,
                "Zip_2": zip2,
                "Address_Match": addr_result['same_address'],
                "Address_Score": f"{addr_score:.2%}",
                "Address_Reason": addr_result['reason'],
                "Recommendation": recommendation,
                "Action": action,
                "Normalized_Name_1": normalize_name(name1),
                "Normalized_Name_2": normalize_name(name2),
                "Normalized_Addr_1": normalize_address(addr1),
                "Normalized_Addr_2": normalize_address(addr2)
            })

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
print("\nNOTE: Records with different IDs are NEVER compared.")
print("Each unique ID represents a separate BA and will be processed independently.")

# Write CSV with timestamp
import datetime
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
csv_file = f"address_comparison_results_{timestamp}.csv"
with open(csv_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=[
        "ID", "Name_1", "Name_2", "BA_Match", "BA_Score", "BA_Reason",
        "Address_1", "City_1", "State_1", "Zip_1",
        "Address_2", "City_2", "State_2", "Zip_2",
        "Address_Match", "Address_Score", "Address_Reason",
        "Recommendation", "Action",
        "Normalized_Name_1", "Normalized_Name_2", "Normalized_Addr_1", "Normalized_Addr_2"
    ])
    writer.writeheader()
    writer.writerows(csv_results)

print(f"\nCSV written to: {csv_file}")
print(f"Total comparisons: {len(csv_results)}")
