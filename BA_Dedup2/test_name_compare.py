"""Test name_compare with all improvements."""
import sys
sys.path.insert(0, '.')
from canvas_to_dec_match import name_compare

pairs = [
    ("CHUCK", "CHARLES"),
    ("SONDRA L PEACOCK", "SANDRA LEE PEACOCK"),
    ("ARTHUR WILLIAM & DELORES M SUNDHOLM", "ARTHUR W&DOLORES SUNDHOLM"),
    ("PRICE OIL AND GAS LTD", "PRICE OIL&GAS COMPANY, LP"),
    ("TWO JOHNS AND ELEESA LIMITED PARTNE", "2 JOHNS & ELEESA LTD"),
    ("DONAD CHARLES BOECKMAN", "DONALD BOECKMAN"),
    ("MIKE SWAFFORD", "MICHAEL SWAFFORD"),
    ("EOG RESOURCES INC", "ENRON OIL&GAS CO"),
    ("CSC", "CORPORATION SERVICE COMPANY"),
    ("NEVA M KING", "NEVA RANCIER KING"),
    ("LINN OPERATING LLC", "LINN ENERGY HOLDINGS LLC"),
    # New pairs
    ("KLX ENERGY SERVICES LLC", "KLX ENERGY HOLDINGS LLC"),
    ("R & G INVESTMENT CO", "R & G HOLDINGS"),
    ("PDIII EXPLORATION LTD", "PD III EXPLORATION LTD"),
    ("TEEPEE PETROLEUM COMPANY INC", "TEPEE PETROLEUM COMPANY INC"),
]
print("USER PAIRS:")
for a, b in pairs:
    r = name_compare(a, b)
    s = r["name_score"]
    print(f"  {int(s*100):>3}%  {a}  =  {b}")

bad = [
    ("RUSSELL V JOHNSON III", "JEC OPERATING LLC"),
    ("W W ALLEN", "WYNNE ROYALTY LLC"),
    ("NELSON H NEWMAN", "LEVASHEFF LIVING TRUST"),
]
print("\nBAD PAIRS (should stay low):")
for a, b in bad:
    r = name_compare(a, b)
    s = r["name_score"]
    print(f"  {int(s*100):>3}%  {a}  =  {b}")

good = [
    ("JOHN SMITH", "JOHN A SMITH"),
    ("ROBERT JOHNSON JR", "ROBERT JOHNSON"),
    ("J SMITH", "JOHN SMITH"),
    ("JOHN SMITH JR", "JOHN SMITH SR"),
    ("MARILYN CAVIN^^", "MARILYN CAVIN"),
    ("CONOCOPHILLIPS COMPANY", "CONOCO PHILLIPS COMPANY"),
    ("K E ANDREWS & COMPANY", "KE Andrews"),
    ("ANN/STAN LLC", "ANN STAN LLC"),
    ("JAMES L MURPHY JR", "J L MURPHY"),
]
print("\nPREVIOUSLY FIXED PAIRS (should stay high):")
for a, b in good:
    r = name_compare(a, b)
    s = r["name_score"]
    print(f"  {int(s*100):>3}%  {a}  =  {b}")
