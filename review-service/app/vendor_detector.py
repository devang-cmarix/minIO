import re
from rapidfuzz import fuzz

KNOWN_VENDORS = [
    "SuperStore",
    "Walmart",
    "Amazon",
    "Costco"
]

def detect_vendor(raw_text):
    lines = raw_text.splitlines()
    candidates = [l.strip() for l in lines if len(l.strip()) > 3]

    best_match = None
    best_score = 0

    for candidate in candidates[:10]:
        for vendor in KNOWN_VENDORS:
            score = fuzz.ratio(candidate.lower(), vendor.lower())
            if score > best_score:
                best_score = score
                best_match = vendor

    if best_score > 80:
        return best_match, best_score / 100

    # fallback heuristic
    return candidates[2] if len(candidates) > 2 else None, 0.5