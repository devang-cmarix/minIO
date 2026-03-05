import re
from datetime import datetime

DATE_PATTERNS = [
    r"\b[A-Z][a-z]{2} \d{2} \d{4}\b",
    r"\d{2}/\d{2}/\d{4}"
]

def extract_dates(text):
    for pattern in DATE_PATTERNS:
        match = re.search(pattern, text)
        if match:
            try:
                return datetime.strptime(match.group(), "%b %d %Y").date()
            except:
                pass
    # no valid date found, return None instead of crashing
    return None


def extract_key_value_pairs(text):
    pairs = {}
    lines = text.splitlines()

    for line in lines:
        if ":" in line:
            key, value = line.split(":", 1)
            pairs[key.strip().lower()] = value.strip()

    return pairs