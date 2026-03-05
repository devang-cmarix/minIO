import re

def detect_table_header(lines):
    for i, line in enumerate(lines):
        if (
            "item" in line.lower()
            and "quantity" in line.lower()
            and "amount" in line.lower()
        ):
            return i
    return None

def is_money(value):
    return re.match(r"\$[\d,]+\.\d{2}", value.strip())

import re

def parse_money(val):
    return float(val.replace("$", "").replace(",", ""))

def detect_items(raw_text):
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    items = []

    i = 0
    while i < len(lines) - 3:
        description = lines[i]

        # Look for numeric quantity
        if re.match(r"^\d+(\.\d+)?$", lines[i+1]):

            # Look for rate and amount
            if (
                re.match(r"^\$[\d,]+\.\d{2}$", lines[i+2]) and
                re.match(r"^\$[\d,]+\.\d{2}$", lines[i+3])
            ):
                quantity = float(lines[i+1])
                rate = parse_money(lines[i+2])
                amount = parse_money(lines[i+3])

                items.append({
                    "description": description,
                    "quantity": quantity,
                    "rate": rate,
                    "amount": amount
                })

                # move pointer AFTER amount line
                i += 1
                continue

        i += 1

    return items