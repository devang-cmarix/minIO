# import re
# from datetime import datetime

# def parse_money(value):
#     if not value:
#         return 0.0
#     value = value.replace("$", "").replace(",", "").strip()
#     try:
#         return float(value)
#     except:
#         return 0.0

# def parse_date(text):
#     try:
#         return datetime.strptime(text.strip(), "%b %d %Y").date()
#     except:
#         return None

# def extract_invoice_number(text):
#     match = re.search(r"#\s*(\d+)", text)
#     return match.group(1) if match else None

# def extract_vendor(text):
#     lines = text.splitlines()
#     for i, line in enumerate(lines):
#         if line.strip().upper() == "INVOICE":
#             if i + 2 < len(lines):
#                 return lines[i + 2].strip()
#     return None

# def extract_customer(text):
#     match = re.search(r"Bill To:\s*(.+)", text)
#     return match.group(1).strip() if match else None

# def extract_invoice_date(text):
#     match = re.search(r"([A-Z][a-z]{2}\s+\d{2}\s+\d{4})", text)
#     return parse_date(match.group(1)) if match else None

# def extract_totals(text):
#     subtotal_match = re.search(r"Subtotal:\s*\$?([\d,]+\.\d{2})", text)
#     total_match = re.search(r"Total:\s*\$?([\d,]+\.\d{2})", text)
#     subtotal = parse_money(subtotal_match.group(1)) if subtotal_match else 0
#     total = parse_money(total_match.group(1)) if total_match else 0
#     return subtotal, total

# def extract_shipping(text):
#     match = re.search(r"Shipping:\s*\$?([\d,]+\.\d{2})", text)
#     if match:
#         return parse_money(match.group(1))
#     return 0.0

# def extract_items(text):
#     lines = text.splitlines()
#     items = []
#     for i in range(len(lines)):
#         # Match: Description
#         if re.search(r"\$[\d,]+\.\d{2}", lines[i]):
#             # Try reading previous lines for structure
#             parts = lines[i-3:i+1]
#             if len(parts) == 4:
#                 description = parts[0].strip()
#                 quantity = parts[1].strip()
#                 rate = parts[2].strip()
#                 amount = parts[3].strip()

#                 if quantity.isdigit():
#                     items.append({
#                         "description": description,
#                         "quantity": float(quantity),
#                         "rate": parse_money(rate),
#                         "amount": parse_money(amount)
#                     })
#     return items

# def apply_mapping(raw_text: str) -> dict:
#     data = {}
#     vendor = extract_vendor(raw_text)
#     customer = extract_customer(raw_text)
#     invoice_date = extract_invoice_date(raw_text)
#     subtotal, total = extract_totals(raw_text)
#     shipping = extract_shipping(raw_text)
#     items = extract_items(raw_text)
#     taxes = []
#     if shipping > 0:
#         taxes.append({
#             "tax_type": "Shipping",
#             "tax_rate": 0,
#             "tax_amount": shipping
#         })
#     data["invoice_date"] = invoice_date
#     data["due_date"] = None
#     data["vendor_name"] = vendor
#     data["customer_name"] = customer
#     data["subtotal"] = subtotal
#     data["total"] = total
#     data["invoice_items"] = items
#     data["invoice_taxes"] = taxes
#     return data

from app.vendor_detector import detect_vendor
from app.nlp_engine import extract_dates, extract_key_value_pairs
from app.item_parser import detect_items
from app.confidence import calculate_confidence
import re

def parse_money(value):
    return float(value.replace("$","").replace(",",""))

def extract_totals(text):
    money_values = re.findall(r"\$[\d,]+\.\d{2}", text)

    if not money_values:
        return 0, 0

    amounts = [float(v.replace("$","").replace(",","")) for v in money_values]

    total = max(amounts)
    subtotal = sorted(amounts)[-2] if len(amounts) > 1 else total

    return subtotal, total

def infer_tax(raw_text,subtotal, total):
    # if subtotal and total and total > subtotal:
    #     tax_amount = round(total - subtotal, 2)
    #     tax_rate = round((tax_amount / subtotal) * 100, 2)
    #     return [{
    #         "tax_type": "Inferred Tax",
    #         "tax_rate": tax_rate,
    #         "tax_amount": tax_amount
    #     }]

    money_values = re.findall(r"\$[\d,]+\.\d{2}", raw_text)
    amounts = [float(v.replace("$","").replace(",","")) for v in money_values]

    if not subtotal or not total:
        return []

    difference = round(total - subtotal, 2)

    for amt in amounts:
        if round(amt, 2) == difference:
            return [{
                "tax_type": "Shipping/Tax",
                "tax_rate": round((difference / subtotal) * 100, 2),
                "tax_amount": difference
            }]
            
    return []

def max_amount_from_text(text):
    import re
    money_values = re.findall(r"\$[\d,]+\.\d{2}", text)
    amounts = [float(v.replace("$","").replace(",","")) for v in money_values]
    return max(amounts) if amounts else 0

def apply_mapping(raw_text):
    vendor, vendor_conf = detect_vendor(raw_text)
    invoice_date = extract_dates(raw_text)
    kv_pairs = extract_key_value_pairs(raw_text)
    items = detect_items(raw_text)
    total = max_amount_from_text(raw_text)
    items = detect_items(raw_text)
    date=extract_dates(raw_text)
    subtotal = round(sum(item["amount"] for item in items), 2)
    taxes = infer_tax(raw_text,subtotal, total)

    data = {
        "invoice_date": invoice_date,
        "due_date": None,
        "vendor_name": vendor,
        "customer_name": kv_pairs.get("bill to"),
        "subtotal": subtotal,
        "total": total,
        "invoice_items": items,
        "invoice_taxes": taxes
    }

    data["confidence_score"] = calculate_confidence(data)

    return data