def calculate_confidence(data):
    score = 0
    max_score = 6

    if data.get("vendor_name"): score += 1
    if data.get("invoice_date"): score += 1
    if data.get("customer_name"): score += 1
    if data.get("subtotal"): score += 1
    if data.get("total"): score += 1
    if data.get("invoice_items"): score += 1

    return round(score / max_score, 2)