def _to_float(val, default=0.0):
    """Safely convert values to float."""
    try:
        if val is None or val == "":
            return default
        return float(val)
    except Exception:
        return default


def validate_invoice(data: dict):
    errors = []

    # ---- Required fields ----
    required = ["invoice_date", "vendor_name", "total"]

    for field in required:
        if not data.get(field):
            errors.append(f"missing {field}")

    # ---- Safe numeric parsing ----
    subtotal = _to_float(data.get("subtotal"))
    total = _to_float(data.get("total"))

    taxes = data.get("taxes") or []
    items = data.get("items") or []

    tax_sum = sum(_to_float(t.get("tax_amount")) for t in taxes)
    item_sum = sum(_to_float(i.get("amount")) for i in items)

    # ---- Subtotal check ----
    if subtotal > 0 and item_sum > 0:
        if abs(subtotal - item_sum) > 1.0:
            errors.append("subtotal mismatch")

    # ---- Total check ----
    if subtotal > 0 and total > 0:
        if abs((subtotal + tax_sum) - total) > 1.0:
            errors.append("total mismatch")

    is_valid = len(errors) == 0
    return is_valid, errors
