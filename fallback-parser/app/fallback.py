def fallback_parse(data):
    """
    Fallback parser for invoices with unknown/different layouts.
    Ensures minimum required structure before DB insert.
    """

    if not isinstance(data, dict):
        return {}

    # -------------------------
    # Normalize common fields
    # -------------------------

    # Invoice number variations
    data["invoice_number"] = (
        data.get("invoice_number")
        or data.get("invoiceNo")
        or data.get("invoice_id")
        or data.get("bill_no")
        or "UNKNOWN"
    )

    # Date variations
    data["date"] = (
        data.get("date")
        or data.get("invoice_date")
        or data.get("bill_date")
        or None
    )

    # Vendor / Supplier variations
    data["vendor"] = (
        data.get("vendor")
        or data.get("supplier")
        or data.get("company")
        or "UNKNOWN"
    )

    # -------------------------
    # Items Handling
    # -------------------------

    items = data.get("items")

    if not isinstance(items, list):
        items = []

    normalized_items = []

    for item in items:
        if not isinstance(item, dict):
            continue

        description = (
            item.get("description")
            or item.get("name")
            or item.get("item")
            or "Unknown Item"
        )

        quantity = float(item.get("quantity", 1) or 1)
        price = float(item.get("price", 0) or 0)

        amount = item.get("amount")
        if amount is None:
            amount = quantity * price

        normalized_items.append({
            "description": description,
            "quantity": quantity,
            "price": price,
            "amount": float(amount)
        })

    data["items"] = normalized_items

    # -------------------------
    # Subtotal Calculation
    # -------------------------

    if not data.get("subtotal"):
        data["subtotal"] = sum(i["amount"] for i in normalized_items)

    # -------------------------
    # Tax Handling
    # -------------------------

    data["tax"] = float(data.get("tax", 0) or 0)

    # -------------------------
    # Grand Total
    # -------------------------

    if not data.get("total"):
        data["total"] = float(data["subtotal"]) + float(data["tax"])

    # -------------------------
    # Final Safety Defaults
    # -------------------------

    data["subtotal"] = float(data.get("subtotal", 0) or 0)
    data["total"] = float(data.get("total", 0) or 0)

    return data
