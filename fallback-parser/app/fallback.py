def fallback_parse(data):
    # very simple fallback logic
    if "amount" not in data:
        data["amount"] = 0.0
    return data
