FIELD_MAPPING = {
    "Vendor": "supplier_name",
    "Invoice No": "invoice_number",
    "Total": "invoice_total",
    "Date": "invoice_date"
}

def apply_mapping(extracted_text: dict) -> dict:
    if not isinstance(extracted_text, dict):
        return {}

    output = {}
    for key, value in extracted_text.items():
        if key in FIELD_MAPPING:
            output[FIELD_MAPPING[key]] = value

    return output