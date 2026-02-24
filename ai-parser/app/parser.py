import json
import re
import os
from groq import Groq

client = Groq(api_key=os.environ["GROQ_API_KEY"])

SYSTEM_PROMPT = """
You are an invoice parsing AI.

You MUST return ONLY valid JSON.
DO NOT include explanations, markdown, or text.

Schema:
{
  "invoice_date": "YYYY-MM-DD",
  "due_date": "YYYY-MM-DD or null",
  "vendor_name": "string",
  "customer_name": "string",
  "subtotal": number,
  "total": number,
  "invoice_taxes": [
    {
      "tax_type": "string",
      "tax_rate": number,
      "tax_amount": number
    }
  ],
  "invoice_items": [
    {
      "description": "string",
      "quantity": number,
      "rate": number,
      "amount": number
    }
  ]
}

Return STRICT valid JSON.
Do NOT use numeric keys like 0:, 1:.
invoice_items and invoice_taxes MUST be proper JSON arrays.
Example:
"invoice_items": [
  { "description": "...", "quantity": 1, "rate": 10, "amount": 10 }
]

"""

def ai_parse(text: str) -> dict:
    try:
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": text}
            ],
            temperature=0,
        )

        raw = response.choices[0].message.content.strip()

        # Extract JSON safely (very important)
        match = re.search(r"\{.*\}", raw, re.S)
        if not match:
            raise ValueError("No JSON found in AI response")

        parsed = json.loads(match.group())

        if not isinstance(parsed.get("invoice_items"), list):
            parsed["invoice_items"] = []

        if not isinstance(parsed.get("invoice_taxes"), list):
            parsed["invoice_taxes"] = [] 

        return parsed

    except Exception as e:
        # CRITICAL: log error but do NOT crash pipeline
        print(f"[AI PARSER ERROR] {e}")
        return {}


# llama-3.1-8b-instant