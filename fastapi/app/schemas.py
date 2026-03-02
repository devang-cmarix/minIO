from pydantic import BaseModel
from typing import Optional
from datetime import date

class InvoiceResponse(BaseModel):
    id: int
    customer_name: Optional[str]
    vendor_name: Optional[str]
    total: Optional[float]
    subtotal: Optional[float]
    invoice_date: Optional[date]
    due_date: Optional[date]
    raw_text: Optional[str]
    class Config:
        orm_mode = True
        
class InvoiceItemResponse(BaseModel):
    id: int
    invoice_id: Optional[int]
    description: Optional[str]
    quantity: Optional[float]
    rate: Optional[float]
    amount: Optional[float]
    class Config:
        orm_mode = True
        
class InvoiceTaxResponse(BaseModel):
    id: int
    invoice_id: Optional[int]
    tax_type: Optional[str]
    tax_rate: Optional[float]
    tax_amount: Optional[float]
    class Config:
        orm_mode = True
