import strawberry
from typing import List,Optional
from sqlalchemy.orm import Session
from .database import get_db,SessionLocal
from .models import Invoice, InvoiceItems, InvoiceTaxes
from .schemas import InvoiceItemResponse, InvoiceTaxResponse,InvoiceResponse

@strawberry.type
class InvoiceType:
    id: int
    vendor_name: Optional[str]
    customer_name: Optional[str]
    subtotal: Optional[float]
    total: Optional[float]
    invoice_date: Optional[str]
    due_date: Optional[str]
    raw_text: Optional[str]

@strawberry.type
class InvoiceItemType:
    id: int
    invoice_id: Optional[int]
    description: Optional[str]
    quantity: Optional[float]
    rate: Optional[float]
    amount: Optional[float]
    
@strawberry.type
class InvoiceTaxType:
    id: int
    invoice_id: Optional[int]
    tax_type: Optional[str]
    tax_rate: Optional[float]
    tax_amount: Optional[float]

@strawberry.type
class Query:
    
    @strawberry.field
    def invoices(self)->List[InvoiceType]:
        db: Session=SessionLocal()
        return db.query(Invoice).all()
    
    @strawberry.field
    def invoice_items(self)->List[InvoiceItemType]:
        db:Session=SessionLocal()
        return db.query(InvoiceItems).all()
    
    @strawberry.field
    def invoice_taxes(self)->List[InvoiceTaxType]:
        db:Session=SessionLocal()
        return db.query(InvoiceTaxes).all()
    
schema=strawberry.Schema(query=Query)