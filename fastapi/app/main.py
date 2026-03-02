from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from typing import List

from .database import get_db
from .models import Invoice, InvoiceItems, InvoiceTaxes
from .schemas import InvoiceItemResponse, InvoiceTaxResponse,InvoiceResponse

app = FastAPI(title="SQL Invoice API")

# REST API
# @app.get("/health")
# def health_check():
#     return {"status": "ok"}

# @app.get("/invoices", response_model=List[InvoiceResponse])
# def get_invoices(db: Session = Depends(get_db)):
#     return db.query(Invoice).all()

# @app.get("/invoicesItems", response_model=List[InvoiceItemResponse])
# def get_invoices_items(db: Session = Depends(get_db)):
#     return db.query(InvoiceItems).all()

# @app.get("/invoicesTaxes", response_model=List[InvoiceTaxResponse])
# def get_invoices_taxes(db: Session = Depends(get_db)):
#     return db.query(InvoiceTaxes).all()


# @app.get("/invoices/{invoice_id}", response_model=InvoiceResponse)
# def get_invoice(invoice_id: int, db: Session = Depends(get_db)):
#     return db.query(Invoice).filter(Invoice.id == invoice_id).first()

# GraphQL
from strawberry.fastapi import GraphQLRouter
from .graphql_schema import schema

graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")

@app.get("/health")
def health_check():
    return {"status": "ok"}