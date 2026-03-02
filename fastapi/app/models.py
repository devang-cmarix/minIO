from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Invoice(Base):
    __tablename__ = "invoices"

    id = Column(Integer, primary_key=True, index=True)
    vendor_name = Column(String(255))
    customer_name = Column(String(255))
    subtotal = Column(Float)
    total = Column(Float)
    invoice_date = Column(String)
    due_date = Column(String)
    raw_text = Column(String)
    
class InvoiceItems(Base):
    __tablename__ = "invoice_items"

    id = Column(Integer, primary_key=True, index=True)
    invoice_id = Column(Integer)
    description = Column(String(255))
    quantity = Column(Float)
    rate = Column(Float)
    amount = Column(Float)
    
class InvoiceTaxes(Base):
    __tablename__ = "invoice_taxes"

    id = Column(Integer, primary_key=True, index=True)
    invoice_id = Column(Integer)
    tax_type = Column(String(255))
    tax_rate = Column(Float)
    tax_amount = Column(Float)