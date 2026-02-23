import mysql.connector
import time
from .config import settings
from .logger import get_logger

logger = get_logger("mysql")

conn = None


def connect():
    global conn
    while True:
        try:
            logger.info(
                f"Connecting to MySQL at {settings.MYSQL_HOST}:{settings.MYSQL_PORT}..."
            )
            conn = mysql.connector.connect(
                host=settings.MYSQL_HOST,
                port=settings.MYSQL_PORT,
                user=settings.MYSQL_USER,
                password=settings.MYSQL_PASSWORD,
                database=settings.MYSQL_DB,
                autocommit=True
            )
            logger.info("Connected to MySQL")
            return conn
        except Exception as e:
            logger.error(f"MySQL not ready: {e}")
            time.sleep(5)


def get_cursor():
    global conn

    try:
        if conn is None or not conn.is_connected():
            conn = connect()

        return conn.cursor()

    except Exception:
        conn = connect()
        return conn.cursor()


def init_tables():
    cursor = get_cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS invoices (
        id INT AUTO_INCREMENT PRIMARY KEY,
        invoice_date DATE,
        due_date DATE,
        vendor_name VARCHAR(255),
        customer_name VARCHAR(255),
        subtotal DECIMAL(10,2),
        total DECIMAL(10,2),
        raw_text LONGTEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS invoice_items (
        id INT AUTO_INCREMENT PRIMARY KEY,
        invoice_id INT,
        description TEXT,
        quantity DECIMAL(10,2),
        rate DECIMAL(10,2),
        amount DECIMAL(10,2)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS invoice_taxes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        invoice_id INT,
        tax_type VARCHAR(50),
        tax_rate DECIMAL(5,2),
        tax_amount DECIMAL(10,2)
    )
    """)


def insert_invoice(file, data, raw_text=""):
    cursor = get_cursor()

    cursor.execute("""
        INSERT INTO invoices (
            invoice_date,
            due_date,
            vendor_name,
            customer_name,
            subtotal,
            total,
            raw_text
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (
        data.get("invoice_date"),
        data.get("due_date"),
        data.get("vendor_name"),
        data.get("customer_name"),
        data.get("subtotal"),
        data.get("total"),
        raw_text
    ))

    invoice_id = cursor.lastrowid

    # --- Handle items ---
    items = (
        data.get("invoice_items") or
        data.get("items") or
        []
    )

    for item in items:
        cursor.execute("""
            INSERT INTO invoice_items
            (invoice_id, description, quantity, rate, amount)
            VALUES (%s,%s,%s,%s,%s)
        """, (
            invoice_id,
            item.get("description"),
            item.get("quantity"),
            item.get("rate"),
            item.get("amount")
        ))

    # --- Handle taxes ---
    taxes = (
        data.get("invoice_taxes") or
        data.get("taxes") or
        []
    )

    for tax in taxes:
        cursor.execute("""
            INSERT INTO invoice_taxes
            (invoice_id, tax_type, tax_rate, tax_amount)
            VALUES (%s,%s,%s,%s)
        """, (
            invoice_id,
            tax.get("tax_type"),
            tax.get("tax_rate"),
            tax.get("tax_amount")
        ))


# Initialize on startup
connect()
init_tables()
