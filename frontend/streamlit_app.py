import streamlit as st
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient
from PyPDF2 import PdfReader
import os
import io
from datetime import datetime
import pandas as pd
import json
from pathlib import Path
import time
import sys
from threading import Thread
from queue import Queue
sys.path.append(str(Path(__file__).resolve().parents[1]))
from pdf_processor.app.pdf_processor import extract_text_from_pdf_bytes
import pika


STATUS_FILE = Path(__file__).parent / "statuses.json"

st.set_page_config(page_title="File Pipeline Status", layout="wide")
st.title("📊 File Pipeline Status - Live")

# MySQL connection settings
mysql_host = os.environ.get("MYSQL_HOST", "minio")
mysql_port = int(os.environ.get("MYSQL_PORT", "3308"))
mysql_user = os.environ.get("MYSQL_USER", "root")
mysql_password = os.environ.get("MYSQL_PASSWORD", "")
mysql_db = os.environ.get("MYSQL_DB", "documents")
mysql_table = os.environ.get("MYSQL_TABLE", "parsed_documents")

# Sidebar: MinIO and MongoDB settings (Connect with access/secret only)
st.sidebar.header("🔗 Connections")
minio_endpoint = st.sidebar.text_input("MinIO Endpoint", value=os.environ.get("MINIO_ENDPOINT", "minio:9000"))
minio_access = st.sidebar.text_input("MinIO Access Key", value=os.environ.get("MINIO_ACCESS_KEY", ""))
minio_secret = st.sidebar.text_input("MinIO Secret Key", value=os.environ.get("MINIO_SECRET_KEY", ""), type="password")
minio_bucket = st.sidebar.text_input("MinIO Bucket", value=os.environ.get("MINIO_BUCKET", "drive-bucket"))

connect_anonymous = st.sidebar.checkbox("Connect anonymously", value=False)

mongo_uri = st.sidebar.text_input("MongoDB URI", value=os.environ.get("MONGO_URI", "mongodb://mongodb:27017"))
mongo_db = st.sidebar.text_input("MongoDB Database", value=os.environ.get("MONGO_DB", "files_db"))
mongo_coll = st.sidebar.text_input("MongoDB Collection", value=os.environ.get("MONGO_COLL", "pdf_texts"))

# MySQL Settings
st.sidebar.subheader("🗄️ MySQL")
mysql_host = st.sidebar.text_input("MySQL Host", value=mysql_host)
mysql_port = st.sidebar.number_input("MySQL Port", value=mysql_port, min_value=1, max_value=65535)
mysql_user = st.sidebar.text_input("MySQL User", value=mysql_user)
mysql_password = st.sidebar.text_input("MySQL Password", value=mysql_password, type="password")
mysql_db = st.sidebar.text_input("MySQL Database", value=mysql_db)
# Table selection is decided automatically by backend; no manual selector shown here.
st.sidebar.caption("Target table is chosen automatically when uploading validated documents.")

import mysql.connector

def test_mysql_connection(mysql_host, mysql_port, mysql_user, mysql_password, mysql_db):
    """Test MySQL connection."""
    try:
        conn = mysql.connector.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_db,
            connection_timeout=5
        )
        conn.close()
        return True, "✅ Connected successfully"
    except mysql.connector.Error as err:
        if err.errno == 2003:
            return False, f"❌ Cannot connect to host {mysql_host}. Check host and port."
        elif err.errno == 1045:
            return False, f"❌ Access denied for user '{mysql_user}'. Check username/password."
        elif err.errno == 1049:
            return False, f"❌ Unknown database '{mysql_db}'. Verify database name or create it."
        else:
            return False, f"❌ MySQL error: {err}"
    except Exception as e:
        return False, f"❌ Connection error: {str(e)}"
    
    
# Test MySQL Connection Button
if st.sidebar.button("🧪 Test MySQL Connection"):
    try:
        import mysql.connector
        success, message = test_mysql_connection(mysql_host, int(mysql_port), mysql_user, mysql_password, mysql_db)
        if success:
            st.sidebar.success(message)
        else:
            st.sidebar.error(message)
    except Exception as e:
        st.sidebar.error(f"❌ Error: {str(e)}")

rabbitmq_url = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
ai_parse_queue = "ai.parse"

# Initialize session state for live updates
if 'update_queue' not in st.session_state:
    st.session_state.update_queue = Queue()
if 'pipeline_statuses' not in st.session_state:
    st.session_state.pipeline_statuses = {}
if 'file_metrics' not in st.session_state:
    st.session_state.file_metrics = {"total": 0, "processing": 0, "completed": 0, "failed": 0}

def load_statuses():
    """Load statuses from JSON file or MongoDB."""
    try:
        if STATUS_FILE.exists():
            data = json.loads(STATUS_FILE.read_text())
            return pd.DataFrame(data)
    except Exception as e:
        st.warning(f"Status file error: {e}")
    return pd.DataFrame([])

def fetch_pipeline_status_from_mongo(mongo_uri, mongo_db, mongo_coll):
    """Fetch live pipeline status from MongoDB."""
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        db = client[mongo_db]
        coll = db[mongo_coll]
        
        # Get recent documents
        docs = list(coll.find().sort("ingested_at", -1).limit(100))
        
        # Transform for display
        records = []
        for doc in docs:
            records.append({
                "filename": doc.get("filename", ""),
                "extracted_text": len(doc.get("extracted_text", "")),
                "ingested_at": doc.get("ingested_at", ""),
                "status": "completed"
            })
        return pd.DataFrame(records)
    except Exception as e:
        return None

#NaN Cases
def get_live_pipeline_metrics(df):
    metrics = {
        "total": len(df),
        "processing": 0,
        "completed": 0,
        "failed": 0
    }

    if df.empty:
        return metrics

    df.columns = df.columns.str.strip().str.lower()

    if "status" not in df.columns:
        return metrics

    status = df["status"].astype(str).str.lower()

    metrics["completed"] = (status == "completed").sum()
    metrics["failed"] = (status == "failed").sum()
    metrics["processing"] = (
        (status != "completed") &
        (status != "failed")
    ).sum()

    return metrics




def fetch_minio_files_live(minio_client, bucket_name):
    """Fetch files from MinIO with real-time listing."""
    try:
        files = []
        objs = minio_client.list_objects(bucket_name, recursive=True)
        for obj in objs:
            if obj.object_name.lower().endswith('.pdf'):
                files.append({
                    "name": obj.object_name,
                    "size": obj.size,
                    "modified": obj.last_modified
                })
        return files
    except Exception as e:
        st.error(f"MinIO fetch error: {e}")
        return []

def get_minio_only_files(minio_files, mongodb_files):
    """Get files in MinIO that haven't been extracted to MongoDB yet."""
    minio_names = {f["name"] for f in minio_files}
    mongodb_names = set(mongodb_files)
    pending = minio_names - mongodb_names
    return [f for f in minio_files if f["name"] in pending]

def get_mongodb_processing_files(mongo_uri, mongo_db, mongo_coll):
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        db = client[mongo_db]
        coll = db[mongo_coll]

        docs = list(coll.find({
            "status": {"$in": ["extracted", "processing"]},
            "$or": [
                {"pipeline.ai_parsed": False},
                {"pipeline.validated": False}
            ]
        }).sort("ingested_at", -1).limit(100))

        records = []
        for doc in docs:
            pipeline = doc.get("pipeline", {})
            records.append({
                "filename": doc.get("filename", ""),
                "doc_id": str(doc.get("_id", "")),
                "extracted": pipeline.get("extracted", False),
                "ai_parsed": pipeline.get("ai_parsed", False),
                "validated": pipeline.get("validated", False),
                "stored_mysql": pipeline.get("stored_mysql", False),
                "ingested_at": doc.get("ingested_at", ""),
                "status": doc.get("status", "unknown")
            })

        df = pd.DataFrame(records)

        # Ensure required columns always exist
        required_cols = [
            "filename", "doc_id", "extracted",
            "ai_parsed", "validated",
            "stored_mysql", "ingested_at", "status"
        ]

        for col in required_cols:
            if col not in df.columns:
                df[col] = False

        return df

    except Exception as e:
        # Return empty dataframe with correct schema
        return pd.DataFrame(columns=[
            "filename", "doc_id", "extracted",
            "ai_parsed", "validated",
            "stored_mysql", "ingested_at", "status"
        ])




def get_mysql_stored_files(mysql_host, mysql_port, mysql_user, mysql_password, mysql_db, mysql_table):
    """Get data from MySQL table."""
    try:
        conn = mysql.connector.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_db
        )
        cursor = conn.cursor(dictionary=True)
        
        # Generic query - fetch all data with a limit
        query = f"SELECT * FROM {mysql_table} ORDER BY id DESC LIMIT 100"
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return pd.DataFrame(records)
    except Exception as e:
        st.warning(f"MySQL error: {e}")
        return pd.DataFrame([])


def decide_mysql_table(full_doc: dict) -> str:
    """Decide target MySQL table for a document using simple heuristics on filename/text."""
    # Prefer explicit field if present
    try:
        if isinstance(full_doc, dict):
            pipeline = full_doc.get("pipeline", {}) if full_doc else {}
            target = pipeline.get("target_table") if pipeline else None
            if target in ("invoices", "invoice_items", "invoice_taxes"):
                return target

            filename = (full_doc.get("filename") or "").lower() if full_doc else ""
            text = (full_doc.get("extracted_text") or "").lower() if full_doc else ""

            # Filename/text heuristics
            if "tax" in filename or "tax" in text:
                return "invoice_taxes"
            if "item" in filename or "items" in filename or "qty" in text or "quantity" in text:
                return "invoice_items"
            if "invoice" in filename or "invoice" in text or "subtotal" in text or "total" in text:
                return "invoices"
    except Exception:
        pass

    # Default
    return "invoices"

def get_mongodb_completed_files(mongo_uri, mongo_db, mongo_coll):
    """Get files from MongoDB that have completed processing."""
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        db = client[mongo_db]
        coll = db[mongo_coll]
        
        # Find documents that are marked as stored in MySQL
        docs = list(coll.find({
            "pipeline.stored_mysql": True
        }).sort("ingested_at", -1).limit(100))
        
        records = []
        for doc in docs:
            pipeline = doc.get("pipeline", {})
            records.append({
                "filename": doc.get("filename", ""),
                "doc_id": str(doc.get("_id", "")),
                "extracted": pipeline.get("extracted", False),
                "ai_parsed": pipeline.get("ai_parsed", False),
                "validated": pipeline.get("validated", False),
                "stored_mysql": pipeline.get("stored_mysql", False),
                "ingested_at": doc.get("ingested_at", ""),
                "status": doc.get("status", "completed")
            })
        return pd.DataFrame(records)
    except Exception as e:
        return pd.DataFrame([])

cols = st.columns([1, 3, 1])
with cols[0]:
    refresh = st.button("🔄 Refresh Now")
with cols[1]:
    st.markdown("_Real-time pipeline status across services._")
with cols[2]:
    auto = st.checkbox("⚡ Live (5s)", value=False)

if refresh:
    st.rerun()

# Live data source selection
st.sidebar.header("📡 Data Sources")
use_mongo = st.sidebar.checkbox("Use MongoDB (live)", value=False)

df = None
if use_mongo:
    # df = fetch_pipeline_status_from_mongo(mongo_uri, mongo_db, mongo_coll)
    def fetch_pipeline_status_from_mongo(mongo_uri, mongo_db, mongo_coll):
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            db = client[mongo_db]
            coll = db[mongo_coll]

            docs = list(coll.find().sort("ingested_at", -1).limit(100))

            records = []
            for doc in docs:
                pipeline = doc.get("pipeline", {})
                status = doc.get("status", "unknown")

                records.append({
                    "filename": doc.get("filename", ""),
                    "status": status,
                    "extracted": pipeline.get("extracted", False),
                    "ai_parsed": pipeline.get("ai_parsed", False),
                    "validated": pipeline.get("validated", False),
                    "stored_mysql": pipeline.get("stored_mysql", False),
                    "fallback": pipeline.get("fallback", False),
                    "manual_review": pipeline.get("manual_review", False),
                    "ingested_at": doc.get("ingested_at", "")
                })

            return pd.DataFrame(records)

        except Exception:
            return None


if df is None:
    df = load_statuses()

# Live Metrics Section
st.subheader("📈 Live Metrics")
metrics = get_live_pipeline_metrics(df)

metric_cols = st.columns(4)
metric_cols[0].metric("Total Files", metrics["total"], delta=None)
metric_cols[1].metric("✅ Completed", metrics["completed"])
metric_cols[2].metric("⏳ Processing", metrics["processing"])
metric_cols[3].metric("❌ Failed", metrics["failed"])

if 'minio_client' not in st.session_state:
    st.session_state.minio_client = None
    st.session_state.minio_connected = False

if st.sidebar.button("🔌 Connect to MinIO"):
    try:
        if connect_anonymous or (not minio_access and not minio_secret):
            client = Minio(minio_endpoint, secure=False)
            _ = client.list_buckets()
            st.session_state.minio_client = client
            st.session_state.minio_connected = True
            st.sidebar.success("✅ Connected to MinIO (anonymous)")
        else:
            if not minio_access or not minio_secret:
                st.sidebar.error("❌ Provide both Access Key and Secret Key, or enable anonymous connect.")
            else:
                client = Minio(minio_endpoint, access_key=minio_access, secret_key=minio_secret, secure=False)
                _ = client.list_buckets()
                st.session_state.minio_client = client
                st.session_state.minio_connected = True
                st.sidebar.success("✅ Connected to MinIO")
    except Exception as e:
        st.session_state.minio_client = None
        st.session_state.minio_connected = False
        st.sidebar.error(f"❌ MinIO error: {e}")

# Fetch data for all pipeline stages
minio_files = []
mongodb_processing_df = pd.DataFrame([])
mysql_files_df = pd.DataFrame([])

if st.session_state.get("minio_connected") and st.session_state.get("minio_client"):
    try:
        minio_files = fetch_minio_files_live(
            st.session_state.minio_client, minio_bucket
        )
    except Exception as e:
        st.warning(f"MinIO list error: {e}")

# Fetch MongoDB processing data
if use_mongo:
    mongodb_processing_df = get_mongodb_processing_files(mongo_uri, mongo_db, mongo_coll)

# Fetch MySQL data later in Stage 3 (per-table). Initialize empty placeholder.
try:
    import mysql.connector
    mysql_files_df = pd.DataFrame([])
except ImportError:
    st.info("💡 To view MySQL data, install: pip install mysql-connector-python")
    mysql_files_df = pd.DataFrame([])
except Exception as e:
    mysql_files_df = pd.DataFrame([])

# Get minio-only files (pending extraction)
# We want to exclude any file that has ever been ingested into MongoDB,
# not just those currently in the processing pipeline. Otherwise a document
# that has made it all the way through to step‑3 (stored_mysql=True) would
# re‑appear under "pending extraction" when we refresh.

def get_all_mongo_filenames(mongo_uri, mongo_db, mongo_coll):
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        db = client[mongo_db]
        coll = db[mongo_coll]
        # distinct returns all filenames present in the collection
        names = coll.distinct("filename")
        return set(names)
    except Exception:
        return set()

minio_only_files = []
if minio_files:
    # build a set of names that exist *anywhere* in MongoDB (processing or stored)
    mongo_names = set()
    if use_mongo:
        mongo_names = get_all_mongo_filenames(mongo_uri, mongo_db, mongo_coll)
    elif not mongodb_processing_df.empty:
        mongo_names = set(mongodb_processing_df["filename"].tolist())

    if mongo_names:
        minio_only_files = [f for f in minio_files if f["name"] not in mongo_names]
    else:
        minio_only_files = minio_files


# Live file listing from MinIO
available_files = []

if st.session_state.get("minio_connected") and st.session_state.get("minio_client"):
    try:
        files = fetch_minio_files_live(
            st.session_state.minio_client, minio_bucket
        )
        available_files = [f["name"] for f in files]
    except Exception as e:
        st.warning(f"MinIO list error: {e}")
else:
    if not df.empty and "filename" in df.columns:
        if "status" in df.columns:
            present = df[
                df["status"].astype(str).str.lower().isin(
                    ["completed", "processing"]
                )
            ]
        else:
            present = df.iloc[0:0]

        available_files = present["filename"].tolist()

# Safe usage
if not available_files:
    st.info("📁 No files detected in MinIO. Connect to MinIO to see live files.")
else:
    st.success(f"📁 {len(available_files)} files detected in MinIO")


def _safe_key(name: str) -> str:
    return "store_" + name.replace("/", "__").replace(" ", "_")

def store_to_mongo(document: dict):
    try:
        st.write("DEBUG: storing to mongo", document["filename"])

        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        coll = db[mongo_coll]

        document["status"] = "extracted"
        document["pipeline"] = {
            "extracted": True,
            "ai_parsed": False,
            "validated": False,
            "stored_mysql": False,
            "fallback": False,
            "manual_review": False
        }

        res = coll.insert_one(document)

        st.write("DEBUG: inserted id", str(res.inserted_id))
        return res.inserted_id

    except Exception as e:
        st.error(f"MongoDB insert error: {e}")
        return None



def publish_to_ai_queue(doc_id, filename):
    try:
        params = pika.URLParameters(rabbitmq_url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=ai_parse_queue, durable=True)

        message = {
            "doc_id": str(doc_id),
            "file": filename
        }

        channel.basic_publish(
            exchange="",
            routing_key=ai_parse_queue,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2),
        )

        connection.close()
        return True
    except Exception as e:
        st.error(f"Queue publish error: {e}")
        return False

def approve_and_validate(doc_id: str, mongo_uri: str, mongo_db: str, mongo_coll: str):
    """Mark a document as validated by admin."""
    try:
        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        coll = db[mongo_coll]
        
        from bson.objectid import ObjectId
        result = coll.update_one(
            {"_id": ObjectId(doc_id)},
            {"$set": {
                "pipeline.validated": True,
                "validated_at": datetime.utcnow(),
                "validated_by": "admin"
            }}
        )
        return result.modified_count > 0
    except Exception as e:
        st.error(f"Validation update error: {e}")
        return False

def get_extracted_pending_validation(mongo_uri, mongo_db, mongo_coll):
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        db = client[mongo_db]
        coll = db[mongo_coll]

        docs = list(coll.find({
            "pipeline.extracted": True,
            "pipeline.validated": False
        }).sort("ingested_at", -1).limit(100))

        records = []
        for doc in docs:
            pipeline = doc.get("pipeline", {})
            records.append({
                "filename": doc.get("filename", ""),
                "doc_id": str(doc.get("_id", "")),
                "extracted_text": doc.get("extracted_text", ""),
                "structured_data": doc.get("structured_data", {}),
                "ai_parsed": pipeline.get("ai_parsed", False),
                "ingested_at": doc.get("ingested_at", ""),
                "status": doc.get("status", "pending_validation")
            })

        return pd.DataFrame(records)

    except Exception:
        return pd.DataFrame(columns=[
            "filename",
            "doc_id",
            "extracted_text",
            "structured_data",
            "ai_parsed",
            "ingested_at",
            "status"
        ])


def get_validated_pending_storage(mongo_uri, mongo_db, mongo_coll):
    """Get validated documents ready to be stored in MySQL."""
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        db = client[mongo_db]
        coll = db[mongo_coll]

        docs = list(coll.find({
            "pipeline.validated": True,
            "pipeline.stored_mysql": False
        }).sort("validated_at", -1).limit(100))

        records = []
        for doc in docs:
            records.append({
                "filename": doc.get("filename", ""),
                "doc_id": str(doc.get("_id", "")),
                "extracted_text": doc.get("extracted_text", ""),
                "ingested_at": doc.get("ingested_at", ""),
                "validated_at": doc.get("validated_at", ""),
                "status": "ready_for_storage",
                "full_doc": doc
            })

        return pd.DataFrame(records)

    except Exception as e:
        return pd.DataFrame(columns=["filename", "doc_id", "extracted_text", "ingested_at", "validated_at", "status", "full_doc"])

def upload_to_mysql(mysql_host, mysql_port, mysql_user, mysql_password, mysql_db, document_data):
    
    """Upload structured invoice data to MySQL."""

    try:
        conn = mysql.connector.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_db
        )

        cursor = conn.cursor()

        # structured = document_data.get("structured_data", {})
        # Determine which parsing admin selected
        selected_parsing = document_data.get("selected_parsing", "ai")

        if selected_parsing == "manual":
            structured = document_data.get("review_output", {})
        else:
            structured = document_data.get("structured_data", {})

        raw_text = document_data.get("extracted_text", "")

        # Insert invoice
        
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
            structured.get("invoice_date"),
            structured.get("due_date"),
            structured.get("vendor_name"),
            structured.get("customer_name"),
            structured.get("subtotal"),
            structured.get("total"),
            raw_text
        ))

        invoice_id = cursor.lastrowid

        # Resolve items list

        items = (
            structured.get("invoice_items") or
            structured.get("items") or
            []
        )

        if not isinstance(items, list):
            items = []

        for item in items:

            quantity = float(str(item.get("quantity", 0)).replace("$", "").replace(",", ""))
            rate = float(str(item.get("rate", 0)).replace("$", "").replace(",", ""))
            amount = float(str(item.get("amount", 0)).replace("$", "").replace(",", ""))

            cursor.execute("""
                INSERT INTO invoice_items
                (invoice_id, description, quantity, rate, amount)
                VALUES (%s,%s,%s,%s,%s)
            """, (
                invoice_id,
                item.get("description"),
                quantity,
                rate,
                amount
            ))

        # -----------------------
        # Resolve taxes list
        # -----------------------
        taxes = (
            structured.get("invoice_taxes") or
            structured.get("taxes") or
            []
        )

        if not isinstance(taxes, list):
            taxes = []

        for tax in taxes:

            tax_rate = float(str(tax.get("tax_rate", 0)).replace("%", "").replace(",", ""))
            tax_amount = float(str(tax.get("tax_amount", 0)).replace("$", "").replace(",", ""))

            cursor.execute("""
                INSERT INTO invoice_taxes
                (invoice_id, tax_type, tax_rate, tax_amount)
                VALUES (%s,%s,%s,%s)
            """, (
                invoice_id,
                tax.get("tax_type"),
                tax_rate,
                tax_amount
            ))

        conn.commit()

        cursor.close()
        conn.close()

        return True, False

    except mysql.connector.Error as err:

        if err.errno == 1062:
            st.warning(f"MySQL upload warning: {err}")
            return True, True

        else:
            st.error(f"MySQL upload error: {err}")
            return False, False

    except Exception as e:
        st.error(f"MySQL upload error: {e}")
        return False, False

def mark_stored_in_mysql(doc_id, mongo_uri, mongo_db, mongo_coll):
    """Mark document as stored in MySQL."""
    try:
        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        coll = db[mongo_coll]
        
        from bson.objectid import ObjectId
        result = coll.update_one(
            {"_id": ObjectId(doc_id)},
            {"$set": {
                "pipeline.stored_mysql": True,
                "stored_mysql_at": datetime.utcnow()
            }}
        )
        return result.modified_count > 0
    except Exception as e:
        st.error(f"MongoDB update error: {e}")
        return False


def mark_duplicates_stored(filename, mongo_uri, mongo_db, mongo_coll, full_doc=None):
    """After one document is stored, mark any remaining documents as stored.

    We look for duplicates in two ways:
    1. identical filename
    2. matching structured_data key fields (date/vendor/total) if available

    The queue may enqueue the same PDF multiple times with slightly
    different names, so the structured_data check is important for catching
    those cases where filenames differ but the invoice itself is the same.
    """
    modified = 0
    try:
        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        coll = db[mongo_coll]

        # basic filename-based sweep
        res1 = coll.update_many(
            {"filename": filename, "pipeline.stored_mysql": False},
            {"$set": {"pipeline.stored_mysql": True,
                      "stored_mysql_at": datetime.utcnow()}}
        )
        modified += res1.modified_count

        # try structured-data based duplicate detection
        if full_doc:
            sd = full_doc.get("structured_data", {}) if isinstance(full_doc, dict) else {}
            # build a query using the fields that the MySQL unique constraint
            # is likely based on (invoice_date, vendor_name, total)
            q = {}
            if sd.get("invoice_date"):
                q["structured_data.invoice_date"] = sd.get("invoice_date")
            if sd.get("vendor_name"):
                q["structured_data.vendor_name"] = sd.get("vendor_name")
            if sd.get("total") is not None:
                q["structured_data.total"] = sd.get("total")

            if q:
                # combine with stored_mysql False
                q["pipeline.stored_mysql"] = False
                res2 = coll.update_many(
                    q,
                    {"$set": {"pipeline.stored_mysql": True,
                              "stored_mysql_at": datetime.utcnow()}}
                )
                modified += res2.modified_count

        return modified
    except Exception as e:
        # we avoid crashing the UI on cleanup failures
        st.error(f"MongoDB duplicate cleanup error: {e}")
        return modified

def process_and_store(filename: str):
    if not st.session_state.get('minio_connected') or not st.session_state.get('minio_client'):
        st.error("❌ MinIO client not connected.")
        return

    with st.spinner(f"⏳ Extracting {filename}..."):
        try:
            response = st.session_state.minio_client.get_object(minio_bucket, filename)
            pdf_bytes = response.read()
            response.close()
            response.release_conn()

            text = extract_text_from_pdf_bytes(pdf_bytes)

            doc = {
                "filename": filename,
                "extracted_text": text,
                "ingested_at": datetime.utcnow(),
                "source": "minio"
            }

            inserted_id = store_to_mongo(doc)

            if inserted_id:
                # CRITICAL: send to AI parser
                success = publish_to_ai_queue(inserted_id, filename)

                if success:
                    st.success(f"📄 Extracted {filename} → sent to AI parser")
                else:
                    st.warning("Extracted but failed to send to AI parser")

                st.rerun()

        except S3Error as e:
            st.error(f"❌ MinIO error: {e}")
        except Exception as e:
            st.error(f"❌ Error: {e}")


# Create tabs for pipeline stages
tab1, tab2, tab3 = st.tabs([
    "📥 Stage 1: MinIO → MongoDB (Extraction)",
    "🔄 Stage 2: MongoDB → Validation (Processing)",
    "✅ Stage 3: Output → MySQL (Storage)"
])

# ============ STAGE 1: MinIO to MongoDB (Extraction) ============
with tab1:
    st.subheader("📥 Files Pending Extraction")
    st.caption("Files in MinIO waiting to be extracted and stored in MongoDB")
    
    if minio_only_files:
        st.metric("Pending Extraction", len(minio_only_files))
        st.divider()
        
        header_cols = st.columns([4, 2, 2, 1])
        header_cols[0].write("**Filename**")
        header_cols[1].write("**Size (MB)**")
        header_cols[2].write("**Modified**")
        header_cols[3].write("**Action**")
        st.divider()
        
        for file_info in minio_only_files:
            file_cols = st.columns([4, 2, 2, 1])
            file_cols[0].write(file_info["name"])
            file_cols[1].write(f"{file_info['size'] / (1024*1024):.2f}")
            file_cols[2].write(file_info["modified"].strftime("%Y-%m-%d %H:%M"))
            
            key = _safe_key(file_info["name"])
            if file_cols[3].button("Extract", key=f"extract_{key}", help="Extract text and send to MongoDB"):
                process_and_store(file_info["name"])
    else:
        st.info("✅ No files pending extraction - all MinIO files are in MongoDB")

if not mongodb_processing_df.empty:

    # Ensure columns exist
    for col in ["extracted", "ai_parsed", "validated", "stored_mysql"]:
        if col not in mongodb_processing_df.columns:
            mongodb_processing_df[col] = False

# imports required for publishing helper
import orjson
from bson import ObjectId

def json_serializer(obj):
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    if isinstance(obj, ObjectId):
        return str(obj)
    return str(obj)


def publish_to_queue(queue_name, message):

    print("========== PUBLISH START ==========")
    print("QUEUE:", queue_name)
    print("MESSAGE:", message)

    try:
        params = pika.URLParameters("amqp://guest:guest@rabbitmq:5672/")
        connection = pika.BlockingConnection(params)
        print("CONNECTED TO RABBITMQ")

        channel = connection.channel()

        channel.queue_declare(
            queue=queue_name,
            durable=True
        )

        channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=orjson.dumps(message, default=json_serializer),
            properties=pika.BasicProperties(delivery_mode=2),
        )

        print("MESSAGE PUBLISHED SUCCESSFULLY")

        connection.close()

    except Exception as e:
        print("ERROR PUBLISHING:", e)


# ============ STAGE 2: MongoDB to Validation (Processing) ============
with tab2:
    st.subheader("🔄 Admin Validation & Review")
    st.caption("Extracted PDFs waiting for admin approval and validation")
    
    # Fetch PDFs pending validation
    pending_validation_df = get_extracted_pending_validation(mongo_uri, mongo_db, mongo_coll)
    
    if not pending_validation_df.empty:
        st.metric("⏳ Pending Admin Approval", len(pending_validation_df))
        st.divider()
        
        # Admin validation interface
        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        coll = db[mongo_coll]

        for idx, row in pending_validation_df.iterrows():

            doc_id = row["doc_id"]
            object_id = ObjectId(doc_id)

            with st.container(border=True):

                col_header = st.columns([4, 1])
                col_header[0].write(f"📄 **{row['filename']}**")
                col_header[1].caption(f"ID: {doc_id[:8]}...")

                st.caption(f"Ingested: {row['ingested_at']}")

                # Fetch latest Mongo document
                doc = coll.find_one({"_id": object_id})

                # ----------------------------
                # Extracted Text Preview
                # ----------------------------
                with st.expander("📖 View Extracted Text"):
                    st.text_area(
                        label="Extracted Content",
                        value=row.get("extracted_text", ""),
                        height=200,
                        disabled=True,
                        key=f"text_preview_{doc_id}"
                    )

                # ----------------------------
                # AI & Manual Structured Output
                # ----------------------------
                ai_data = doc.get("structured_data") if doc else None
                manual_data = doc.get("review_output") if doc else None

                if ai_data:
                    st.success("🤖 AI Structured Data Available")
                    with st.expander("🤖 View AI Structured Data"):
                        st.json(ai_data)

                if manual_data:
                    st.info("🧑 Manual Structured Data Available")
                    with st.expander("🧾 View Manual Structured Data"):
                        st.json(manual_data)

                # ----------------------------
                # Admin Parsing Selection
                # ----------------------------
                st.markdown("### 🔎 Select Parsing Method")

                parsing_choice = st.radio(
                    "Choose parsing result to view:",
                    ["🤖 AI Parsed", "🧑 Manual Parsing"],
                    horizontal=True,
                    key=f"parsing_choice_{doc_id}"
                )

                # Refresh Mongo doc
                doc = coll.find_one({"_id": object_id})

                # Session state key to avoid duplicate queue publish
                trigger_key = f"manual_triggered_{doc_id}"

                if trigger_key not in st.session_state:
                    st.session_state[trigger_key] = False

                # ----------------------------
                # AUTO TRIGGER MANUAL PARSING
                # ----------------------------
                if "manual" in parsing_choice.lower():

                    if not doc.get("review_output") and not st.session_state[trigger_key]:

                        st.session_state[trigger_key] = True

                        publish_to_queue(
                            "ai.review",
                            {
                                "doc_id": doc_id,
                                "extracted_text": doc.get("extracted_text", "")
                            }
                        )

                        st.info("🧑 Manual parsing triggered...")
                        st.rerun()

                # ----------------------------
                # DISPLAY OUTPUT
                # ----------------------------
                if "ai" in parsing_choice.lower():

                    if doc and doc.get("structured_data"):
                        st.json(doc.get("structured_data"))
                    else:
                        st.warning("AI output not available")

                elif "manual" in parsing_choice.lower():

                    if doc and doc.get("review_output"):
                        st.json(doc.get("review_output"))
                    else:
                        st.info("⏳ Waiting for manual review to complete...")

                # ----------------------------
                # CONFIRM SELECTION
                # ----------------------------
                if st.button("🚀 Confirm Selection", key=f"confirm_{doc_id}", type="primary"):

                    selected = "ai" if "ai" in parsing_choice.lower() else "manual"

                    coll.update_one(
                        {"_id": object_id},
                        {
                            "$set": {
                                "status": "validated",
                                "selected_parsing": selected,
                                "selection_time": datetime.utcnow(),
                                "pipeline.validated": True
                            }
                        }
                    )

                    st.success(f"✅ {selected.upper()} parsing selected")
                    st.rerun()

                st.divider()

        else:
            st.info("✅ All files validated - no pending approvals!")
    
    # Show all files in processing pipeline
    if not mongodb_processing_df.empty:
        st.subheader("📊 Overall Pipeline Status")
        st.caption("All files in the extraction & validation pipeline")
        
        # Calculate stage metrics
        extracted_only = len(mongodb_processing_df[
            (mongodb_processing_df["extracted"]) & 
            (~mongodb_processing_df["validated"])
        ])
        validated = len(mongodb_processing_df[
            (mongodb_processing_df["validated"]) & 
            (~mongodb_processing_df["stored_mysql"])
        ])
        
        metric_cols2 = st.columns(2)
        metric_cols2[0].metric("📄 Awaiting Validation", extracted_only)
        metric_cols2[1].metric("✔️ Ready for Storage", validated)
        
        st.divider()
        
        # Show processing table
        display_df = mongodb_processing_df[[
            "filename", "extracted", "validated", "stored_mysql", "status", "ingested_at"
        ]].copy()
        
        display_df["Pipeline Progress"] = display_df.apply(
            lambda r: (
                "📄→✔️→💾" if r["stored_mysql"] 
                else "📄→✔️" if r["validated"]
                else "📄"
            ), axis=1
        )
        
        st.dataframe(
            display_df.reset_index(drop=True),
            use_container_width=True,
            height=300,
            column_config={
                "filename": st.column_config.TextColumn("Filename", width="large"),
                "extracted": st.column_config.CheckboxColumn("Extracted"),
                "validated": st.column_config.CheckboxColumn("Validated"),
                "status": st.column_config.TextColumn("Status", width="small"),
                "ingested_at": st.column_config.DatetimeColumn("Ingested", width="medium"),
                "Pipeline Progress": st.column_config.TextColumn("Progress", width="small"),
            }
        )


# ============ STAGE 3: Validation to MySQL Storage ============
with tab3:
    st.subheader("✅ Upload Validated Data to MySQL")
    st.caption("Validated documents ready to be uploaded into MySQL tables")
    
    # Get validated documents pending storage
    validated_pending_df = get_validated_pending_storage(mongo_uri, mongo_db, mongo_coll)
    # drop any repeat filenames so that once one entry for a PDF is uploaded
    # the others are hidden from the UI
    if not validated_pending_df.empty:
        validated_pending_df = validated_pending_df.drop_duplicates(subset=["filename"])

    if not validated_pending_df.empty:
        st.metric("⏳ Validated & Ready for Upload", len(validated_pending_df))
        st.divider()
        
        # Upload interface
        for idx, row in validated_pending_df.iterrows():
            with st.container(border=True):
                col_header = st.columns([4, 1])
                col_header[0].write(f"📤 **{row['filename']}**")
                col_header[1].caption(f"ID: {row['doc_id'][:8]}...")
                
                col_info = st.columns([2, 2])
                col_info[0].caption(f"✓ Validated: {row['validated_at']}")
                col_info[1].caption(f"📄 Table: {mysql_table}")
                
                # Show extracted text preview
                extracted_text = row['extracted_text']
                with st.expander("📖 Preview Extracted Text"):
                    st.text_area(
                        label="Content Preview",
                        value=extracted_text[:1000],
                        height=150,
                        disabled=True,
                        key=f"preview_{idx}"
                    )
                
                # Upload button (backend decides target table)
                upload_cols = st.columns([1, 3])
                target_table = decide_mysql_table(row.get('full_doc', {}))
                upload_cols[1].caption(f"Will upload to: {target_table}")
                if upload_cols[0].button(
                    "⬆️ Upload to MySQL",
                    key=f"upload_{idx}",
                    help=f"Upload to {target_table} table",
                    type="primary"
                ):
                    with st.spinner(f"⏳ Uploading {row['filename']} to {target_table}..."):
                        # Upload to MySQL
                        
                        # success, dup = upload_to_mysql(mysql_host, int(mysql_port), mysql_user, mysql_password, mysql_db, row['full_doc'])
                        
                        from bson.objectid import ObjectId

                        client = MongoClient(mongo_uri)
                        db = client[mongo_db]
                        coll = db[mongo_coll]

                        latest_doc = coll.find_one({"_id": ObjectId(row["doc_id"])})

                        success, dup = upload_to_mysql(
                            mysql_host,
                            int(mysql_port),
                            mysql_user,
                            mysql_password,
                            mysql_db,
                            latest_doc
                        )

                        if success:
                            # Mark as stored in MongoDB regardless of whether the insert
                            # actually inserted a row or simply collided with an existing
                            # entry.
                            if mark_stored_in_mysql(row['doc_id'], mongo_uri, mongo_db, mongo_coll):
                                # sweep any other docs by filename or invoice data
                                mark_duplicates_stored(
                                    row['filename'],
                                    mongo_uri, mongo_db, mongo_coll,
                                    row.get('full_doc')
                                )
                                if dup:
                                    st.warning(f"⚠️ Document already exists in MySQL; marked as stored ({row['filename']})")
                                else:
                                    st.success(f"✅ Uploaded: {row['filename']} → {target_table}")
                                st.rerun()
                            else:
                                st.error("Uploaded to MySQL but failed to update MongoDB status")
                        else:
                            st.error("Failed to upload to MySQL")
                
                st.divider()
    
    else:
        st.info("✅ No validated documents waiting - all data has been uploaded to MySQL")
    
    # Show stored data
    st.subheader("📊 Stored Data in MySQL")
    st.caption(f"All records in {mysql_table} table")
    
    if mysql_table == "invoices":
        st.info("📋 **Invoices Table** - Contains full invoice data. Auto-populated fields: invoice_date (today), due_date (today), raw_text (PDF content). Manual review needed: vendor_name, customer_name, subtotal, total")
    elif mysql_table == "invoice_items":
        st.info("🛒 **Invoice Items Table** - Line items for invoices. Auto-populated fields: invoice_id (default 1), description (PDF content). Manual review needed: quantity, rate, amount. Link to correct invoice_id in post-processing")
    elif mysql_table == "invoice_taxes":
        st.info("💰 **Invoice Taxes Table** - Tax information. Auto-populated fields: invoice_id (default 1), tax_type (General). Manual review needed: tax_rate, tax_amount. Link to correct invoice_id in post-processing")
    
    # Fetch and display each table separately
    try:
        invoices_df = get_mysql_stored_files(mysql_host, int(mysql_port), mysql_user, mysql_password, mysql_db, 'invoices')
        items_df = get_mysql_stored_files(mysql_host, int(mysql_port), mysql_user, mysql_password, mysql_db, 'invoice_items')
        taxes_df = get_mysql_stored_files(mysql_host, int(mysql_port), mysql_user, mysql_password, mysql_db, 'invoice_taxes')

        with st.expander('📋 Invoices (latest)'):
            if not invoices_df.empty:
                st.metric('Invoices', len(invoices_df))
                st.dataframe(invoices_df.reset_index(drop=True), use_container_width=True)
            else:
                st.info('No invoices found')

        with st.expander('🛒 Invoice Items (latest)'):
            if not items_df.empty:
                st.metric('Invoice Items', len(items_df))
                st.dataframe(items_df.reset_index(drop=True), use_container_width=True)
            else:
                st.info('No invoice items found')

        with st.expander('💰 Invoice Taxes (latest)'):
            if not taxes_df.empty:
                st.metric('Invoice Taxes', len(taxes_df))
                st.dataframe(taxes_df.reset_index(drop=True), use_container_width=True)
            else:
                st.info('No invoice taxes found')
    except Exception as e:
        st.info('Could not fetch MySQL tables: ' + str(e))

st.markdown("---")
st.caption("🔄 Live Pipeline Dashboard: Track data from MinIO extraction → MongoDB processing → MySQL storage")

# Auto-refresh logic
if auto:
    time.sleep(5)
    st.rerun()
