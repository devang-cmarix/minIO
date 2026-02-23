# Streamlit Frontend — File Pipeline Status

Run the simple Streamlit UI that shows file statuses across pipeline stages.

Setup

```bash
python3 -m pip install -r frontend/requirements.txt
```

Run

```bash
streamlit run frontend/streamlit_app.py
```

By default the app reads `frontend/statuses.json`. Replace that file or modify `streamlit_app.py` to fetch real statuses from your services or MinIO.
