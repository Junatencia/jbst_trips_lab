import nbformat as nbf

# Create a new Jupyter notebook object
nb = nbf.v4.new_notebook()

# Markdown and code cells content
cells = []

cells.append(nbf.v4.new_markdown_cell("""
# 🚀 Trip Ingestion App – Test Notebook

This notebook demonstrates how to interact with the **FastAPI CSV ingestion application**.

You will learn how to:
- Upload a CSV file to the `/ingest` endpoint
- Retrieve the job ID
- Check ingestion status with the `/status/{job_id}` endpoint
- Subscribe to real-time updates using the WebSocket `/ws/{job_id}` endpoint
"""))

cells.append(nbf.v4.new_markdown_cell("""
## 🔧 Setup

Make sure the FastAPI app, Redis, Celery workers, TimescaleDB, PgBouncer, and MinIO/S3 are running.

We’ll use `requests` for HTTP calls and `websockets` for real-time updates.
"""))

cells.append(nbf.v4.new_code_cell("!pip install requests websockets aiohttp"))

cells.append(nbf.v4.new_markdown_cell("""
## 📂 Upload a CSV File

We send a file to the `/ingest` endpoint. The response will include a **job ID** used to track ingestion.
"""))

cells.append(nbf.v4.new_code_cell("""
import requests

API_URL = "http://localhost:8000"
file_path = "./sample_trips.csv"  # replace with your CSV

with open(file_path, "rb") as f:
    response = requests.post(f"{API_URL}/ingest", files={"file": f})

response.raise_for_status()
job_id = response.json()["job_id"]
print(f"✅ Job submitted, ID: {job_id}")
"""))

cells.append(nbf.v4.new_markdown_cell("""
## 📊 Check Job Status

Use the `/status/{job_id}` endpoint to retrieve the current ingestion state (pending, running, success, failed, etc.).
"""))

cells.append(nbf.v4.new_code_cell("""
status = requests.get(f"{API_URL}/status/{job_id}").json()
print(status)
"""))

cells.append(nbf.v4.new_markdown_cell("""
## 🔔 Real-Time Updates with WebSocket

The `/ws/{job_id}` endpoint streams ingestion progress live.
"""))

cells.append(nbf.v4.new_code_cell("""
import asyncio
import websockets

async def listen_ws(job_id):
    uri = f"ws://localhost:8000/ws/{job_id}"
    async with websockets.connect(uri) as websocket:
        try:
            while True:
                message = await websocket.recv()
                print(f"📩 {message}")
        except websockets.exceptions.ConnectionClosed:
            print("🔌 WebSocket connection closed.")

asyncio.run(listen_ws(job_id))
"""))

cells.append(nbf.v4.new_markdown_cell("""
## ✅ Summary
- **Upload** a CSV via `/ingest`
- **Track** progress using `/status/{job_id}`
- **Subscribe** to live updates via `/ws/{job_id}`

This workflow ensures ingestion is efficient (PostgreSQL COPY, TimescaleDB, PgBouncer) and scalable (Celery workers, Kubernetes autoscaling).
"""))

# Assign cells to notebook
nb['cells'] = cells

# Save the notebook file
output_path = "/mnt/data/trip_ingestion_testII.ipynb"
with open(output_path, "w", encoding="utf-8") as f:
    nbf.write(nb, f)

output_path

# Save notebook
output_path = "./trip_ingestion_test_III.ipynb"
with open(output_path, "w") as f:
    nbf.write(nb, f)

output_path
