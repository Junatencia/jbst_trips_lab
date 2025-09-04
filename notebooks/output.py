import nbformat as nbf

# Create a new Jupyter Notebook
nb = nbf.v4.new_notebook()

# Add cells
nb.cells = []

# Intro markdown
nb.cells.append(nbf.v4.new_markdown_cell("""
# Trip Ingestion Test Notebook

This notebook demonstrates how to test the mandatory features of the **Trip Ingestion API**.

It will cover:
1. Uploading and ingesting a CSV file.
2. Receiving ingestion status updates (via WebSockets).
3. Querying the weekly average number of trips by region or bounding box.
"""))

# Environment setup
nb.cells.append(nbf.v4.new_code_cell("""
import os
import requests
import pandas as pd
from dotenv import load_dotenv
import websockets
import asyncio

# Load environment variables
load_dotenv("../.env")
API_PORT = os.getenv("API_PORT", "8000")
API_URL = f"http://localhost:{API_PORT}"
"""))

# Upload CSV
nb.cells.append(nbf.v4.new_code_cell("""
# Upload a CSV for ingestion
csv_path = "../sample_data/trips_sample.csv"

with open(csv_path, "rb") as f:
    response = requests.post(f"{API_URL}/ingest_csv/", files={"file": f})
    print(response.json())
"""))

# WebSocket status check
nb.cells.append(nbf.v4.new_code_cell("""
# Check ingestion status via WebSocket
async def check_status():
    uri = f"ws://localhost:{API_PORT}/ws/status"
    async with websockets.connect(uri) as websocket:
        status = await websocket.recv()
        print("Status:", status)

await check_status()
"""))

# Weekly average by region
nb.cells.append(nbf.v4.new_code_cell("""
# Query weekly average trips by region
params = {"region": "Prague"}
response = requests.get(f"{API_URL}/trips/weekly_average/", params=params)
print(response.json())
"""))

# Weekly average by bounding box
nb.cells.append(nbf.v4.new_code_cell("""
# Query weekly average trips by bounding box
params = {"bbox": "14.40,49.90,14.60,50.10"}
response = requests.get(f"{API_URL}/trips/weekly_average/", params=params)
print(response.json())
"""))

# Save notebook
output_path = "./trip_ingestion_test_II.ipynb"
with open(output_path, "w") as f:
    nbf.write(nb, f)

output_path
