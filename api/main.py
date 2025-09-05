# main.py
import os
import uuid
import shutil
import json
from fastapi import FastAPI, UploadFile, WebSocket, WebSocketDisconnect
from celery_app import celery_app as celery
from tasks import ingest_csv_task
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/app/uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI(title="Trips API with Celery + WebSockets")

@app.get("/health")
def healthcheck():
    try:
        # check DB connectivity
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok", "message": "API healthy and DB reachable"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Enqueue ingestion job
@app.post("/ingest")
async def ingest(file: UploadFile):
    job_id = str(uuid.uuid4())
    original_name = file.filename
    target = os.path.join(UPLOAD_DIR, f"{job_id}.csv")

    # save uploaded file
    with open(target, "wb") as f:
        content = await file.read()
        f.write(content)

    # enqueue Celery task, pass filename along
    ingest_csv_task.delay(job_id, target, 10000, original_name)
    return {"job_id": job_id, "filename": original_name, "message": "Ingestion queued"}


# WebSocket endpoint forwards Redis pubsub messages to client
from fastapi import WebSocket
from sqlalchemy import text

@app.websocket("/ws/{job_id}")
async def ws_progress(websocket: WebSocket, job_id: str):
    await websocket.accept()

    # 🔎 Get current snapshot from ingestion_status
    with SessionLocal() as session:
        result = session.execute(
            text("""
                SELECT job_id, filename, status, inserted_so_far, total_expected, last_message
                FROM ingestion_status
                WHERE job_id = :job_id
            """),
            {"job_id": job_id}
        ).mappings().first()

    if result:
        # Send initial snapshot to client
        await websocket.send_json({
            "job_id": result["job_id"],
            "filename": result["filename"],
            "status": result["status"],
            "inserted_so_far": result["inserted_so_far"] or 0,
            "total_expected": result["total_expected"],
            "last_message": result["last_message"]
        })
    else:
        await websocket.send_json({
            "job_id": job_id,
            "error": "Job not found"
        })
        await websocket.close()
        return

    # 🔄 Now stream progress updates
    try:
        while True:
            await asyncio.sleep(2)
            with SessionLocal() as session:
                row = session.execute(
                    text("""
                        SELECT status, inserted_so_far, total_expected, last_message
                        FROM ingestion_status
                        WHERE job_id = :job_id
                    """),
                    {"job_id": job_id}
                ).mappings().first()

            if row:
                await websocket.send_json({
                    "job_id": job_id,
                    "filename": result["filename"],   # include filename always
                    "status": row["status"],
                    "inserted_so_far": row["inserted_so_far"] or 0,
                    "total_expected": row["total_expected"],
                    "last_message": row["last_message"]
                })

            if row and row["status"] in ("completed", "failed"):
                break

    except Exception:
        await websocket.close()

@app.get("/status/{job_id}")
def get_status(job_id: str):
    """
    Check the current status of an ingestion job from DB.
    """
    with SessionLocal() as session:
        row = session.execute(
            text("""
                SELECT job_id, filename, status, inserted_so_far, total_expected, last_message
                FROM ingestion_status
                WHERE job_id = :job_id
            """),
            {"job_id": job_id}
        ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail={"error": "Job not found"})

    return {
        "job_id": row["job_id"],
        "filename": row["filename"],
        "status": row["status"],
        "inserted_so_far": row["inserted_so_far"] or 0,
        "total_expected": row["total_expected"],
        "last_message": row["last_message"]
    }

        
