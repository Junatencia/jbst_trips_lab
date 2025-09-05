# tasks.py
from celery_app import celery
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import csv, os, json
import redis
from datetime import datetime
import geohash2
import math

# DB config from env
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "tripsdb")
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

# Redis for progress pubsub (sync is fine inside Celery worker)
REDIS_PUB_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
redis_client = redis.Redis.from_url(REDIS_PUB_URL)


def publish_progress(job_id, payload: dict):
    channel = f"job:{job_id}"
    redis_client.publish(channel, json.dumps(payload))


def parse_point_wkt(point_str):
    # expects 'POINT (lon lat)'
    if point_str is None:
        return None, None
    s = point_str.strip()
    if not s.startswith("POINT"):
        return None, None
    # simple parse
    inside = s[s.find("(")+1:s.find(")")]
    parts = inside.split()
    lon = float(parts[0])
    lat = float(parts[1])
    return lat, lon


def get_geohash(lat, lon, precision=5):
    return geohash2.encode(lat, lon, precision=precision)


def get_time_bucket(dt_str):
    dt = datetime.fromisoformat(dt_str)
    hour = dt.hour
    if 6 <= hour < 12:
        return "morning"
    elif 12 <= hour < 18:
        return "afternoon"
    elif 18 <= hour < 24:
        return "evening"
    else:
        return "night"



# Use COPY for fast ingestion, chunk_size configurable via env
import psycopg2

@celery.task(bind=True)
def ingest_csv_task(self, job_id: str, filepath: str, chunk_size: int = None, filename: str = None):
    session = SessionLocal()
    try:
        # mark started + save filename
        session.execute(
            text("""
            INSERT INTO ingestion_status (job_id, filename, submitted_at, status)
            VALUES (:job_id, :filename, now(), 'queued')
            ON CONFLICT (job_id) DO UPDATE
              SET filename = EXCLUDED.filename,
                  submitted_at = EXCLUDED.submitted_at,
                  status = 'queued'
            """),
            {"job_id": job_id, "filename": filename}
        )
        session.commit()

        # mark running
        session.execute(
            text("UPDATE ingestion_status SET started_at = now(), status='running' WHERE job_id = :job_id"),
            {"job_id": job_id}
        )
        session.commit()

        # get total lines (optional)
        total = 0
        with open(filepath, "r", encoding="utf-8") as f:
            for _ in f:
                total += 1
        # subtract header
        if total > 0:
            total -= 1

        # update expected total
        session.execute(
            text("UPDATE ingestion_status SET total_expected = :total WHERE job_id = :job_id"),
            {"total": total, "job_id": job_id},
        )
        session.commit()

        # Use chunk_size from env if not provided
        if chunk_size is None:
            chunk_size = int(os.getenv("CHUNK_SIZE", "10000"))

        # Use COPY to temp table for fastest ingest
        temp_table = f"temp_trips_{job_id.replace('-', '_')}"
        main_table = "trips"
        # Create temp table
        session.execute(text(f"""
            CREATE TEMP TABLE {temp_table} (LIKE {main_table} INCLUDING ALL)
        """))
        session.commit()

        # Use psycopg2 for COPY
        raw_conn = engine.raw_connection()
        cursor = raw_conn.cursor()
        with open(filepath, "r", encoding="utf-8") as f:
            next(f)  # skip header
            cursor.copy_expert(f"COPY {temp_table} FROM STDIN WITH CSV", f)
        raw_conn.commit()
        cursor.close()
        raw_conn.close()

        # Upsert from temp table to main table
        session.execute(text(f"""
            INSERT INTO {main_table} (SELECT * FROM {temp_table})
            ON CONFLICT DO NOTHING
        """))
        session.commit()

        # finished
        session.execute(
            text("UPDATE ingestion_status SET finished_at = now(), status='completed', last_message = :msg, inserted_so_far = :inserted WHERE job_id = :job_id"),
            {"msg": f"Completed: inserted {total}", "inserted": total, "job_id": job_id},
        )
        session.commit()
        publish_progress(job_id, {"job_id": job_id, "status": "completed", "inserted": total, "total": total})

        return {"inserted": total, "total": total}

    except Exception as exc:
        session.rollback()
        session.execute(
            text("UPDATE ingestion_status SET status='failed', last_message = :msg WHERE job_id = :job_id"),
            {"msg": str(exc), "job_id": job_id},
        )
        session.commit()
        publish_progress(job_id, {"job_id": job_id, "status": "failed", "message": str(exc)})
        raise
    finally:
        session.close()


def insert_batch(session, rows):
    """
    Insert a batch of rows using one multi-row INSERT (faster than single inserts).
    Uses ST_GeomFromText for geometry.
    """
    if not rows:
        return
    # Build a big VALUES clause
    vals = []
    params = {}
    for i, r in enumerate(rows):
        vals.append(f"(:region{i}, ST_GeomFromText(:origin_wkt{i}, 4326), ST_GeomFromText(:dest_wkt{i}, 4326), :trip_dt{i}, :datasource{i}, :origin_geohash{i}, :dest_geohash{i}, :tod_bucket{i})")
        params.update({
            f"region{i}": r["region"],
            f"origin_wkt{i}": r["origin_wkt"],
            f"dest_wkt{i}": r["dest_wkt"],
            f"trip_dt{i}": r["trip_dt"],
            f"datasource{i}": r["datasource"],
            f"origin_geohash{i}": r["origin_geohash"],
            f"dest_geohash{i}": r["dest_geohash"],
            f"tod_bucket{i}": r["tod_bucket"],
        })

    sql = f"""
    INSERT INTO trips (region, origin_coord, destination_coord, trip_datetime, datasource, origin_geohash, dest_geohash, tod_bucket)
    VALUES {', '.join(vals)}
    """
    session.execute(text(sql), params)
