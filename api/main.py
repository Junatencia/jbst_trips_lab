
import os
import csv
import asyncio
from fastapi import FastAPI, UploadFile, File, WebSocket
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "tripsdb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Database engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

# FastAPI app
app = FastAPI(title="Trips API")


# In-memory ingestion status (for WebSocket updates)
ingestion_status = {"status": "idle", "message": ""}

# In-memory job status tracking (per job_id)
job_status = {}

@app.get("/")
def root():
    return {"status": "Trip Ingestion API is running"}

@app.post("/ingest_csv/")
async def ingest_csv(file: UploadFile = File(...)):
    """
    Ingest trips from an uploaded CSV file.
    """
    import uuid
    job_id = str(uuid.uuid4())
    job_status[job_id] = {"status": "running", "message": f"Started ingestion: {file.filename}", "rows": 0}
    contents = await file.read()  # Read file contents before async task

    # Helper functions for grouping
    import geohash2
    import re
    from datetime import datetime
    def parse_point(point_str):
        """
        Convert 'POINT (lon lat)' string into (lat, lon) tuple.
        """
        match = re.match(r"POINT \(([-\d.]+) ([-\d.]+)\)", point_str)
        if match:
            lon, lat = match.groups()
            return float(lat), float(lon)
        else:
            raise ValueError(f"Invalid POINT format: {point_str}")

    def get_geohash(lat, lon, precision=5):
        return geohash2.encode(float(lat), float(lon), precision=precision)
    
    def get_time_bucket(dt_str):
        dt = datetime.fromisoformat(dt_str)
        hour = dt.hour
        if 6 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 18:
            return 'afternoon'
        elif 18 <= hour < 24:
            return 'evening'
        else:
            return 'night'

    async def ingest_task(contents):
        session = SessionLocal()
        inserted_rows = 0
        try:
            decoded = contents.decode("utf-8").splitlines()
            reader = csv.DictReader(decoded)
            for row in reader:
                # Parse coordinates and datetime
                origin_lat, origin_lon = parse_point(row.get("origin_coord"))
                dest_lat, dest_lon = parse_point(row.get("destination_coord"))
                trip_dt = row.get("datetime")
                # Compute grouping fields
                origin_geohash = get_geohash(origin_lat, origin_lon)
                dest_geohash = get_geohash(dest_lat, dest_lon)
                tod_bucket = get_time_bucket(trip_dt)
                # Insert with grouping fields
                session.execute(
                    text(
                        """
                        INSERT INTO trips (
                            region, origin_coord, destination_coord, trip_datetime, datasource,
                            origin_geohash, dest_geohash, tod_bucket
                        ) VALUES (
                            :region,
                            ST_Point(:origin_lon, :origin_lat),
                            ST_Point(:dest_lon, :dest_lat),
                            :trip_datetime,
                            :datasource,
                            :origin_geohash,
                            :dest_geohash,
                            :tod_bucket
                        )
                        """
                    ),
                    {
                        "region": row["region"],
                        "origin_lat": origin_lat,
                        "origin_lon": origin_lon,
                        "dest_lat": dest_lat,
                        "dest_lon": dest_lon,
                        "trip_datetime": trip_dt,
                        "datasource": row["datasource"],
                        "origin_geohash": origin_geohash,
                        "dest_geohash": dest_geohash,
                        "tod_bucket": tod_bucket,
                    },
                )
                inserted_rows += 1
                job_status[job_id]["rows"] = inserted_rows
            session.commit()
            job_status[job_id]["status"] = "completed"
            job_status[job_id]["message"] = f"Ingested {inserted_rows} rows from {file.filename}"
        except Exception as e:
            session.rollback()
            job_status[job_id]["status"] = "failed"
            if "closed file" in str(e):
                job_status[job_id]["message"] = (
                    f"Error: {str(e)}. Hint: Read the file contents before starting async/background tasks."
                )
            else:
                job_status[job_id]["message"] = f"Error: {str(e)}"
        finally:
            session.close()

    import asyncio
    asyncio.create_task(ingest_task(contents))
    return {"job_id": job_id, "status": job_status[job_id]["status"], "message": job_status[job_id]["message"]}


@app.websocket("/ws/{job_id}")
async def ws_ingestion_status(websocket: WebSocket, job_id: str):
    """
    WebSocket endpoint to get ingestion status updates.
    """
    from starlette.websockets import WebSocketDisconnect
    await websocket.accept()
    try:
        while True:
            if job_id in job_status:
                await websocket.send_json(job_status[job_id])
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for job {job_id}")


@app.get("/trips/weekly_average/")
def weekly_average(region: str = None, bbox: str = None):
    """
    Get weekly average number of trips, filtered by region or bounding box.
    - region: filter by region name
    - bbox: bounding box as 'min_lon,min_lat,max_lon,max_lat'
    """
    session = SessionLocal()
    try:
        query = """
            SELECT DATE_TRUNC('week', trip_datetime) AS week,
                   COUNT(*)::float / COUNT(DISTINCT DATE_TRUNC('day', trip_datetime)) AS avg_trips_per_day
            FROM trips
        """

        filters = []
        params = {}

        if region:
            filters.append("region = :region")
            params["region"] = region

        if bbox:
            min_lon, min_lat, max_lon, max_lat = map(float, bbox.split(","))
            filters.append("ST_Within(origin_coord, ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326))")
            params.update({"min_lon": min_lon, "min_lat": min_lat, "max_lon": max_lon, "max_lat": max_lat})

        if filters:
            query += " WHERE " + " AND ".join(filters)

        query += " GROUP BY week ORDER BY week;"

        result = session.execute(text(query), params).fetchall()
        return {"weekly_average": [dict(row._mapping) for row in result]}
    finally:
        session.close()
