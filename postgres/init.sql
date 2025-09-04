-- init.sql
-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create trips table
CREATE TABLE IF NOT EXISTS trips (
    id SERIAL PRIMARY KEY,
    region TEXT NOT NULL,
    origin_coord GEOMETRY(Point, 4326) NOT NULL,
    destination_coord GEOMETRY(Point, 4326) NOT NULL,
    trip_datetime TIMESTAMP NOT NULL,
    datasource TEXT NOT NULL,
    origin_geohash TEXT,
    dest_geohash TEXT,
    tod_bucket TEXT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_trips_region ON trips(region);
CREATE INDEX IF NOT EXISTS idx_trips_datetime ON trips(trip_datetime);
CREATE INDEX IF NOT EXISTS idx_trips_origin ON trips USING GIST (origin_coord);
CREATE INDEX IF NOT EXISTS idx_trips_destination ON trips USING GIST (destination_coord);

-- View to group trips by geohash + time of day
CREATE OR REPLACE VIEW trip_groups AS
SELECT
    ST_GeoHash(origin_coord::geometry, 5) AS origin_geohash,
    ST_GeoHash(destination_coord::geometry, 5) AS destination_geohash,
    CASE
        WHEN EXTRACT(HOUR FROM trip_datetime) BETWEEN 6 AND 11 THEN 'morning'
        WHEN EXTRACT(HOUR FROM trip_datetime) BETWEEN 12 AND 17 THEN 'afternoon'
        WHEN EXTRACT(HOUR FROM trip_datetime) BETWEEN 18 AND 22 THEN 'evening'
        ELSE 'night'
    END AS tod_bucket,
    COUNT(*) AS trip_count
FROM trips
GROUP BY 1,2,3;

-- Example queries for testing

-- Q1: Weekly average number of trips in bounding box
-- SELECT AVG(c) FROM (
--   SELECT date_trunc('week', trip_ts) wk, COUNT(*) c
--   FROM trips
--   WHERE ST_Within(origin, ST_MakeEnvelope(-74.2,4.5,-74.0,4.8,4326))
--   GROUP BY 1
-- ) sub;

-- Q2: From the two most commonly appearing regions, which is the latest datasource?
-- SELECT city, MAX(trip_ts), datasource
-- FROM trips
-- GROUP BY city, datasource
-- ORDER BY COUNT(*) DESC
-- LIMIT 2;

-- Q3: What regions has the "cheap_mobile" datasource appeared in?
-- SELECT DISTINCT city
-- FROM trips
-- WHERE datasource = 'cheap_mobile';
