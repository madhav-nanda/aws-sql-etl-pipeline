# AWS Glue SQL Transformation (ETL Job)

Below is the visual ETL workflow and transformation SQL used in the AWS Glue Job `ETL2025`.

---

## 📊 Visual Workflow
![AWS Glue ETL Visual](architecture/aws_glue_etl_flow.png)

---

## 🧩 SQL Transformation Code
```sql
-- ============================================
-- AWS Glue SQL Transformation Script
-- Source: AWS RDS (MySQL)
-- Target: Amazon S3 (Parquet)
-- Author: Madhav Nanda
-- Description:
-- Cleans, standardizes, and enriches weather data
-- before exporting to Amazon S3.
-- ============================================

SELECT
    city,
    weather_desc AS description,
    temperature,
    humidity,
    wind_speed,
    recorded_at
FROM
    weather_db.us_weather;

-- Clean and validate
SELECT
    *
FROM
    weather_db.us_weather
WHERE
    temperature IS NOT NULL
    AND temperature BETWEEN -50 AND 60;

-- Standardize data
SELECT
    city,
    description,
    ROUND(temperature, 2) AS temperature_celsius,
    humidity,
    ROUND(wind_speed * 3.6, 2) AS wind_speed_kmh,
    DATE_FORMAT(recorded_at, '%Y-%m-%d %H:%i:%s') AS timestamp_utc
FROM
    weather_db.us_weather;

-- Final view for Parquet export
CREATE OR REPLACE VIEW weather_transformed AS
SELECT
    city,
    description,
    temperature_celsius,
    humidity,
    wind_speed_kmh,
    timestamp_utc
FROM
    weather_db.us_weather
WHERE
    temperature IS NOT NULL;
