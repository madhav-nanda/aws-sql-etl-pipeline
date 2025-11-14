# -*- coding: utf-8 -*-
"""
AWS Lambda Function: Weather Data Ingestion
-------------------------------------------
Triggered automatically every 1 hour via AWS CloudWatch.
Fetches live weather data from OpenWeatherMap API and inserts it into
AWS RDS (MySQL) for further ETL processing in AWS Glue.

Author: Madhav Nanda
Date: October 2025
"""

import pymysql
import requests
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda entry point function.
    Extracts weather data from OpenWeatherMap API and inserts into AWS RDS.
    """

    # --- 🔐 AWS RDS (MySQL) Connection Details ---
    host = "database2025.cpe0uka0cn1s.us-east-2.rds.amazonaws.com"
    dbname = "weather_db"
    user = "admin"
    password = "Madhava1435"  # ⚠️ Replace with environment variable in production
    port = 3306

    # --- 🌦 Cities to Collect Weather Data For ---
    cities = [
        "New York,US", "Los Angeles,US", "Chicago,US", "Houston,US", "Phoenix,US",
        "Philadelphia,US", "San Antonio,US", "San Diego,US", "Dallas,US", "San Jose,US",
        "Austin,US", "Jacksonville,US", "San Francisco,US", "Columbus,US", "Fort Worth,US",
        "Charlotte,US", "Indianapolis,US", "Seattle,US", "Denver,US", "Washington,US"
    ]

    # --- 🌐 API Setup ---
    api_key = "dc67b45ffb555182afeed2b864ee1695"
    base_url = "http://api.openweathermap.org/data/2.5/weather"

    # --- 🧩 Connect to MySQL Database ---
    try:
        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=dbname,
            port=port,
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()
        print("✅ Connected to RDS successfully.")

    except Exception as e:
        print("❌ Database connection failed:", e)
        return {"statusCode": 500, "body": f"Database connection error: {e}"}

    # --- 🔁 Fetch Weather Data and Insert into DB ---
    try:
        for city in cities:
            params = {"q": city, "appid": api_key, "units": "metric"}
            response = requests.get(base_url, params=params, timeout=15)
            data = response.json()

            # Insert weather data into RDS table
            cursor.execute(
                """
                INSERT INTO us_weather
                    (city, weather_desc, temperature, humidity, wind_speed, recorded_at)
                VALUES
                    (%s, %s, %s, %s, %s, %s)
                """,
                (
                    city,
                    data["weather"][0]["description"],
                    data["main"]["temp"],
                    data["main"]["humidity"],
                    data["wind"]["speed"],
                    datetime.utcnow(),
                ),
            )
            print(f"Inserted weather data for: {city}")

        conn.commit()
        print("✅ All weather data committed to RDS successfully.")

    except Exception as e:
        print("❌ Error inserting data:", e)
        conn.rollback()
        return {"statusCode": 500, "body": f"Data insertion error: {e}"}

    finally:
        cursor.close()
        conn.close()

    # --- ✅ Success Response ---
    return {
        "statusCode": 200,
        "body": "Weather data inserted successfully into AWS RDS (MySQL)."
    }
