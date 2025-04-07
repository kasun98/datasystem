import psycopg2
from psycopg2 import sql
import os
from configs import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD


# Create Connection
def create_connection(name):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=name,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        print(e)
        return None

# Create Database
def create_db():
    """Check if the database exists, then create it if it doesn't"""
    conn = create_connection("postgres")  # Connect to default db
    if conn:
        try:
            conn.autocommit = True
            cursor = conn.cursor()

            # Check if database already exists
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (DB_NAME,))
            exists = cursor.fetchone()

            if not exists:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
                print(f"Database '{DB_NAME}' created successfully!")
            else:
                print(f"Database '{DB_NAME}' already exists.")

            cursor.close()
            conn.close()
        except psycopg2.Error as e:
            print(f"Error creating database: {e}")

# Create Tables
def create_tables():
    """Create tables in the database if they do not exist"""
    create_db()  # Ensure DB exists before creating tables
    conn = create_connection(DB_NAME)
    if conn:
        try:
            cursor = conn.cursor()

            # Air Quality Data Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS air_quality_data (
                    id SERIAL PRIMARY KEY,
                    aqi INTEGER NOT NULL,
                    idx INTEGER NOT NULL,
                    dominentpol VARCHAR(10),
                    iaqi_h INTEGER,
                    iaqi_t FLOAT,
                    iaqi_pm1 INTEGER,
                    iaqi_pm10 INTEGER,
                    iaqi_pm25 INTEGER,
                    station VARCHAR(255) NOT NULL,
                    station_coords_lat DOUBLE PRECISION,
                    station_coords_lon DOUBLE PRECISION,
                    time TIMESTAMP NOT NULL,
                    insert_time TIMESTAMP DEFAULT NOW()
                );
            """)

            # Feature Store Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS air_quality_feature_store (
                    id SERIAL PRIMARY KEY,
                    location_id VARCHAR(50),
                    date TIMESTAMP,
                    pm1 FLOAT,
                    pm10 FLOAT,
                    pm25 FLOAT,
                    rh FLOAT,
                    temp FLOAT,
                    hour INT,
                    day_of_week INT,
                    month INT,
                    is_weekend BOOLEAN,
                    temp_x_rh FLOAT,
                    pm1_x_pm10 FLOAT,
                    pm1_x_pm25 FLOAT,
                    pm10_x_pm25 FLOAT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS air_quality_index_predictions (
                    id SERIAL PRIMARY KEY,
                    location_id TEXT NOT NULL,
                    date TIMESTAMP NOT NULL,
                    predicted_at TIMESTAMP NOT NULL,
                    pm25_t_5min DOUBLE PRECISION,
                    pm25_t_10min DOUBLE PRECISION,
                    pm25_t_15min DOUBLE PRECISION,
                    pm25_t_20min DOUBLE PRECISION,
                    pm25_t_25min DOUBLE PRECISION,
                    pm25_t_30min DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)

            conn.commit()
            cursor.close()
            conn.close()
            print("Tables created successfully!")
        except psycopg2.Error as e:
            print(f"Error creating tables: {e}")

if __name__ == "__main__":
    create_tables()
