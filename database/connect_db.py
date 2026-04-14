import sqlite3
from config import DB_PATH
from loguru import logger

def get_connection():
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return None


def create_tables():
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_raw (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            email TEXT,
            age INTEGER,
            raw_json TEXT
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_cleaned (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            email TEXT,
            age INTEGER
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_error (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_id INTEGER,
            column_name TEXT,
            error_type TEXT,
            bad_value TEXT,
            error_message TEXT
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fix_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            email TEXT,
            age INTEGER
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time TEXT,
            end_time TEXT,
            total_records INTEGER,
            error_count INTEGER,
            fix_count INTEGER,
            quality_score REAL,
            status TEXT
        )
        """)

        conn.commit()
        conn.close()

        logger.success("Tables created successfully")

    except Exception as e:
        logger.error(f"Table creation failed: {e}")