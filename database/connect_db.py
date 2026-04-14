import mysql.connector
from config import DB_CONFIG
from loguru import logger 

def get_connection():
    try:
        logger.info("Database connection started")

        conn = mysql.connector.connect(**DB_CONFIG)

        if conn.is_connected():
            logger.success("Database connected successfully")
            return conn
        else:
            logger.error("Connection created but not connected")
            return None

    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None