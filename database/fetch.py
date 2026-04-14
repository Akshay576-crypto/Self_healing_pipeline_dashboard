from database.connect_db import get_connection
from loguru import logger



class FetchData:

    
    @staticmethod
    def fetch_cleaned():
        conn = None
        cursor = None

        try:
            conn = get_connection()
            if not conn:
                logger.error("DB connection failed (fetch cleaned)")
                return []

            cursor = conn.cursor(dictionary=True)

            query = "SELECT * FROM customer_cleaned"
            cursor.execute(query)

            result = cursor.fetchall()
            logger.info(f"Fetched {len(result)} cleaned records")

            return result

        except Exception as e:
            logger.error(f"Fetch cleaned failed: {e}")
            return []

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    # Fetch error data 
    @staticmethod
    def fetch_errors():
        conn = None
        cursor = None

        try:
            conn = get_connection()
            if not conn:
                logger.error("DB connection failed (fetch error)")
                return []

            cursor = conn.cursor(dictionary=True)

            query = "SELECT * FROM customer_error"
            cursor.execute(query)

            result = cursor.fetchall()
            logger.info(f"Fetched {len(result)} error records")

            return result

        except Exception as e:
            logger.error(f"Fetch error failed: {e}")
            return []

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    
    @staticmethod
    def fetch_error_records():
        try:
            conn = get_connection()
            cursor = conn.cursor(dictionary=True)

            query = "SELECT * FROM customer_error"
            cursor.execute(query)

            data = cursor.fetchall()
            logger.info(f"Fetched {len(data)} error records")

            return data

        except Exception as e:
            logger.error(f"Error fetching error records: {e}")
            return []

        
    @staticmethod
    def fetch_cleaned_records():
        try:
            conn = get_connection()
            cursor = conn.cursor(dictionary=True)

            cursor.execute("SELECT * FROM customer_cleaned")
            data = cursor.fetchall()

            return data

        except Exception as e:
            logger.error(f"Error fetching cleaned records: {e}")
            return []

    
    @staticmethod
    def fetch_pipeline_runs():
        try:
            conn = get_connection()
            cursor = conn.cursor(dictionary=True)

            cursor.execute("SELECT * FROM pipeline_runs ORDER BY run_id DESC")
            data = cursor.fetchall()

            return data

        except Exception as e:
            logger.error(f"Error fetching pipeline runs: {e}")
            return []