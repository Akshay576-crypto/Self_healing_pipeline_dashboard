from database.connect_db import get_connection
from loguru import logger


class FetchData:

    @staticmethod
    def _execute_query(query):
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()

            cursor.execute(query)
            rows = cursor.fetchall()

            # Convert to list of dict
            columns = [col[0] for col in cursor.description]
            data = [dict(zip(columns, row)) for row in rows]

            return data

        except Exception as e:
            logger.error(f"Query failed: {e}")
            return []

        finally:
            if conn:
                conn.close()


    @staticmethod
    def fetch_cleaned():
        data = FetchData._execute_query("SELECT * FROM customer_cleaned")
        logger.info(f"Fetched {len(data)} cleaned records")
        return data


    @staticmethod
    def fetch_errors():
        data = FetchData._execute_query("SELECT * FROM customer_error")
        logger.info(f"Fetched {len(data)} error records")
        return data


    @staticmethod
    def fetch_pipeline_runs():
        data = FetchData._execute_query(
            "SELECT * FROM pipeline_runs ORDER BY run_id DESC"
        )
        logger.info(f"Fetched {len(data)} pipeline runs")
        return data