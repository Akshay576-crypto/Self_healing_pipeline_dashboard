from loguru import logger
from database.connect_db import get_connection


class InsertData:


    @staticmethod

    def insert_raw(record):
        conn = None
        cursor = None

        try:
            # Safety check
            if not isinstance(record, dict):
                logger.error(f"Invalid raw record: {record}")
                return

            conn = get_connection()
            if not conn:
                logger.error("DB connection failed (raw insert)")
                return

            cursor = conn.cursor()

            query = """
            INSERT INTO customer_raw (name, email, age, raw_json)
            VALUES (%s, %s, %s, %s)
            """

            cursor.execute(query, (
                record.get("name"),
                record.get("email"),
                record.get("age"),   #  FIXED (not id)
                str(record)
            ))

            conn.commit()

        except Exception as e:
            logger.error(f"Raw insert failed: {e}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    @staticmethod
        
    def insert_cleaned(record):
        conn = None
        cursor = None

        try:
            # Safety check
            if not isinstance(record, dict):
                logger.error(f"Invalid cleaned record: {record}")
                return

            conn = get_connection()
            if not conn:
                logger.error("DB connection failed (cleaned insert)")
                return

            cursor = conn.cursor()

            query = """
            INSERT INTO customer_cleaned (name, email, age)
            VALUES (%s, %s, %s)
            """

            cursor.execute(query, (
                record.get("name"),
                record.get("email"),
                record.get("age")
            ))

            conn.commit()

        except Exception as e:
            logger.error(f"Cleaned insert failed: {e}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    @staticmethod
    def insert_error(data):
        conn = None
        cursor = None

        try:
            conn = get_connection()
            if not conn:
                logger.error("DB connection failed (error insert)")
                return

            cursor = conn.cursor()

            query = """
            INSERT INTO error_logs 
            (raw_id, column_name, error_type, bad_value, error_message)
            VALUES (%s, %s, %s, %s, %s)
            """

            for record in data:
                cursor.execute(query, (
                    record.get("raw_id"),
                    record.get("column"),
                    record.get("type"),
                    str(record.get("value")),
                    record.get("message")
                ))

            conn.commit()
            logger.warning(f"{len(data)} error logs inserted")

        except Exception as e:
            logger.error(f"Error insert failed: {e}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()



    @staticmethod
    def insert_fixed(data):
        conn = None
        cursor = None

        try:
            conn = get_connection()
            if not conn:
                logger.error("DB connection failed (fix insert)")
                return

            cursor = conn.cursor()

            query = """
            INSERT INTO fix_logs (name, email, age)
            VALUES (%s, %s, %s)
            """

            for record in data:
                cursor.execute(query, (
                    record["name"],
                    record["email"],
                    record["age"]
                ))

            conn.commit()
            logger.success(f"{len(data)} fixed records inserted")

        except Exception as e:
            logger.error(f"Fix insert failed: {e}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    
    @staticmethod
    def log_pipeline_run(status, message):
        conn = None
        cursor = None

        try:
            conn = get_connection()
            if not conn:
                logger.error("DB connection failed (pipeline log)")
                return

            cursor = conn.cursor()

            query = """
            INSERT INTO pipeline_run (status, message)
            VALUES (%s, %s)
            """

            cursor.execute(query, (status, message))
            conn.commit()

            logger.info("Pipeline run logged")

        except Exception as e:
            logger.error(f"Pipeline log failed: {e}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @staticmethod
        
    def log_pipeline_run(start_time, end_time, total_records, error_count, fix_count, quality_score, status):
        conn = None
        cursor = None

        try:
            conn = get_connection()
            if not conn:
                logger.error("DB connection failed (pipeline log)")
                return

            cursor = conn.cursor()

            query = """
            INSERT INTO pipeline_runs 
            (start_time, end_time, total_records, error_count, fix_count, quality_score, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(query, (
                start_time,
                end_time,
                total_records,
                error_count,
                fix_count,
                quality_score,
                status
            ))

            conn.commit()
            logger.info("Pipeline run logged")

        except Exception as e:
            logger.error(f"Pipeline log failed: {e}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()