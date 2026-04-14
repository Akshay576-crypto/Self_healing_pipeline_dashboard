from loguru import logger
from database.connect_db import get_connection


class InsertData:

    @staticmethod
    def insert_raw(record):
        conn = None
        try:
            if not isinstance(record, dict):
                logger.error(f"Invalid raw record: {record}")
                return

            conn = get_connection()
            cursor = conn.cursor()

            query = """
            INSERT INTO customer_raw (name, email, age, raw_json)
            VALUES (?, ?, ?, ?)
            """

            cursor.execute(query, (
                record.get("name"),
                record.get("email"),
                record.get("age"),
                str(record)
            ))

            conn.commit()
            logger.info("Raw data inserted")

        except Exception as e:
            logger.error(f"Raw insert failed: {e}")

        finally:
            if conn:
                conn.close()


    @staticmethod
    def insert_cleaned(record):
        conn = None
        try:
            if not isinstance(record, dict):
                logger.error(f"Invalid cleaned record: {record}")
                return

            conn = get_connection()
            cursor = conn.cursor()

            query = """
            INSERT INTO customer_cleaned (name, email, age)
            VALUES (?, ?, ?)
            """

            cursor.execute(query, (
                record.get("name"),
                record.get("email"),
                record.get("age")
            ))

            conn.commit()
            logger.info("Cleaned data inserted")

        except Exception as e:
            logger.error(f"Cleaned insert failed: {e}")

        finally:
            if conn:
                conn.close()


    @staticmethod
    def insert_error(data):
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()

            query = """
            INSERT INTO customer_error 
            (raw_id, column_name, error_type, bad_value, error_message)
            VALUES (?, ?, ?, ?, ?)
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
            if conn:
                conn.close()


    @staticmethod
    def insert_fixed(data):
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()

            query = """
            INSERT INTO fix_logs (name, email, age)
            VALUES (?, ?, ?)
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
            if conn:
                conn.close()


    @staticmethod
    def log_pipeline_run(start_time, end_time, total_records, error_count, fix_count, quality_score, status):
        conn = None
        try:
            conn = get_connection()
            cursor = conn.cursor()

            query = """
            INSERT INTO pipeline_runs 
            (start_time, end_time, total_records, error_count, fix_count, quality_score, status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
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
            if conn:
                conn.close()