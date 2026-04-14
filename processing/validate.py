from loguru import logger


class DataValidator:

    @staticmethod
    def validate(data):
        valid_data = []
        error_data = []

        for record in data:
            try:
                if not record.get("name") or not record.get("email"):
                    raise ValueError("Missing required fields")

                valid_data.append(record)

            except Exception as e:
                logger.error(f"Validation error: {e}")

                error_data.append({
                    "error": str(e),
                    "data": record
                })

        logger.info(f"Valid records: {len(valid_data)}")
        logger.warning(f"Invalid records: {len(error_data)}")

        return valid_data, error_data