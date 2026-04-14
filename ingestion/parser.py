from loguru import logger


class DataParser:

    @staticmethod
    def parse(data):
        """
        Convert raw API response → structured format
        Returns:
            list of dictionaries
        """

        structured_data = []

        try:
            #  Handle None or empty input
            if not data:
                logger.error(" Empty input data")
                return []

            #  Handle case: data is wrapped inside a dict (like API response)
            if isinstance(data, dict):
                if "data" in data:
                    data = data["data"]
                elif "users" in data:
                    data = data["users"]
                else:
                    logger.error(" Unsupported dict structure")
                    return []

            #  Ensure it's a list now
            if not isinstance(data, list):
                logger.error(f" Expected list, got {type(data)}")
                return []

            #  Process each record
            for i, record in enumerate(data):

                if not isinstance(record, dict):
                    logger.warning(f" Skipping invalid record at index {i}: {record}")
                    continue

                structured_record = {
                    "name": record.get("name") or record.get("username"),
                    "email": record.get("email"),
                    "age": record.get("id"),  # using id as dummy age
                    "city": (
                        record.get("address", {}).get("city")
                        if isinstance(record.get("address"), dict)
                        else None
                    )
                }

                structured_data.append(structured_record)

            logger.success(f" Parsed {len(structured_data)} records successfully")

            return structured_data

        except Exception as e:
            logger.error(f" Parsing failed: {e}")
            return []