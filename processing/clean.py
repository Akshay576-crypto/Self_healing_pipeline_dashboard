from loguru import logger
from processing.rules import RulesEngine


class DataCleaner:

    @staticmethod
    def clean(data):
        cleaned_data = []
        error_data = []
        total_fixes = 0

        for record in data:
            try:
                # Step 1: Basic formatting (your original logic)
                formatted_record = {
                    "name": record.get("name", "").strip().title() if record.get("name") else None,
                    "email": record.get("email", "").strip().lower() if record.get("email") else None,
                    "age": record.get("age") if isinstance(record.get("age"), int) else None,
                    "city": record.get("city", "").strip() if record.get("city") else None
                }

                # Step 2: Apply Rules Engine (self-healing)
                fixed_record, errors, fixes = RulesEngine.apply_rules(formatted_record)

                total_fixes += fixes

                # Step 3: Separate clean vs error
                if errors:
                    record["errors"] = errors
                    error_data.append(record)
                else:
                    cleaned_data.append(fixed_record)

            except Exception as e:
                logger.error(f"Cleaning error: {e}")
                error_data.append(record)

        logger.success(f" Cleaned records: {len(cleaned_data)}")
        logger.info(f" Total fixes applied: {total_fixes}")

        return cleaned_data