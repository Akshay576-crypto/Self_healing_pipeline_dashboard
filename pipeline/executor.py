from loguru import logger
from ingestion.api_client import APIClient
from ingestion.parser import DataParser
from processing.validate import DataValidator
from processing.clean import DataCleaner
from database.insert import InsertData
from datetime import datetime
from processing.quality_score import QualityScore
from database.fetch import FetchData
from processing.rules import RulesEngine

class PipelineExecutor:

    @staticmethod
    def run():
        logger.info(" Pipeline started")
        start_time = datetime.now()

        try:
            # Step 1: Fetch data
            raw_data = APIClient.fetch_data()

            if not raw_data:
                raise Exception("No data fetched from API")

            # Step 2: Parse data
            parsed_data = DataParser.parse(raw_data)

            if not parsed_data:
                raise Exception("Parsed data is empty")

            print(type(parsed_data))
            print(parsed_data[0])

            # Step 3: Validate
            valid_data, error_data = DataValidator.validate(parsed_data)

            # Step 4: Clean
            cleaned_data = DataCleaner.clean(valid_data)

            # Step 5: Insert RAW
            for record in parsed_data:
                InsertData.insert_raw(record)

            # Step 6: Insert CLEANED
            for record in cleaned_data:
                InsertData.insert_cleaned(record)

            # Step 7: Insert ERROR
            for record in error_data:
                InsertData.insert_error(record)

            # Step 8:error records 
            error_records = FetchData.fetch_error_records()
            reprocessed_clean = []
            for record in error_records:
                try:
                    fixed_record, errors, fixes = RulesEngine.apply_rules(record)

                    if not errors:
                        InsertData.insert_cleaned(fixed_record)
                        reprocessed_clean.append(fixed_record)
                except Exception as e:
                    logger.error(f"Reprocessing error: {e}")
            logger.info(f" Reprocessed and fixed: {len(reprocessed_clean)} records")

            end_time = datetime.now()
            # Step 9: Calculate Quality Score
            quality_score = QualityScore.calculate(
                parsed_data=parsed_data,
                valid_data=valid_data,
                error_data=error_data
            )

            end_time = datetime.now()

            InsertData.log_pipeline_run(
                start_time=start_time,
                end_time=end_time,
                total_records=len(parsed_data),
                error_count=len(error_data),
                fix_count=0,
                quality_score=quality_score,
                status="SUCCESS"
            )   
            logger.success(" Pipeline completed successfully")

        except Exception as e:
            logger.error(f" Pipeline failed: {e}")

            end_time = datetime.now()
            InsertData.log_pipeline_run(
                start_time=start_time,
                end_time=end_time,
                total_records=0,
                error_count=0,
                fix_count=0,
                quality_score=0,
                status="FAILED"
            )