from loguru import logger


class QualityScore:

    @staticmethod
    def calculate(parsed_data, valid_data, error_data):
        try:
            total_records = len(parsed_data)

            if total_records == 0:
                return 0

            #  Completeness (no missing fields)
            complete_records = 0
            for record in parsed_data:
                if all(record.values()):
                    complete_records += 1

            completeness = complete_records / total_records

            # Validity (passed validation)
            validity = len(valid_data) / total_records

            #  Error impact (fewer errors = better)
            error_rate = len(error_data) / total_records
            accuracy = 1 - error_rate

            #  Final weighted score
            quality_score = (
                (0.4 * completeness) +
                (0.4 * validity) +
                (0.2 * accuracy)
            ) * 100

            quality_score = round(quality_score, 2)

            logger.info(f" Completeness: {round(completeness*100,2)}%")
            logger.info(f" Validity: {round(validity*100,2)}%")
            logger.info(f" Accuracy: {round(accuracy*100,2)}%")
            logger.success(f" Final Quality Score: {quality_score}%")

            return quality_score

        except Exception as e:
            logger.error(f"Quality score calculation error: {e}")
            return 0