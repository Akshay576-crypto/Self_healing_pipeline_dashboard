import requests
from loguru import logger


class APIClient:

    @staticmethod
    def fetch_data():
        """
        Fetch data from public API
        """
        url = "https://jsonplaceholder.typicode.com/users"

        try:
            logger.info("Fetching data from API...")
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                logger.success(f"Fetched {len(data)} records from API")
                return data
            else:
                logger.error(f"API failed with status code: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"API request failed: {e}")
            return []