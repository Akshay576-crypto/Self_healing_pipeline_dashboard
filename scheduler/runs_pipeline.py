from loguru import logger
import schedule
import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



def job():
    logger.info(" Starting scheduled pipeline run...")
    

def start_scheduler():
    logger.info(" Scheduler started...")

    schedule.every(10).seconds.do(job)

    job()

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)

    except KeyboardInterrupt:
        logger.warning(" Scheduler stopped manually by user")

        
if __name__ == "__main__":
    start_scheduler()