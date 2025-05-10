import logging
from run_pipeline import run_etl_pipeline

# Configure logging
logging.basicConfig(filename='logs/etl.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Starting ETL process...")
    try:
        run_etl_pipeline()
        logging.info("ETL pipeline completed successfully.")
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")

if __name__ == "__main__":
    main()
