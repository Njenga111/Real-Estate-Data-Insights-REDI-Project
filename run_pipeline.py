import logging
import pandas as pd
from etl.extract.extractor import extract_data_from_websites
from etl.transform.cleaner import clean_data
from etl.load.db_loader import load_data_to_postgresql

def run_etl_pipeline():
    logging.info("ETL pipeline started.")
    
    # Step 1: Extract data from the two websites
    # Passing the 'property254' and 'buyrentkenya' as source arguments
    df_property254 = extract_data_from_websites('property254')
    df_buy_rent_kenya = extract_data_from_websites('buyrentkenya')
    
    # Combine the data from both sources
    df_combined = pd.concat([df_property254, df_buy_rent_kenya], ignore_index=True)
    logging.info(f"Data extracted. Combined shape: {df_combined.shape}")
    
    # Step 2: Clean the data
    df_cleaned = clean_data(df_combined)
    logging.info(f"Data cleaned. Shape: {df_cleaned.shape}")
    
    # Step 3: Load data into PostgreSQL
    load_data_to_postgresql(df_cleaned)
    logging.info("Data loaded into PostgreSQL successfully.")

    logging.info("ETL pipeline completed.")
