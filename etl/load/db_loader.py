# etl/load/db_loader.py

from io import StringIO
import psycopg2
from config.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
import pandas as pd
import logging

# etl/load/db_loader.py
def load_data_to_postgresql(df):
    """Load data more efficiently with batch insert"""
    if df.empty:
        logging.warning("Empty DataFrame - nothing to load")
        return
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        # Create table if not exists
        with conn.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS properties (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                price NUMERIC,
                location VARCHAR(255),
                source VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)
            
            # Create temp table for batch upsert
            cursor.execute("""
            CREATE TEMP TABLE temp_properties (LIKE properties) ON COMMIT DROP;
            """)
            
            # Batch insert to temp table
            with StringIO() as buffer:
                df.to_csv(buffer, index=False, header=False, sep='\t')
                buffer.seek(0)
                cursor.copy_from(buffer, 'temp_properties', columns=df.columns.tolist())
            
            # Upsert from temp to main table
            cursor.execute("""
            INSERT INTO properties (title, price, location, source)
            SELECT title, price, location, source FROM temp_properties
            ON CONFLICT (title, location) DO UPDATE 
            SET price = EXCLUDED.price;
            """)
            
        conn.commit()
    except Exception as e:
        logging.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()