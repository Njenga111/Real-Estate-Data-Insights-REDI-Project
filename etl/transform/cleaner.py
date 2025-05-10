import pandas as pd

def clean_data(df):
    # Print column names for debugging
    print("Columns in DataFrame:", df.columns)
    
    # Check if 'price' column exists before cleaning
    if 'price' in df.columns:
        # Use raw string to avoid invalid escape sequence warning
        df['price'] = df['price'].replace({r'\$': '', ',': ''}, regex=True)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')  # Convert to numeric, force errors to NaN
    else:
        print("Warning: 'price' column not found!")

    # Clean other columns as necessary
    # Example: Remove extra spaces from column names if needed
    df.columns = df.columns.str.strip()

    return df
# etl/transform/cleaner.py
def clean_data(df):
    """Clean data with more robust handling"""
    if df.empty:
        return df
        
    # Clean column names
    df.columns = df.columns.str.strip().str.lower()
    
    # Price cleaning
    if 'price' in df.columns:
        # Remove all non-numeric characters except decimal point
        df['price'] = df['price'].str.replace(r'[^\d.]', '', regex=True)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    # Clean text columns
    text_cols = ['title', 'location']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].str.strip()
    
    return df.dropna(subset=['title', 'price'])