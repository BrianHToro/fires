"""
Fire Data Download and Analysis using Earthkit

This script downloads the latest fire data from NASA FIRMS and processes it using earthkit.
"""

import requests
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
import os
import tempfile
import json
from pathlib import Path

# Try to import earthkit, install if not available
try:
    import earthkit.data as ek
    print("Earthkit imported successfully")
except ImportError:
    print("Installing earthkit...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "earthkit"])
    import earthkit.data as ek

def get_latest_fire_data():
    """
    Download the latest fire data from NASA FIRMS for today only.
    
    Returns:
        pandas.DataFrame: Fire data as a DataFrame
    """
    # NASA FIRMS API endpoint
    base_url = "https://firms.modaps.eosdis.nasa.gov/data/active_fire"
    
    # Available data sources
    sources = {
        'modis': 'modis-c6.1',
        'viirs': 'viirs-snpp',
        'viirs_noaa20': 'viirs-noaa20'
    }
    
    # Get current date
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")
    
    # Try different data sources
    for source_name, source_path in sources.items():
        try:
            # Use 24h data but filter to today only
            url = f"{base_url}/{source_path}/txt/MODIS_C6_1_Global_24h.csv"
            
            print(f"Downloading fire data from {source_name}...")
            print(f"URL: {url}")
            
            # Download the data
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse CSV data
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            if not df.empty:
                # Filter to today's data only
                if 'acq_date' in df.columns:
                    df_today = df[df['acq_date'] == today_str]
                    if not df_today.empty:
                        print(f"Successfully downloaded {len(df_today)} fire detections from {source_name} for {today_str}")
                        return df_today, source_name
                    else:
                        print(f"No fire data for {today_str} from {source_name}")
                        continue
                else:
                    print(f"No date column found in {source_name} data")
                    continue
            else:
                print(f"No data available from {source_name}")
                continue
                
        except Exception as e:
            print(f"Failed to download from {source_name}: {e}")
            continue
    
    raise Exception("Failed to download fire data from all sources")

def process_fire_data_with_earthkit(df, source_name):
    """
    Process fire data using earthkit.
    
    Args:
        df (pandas.DataFrame): Fire data DataFrame
        source_name (str): Name of the data source
    
    Returns:
        earthkit.data.Data: Processed data using earthkit
    """
    print(f"Processing {len(df)} fire detections with earthkit...")
    
    # Create a temporary file to store the data
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
        df.to_csv(tmp_file.name, index=False)
        tmp_path = tmp_file.name
    
    try:
        # Load data with earthkit
        data = ek.from_source("file", tmp_path)
        print(f"Loaded data with earthkit: {data}")
        
        # Get basic information about the data
        print(f"Data source: {source_name}")
        print(f"Number of records: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        
        # Show sample of the data
        print("\nSample data:")
        print(df.head())
        
        return data, df
        
    finally:
        # Clean up temporary file
        os.unlink(tmp_path)

def save_fire_data(df, source_name, output_dir="fire_data"):
    """
    Save fire data as single CSV, sorted by fire size (largest first).
    
    Args:
        df (pandas.DataFrame): Fire data DataFrame
        source_name (str): Name of the data source
        output_dir (str): Directory to save files
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Clear previous files
    import glob
    for old_file in glob.glob(os.path.join(output_dir, "fire_snapshot*.csv")):
        os.remove(old_file)
    
    # Sort by FRP (Fire Radiative Power) - largest fires first
    if 'frp' in df.columns:
        df_sorted = df.sort_values('frp', ascending=False)
        print(f"Sorted {len(df_sorted)} fires by size (largest first)")
    else:
        df_sorted = df
        print("No FRP column found - keeping original order")
    
    # Save as single CSV
    csv_file = os.path.join(output_dir, "fire_snapshot.csv")
    df_sorted.to_csv(csv_file, index=False)
    print(f"Saved CSV: {csv_file}")
    
    return {"csv": csv_file}

def analyze_fire_data(df):
    """
    Perform basic analysis on fire data.
    
    Args:
        df (pandas.DataFrame): Fire data DataFrame
    """
    print("\n=== Fire Data Analysis ===")
    
    # Basic statistics
    print(f"Total fire detections: {len(df)}")
    
    if 'confidence' in df.columns:
        print(f"Average confidence: {df['confidence'].mean():.2f}")
        print(f"High confidence fires (>80%): {len(df[df['confidence'] > 80])}")
    
    if 'latitude' in df.columns and 'longitude' in df.columns:
        print(f"Latitude range: {df['latitude'].min():.2f} to {df['latitude'].max():.2f}")
        print(f"Longitude range: {df['longitude'].min():.2f} to {df['longitude'].max():.2f}")
    
    # Group by date if available
    if 'acq_date' in df.columns:
        daily_counts = df['acq_date'].value_counts().sort_index()
        print(f"\nDaily fire counts:")
        for date, count in daily_counts.items():
            print(f"  {date}: {count} fires")
    
    # Most recent fires
    if 'acq_time' in df.columns:
        print(f"\nMost recent fire detection times:")
        recent_fires = df.nlargest(5, 'acq_time') if 'acq_time' in df.columns else df.head(5)
        for idx, fire in recent_fires.iterrows():
            print(f"  {fire.get('acq_date', 'N/A')} {fire.get('acq_time', 'N/A')} - "
                  f"Lat: {fire.get('latitude', 'N/A'):.2f}, Lon: {fire.get('longitude', 'N/A'):.2f}")

def main():
    """
    Main function to download and process latest fire data.
    """
    print("=== Fire Data Download and Analysis ===")
    print(f"Current time: {datetime.now()}")
    
    try:
        # Download latest fire data (today only)
        df, source_name = get_latest_fire_data()
        
        # Process with earthkit
        earthkit_data, processed_df = process_fire_data_with_earthkit(df, source_name)
        
        # Analyze the data
        analyze_fire_data(processed_df)
        
        # Save the data to files
        print("\n=== Saving Data ===")
        saved_files = save_fire_data(processed_df, source_name)
        
        print("\n=== Summary ===")
        print(f"Successfully downloaded and processed {len(processed_df)} fire detections")
        print(f"Data source: {source_name}")
        print(f"Earthkit data object: {earthkit_data}")
        print(f"File saved: {saved_files['csv']}")
        
        return processed_df, earthkit_data, saved_files
        
    except Exception as e:
        print(f"Error: {e}")
        return None, None, None

if __name__ == "__main__":
    fire_data, earthkit_data, saved_files = main()
