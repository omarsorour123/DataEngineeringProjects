"""
Weather Data ETL Utilities
This module contains the core ETL functions for the weather data pipeline:
- extract: Fetches weather data from API and stores in staging table
- transform: Processes staged data with additional metrics
- load: Saves transformed data to CSV file
"""

import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy import Table, MetaData, insert, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherDataETL:
    """Class to handle Weather Data ETL operations."""
    
    @staticmethod
    def fetch_weather_data(city: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch weather data for a single city from the API.
        
        Args:
            city: Name of the city
            config: Configuration dictionary containing API details
            
        Returns:
            Dictionary containing weather data
        
        Raises:
            requests.exceptions.RequestException: If API request fails
        """
        params = {
            'q': city,
            'appid': config['APP']['API_KEY'],
            'units': 'metric'
        }
        response = requests.get(config['APP']['URL'], params=params)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def prepare_weather_record(city: str, data: Dict[str, Any], extraction_timestamp: datetime) -> Dict[str, Any]:
        """
        Prepare a weather record for database insertion.
        
        Args:
            city: Name of the city
            data: Raw weather data from API
            extraction_timestamp: Timestamp of data extraction
            
        Returns:
            Dictionary containing formatted weather data
        """
        return {
            'city': city,
            'temperature': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'description': data['weather'][0]['description'],
            'wind_speed': data['wind']['speed'],
            'timestamp': datetime.now(),
            'dt': data['dt'],
            'extraction_timestamp': extraction_timestamp
        }

def extract(cities: List[str], config: Dict[str, Any], database: Any) -> datetime:
    """
    Extract weather data from API and store in staging table.
    
    Args:
        cities: List of city names
        config: Configuration dictionary
        database: Database connection object
        
    Returns:
        datetime: Timestamp of extraction
        
    Raises:
        Exception: If database insertion fails
    """
    weather_data = []
    extraction_timestamp = datetime.now()
    etl = WeatherDataETL()
    
    logger.info(f"Starting extraction for cities: {cities}")
    
    for city in cities:
        try:
            data = etl.fetch_weather_data(city, config)
            weather_record = etl.prepare_weather_record(city, data, extraction_timestamp)
            weather_data.append(weather_record)
            
            logger.info(f"Successfully fetched data for {city}")
            time.sleep(1)  # Rate limiting
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {city}: {e}")
    
    try:
        metadata = MetaData()
        staging_weather_table = Table('stagging_weather_data', metadata, autoload_with=database.engine)
        
        with database.engine.begin() as connection:
            connection.execute(insert(staging_weather_table), weather_data)
            logger.info(f"Successfully inserted {len(weather_data)} records into staging table")
        
        return extraction_timestamp
            
    except Exception as e:
        logger.error(f"Error inserting data into the database: {e}")
        raise

def transform(extraction_timestamp: datetime, database: Any) -> pd.DataFrame:
    """
    Transform weather data with additional metrics.
    
    Args:
        extraction_timestamp: Timestamp of data extraction
        database: Database connection object
        
    Returns:
        pandas.DataFrame: Transformed weather data
    """
    query = text('SELECT * FROM stagging_weather_data WHERE extraction_timestamp = :extraction_timestamp')
    
    # Fetch data from database
    with database.engine.connect() as connection:
        result = connection.execute(query, {"extraction_timestamp": extraction_timestamp})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    logger.info(f"Retrieved {len(df)} records for transformation")
    
    # Time-based transformations
    df['processing_timestamp'] = pd.to_datetime(df['timestamp'])
    df['event_timestamp'] = pd.to_datetime(df['dt'], unit='s')
    df['data_lag'] = (df['processing_timestamp'] - df['event_timestamp'])
    df['hour'] = df['event_timestamp'].dt.hour
    df['day_of_week'] = df['event_timestamp'].dt.day_name()
    df['is_daytime'] = df['hour'].between(6, 18)
    
    # Temperature transformations
    df['temp_category'] = pd.cut(
        df['temperature'],
        bins=[-float('inf'), 0, 15, 25, float('inf')],
        labels=['Cold', 'Cool', 'Moderate', 'Hot']
    )
    df['temperature_fahrenheit'] = df['temperature'] * 9/5 + 32
    
    # Wind speed transformations
    df['wind_speed_mph'] = df['wind_speed'] * 2.237
    
    # Text transformations
    df['description'] = df['description'].str.title()
    
    # Weather severity calculation
    df['weather_severity'] = (
        (df['temperature'].abs() / 40) * 0.4 +
        (df['wind_speed'] / 20) * 0.3 +
        (df['humidity'] / 100) * 0.3
    )
    
    # Final column selection and ordering
    columns_order = [
        'city', 'event_timestamp', 'processing_timestamp', 'data_lag',
        'temperature', 'temperature_fahrenheit', 'feels_like', 'humidity',
        'pressure', 'wind_speed', 'wind_speed_mph', 'description',
        'temp_category', 'is_daytime', 'weather_severity', 'hour', 'day_of_week'
    ]
    
    return df[columns_order]

def load(df: pd.DataFrame, output_path: str) -> None:
    """
    Save transformed data to CSV file.
    
    Args:
        df: Transformed weather data
        output_path: Path to output CSV file
        
    Raises:
        ValueError: If input is not a DataFrame
        Exception: If file operations fail
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")
    
    try:
        logger.info(f"Preparing to save DataFrame of shape {df.shape}")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save data
        if os.path.exists(output_path):
            df.to_csv(output_path, mode='a', header=False, index=False)
            logger.info(f"Data appended to existing file: {output_path}")
        else:
            df.to_csv(output_path, index=False)
            logger.info(f"New file created: {output_path}")
        
        # Verify file
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            logger.info(f"File size after writing: {file_size} bytes")
        else:
            raise Exception("File was not created successfully")
            
    except Exception as e:
        logger.error(f"Error in saving the DataFrame: {e}")
        raise