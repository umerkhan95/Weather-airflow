"""
Weather Data Collector - Standalone Version
-----------------------------------------
This script collects weather data from OpenWeatherMap API without requiring Airflow.
It implements the same functionality as the DAG but runs as a standalone Python script.
"""

import os
import json
import requests
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# For Google Drive integration
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

# Load environment variables
load_dotenv()

# Define constants
CITY_NAME = "Karachi"
TOTAL_HOURS = 8
INTERVAL_MINUTES = 10
API_KEY = os.getenv('OPENWEATHERMAP_API_KEY', '')
BASE_URL = "http://api.openweathermap.org/data/2.5/forecast?"

def get_weather_data(timestamp):
    """Get weather data for a specific timestamp."""
    try:
        # Convert timestamp to Unix time (seconds since epoch)
        unix_time = int(timestamp.timestamp())
        
        # Build URL with API key and parameters
        url = f"{BASE_URL}q={CITY_NAME}&appid={API_KEY}&dt={unix_time}"
        
        # Make request to OpenWeatherMap API
        print(f"Fetching weather data for {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # Parse response
        data = response.json()
        
        # Check if we have forecast data
        if "list" in data and len(data["list"]) > 0:
            weather = data["list"][0]
            return {
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "temperature_kelvin": weather["main"]["temp"],
                "pressure": weather["main"]["pressure"],
                "humidity": weather["main"]["humidity"],
                "description": weather["weather"][0]["description"],
                "wind_speed": weather["wind"]["speed"] if "wind" in weather else None,
                "clouds": weather["clouds"]["all"] if "clouds" in weather else None,
                "visibility": weather.get("visibility", None),
            }
        else:
            print(f"No forecast data found for {timestamp}")
            return None
            
    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None
    except (KeyError, IndexError) as e:
        print(f"Error parsing weather data: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def extract_weather_data():
    """Extract weather data for 8 hours with 10-minute intervals."""
    print("Starting weather data extraction...")
    
    # Start from the current date and time
    start_time = datetime.now().replace(microsecond=0)
    print(f"Extraction start time: {start_time}")
    
    # Calculate the total number of intervals
    total_intervals = (TOTAL_HOURS * 60) // INTERVAL_MINUTES
    print(f"Will fetch {total_intervals} data points at {INTERVAL_MINUTES}-minute intervals")
    
    # Initialize list to store raw data
    raw_data = []
    
    # Fetch data for each interval
    for i in range(total_intervals):
        # Calculate the current time for this interval
        current_time = start_time + timedelta(minutes=i * INTERVAL_MINUTES)
        
        # Get weather data
        weather_data = get_weather_data(current_time)
        
        if weather_data:
            raw_data.append(weather_data)
            print(f"Successfully fetched data for {current_time}")
        else:
            print(f"Failed to fetch data for {current_time}")
    
    # Log the results
    print(f"Extracted {len(raw_data)} data points out of {total_intervals} intervals")
    
    return raw_data

def transform_weather_data(raw_data):
    """Transform raw weather data."""
    print("Starting weather data transformation...")
    
    # Check if we have any data
    if not raw_data or len(raw_data) == 0:
        print("No raw data found, aborting transformation")
        return None
    
    # Transform each data point
    transformed_data = []
    for data_point in raw_data:
        transformed_point = {
            "timestamp": data_point["timestamp"],
            "temperature_celsius": round(data_point["temperature_kelvin"] - 273.15, 2),
            "temperature_kelvin": data_point["temperature_kelvin"],
            "pressure": data_point["pressure"],
            "humidity": data_point["humidity"],
            "description": data_point["description"],
        }
        transformed_data.append(transformed_point)
    
    # Add metadata
    weather_data = {
        "city": CITY_NAME,
        "collection_date": datetime.now().strftime("%Y-%m-%d"),
        "interval_minutes": INTERVAL_MINUTES,
        "total_hours": TOTAL_HOURS,
        "data_points": len(transformed_data),
        "measurements": transformed_data
    }
    
    print(f"Transformed {len(transformed_data)} data points successfully")
    
    return weather_data

def save_to_file(transformed_data):
    """Save the transformed weather data to a JSON file."""
    print("Starting data save operation...")
    
    if not transformed_data:
        print("No transformed data found, aborting save operation")
        return None
    
    # Create output directory
    output_dir = Path("weather_data")
    output_dir.mkdir(exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"{CITY_NAME.lower()}_weather_data_{timestamp}.json"
    
    try:
        # Save data to file
        with open(output_file, 'w') as f:
            json.dump(transformed_data, f, indent=2)
        
        print(f"Successfully saved data to {output_file}")
        
        return str(output_file)
    
    except Exception as e:
        print(f"Error saving data to file: {e}")
        raise

def upload_to_google_drive(file_path):
    """Upload the JSON file to Google Drive using a service account."""
    print("Starting Google Drive upload operation...")
    
    if not file_path:
        print("No file path provided")
        return
    
    # Path to the service account credentials JSON file
    SERVICE_ACCOUNT_FILE = '/Users/umerkhan/code/ory/fun_projects/weatherApp_airflow/murtuza-weather-1688bc99d8e1.json'
    
    # Define the scopes for Google Drive API
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    try:
        # Authenticate using service account
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        
        # Build the Drive API service
        service = build('drive', 'v3', credentials=credentials)
        
        # The folder ID from the provided Google Drive URL
        folder_id = '1qWb9DWctbYKJGB0pahx-AlD6b7RFqeXz'
        
        # File metadata with the specific folder as parent
        file_metadata = {
            'name': os.path.basename(file_path),
            'parents': [folder_id]
        }
        
        # Upload the file
        media = MediaFileUpload(file_path, mimetype='application/json')
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,name,webViewLink'
        ).execute()
        
        print(f"File successfully uploaded to Google Drive in the specified folder!")
        print(f"File ID: {file.get('id')}")
        print(f"File Name: {file.get('name')}")
        if 'webViewLink' in file:
            print(f"Web View Link: {file.get('webViewLink')}")
        
        return file.get('id')
            
    except Exception as e:
        print(f"Error uploading data to Google Drive: {e}")
        print(f"Detailed exception info: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Run the weather data collection workflow."""
    print("=" * 50)
    print("Weather Data Collection")
    print("=" * 50)
    
    try:
        # Extract weather data
        raw_data = extract_weather_data()
        
        # Transform weather data
        transformed_data = transform_weather_data(raw_data)
        
        # Save to file
        if transformed_data:
            output_file = save_to_file(transformed_data)
            print(f"\nData collection complete. File saved to: {output_file}")
            
            # Upload to Google Drive
            print("\nUploading to Google Drive...")
            file_id = upload_to_google_drive(output_file)
            if file_id:
                print(f"Successfully uploaded file to Google Drive with ID: {file_id}")
            else:
                print("Failed to upload file to Google Drive")
        else:
            print("\nNo data to save. Data collection failed.")
        
        print("=" * 50)
    
    except Exception as e:
        print(f"Error in weather data collection: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
