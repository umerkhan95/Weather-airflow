# Weather App with Airflow Integration

This project fetches weather data from OpenWeatherMap API and uploads it to Google Drive. It includes both a standalone Python script and an Airflow DAG implementation.

## Features

- Fetches current weather data for Karachi from OpenWeatherMap API
- Transforms data by converting temperature from Kelvin to Celsius and adding metadata
- Saves data as JSON files with timestamps
- Uploads files to Google Drive using service account authentication
- Includes both a standalone Python script and an Airflow DAG implementation

## Project Components

### Standalone Application
- `weather_standalone.py`: Run this script directly to fetch weather data without Airflow

### Airflow DAG
- `dags/weather_etl.py`: An Airflow DAG that can be deployed to an existing Airflow instance

## Prerequisites

- Python 3.8+
- OpenWeatherMap API key
- Google Drive API credentials (service account)

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd weatherApp_airflow
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Set up Environment Variables

Create a `.env` file in the project root:

```
OPENWEATHERMAP_API_KEY=your_api_key
```

### 4. Google Drive Integration

Place your service account JSON file in the project root and update the path in the scripts if necessary.

## Usage

### Standalone Script

```bash
python weather_standalone.py
```

### Airflow DAG

Copy the DAG file to your Airflow dags folder:

```bash
cp dags/weather_etl.py ~/airflow/dags/
```

Then activate the DAG from the Airflow UI.

## Project Structure

```
weatherApp_airflow/
├── .env                        # Environment variables
├── .gitignore                  # Git ignore file
├── README.md                   # This file
├── requirements.txt            # Python dependencies
├── dags/                       # Airflow DAGs
│   └── weather_etl.py          # Weather ETL DAG
├── weather_standalone.py       # Standalone weather script
└── service-account-key.json    # Google Drive API service account key (you must add this)
```

## Customization

You can modify the following variables in the `dags/weather_etl.py` file:

- `city_name`: The city for which to fetch weather data (default: "Karachi")
- `SERVICE_ACCOUNT_FILE`: Path to your service account key file
- `FOLDER_ID`: Your Google Drive folder ID where files will be uploaded

## Troubleshooting

### Service Account Authentication Issues

If you encounter issues with Google Drive authentication, make sure:
1. Your service account key file is correctly placed and the path is properly set in the code
2. The service account has permission to access the specified Google Drive folder
3. The Google Drive API is enabled for your project

## License

This project is licensed under the MIT License - see the LICENSE file for details.
