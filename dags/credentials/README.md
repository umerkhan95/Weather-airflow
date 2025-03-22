# Google Drive API Credentials Setup

This guide will help you set up Google Drive API credentials for the Weather App to upload data files to Google Drive.

## Steps to Create Google Drive Service Account

1. **Go to Google Cloud Console**
   - Visit [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing project

2. **Enable Google Drive API**
   - In the left sidebar, navigate to "APIs & Services" > "Library"
   - Search for "Google Drive API" and select it
   - Click "Enable" to enable the API for your project

3. **Create Service Account**
   - In the left sidebar, navigate to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "Service Account"
   - Fill in the service account details (e.g., name: "weatherapp-service")
   - Grant the service account the "Editor" role
   - Click "Done"

4. **Create Service Account Key**
   - From the service accounts list, find the one you just created
   - Click on the three dots menu (actions) > "Manage keys"
   - Click "Add Key" > "Create new key"
   - Choose "JSON" format and click "Create"
   - The key file will be downloaded automatically to your computer

5. **Share Google Drive Folder**
   - Create a folder in your Google Drive where you want to store the weather data
   - Right-click on the folder and click "Share"
   - Enter the service account email (e.g., `weatherapp-service@your-project-id.iam.gserviceaccount.com`)
   - Set permission to "Editor" and click "Share"

6. **Add Credentials to Airflow**
   - Rename the downloaded JSON key file to `google_drive_credentials.json`
   - Place it in this directory (`/opt/airflow/dags/credentials/` inside the container)
   - Make sure the file is readable by Airflow (chmod 644)

## Security Considerations

- Keep your credentials file secure and never commit it to version control
- Consider using Airflow's Variable or Connection features for production environments
- Regularly rotate your service account keys for enhanced security

With these credentials in place, the Weather App will now be able to upload both JSON and TXT files to your Google Drive automatically every 2 hours.
