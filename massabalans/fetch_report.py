import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from typing import List, Optional

# Import keys.env
load_dotenv()

# Define variables
api_url: str = os.getenv("MONTA_API_URL", "")
username: str = os.getenv("MONTA_USERNAME", "")
password: str = os.getenv("MONTA_PASSWORD", "")

if api_url is None or username is None or password is None:
    raise ValueError("API URL, username, or password environment variables are not set.")

def fetch_report_details(created_after: str):
    endpoint = f"reports?createdAfter={created_after}"
    full_url = api_url + endpoint
    response = requests.get(full_url, auth=HTTPBasicAuth(username, password))
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch report: {response.status_code} - {response.text}")
        return None

def fetch_report(report_id: str):
    endpoint = f"reports/{report_id}/file"
    full_url = api_url + endpoint
    response = requests.get(full_url, auth=HTTPBasicAuth(username, password))
    
    if response.status_code == 200:
        return response.content
    else:
        print(f"Failed to fetch report: {response.status_code} - {response.text}")
        return None

# Generate report details
created_after = "2023-01-01T00:48:45.107"
report_details = fetch_report_details(created_after)
print(report_details)

# Extract report ID
if report_details:
    report_id = report_details[0]['Id']
    print("Report ID:", report_id)

    # Use report ID to retrieve report
    report_content = fetch_report(report_id)
    
    if report_content:
        # Save the report content to a file
        with open('report_file.csv', 'wb') as file:
            file.write(report_content)
        print("Report has been saved to 'report_file.csv'.")
    else:
        print("Failed to download the report.")
else:
    print("No report found.")