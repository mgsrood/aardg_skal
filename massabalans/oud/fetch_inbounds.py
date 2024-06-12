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

def retrieve_inboud():
    endpoint = 'inbounds'
    full_url = api_url + endpoint
    print(full_url)
    response = requests.get(full_url, auth=HTTPBasicAuth(username, password))
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve inbound information: {response.status_code} - {response.text}")
        return None

if __name__ == "__main__":
    print(retrieve_inboud())