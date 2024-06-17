import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

# Import keys.env
load_dotenv()

# Define variables
api_url = os.getenv("MONTA_API_URL", "")
username = os.getenv("MONTA_USERNAME", "")
password = os.getenv("MONTA_PASSWORD", "")

def retrieve_inboud():
    endpoint = 'inbounds?sinceid=1'
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