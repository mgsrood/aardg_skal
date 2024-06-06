import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Import keys.env
load_dotenv()

# Define variables
api_url = os.getenv("MONTA_API_URL")
username = os.getenv("MONTA_USERNAME")
password = os.getenv("MONTA_PASSWORD")

def retrieve_order_ids(created_since, created_until, page, page_size=30):
    endpoint = f"orders?created_since={created_since}&created_until={created_until}&page={page}&page_size={page_size}"
    full_url = api_url + endpoint
    response = requests.get(full_url, auth=HTTPBasicAuth(username, password))
    
    if response.status_code == 200:
        order_data = response.json()
        return order_data
    else:
        print(f"Failed to retrieve orders: {response.status_code}")
        return None

def extract_order_ids(order_data):
    order_ids = []
    
    for order in order_data:
        order_id = order['WebshopOrderId']
        order_ids.append(order_id)
    
    return order_ids

if __name__ == "__main__":

    created_since = '2023-01-01'
    created_until = '2023-01-02'
    page_size = 30  # Maximum page size
    max_orders = 10000
    all_order_ids = []

    page = 0
    while len(all_order_ids) < max_orders:
        data = retrieve_order_ids(created_since, created_until, page, page_size)
        if data:
            order_ids = extract_order_ids(data)
            if not order_ids:
                break
            all_order_ids.extend(order_ids)
            if len(order_ids) < page_size:
                break
            page += 1
        else:
            break

    all_order_ids = all_order_ids[:max_orders]
    print(f"Total orders retrieved: {len(all_order_ids)}")
    print(all_order_ids)
