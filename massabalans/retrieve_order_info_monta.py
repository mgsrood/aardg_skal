import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import os
import pandas_gbq
from retrieve_order_ids_monta import retrieve_order_ids, extract_order_ids
from dotenv import load_dotenv
import os
from google.oauth2 import service_account

# Import keys.env
load_dotenv()

# Define variables
api_url = os.getenv("MONTA_API_URL")
username = os.getenv("MONTA_USERNAME")
password = os.getenv("MONTA_PASSWORD")

# Product dictionary
sku_to_product_name = {
    '8719326399355': 'Citroen Kombucha 12x 250ml',
    '8719326399362': 'Bloem Kombucha 12x 250ml',
    '8719326399379': 'Gember Limonade 12x 250ml',
    '8719326399386': 'Kombucha Original 4x 1L',
    '8719326399393': 'Waterkefir Original 4x 1L',
    '8719327215111': 'Starter Box',
    '8719327215128': 'Frisdrank Mix 12x 250ml',
    '8719327215135': 'Mix Originals 4x 1L',
    '8719327215159': 'EAN 30 dagen challenge kalender',
    '8719327215166': 'EAN Moederdag Kaart',
    '8719327215173': 'Magnetische Flesopener voor Koelkast',
    '8719327215180': 'Probiotica Ampullen 28x 9ml',
    '8719327215197': 'Verjaardagskalender'
}

def retrieve_order_data(order_id):
    endpoint = f"order/{order_id}"
    full_url = api_url + endpoint
    response = requests.get(full_url, auth=HTTPBasicAuth(username, password))
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve order {order_id}: {response.status_code}")
        return None

def retrieve_order_batches(order_id):
    endpoint = f"order/{order_id}/batches"
    full_url = api_url + endpoint
    response = requests.get(full_url, auth=HTTPBasicAuth(username, password))
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve batches for order {order_id}: {response.status_code}")
        return None

def create_order_dataframe(order_ids):
    order_data = []
    
    for idx, order_id in enumerate(order_ids):
        print(f"Processing order {idx+1} of {len(order_ids)}")
        order = retrieve_order_data(order_id)
        if order:
            batches = retrieve_order_batches(order_id)
            if batches:
                for batch in batches['m_Item3']:
                    sku = batch['sku']
                    product_name = sku_to_product_name.get(sku, 'Unknown')
                    if product_name != 'Unknown':
                        batch_info = {
                            'order_id': order['WebshopOrderId'],
                            'first_name': order['ConsumerDetails']['DeliveryAddress']['FirstName'],
                            'last_name': order['ConsumerDetails']['DeliveryAddress']['LastName'],
                            'email': order['ConsumerDetails']['DeliveryAddress']['EmailAddress'],
                            'street': order['ConsumerDetails']['DeliveryAddress']['Street'],
                            'house_number': order['ConsumerDetails']['DeliveryAddress']['HouseNumber'],
                            'house_number_addition': order['ConsumerDetails']['DeliveryAddress']['HouseNumberAddition'],
                            'postal_code': order['ConsumerDetails']['DeliveryAddress']['PostalCode'],
                            'city': order['ConsumerDetails']['DeliveryAddress']['City'],
                            'country': order['ConsumerDetails']['DeliveryAddress']['CountryCode'],
                            'ordered': order['Received'],
                            'shipped': order.get('Shipped'),
                            'sku': sku,
                            'quantity': abs(batch['quantity']),
                            'batch_title': batch['batch']['title'],
                            'batch_bestbeforedate': batch['batch']['bestbeforedate'],
                            'product_name': product_name
                        }
                        order_data.append(batch_info)
    
    df = pd.DataFrame(order_data)
    return df

def transfer_data_to_bigquery(df):

    # Define variables
    project_id = os.getenv("MONTA_PROJECT_ID")
    dataset_id = os.getenv("MONTA_DATASET_ID")
    table_id = os.getenv("MONTA_TABLE_ID")
    full_table_id = f'{project_id}.{dataset_id}.{table_id}'

    # Get the GCP keys
    gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

    credentials = service_account.Credentials.from_service_account_file(gc_keys)
    project_id = credentials.project_id
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Insert data into newly made table
    pandas_gbq.to_gbq(df, full_table_id, project_id=project_id, if_exists='append')

    print(f"Data is succesvol geüpload naar {full_table_id}.")

if __name__ == "__main__":

    # Define the order ids
    created_since = '2023-04-01'
    created_until = '2023-05-01'
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

    # Load data into bigquery
    df = create_order_dataframe(all_order_ids)
    transfer_data_to_bigquery(df)
