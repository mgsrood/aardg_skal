import pandas_gbq as pd_gbq
import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from datetime import datetime, timedelta
from google.cloud import bigquery
from tabulate import tabulate
import matplotlib.pyplot as plt

# Load .env
load_dotenv()

# Define starting and end date
first_day_of_week = datetime.now() - timedelta(days=datetime.now().weekday())
last_day_of_week = first_day_of_week + timedelta(days=6)

# Load possible environment dates
start_date = os.getenv("START_DATE", first_day_of_week.strftime("%Y-%m-%d"))
print(start_date)
end_date = os.getenv("END_DATE", last_day_of_week.strftime("%Y-%m-%d"))
print(end_date)

# Get the GCP keys
gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

credentials = service_account.Credentials.from_service_account_file(gc_keys)
project_id = credentials.project_id
client = bigquery.Client(credentials=credentials, project=project_id)

# Define variables
dataset_id = os.getenv("STOCK_DATASET_ID")
table_id = os.getenv("STOCK_TABLE_ID")
full_table_id = f'{project_id}.{dataset_id}.{table_id}'

# Get the starting stock from BigQuery
query_1 = f"""
SELECT
    Product,
    Batch,
    Aantal
FROM
    `{full_table_id}`
WHERE DATE(Timestamp) = '{start_date}'
"""

# Get the end stock from BigQuery
query_2 = f"""
SELECT
    Product,
    Batch,
    Aantal
FROM
    `{full_table_id}`
WHERE DATE(Timestamp) = '{end_date}'
"""

starting_stock = pd_gbq.read_gbq(query_1, credentials=credentials, project_id=project_id)
end_stock = pd_gbq.read_gbq(query_2, credentials=credentials, project_id=project_id)

# Function to fill missing Product - Batch combinations
def fill_missing_combinations(df1, df2):
    products_df1 = set(zip(df1['Product'], df1['Batch']))
    products_df2 = set(zip(df2['Product'], df2['Batch']))
    
    missing_in_df1 = products_df2 - products_df1
    missing_in_df2 = products_df1 - products_df2
    
    # Voeg ontbrekende combinaties toe met Aantal = 0
    for product, batch in missing_in_df1:
        df1 = df1._append({'Product': product, 'Batch': batch, 'Aantal': 0}, ignore_index=True)
        
    for product, batch in missing_in_df2:
        df2 = df2._append({'Product': product, 'Batch': batch, 'Aantal': 0}, ignore_index=True)
        
    return df1, df2

# Fill missing Product - Batch combinations
starting_stock, end_stock = fill_missing_combinations(starting_stock, end_stock)

# Calculate the mutations
mutation = end_stock.merge(starting_stock, on=['Product', 'Batch'], suffixes=('_end', '_start'))
mutation['Aantal'] = mutation['Aantal_end'] - mutation['Aantal_start']

'''# Create a function for making a table
def create_table(df, title):
    headers = ['Product', 'Batch', 'Aantal']
    table = tabulate(df[['Product', 'Batch', 'Aantal']], headers=headers, showindex=False, tablefmt='grid')
    return f"\n{title}\n\n{table}\n"

# Create the tables
starting_stock_table = create_table(starting_stock, "Starting Stock (Min Date: 2024-06-27)")
end_stock_table = create_table(end_stock, "End Stock (Max Date: 2024-06-28)")
mutation_table = create_table(mutation[['Product', 'Batch', 'Aantal']], "Mutation (End - Start)")

print(starting_stock_table)
print(end_stock_table)
print(mutation_table)'''

# Function to create table plots
def create_table_plot(df, title, ax):
    ax.axis('tight')
    ax.axis('off')
    table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.2)
    ax.set_title(title, pad=20)

# Create plots
fig, axs = plt.subplots(3, 1, figsize=(10, 15))

create_table_plot(starting_stock[['Product', 'Batch', 'Aantal']], f"Begin Vooraad: {start_date})", axs[0])
create_table_plot(mutation[['Product', 'Batch', 'Aantal']], "Mutatie", axs[1])
create_table_plot(end_stock[['Product', 'Batch', 'Aantal']], f"Eind Vooraad: {end_date})", axs[2])

# Save the figure
plt.tight_layout()
plt.savefig("/Users/maxrood/werk/codering/aardg/projecten/skal/massabalans/stock_tables.jpeg")
plt.show()