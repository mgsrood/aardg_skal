import zipfile
import shutil
import os
import pandas as pd
import re
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery

# Load .env
load_dotenv()

# Path to the original file
report_folder: str = os.getenv("CSV_MAIN_PATH", "")
original_file: str = "report_file.csv"
original_report: str = os.path.join(report_folder, original_file)

# Read the CSV file with pandas
try:
    df = pd.read_csv(original_report)
    # Function to turn Excel date into normal date
    def excel_date(excel_date):
        return pd.to_datetime('1899-12-30') + pd.to_timedelta(excel_date, 'D')

    # Pas de functie toe op de relevante kolommen
    df['BestelDatum'] = df['BestelDatum'].apply(excel_date).dt.strftime('%Y-%m-%d')
    df['Verzenddatum'] = df['Verzenddatum'].apply(excel_date).dt.strftime('%Y-%m-%d')
    df['ThtDatum'] = df['ThtDatum'].apply(excel_date).dt.strftime('%Y-%m-%d')
    
except Exception as e:
    print(f"Er is een fout opgetreden bij het inlezen van het CSV-bestand: {e}")

# Limit to desired columns
desired_columns = ['OrderNummer', 'BestelDatum', 'Verzenddatum', 'Sku', 'Omschrijving', 'Aantal', 'Batch', 'ThtDatum', 'OrderStatus']
df = df[desired_columns]
df['THT_Datum'] = df['ThtDatum'].fillna('')
df = df.drop(columns=['ThtDatum'])
df['SKU'] = df['Sku']
df = df.drop(columns=['Sku'])
df['Orderstatus'] = df['OrderStatus']
df = df.drop(columns=['OrderStatus'])
df['Besteldatum'] = df['BestelDatum']
df = df.drop(columns=['BestelDatum'])

# Remove duplicates
duplicates = df[df.duplicated(subset=['OrderNummer', 'Besteldatum', 'Verzenddatum', 'SKU', 'Omschrijving', 'Batch', 'THT_Datum', 'Orderstatus'], keep=False)]
duplicates_grouped = duplicates.groupby(['OrderNummer', 'Besteldatum', 'Verzenddatum', 'SKU', 'Omschrijving', 'Batch', 'THT_Datum', 'Orderstatus']).agg({'Aantal': 'sum'}).reset_index()
df_cleaned = df.drop_duplicates(subset=['OrderNummer', 'Besteldatum', 'Verzenddatum', 'SKU', 'Omschrijving', 'Batch', 'THT_Datum', 'Orderstatus'], keep=False)
merged_df = pd.concat([df_cleaned, duplicates_grouped], ignore_index=True)

# Print Merged DataFrame
print(merged_df)

# Write to BigQuery
project_id: str = os.getenv("MONTA_PROJECT_ID", "")
dataset_id: str = os.getenv("MONTA_DATASET_ID", "")
table_id: str = os.getenv("MONTA_TABLE_ID", "")
full_table_id: str = f'{project_id}.{dataset_id}.{table_id}'

# Get the GCP keys
gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

credentials = service_account.Credentials.from_service_account_file(gc_keys)
project_id = credentials.project_id
client = bigquery.Client(credentials=credentials, project=project_id)

# Write the data to a temporary table
temp_table_id = f'{dataset_id}.temp_table'
pandas_gbq.to_gbq(merged_df, temp_table_id, project_id=project_id, if_exists='replace')

# Apply a MERGE query to the temporary and original table
merge_query = f"""
MERGE INTO `{full_table_id}` AS target
USING `{temp_table_id}` AS source
ON target.OrderNummer = source.OrderNummer AND target.SKU = source.SKU AND target.Batch = source.Batch AND target.THT_Datum = source.THT_Datum
WHEN MATCHED THEN
  UPDATE SET target.Verzenddatum = source.Verzenddatum,
             target.Orderstatus = source.Orderstatus,
             target.Aantal = source.Aantal
WHEN NOT MATCHED THEN
  INSERT (OrderNummer, Besteldatum, Verzenddatum, SKU, Omschrijving, Aantal, Batch, THT_Datum, Orderstatus)
  VALUES (source.OrderNummer, source.Besteldatum, source.Verzenddatum, source.SKU, source.Omschrijving, source.Aantal, source.Batch, source.THT_Datum, source.Orderstatus);
"""

# Execute the query
query_job = client.query(merge_query)

# Wait till the query is finished
query_job_result = query_job.result()

# Remove the temporary table
try:
    client.delete_table(temp_table_id)
    print("Tijdelijke tabel succesvol verwijderd.")
except Exception as e:
    print(f"Fout bij het verwijderen van de tijdelijke tabel: {e}")