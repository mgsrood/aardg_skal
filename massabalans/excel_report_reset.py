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

def remove_styles_from_xlsx(original_report: str, cleaned_report: str) -> None:
    # Make a temporary directory
    temp_dir = 'temp_excel_extracted'
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    # Take the contents of the original Excel file
    with zipfile.ZipFile(original_report, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
    
    # Remove the styles
    styles_path = os.path.join(temp_dir, 'xl', 'styles.xml')
    if os.path.exists(styles_path):
        os.remove(styles_path)
    
    # Paste the contents of the temporary directory back into the cleaned report
    shutil.make_archive('cleaned_excel', 'zip', temp_dir)
    shutil.move('cleaned_excel.zip', cleaned_report)
    
    # Remove the temporary directory
    shutil.rmtree(temp_dir)

# Path to the original file
report_folder: str = os.getenv("CSV_MAIN_PATH", "")
original_file: str = "verzonden_en_queued_orders_2024-06-11.xlsx"
original_report: str = report_folder + original_file

# Date extension
date_extension = re.search(r'\d{4}-\d{2}-\d{2}', original_report)

# Use date extension to create cleaned report and pandas DataFrame
if date_extension:
    cleaned_report = f'/Users/maxrood/werk/codering/aardg/projecten/skal/massabalans/cleaned_report_{date_extension.group()}.xlsx'
    # Remove styles from xlsx
    remove_styles_from_xlsx(original_report, cleaned_report)

    # Read the cleaned file with pandas
    try:
        df = pd.read_excel(cleaned_report, engine='openpyxl')
# Functie om Excel datum om te zetten naar normale datum
        def excel_date(excel_date):
            return pd.to_datetime('1899-12-30') + pd.to_timedelta(excel_date, 'D')

        # Pas de functie toe op de relevante kolommen
        df['Besteldatum'] = df['Besteldatum'].apply(excel_date).dt.strftime('%Y-%m-%d')
        df['Verzenddatum'] = df['Verzenddatum'].apply(excel_date).dt.strftime('%Y-%m-%d')
        df['THT Datum'] = df['THT Datum'].apply(excel_date).dt.strftime('%Y-%m-%d')
        
    except Exception as e:
        print(f"Er is een fout opgetreden: {e}")
else:
    print("Geen datum gevonden in het bestandspad.")

# Limit to desired columns
desired_columns = ['OrderNummer', 'Besteldatum', 'Verzenddatum', 'SKU', 'Omschrijving', 'Aantal', 'Batch', 'THT Datum', 'Orderstatus']
df = df[desired_columns]
df['THT_Datum'] = df['THT Datum'].fillna('')
df = df.drop(columns=['THT Datum'])

# Remove duplicates
duplicates = df[df.duplicated(subset=['OrderNummer', 'Besteldatum', 'Verzenddatum', 'SKU', 'Omschrijving', 'Batch', 'THT_Datum', 'Orderstatus'], keep=False)]
duplicates_grouped = duplicates.groupby(['OrderNummer', 'Besteldatum', 'Verzenddatum', 'SKU', 'Omschrijving', 'Batch', 'THT_Datum', 'Orderstatus']).agg({'Aantal': 'sum'}).reset_index()
df_cleaned = df.drop_duplicates(subset=['OrderNummer', 'Besteldatum', 'Verzenddatum', 'SKU', 'Omschrijving', 'Batch', 'THT_Datum', 'Orderstatus'], keep=False)
merged_df = pd.concat([df_cleaned, duplicates_grouped], ignore_index=True)

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

# Write the data to the main table
pandas_gbq.to_gbq(merged_df, full_table_id, project_id=project_id, if_exists='replace')