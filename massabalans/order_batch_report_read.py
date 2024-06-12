import zipfile
import shutil
import os
import pandas as pd
import re
import os
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials

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
original_file: str = "Verzonden en queued orders 2024-06-12.xlsx"
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

# Generate aggregated report
df_agg = df.groupby(['SKU', 'Omschrijving', 'Batch']).agg({'Aantal': 'sum'}).reset_index()

# Seperate DataFrame into products
df_probiotica = df_agg[df_agg['Omschrijving'] == 'Probiotica Ampullen 28x 9ml']
df_kombucha = df_agg[df_agg['Omschrijving'] == 'Kombucha Original 4x 1L']
df_bulk_kombucha = df_agg[df_agg['SKU'] == 'Bulk Kombucha']
df_waterkefir = df_agg[df_agg['Omschrijving'] == 'Waterkefir Original 4X 1L']
df_bulk_waterkefir = df_agg[df_agg['SKU'] == 'Bulk Waterkefir']
df_mix = df_agg[df_agg['Omschrijving'] == 'Mix Originals 4x 1L']
df_bloem = df_agg[df_agg['Omschrijving'] == 'Bloem Kombucha 12x 250ml']
df_bulk_bloem = df_agg[df_agg['SKU'] == 'Bulk Bloem']
df_citroen = df_agg[df_agg['Omschrijving'] == 'Citroen Kombucha 12x 250ml']
df_bulk_citroen = df_agg[df_agg['SKU'] == 'Bulk Verse Citroen']
df_gember = df_agg[df_agg['Omschrijving'] == 'Gember Limonade 12x 250ml']
df_bulk_gember = df_agg[df_agg['SKU'] == 'Bulk levende Gember']
df_frisdrank = df_agg[df_agg['Omschrijving'] == 'Frisdrank Mix 12x 250ml']
df_starter_box = df_agg[df_agg['Omschrijving'] == 'Starter Box']

# Initialize Google Sheets connection
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
gc_keys = os.getenv('AARDG_GOOGLE_CREDENTIALS', '')
creds = ServiceAccountCredentials.from_json_keyfile_name(gc_keys, scope)
client = gspread.authorize(creds)

# Open sheet
sheet = client.open('Massabalans Verkoop Monta')

# Write dataframes to sheet
dataframes = [df_probiotica, df_kombucha, df_bulk_kombucha, df_waterkefir, df_bulk_waterkefir, df_mix, df_bloem, df_bulk_bloem, df_citroen, df_bulk_citroen, df_gember, df_bulk_gember, df_frisdrank, df_starter_box]
sheet_names = ['Probiotica', 'Kombucha', 'Bulk Kombucha', 'Waterkefir', 'Bulk Waterkefir', 'Mix', 'Bloem', 'Bulk Bloem', 'Citroen', 'Bulk Citroen', 'Gember', 'Bulk Gember', 'Frisdrank', 'Starter Box']

# Write dataframes to sheet
for df, sheet_name in zip(dataframes, sheet_names):
    try:
        # Zoek het bestaande werkblad
        existing_worksheet = sheet.worksheet(sheet_name)
        # Vind de huidige gegevensbereik in het werkblad
        existing_data_range = existing_worksheet.get('A1').extend('table')
        # Verwijder de oude gegevens in het bereik
        existing_worksheet.batch_clear([existing_data_range])
        # Update de gegevens met de nieuwe dataframe
        existing_worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"{sheet_name} werkblad bijgewerkt.")
    except gspread.exceptions.WorksheetNotFound:
        # Voeg een nieuw werkblad toe als het niet bestaat
        worksheet = sheet.add_worksheet(title=sheet_name, rows=df.shape[0], cols=df.shape[1])
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"{sheet_name} werkblad toegevoegd.")

print("Dataframes zijn succesvol geüpload naar Google Sheets!")