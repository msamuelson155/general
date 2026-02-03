import pandas as pd
from sqlalchemy import create_engine
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import io

# 1. Google Drive Authentication
# PyDrive2 looks for 'client_secrets.json' in this folder automatically
gauth = GoogleAuth()
gauth.LocalWebserverAuth() 
drive = GoogleDrive(gauth)

# 2. Database Connection
# Replace 'YOUR_PASSWORD' with your actual PostgreSQL password
db_url = 'postgresql://postgres:Magnus15@localhost:5432/general'
engine = create_engine(db_url)

# 3. Google Drive Folder Configuration
folder_id = '1x4BZPQb43zRObJaGo4wtDls_y2IEIXXO'
query = f"'{folder_id}' in parents and trashed=false"
file_list = drive.ListFile({'q': query}).GetList()

# 4. Process each file in the folder
for file in file_list:
    # Handle CSVs and Google Sheets (Sheets are exported as CSV)
    if file['mimeType'] in ['text/csv', 'application/vnd.google-apps.spreadsheet']:
        print(f"Reading: {file['title']}...")
        
        # Get content as a string and load into Pandas
        # Note: For Google Sheets, we must use the export method
        if file['mimeType'] == 'application/vnd.google-apps.spreadsheet':
            content = file.GetContentString(mimetype='text/csv')
        else:
            content = file.GetContentString()
            
        df = pd.read_csv(io.StringIO(content))
        
        # Create a safe table name (lowercase, no spaces)
        table_name = file['title'].replace(' ', '_').lower().replace('.csv', '')
        
        # Upload to PostgreSQL 'general' schema
        # if_exists='replace' creates the table for you
        df.to_sql(
            name=table_name, 
            con=engine, 
            schema='dbo', 
            if_exists='replace', 
            index=False
        )
        print(f"Successfully loaded '{table_name}' into dbo.general.")

print("\nAll files from the folder have been synced to DataGrip!")