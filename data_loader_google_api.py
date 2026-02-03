import pandas as pd
from sqlalchemy import create_engine
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import io

#google Drive Authentication - PyDrive2 looks for 'client_secrets.json' in this folder
gauth = GoogleAuth()
gauth.LocalWebserverAuth() 
drive = GoogleDrive(gauth)

#database Connection
db_url = 'postgresql://postgres:Magnus15@localhost:5432/general'
engine = create_engine(db_url)

#target google drive folder
folder_id = '1x4BZPQb43zRObJaGo4wtDls_y2IEIXXO'
query = f"'{folder_id}' in parents and trashed=false"
file_list = drive.ListFile({'q': query}).GetList()

#iterate through each file in folder
for file in file_list:
    #sheets export as CSVs
    if file['mimeType'] in ['text/csv', 'application/vnd.google-apps.spreadsheet']:
        print(f"Reading: {file['title']}...")
        
        #get content as string and load into pandas. use export method for google sheets
        if file['mimeType'] == 'application/vnd.google-apps.spreadsheet':
            content = file.GetContentString(mimetype='text/csv')
        else:
            content = file.GetContentString()
            
        df = pd.read_csv(io.StringIO(content))
        
        table_name = file['title'].replace(' ', '_').lower().replace('.csv', '')
        
        #upload to PostgreSQL
        df.to_sql(
            name=table_name, 
            con=engine, 
            schema='dbo', 
            if_exists='replace',  #creates the tables
            index=False
        )
        print(f"Successfully loaded '{table_name}' into schema.")

print("\nAll files from the folder have been loaded to DataGrip")