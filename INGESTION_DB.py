import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine=create_engine('sqlite:///inventory.db')    
# //name of database is inventory.db
def ingest_db(df,table_name,engine):
    # this function will ingest the dataframe into database table
    df.to_sql(table_name,con=engine,if_exists='replace',index=False)
def load_raw_data():
    # this function will load the CSVs as dataframes and ingest into db
    start=time.time()
    for file in os.listdir('D:\\New folder\\VendorDataAnalyseP3\\data'):
        if file.endswith('.csv'):
            file_path = os.path.join('D:\\New folder\\VendorDataAnalyseP3\\data', file)
            df = pd.read_csv(file_path)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)
    end=time.time()
    total_time=(end-start)/60
    logging.info(f'\nTotal time taken to ingest data: {total_time} minutes')
if __name__=='__main__':
    load_raw_data()