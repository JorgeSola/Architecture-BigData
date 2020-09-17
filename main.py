import pandas as pd
import google.auth
from google.cloud import storage
import requests
import csv
import logging
import time
import os
from os import listdir
import psycopg2
import argparse

LOG_LEVEL = logging.INFO
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=LOG_LEVEL)
logger = logging.getLogger(__name__)

def init_postgresql(config):
    global conn 
    conn = psycopg2.connect(
                host = config['postgresql_host'],
                database = config['postgresql_db'],
                user = config['postgresql_user'],
                password = config['postgresql_password'])

def download_airbnb_dataset_csv():

    logger.info(f"Downloading airbnb dataset....")
    t0 = time.time()

    aribnb_dataset = pd.read_csv('https://public.opendatasoft.com/explore/dataset/airbnb-listings/download/?format=csv&disjunctive.host_verifications=true&disjunctive.amenities=true&disjunctive.features=true&q=Madrid&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B',
                        sep= ';', encoding = 'utf-8',error_bad_lines=False, index_col=False, dtype='unicode',low_memory=False)
    aribnb_dataset.to_csv(f'airbnb_listings.csv', index = False, sep = ';')

    logger.info(f"Airbnb dataset downloaded.")
    t1 = time.time()
    logger.info(f"Download time: {round(t1 - t0, 2)}")

def create_csv_files():

    logger.info(f"Getting the main information....")
    t0 = time.time()

    df = pd.read_csv('airbnb_listings.csv',sep= ';', encoding = 'utf-8',error_bad_lines=False, index_col=False, dtype='unicode',low_memory=False,  lineterminator='\n')

    listening_info = df[['ID', 'Host ID', 'Host Name', 'Host Neighbourhood', 'Host Listings Count', 'City', 'Country', 'Property Type', 'Room Type', 'Price', 'Security Deposit', 'Cleaning Fee', 'Number of Reviews', 'Review Scores Value', 'Cancellation Policy']]
    listening_info = listening_info.drop_duplicates(keep='last')
    flat_info = df[['Host ID','Amenities']]
    flat_info = flat_info.drop_duplicates(keep='last')
    secondary_info = df[['Host ID','Street', 'Zipcode', 'Country Code', 'Latitude', 'Longitude', 'Bedrooms', 'Beds', 'Weekly Price', 'Monthly Price', 'Minimum Nights', 'Review Scores Accuracy', 'Review Scores Cleanliness', 'Review Scores Checkin', 'Review Scores Communication', 'Review Scores Location']]
    
    logger.info(f"Creating the news csv files....")

    listening_info.to_csv('listening_info.csv', index = False, sep = ';')
    process_info(flat_info)
    secondary_info.to_csv('secondary_info.csv', index = False, sep = ';')

    logger.info(f"News csv files created.")
    t1= time.time()
    logger.info(f"Create csv files time: {round (t1 -t0, 2)}")

        
def upload_google_storage_connection():

    logger.info(f"Starting connection with Google Cloud Storage....")
    t0 = time.time()
    client_storage = storage.Client()

    bucket = client_storage.get_bucket('keepcoding_bucket')

    for file in ['listening_info.csv','flat_info.csv','secondary_info.csv']:
        blob = bucket.blob(f'{file}')
        blob.upload_from_string('this is test content!')
        blob.upload_from_filename(filename=f'{file}')

        with open(f'{file}', 'rb') as filename:
            logger.info(f"Saving the new {file} files....")
            blob.upload_from_file(filename)

        blob.make_public()
        url = blob.public_url
        logger.info(f"Csv files saved in google cloud storage.")
        logger.info(f"{url}")
    t1 = time.time()
    logger.info(f"Upload google storage process time: {round(t1 - t0, 2)}")

def check_old_files_google_storage():
    
    t0 = time.time()
    csv_files = ['flat_info.csv','listening_info.csv','secondary_info.csv']
    logger.info(f"Checking old .csv in the bucket....")
    client_storage = storage.Client()

    bucket = client_storage.bucket('keepcoding_bucket')

    for blob_name in csv_files:
        try:
            blob = bucket.blob(blob_name)
            blob.delete()
        except:
            pass
    
    t1 = time.time()
    logger.info(f"Checking finished.")
    logger.info(f"Total time checking process: {round(t1 -t0, 2)}")

def delete_local_csv():
    try:
        os.remove('airbnb_listings.csv')
        os.remove('listening_info.csv')
        os.remove('flat_info.csv')
        os.remove('secondary_info.csv')
    except:
        print(f'There are not old files')

def process_info(df):

    dataset = df.to_dict(orient = 'records')

    new_data = []
    for row in dataset:
        dictionary = {}
        dictionary['Host ID'] = row['Host ID']
        row = str(row['Amenities']).split(',')
        
        if 'TV' in row:
            dictionary['TV'] = 1
        else:
            dictionary['TV'] = 0
        if 'Cable TV' in row:
            dictionary['Cable TV'] = 1
        else:
            dictionary['Cable TV'] = 0
        if 'Kitchen' in row:
            dictionary['Kitchen'] = 1
        else:
            dictionary['Kitchen'] = 0
        if 'Smoking allowed' in row:
            dictionary['Smoking allowed'] = 1
        else:
            dictionary['Smoking allowed'] = 0
        if 'Pets allowed' in row:
            dictionary['Pets allowed'] = 1
        else:
            dictionary['Pets allowed'] = 0
        if 'Heating' in row:
            dictionary['Heating'] = 1
        else:
            dictionary['Heating'] = 0
        if 'Washer' in row:
            dictionary['Washer'] = 1
        else:
            dictionary['Washer'] = 0
        if 'Dryer' in row:
            dictionary['Washer'] = 1
        else:
            dictionary['Washer'] = 0
        if 'Dryer' in row:
            dictionary['Dryer'] = 1 
        else:
            dictionary['Dryer'] = 0
        if '24-hour check-in' in row:
            dictionary['24-hour check-in'] = 1
        else:
            dictionary['24-hour check-in'] = 0

        new_data.append(dictionary)

    df = pd.DataFrame(new_data)
    df = df.drop_duplicates(keep='last')
    df.to_csv('flat_info.csv', index = False, sep = ';')

def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--postgresql-host', help='', required=True)
    parser.add_argument('--postgresql-user', help='', required=True)
    parser.add_argument('--postgresql-password', help='', required=True)
    parser.add_argument('--postgresql-db', help='', required=True)

    args = parser.parse_args()
    logging.info(f"Arguments received: {args}")

    config = {'postgresql_host': args.postgresql_host,
              'postgresql_user': args.postgresql_user,
              'postgresql_password': args.postgresql_password,
              'postgresql_db': args.postgresql_db}

    init_postgresql(config)
    download_airbnb_dataset_csv()
    create_csv_files()
    check_old_files_google_storage()
    upload_google_storage_connection()
    delete_local_csv()

if __name__ == "__main__":
 
    main()
    