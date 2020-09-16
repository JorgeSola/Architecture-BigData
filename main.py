import pandas as pd
import google.auth
from google.cloud import storage
import requests
import csv
import logging
import time
import os
from process_info import process_info
from os import listdir

LOG_LEVEL = logging.INFO
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=LOG_LEVEL)
logger = logging.getLogger(__name__)

def download_airbnb_dataset_csv():

    logger.info(f"Downloading airbnb dataset....")
    t0 = time.time()

    aribnb_dataset = pd.read_csv('https://public.opendatasoft.com/explore/dataset/airbnb-listings/download/?format=csv&disjunctive.host_verifications=true&disjunctive.amenities=true&disjunctive.features=true&q=Madrid&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B',
                        sep= ';', encoding = 'utf-8',error_bad_lines=False, index_col=False, dtype='unicode',low_memory=False)
    aribnb_dataset.to_csv('airbnb_listings.csv', index = False, sep = ';')

    logger.info(f"Airbnb dataset downloaded.")
    t1 = time.time()
    logger.info(f"Download time: {round(t1 - t0, 2)}")

def create_csv_files():

    logger.info(f"Getting the main information....")
    t0 = time.time()

    df = pd.read_csv('airbnb_listings.csv',sep= ';', encoding = 'utf-8',error_bad_lines=False, index_col=False, dtype='unicode',low_memory=False,  lineterminator='\n')

    listening_info = df[['ID', 'Host ID', 'Host Name', 'Host Neighbourhood', 'Host Listings Count', 'City', 'Country', 'Property Type', 'Room Type', 'Price', 'Security Deposit', 'Cleaning Fee', 'Number of Reviews', 'Review Scores Value', 'Cancellation Policy']]
    flat_info = df[['Host ID','Amenities']]
    secondary_info = df[['Street', 'Zipcode', 'Country Code', 'Latitude', 'Longitude', 'Bedrooms', 'Beds', 'Weekly Price', 'Monthly Price', 'Minimum Nights', 'Review Scores Accuracy', 'Review Scores Cleanliness', 'Review Scores Checkin', 'Review Scores Communication', 'Review Scores Location']]
    
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

def main():
    download_airbnb_dataset_csv()
    create_csv_files()
    check_old_files_google_storage()
    upload_google_storage_connection()
    delete_local_csv()

if __name__ == "__main__":
    main()
    