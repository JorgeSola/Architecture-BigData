import pandas as pd
import google.auth
from google.cloud import storage
import requests
import csv
import logging
import time
import os
import psycopg2
import argparse

LOG_LEVEL = logging.INFO
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=LOG_LEVEL)
logger = logging.getLogger(__name__)

def init_postgresql(config):
    logger.info(f'Starting connection with database...')
    global conn 
    conn = psycopg2.connect(
                host = config['postgresql_host'],
                database = config['postgresql_db'],
                user = config['postgresql_user'],
                password = config['postgresql_password'])

def check_tables_postgresql():
    global conn
    cursor = conn.cursor()
    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    result = [list(i) for i in cursor.fetchall()]
    
    return result

def create_table_process_postgresql(tables):
    global conn

    cursor = conn.cursor()
    
    if tables:

        logger.info(f'Creating temporal tables in BD....')

        sentence_1 = f"""create table public.airbnb_listings_temp( 	id int4 primary key not null, 	host_id int4 not null, 	host_name varchar(100), 	host_neighbourhood text, 	host_listings_count int4, 	city varchar(100), 	country varchar(50), 	property_type varchar(50), 	room_type varchar (50), 	price int4, 	security_deposit int4, 	cleaning_fee int4, 	number_of_reviews int4, 	review_scores_value int4, 	cancellation_policy varchar(30));"""
        cursor.execute(sentence_1)

        sentence_2 = f"""create table public.flat_information_temp( 	host_id int4 not null primary key, 	tv int4, 	cable_tv int4, 	kitchen int4, 	smoking_allowed int4, 	pets_allowed int4, 	heating int4, 	washer int4, 	dryer int4, 	checkin_24h int4);"""
        cursor.execute(sentence_2)

        sentence_3 = f"""create table public.airbnb_secondary_information_temp( host_id int4 not null primary key, street text, zipcode text, country_code varchar(4), latitude float, longitude float, bedrooms int4, beds int, weekly_price int4, monthly_price int4, minimum_nights int4, review_scores_accuracy int4, review_scores_cleanliness int4, review_scores_checkin int4, review_scores_communication int4, review_scores_location int4 );"""
        cursor.execute(sentence_3)

        conn.commit()
    
    else:
        logger.info(f'Creating new tables in BD....')

        sentence_1 = f"""create table public.airbnb_listings( 	id int4 primary key not null, 	host_id int4 not null, 	host_name varchar(100), 	host_neighbourhood text, 	host_listings_count int4, 	city varchar(100), 	country varchar(50), 	property_type varchar(50), 	room_type varchar (50), 	price int4, 	security_deposit int4, 	cleaning_fee int4, 	number_of_reviews int4, 	review_scores_value int4, 	cancellation_policy varchar(30));"""
        cursor.execute(sentence_1)

        sentence_2 = f"""create table public.flat_information( 	host_id int4 not null primary key, 	tv int4, 	cable_tv int4, 	kitchen int4, 	smoking_allowed int4, 	pets_allowed int4, 	heating int4, 	washer int4, 	dryer int4, 	checkin_24h int4);"""
        cursor.execute(sentence_2)

        sentence_3 = f"""create table public.airbnb_secondary_information( host_id int4 not null primary key, street text, zipcode text, country_code varchar(4), latitude float, longitude float, bedrooms int4, beds int, weekly_price int4, monthly_price int4, minimum_nights int4, review_scores_accuracy int4, review_scores_cleanliness int4, review_scores_checkin int4, review_scores_communication int4, review_scores_location int4 );"""
        cursor.execute(sentence_3)

        conn.commit()
       

def download_airbnb_dataset_csv():

    logger.info(f"Downloading airbnb dataset....")
    t0 = time.time()

    aribnb_dataset = pd.read_csv('https://public.opendatasoft.com/explore/dataset/airbnb-listings/download/?format=csv&disjunctive.host_verifications=true&disjunctive.amenities=true&disjunctive.features=true&q=Madrid&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B',
                        sep= ';', encoding = 'utf-8',error_bad_lines=False, index_col=False, dtype='unicode',low_memory=False)
    aribnb_dataset.to_csv(f'airbnb_listings.csv', index = False, sep = ';')

    logger.info(f"Airbnb dataset downloaded.")
    t1 = time.time()
    logger.info(f"Download time: {round(t1 - t0, 2)}")

def save_data_postgresql(data, table, tables):

    t0 = time.time()
    logger.info(f'Process {table} data....' )
    data = data.dropna(how='any',axis=0) 
    data = data.to_dict(orient = 'records')
    cursor = conn.cursor()
    columns = [str(column).lower().replace(' ','_') for column in data[0].keys()]
    columns =  ",".join(columns)
    if table == 'flat_information':
        columns = columns.replace('24-hour_check-in','checkin_24h')
    new_data = []
    for ix, row in enumerate(data):
        block = []
        block = [str(value).replace("'",'') for value in row.values()]
        block = str(block).replace('[','').replace(']','')
        new_data.append(block)
        if ix > 1 and ix % 5000 == 0:
            values = "),(".join(new_data)
            sentence = f"INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING" 
            cursor.execute(sentence)
            conn.commit()
            new_data = []

    logger.info(f'Total files processed: {ix}')
    values = "),(".join(new_data)
    if tables:
        sentence = f"INSERT INTO {table}_temp ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING" 
    else: 
        sentence = f"INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING" 
    
    cursor.execute(sentence)
    conn.commit()
    new_data = []
    logger.info(f'{table} finished')
    t1 = time.time()
    logger.info(f'Total time procese: {t1 - t0}')

    if tables:
        sentence_drop = f"""DROP TABLE {table}"""
        logger.info(f'{sentence_drop}')
        cursor.execute(sentence_drop)

        sentence_rename = f"""ALTER TABLE {table}_temp RENAME TO {table}"""
        logger.info(f"{sentence_rename}")
        cursor.execute(sentence_rename)

        conn.commit()
    
def create_csv_files(tables):

    logger.info(f"Getting the main information....")
    t0 = time.time()

    df = pd.read_csv('airbnb_listings.csv',sep= ';', encoding = 'utf-8',error_bad_lines=False, index_col=False, dtype='unicode',low_memory=False,  lineterminator='\n')

    airbnb_listings = df[['ID', 'Host ID', 'Host Name', 'Host Neighbourhood', 'Host Listings Count', 'City', 'Country', 'Property Type', 'Room Type', 'Price', 'Security Deposit', 'Cleaning Fee', 'Number of Reviews', 'Review Scores Value', 'Cancellation Policy']]
    airbnb_listings = airbnb_listings.drop_duplicates(keep='last')
    airbnb_listings.to_csv('airbnb_listings_mod.csv', index = False, sep = ';')
    save_data_postgresql(airbnb_listings, 'airbnb_listings', tables)

    flat_information = df[['Host ID','Amenities']]
    flat_information = flat_information.drop_duplicates(keep='last')
    flat_information = process_info(flat_information) 
    flat_information.to_csv('flat_information.csv', index = False, sep = ';')
    save_data_postgresql(flat_information,'flat_information',tables)


    airbnb_secondary_information = df[['Host ID','Street', 'Zipcode', 'Country Code', 'Latitude', 'Longitude', 'Bedrooms', 'Beds', 'Weekly Price', 'Monthly Price', 'Minimum Nights', 'Review Scores Accuracy', 'Review Scores Cleanliness', 'Review Scores Checkin', 'Review Scores Communication', 'Review Scores Location']]
    airbnb_secondary_information = airbnb_secondary_information.drop_duplicates(keep='last')
    airbnb_secondary_information.to_csv('airbnb_secondary_information.csv', index = False, sep = ';')
    save_data_postgresql(airbnb_secondary_information,'airbnb_secondary_information',tables)
    logger.info(f"Creating the news csv files....")


    logger.info(f"News csv files created.")
    t1= time.time()
    logger.info(f"Create csv files time: {round (t1 -t0, 2)}")

        
def upload_google_storage_connection():

    logger.info(f"Starting connection with Google Cloud Storage....")
    t0 = time.time()
    client_storage = storage.Client()

    bucket = client_storage.get_bucket('keepcoding_bucket')

    for file in ['airbnb_listings_mod.csv','flat_information.csv','airbnb_secondary_information.csv']:
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
    csv_files = ['flat_information.csv','airbnb_listings_mod.csv','airbnb_secondary_information.csv']
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
        os.remove('airbnb_listings_mod.csv')
        os.remove('flat_information.csv')
        os.remove('airbnb_secondary_information.csv')
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
    
    return df

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

    t0 = time.time()
    init_postgresql(config)
    tables = check_tables_postgresql()
    create_table_process_postgresql(tables)
    download_airbnb_dataset_csv()
    create_csv_files(tables)
    check_old_files_google_storage()
    upload_google_storage_connection()
    delete_local_csv()
    conn.close()

    logger.info(f'Connection with database closed.')
    t1 = time.time()
    logger.info(f'TOTAL PROCESS TIME: {t1 - t0}')

if __name__ == "__main__": 
    main()
    