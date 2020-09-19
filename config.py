from google.oauth2 import service_account
from google.cloud import storage
import os

cred = service_account.Credentials.from_service_account_file('C:\\Users\\Usuario\\Desktop\\KeepCoding\\Bootcamp\\Big Data Architecture\\Práctica\\etl_airbnb\\Architecture BigData -807ee48061e5.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:\\Users\\Usuario\\Desktop\\KeepCoding\\Bootcamp\\Big Data Architecture\\Práctica\\etl_airbnb\\Architecture BigData -807ee48061e5.json'
scoped_credentials = cred.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])

client_storage = storage.Client(credentials=cred)

