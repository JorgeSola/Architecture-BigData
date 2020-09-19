from google.oauth2 import service_account
from google.cloud import storage
import os

cred = service_account.Credentials.from_service_account_file('path\key.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'path\key.json'
scoped_credentials = cred.with_scopes(['https://www.googleapis.com/auth/cloud-platform'])

client_storage = storage.Client(credentials=cred)

