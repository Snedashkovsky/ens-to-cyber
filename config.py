from google.cloud import bigquery, bigquery_storage
from google.oauth2 import service_account
from dotenv import dotenv_values


ETH_URL = dotenv_values(".env")['ETH_URL']

MN = dotenv_values(".env")['MN']

NODE_URL = 'https://rpc.space-pussy.cybernode.ai:443'
LCD_URL = 'https://lcd.space-pussy.cybernode.ai/'
NETWORK = 'space-pussy'

# Construct a BigQuery client object.
KEY_PATH = "bigquery_project.json"
credentials = service_account.Credentials.from_service_account_file(
    KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
bqstorage_client = bigquery_storage.BigQueryReadClient(credentials=credentials)
