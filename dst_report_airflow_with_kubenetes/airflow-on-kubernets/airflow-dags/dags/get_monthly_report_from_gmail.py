from airflow.models import DAG
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from google.cloud import storage
import base64
from datetime import *
import pandas as pd

# Get date, year data
today = date.today()
year = str(today.year)
directory = "/foo/files/"

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
# Credential for GCP.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "credentials/dstreportdata-48acf2cbeed0.json"

def get_attachment():
    """
    Sign in with Gmail ang get the attachment from lasted email that forwarded from Opera system.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('credentials/token.json'):
        creds = Credentials.from_authorized_user_file('credentials/token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run.
        with open('credentials/token.json', 'w') as token:
            token.write(creds.to_json())


    try:
        # Call the Gmail API.
        service = build('gmail', 'v1', credentials=creds)
        # Looking for the lastest email and get the message ID.
        get_first_mail = service.users().messages().list(userId='me', maxResults=1).execute()
        message = get_first_mail.get('messages', [])[0]
        # Read the messange body and get attachment ID.
        msg = service.users().messages().get(userId='me', id=message['id']).execute()
        get_attachmentid = msg['payload']['parts'][1]['body']['attachmentId']
        # Get attachment file, save then upload to GCS.
        data = service.users().messages().attachments().get(userId='me', messageId=message['id'], id=get_attachmentid).execute()
        # Decode base64 in a file.
        text_to_write = str(base64.b64decode(data['data']))
        return text_to_write
    

    except HttpError as error:
        # TODO(developer) - Handle errors from gmail API.
        print(f'An error occurred: {error}')

# Write a file to GCS without saving it to the local.
def write_to_blob():
    # Get client credentials to connect with GCS.
    client = storage.Client()
    # Select bucket to upload file.
    bucket = storage.Bucket(client, 'dst_datalake')
    # Get date, year data
    today = date.today()
    year = str(today.year)
    # File name by generated date.
    filename = (str(today) + "_dst_revenue.csv")
    # Create blob with file path to upload.
    blob = bucket.blob(f"report_data/{year}/{filename}")
    # Upload from string.
    blob.upload_from_filename(f"{directory}{today}_dst_revenue.csv")

def data_cleansing():
    if not os.path.exists(directory):
        os.makedirs(directory)
    # Format Column Head and write to a file
    with open(f"{directory}raw_data.csv", "w") as f:
        formatted_output = get_attachment().replace("b'", '').replace('\\n', '\n').replace('\\t', '\t').replace('Date|Time|Room No.|Name|Trn. Code|Description|Check No.|Currency|Debit|Credit|Cash ID|User Name|Supplement/Reference/Credit Card No.|Exp. Date|Receipt No.|', '').replace("'", "")
        f.write('Date|Time|Room_No|Name|transaction_code|Description|Check_No|Currency|Debit|Credit|Cash_ID|User_Name|Supplement/Reference/Credit_Card_No|Exp_Date|Receipt_No|\n')
        f.write(formatted_output)
    # import to pandas and drop unwanted columns
    df = pd.read_csv(f"{directory}raw_data.csv", delimiter="|", index_col=False)
    df = df.drop(columns=['Check_No', 
                      'Time',
                      'Cash_ID', 
                      'User_Name', 
                      'Supplement/Reference/Credit_Card_No', 
                      'Exp_Date', 
                      'Receipt_No', 
                      'Unnamed: 15',
                      ])
    df['Date'] = pd.to_datetime(df['Date'].astype(str), format='%d/%m/%y')
    # Split the "original_name" column into separate "last name", "first name", and "title" columns
    df[['last name', 'first name', 'title']] = df['Name'].str.split(',', expand=True)

    # Concatenate the columns in the desired format
    df['new_name'] = df['title'].str.strip() + ' ' + df['first name'].str.strip() + ' ' + df['last name'].str.strip()
    # Drop columns. 
    df.drop(['last name', 'first name', 'title', 'Name'], axis=1, inplace=True)
    # Replace the "new_name" column with the "Name" column
    df.rename(columns={'new_name': 'Name'}, inplace=True)
    # format name
    df['Name'] = df['Name'].replace(r'[^a-zA-Z\s.]', '', regex=True).astype(str)
    df['Debit'] = df['Debit'].str.replace(',', '').str.replace(' ', '')
    df['Debit'] = df['Debit'].astype(float)
    # move negative values to credit column
    df.loc[df['Debit'] < 0, 'Credit'] = -1 * df['Debit']
    df.loc[df['Debit'] < 0, 'Debit'] = 0.0
    # format the column to string
    df['Description'] = df['Description'].astype('string')
    df['Currency'] = df['Currency'].astype('string')
    df['Name'] = df['Name'].astype('string')

    filename = (f"{directory}{today}_dst_revenue.csv")
    # Write to csv
    df.to_csv(
        path_or_buf=filename,
        index=False,
    )


with DAG(
    "get_attachment_from_dst_gmail",
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 14 L * *",
    tags=["get_attachment_from_dst_gmail"]
) as dag:

    dag.doc_md = """
    
    https://colab.research.google.com/drive/1aIBrljimixMtUMLuNntylvZijZciFs9I?usp=sharing

    """

    t1 = PythonOperator(
        task_id="get_attachment_and_cleansing",
        python_callable=data_cleansing,
    )    

    t2 = PythonOperator(
        task_id="write_to_blob",
        python_callable=write_to_blob,
    )

    t3 = BashOperator(
    task_id="clear_temp_files",
    bash_command='rm -rf /foo/files/',
    )

t1 >> t2 >> t3
