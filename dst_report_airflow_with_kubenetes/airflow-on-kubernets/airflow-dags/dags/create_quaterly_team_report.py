from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import glob
import os.path

# Google Package
from google.cloud import storage
from google.cloud import bigquery
import os

# Get date time infomation
from datetime import *
import time


today = date.today()
year = today.year
# for test use last quarter by (today.month - 1) // 3
quarter = (today.month - 1) // 3 + 1
directory = "/foo/files/"

# Get file from Bucket
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "credentials/dstreportdata-48acf2cbeed0.json"

def get_file_list(year, quarter):
    client = storage.Client()
    blobs = list(client.list_blobs(bucket_or_name="dst_datalake", prefix=f"report_data/{year}"))
    q_list = []
    for blob in blobs:
        if not blob.name.endswith('/'):
            item_quarter = (int(blob.name[22:24]) - 1) // 3 + 1
            if item_quarter == quarter:
                q_list.append(blob.name)
    return q_list

def get_file_from_gcs():
    # Initialise a client
    storage_client = storage.Client()
    # Create a bucket object for our bucket
    bucket = storage_client.get_bucket("dst_datalake")

    if not os.path.exists(directory):
        os.makedirs(directory)

    for item in get_file_list(year, quarter):
        # Create a blob object from the filepath
        blob = bucket.blob(f"{item}")
        # Download the file to a destination
        blob.download_to_filename(f"{directory}{item[17:]}")

    

# Define function to map prices to packages
def get_package(price):
    package_dict = {
        1500: 'Gopro rental',
        5000: 'Capture Moment Package',
        0: 'Photography Discount',
        9000: 'Moving memories package',
    }
    if price in package_dict:
        return package_dict[price]
    elif price > 9000 and price <= 15000:
        return 'Rare Experience Package'
    elif price > 36000:
        return 'Your Soneva Journey Package'
    else:
        return 'Custom Package'

def join_csv():
    # setting the path for joining multiple files
    path = os.path.join(f"{directory}*dst_revenue.csv")
    # list of merged files returned
    files = glob.glob(path)
    # joining files with concat and read_csv
    df_report = pd.concat(map(pd.read_csv, files), ignore_index=True)
    df_report = df_report.drop(columns=['Name'])
    # Apply mapping function to "Debit" column to create new "Package" column
    df_report['Package'] = df_report['Debit'].apply(get_package)
    # Save the dataframe to an Excel file
    with open(f'{directory}{year}_q{quarter}_dst_report.csv', 'w') as writer:
        df_report.to_csv(writer, index=False)


# Upload CSV file to google BigQuary
def upload_to_bigquary():
    # initiate client object
    client = bigquery.Client()
    # Set job config
    job_config = bigquery.LoadJobConfig(
        # Read from CSV format
        source_format = bigquery.SourceFormat.CSV,
        # Auto detect table Schema
        autodetect = True,
    )
    # Set Table ID
    table_id = f"dstreportdata.dst_report.{year}_{quarter}_dst_revenue_report"

    # Load table from the source file
    with open(f'{directory}{year}_q{quarter}_dst_report.csv', 'rb') as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    # Monitor Job Status
    while job.state != 'DONE':
        time.sleep(2)
        job.reload()
        print(job.state)
    print(job.result())

with DAG(
    "create_quaterly_team_report",
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 1 1 */3 *",
) as dag:

    dag.doc_md = """

    https://colab.research.google.com/drive/1aIBrljimixMtUMLuNntylvZijZciFs9I?usp=sharing

    """

    t1 = PythonOperator(
        task_id="get_file_from_gcs",
        python_callable=get_file_from_gcs,
    )    

    t2 = PythonOperator(
        task_id="generate_report",
        python_callable=join_csv,
    )

    t3 = PythonOperator(
        task_id="upload_to_bigquary",
        python_callable=upload_to_bigquary,
    )

    t4 = BashOperator(
    task_id="clear_temp_files",
    bash_command='rm -rf /foo/files/',
)

t1 >> t2 >> t3 >> t4