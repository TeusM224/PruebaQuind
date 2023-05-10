from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
import numpy as np
import io

try:
    import google.cloud.storage
    from google.cloud import storage
    import json
    import os
    import sys

except Exception as e:
    print("Error: {} ".format(e))

import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'gs://southamerica-east1-pruebate-a15e3470-bucket/probando001-377801-ad7abcc1aeb6.json'

client = storage.Client()

bucket_name = 'starprocess'
blob_name = 'meta_full.csv'

bucket = client.bucket(bucket_name)
blob = bucket.blob(blob_name)

content = blob.download_as_bytes()

def upload_dataframe_to_gcs(df, bucket_name, object_name):
   
    hook = GCSHook() #instancia del hook de Google Cloud Storage

    data = df.to_csv().encode()

    hook.upload(bucket_name='finalprocess', 
                object_name='meta_full_delete.csv', 
                data=data, 
                mime_type='text/csv')
    
def modify_dataframe():
    
    client = storage.Client()

    bucket_name = 'starprocess'
    blob_name = 'meta_full.csv'

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    content = blob.download_as_bytes()  


    df = pd.read_csv(io.BytesIO(content),sep=';')
    columns_drop = ['address','price', 'MISC','state', 'relative_results', 'url']
    df = df.drop(columns=columns_drop, axis=1)

    upload_dataframe_to_gcs(df, 'finalprocess', 'meta_full_delete.csv')

default_args = {
    "owner":"Mateo M",
    "depends_on_past":False,
    "email_on_failure":False,
    "email_on_retry":False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('upload_dataframe_to_gcs_dag', default_args=default_args, schedule_interval=None)as dag:
    
    """
    prueba = PythonOperator(
        task_id='iniciando',
        python_callable = iniciando,
        dag=dag
    )"""

    load_drop = PythonOperator(
        task_id='modify_dataframe',
        python_callable=modify_dataframe,
        dag=dag
    )
    
load_drop

"""
def filterCategories(df):

    df['category'] = df['category'].apply(lambda s: str(s).lower()) #if isinstance(s, list) else 'NO DATA')
    new_df = pd.DataFrame(columns=df.columns)

    ""A continuación, se pasa una lista de palabras relacionadas a restaurantes, hoteles, parques y zonas turisticas o de recreación

    keywords = ['hotel', 'motel', 'hostel', 'breakfast', 'b&b', 'inn', 'resort', 'lodging', 'pension',
                'room', 'suite', 'chamber', 'palace', 'cabin', 'mansion', 'apartment', 'mansion',
                'guesthouse', 'boutique hotel', 'spas', 'beach hotel', 'ski hotel', 'casino',
                'pet-friendly hotel', 'luxury hotel', 'historic hotel', 'bed & breakfast',
                'family-friendly hotel', 'airport hotel', 'business hotel', 'cheap hotel',
                'all-inclusive resort', 'design hotel', 'floating hotel', 'health resort', 'hostel', 'boutique resort'

                'restaurant', 'bar', 'diner', 'bbq', 'pizza', 'burger', 'sandwich', 'dining', 'grill', 'dinner',
                'cafeteria', 'barbecue', 'tavern', 'delicatessen', 'food', 'coffee', 'buffet', 'bakery', 'pub',
                'cafe', 'steakhouse', 'bistro', 'gastropub', 'brewery', 'winery', 'tapas', 'sushi', 'seafood',
                'brunch', 'ramen', 'noodle', 'vegetarian', 'vegan', 'fast food', 'ice cream', 'dessert', 'creperie'

                "Nature reserves", "Botanical gardens", "Arboretums", "National parks", 
                "State parks", "Forest preserves", "Greenways", "Land trusts", "Wildlife", 
                "Wetlands", "Beaches", "Picnic areas", "Recreation areas", "Campgrounds", "Amusement parks", "Water parks", 
                "Skate parks", "Dog parks", "Sports fields", "Golf courses", "Tennis courts", "Hiking trails", "Bike paths", 
                "Nature trails", "Scenic overlooks", "Lakeshores", "Riverbanks",

                "Amusement park", "Water park", "Zoo", "Botanical garden", "Nature trail", "Hiking trail",
                "Camping ground", "Picnic area", "Skate park", "Roller skating rink", "Ski resort",
                "Snowboarding park", "Golf course", "Miniature golf", "Tennis", "Basketball", "Baseball",
                "Soccer field", "Football field", "Playground", "Splash pad", "Community center", "Recreation center", "Sports complex",
                "Theme park", "State park", "National park", "Conservation area"]

    for keyword in keywords:
        filter = df[df['category'].str.contains(keyword)]
        new_df = pd.concat([new_df, filter])

    new_df.drop_duplicates(subset='gmap_id', inplace=True)

    new_df['ID_meta'] = np.arange(new_df.shape[0])

    new_df.to_csv('/content/drive/MyDrive/pruebaQuind/data/processed/filtered_meta.csv', sep=';', index = False, mode='a')

    return new_df

def _time_day(biglist: list, day: str):

    for smallList in biglist:
        if smallList[0] == day:
            return smallList[1]

    return biglist  


def _table_hours():
    df_data = pd.read_csv(f"/Users/mateo/Descargas/Clean_data/columns.csv")
    df_hours = df_data[['gmap_id', 'hours']]

    df_hours['Monday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Monday') if isinstance(hour, list) else hour)
    df_hours['Tuesday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Tuesday') if isinstance(hour, list) else hour)
    df_hours['Wednesday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Wednesday') if isinstance(hour, list) else hour)
    df_hours['Thursday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Thursday') if isinstance(hour, list) else hour)
    df_hours['Friday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Friday') if isinstance(hour, list) else hour)
    df_hours['Saturday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Saturday') if isinstance(hour, list) else hour)
    df_hours['Sunday'] = df_hours['hours'].apply(lambda hour : _time_day(hour, 'Sunday') if isinstance(hour, list) else hour)

    df_hours.drop(['hours'], axis=1, inplace=True)
    df_hours.to_csv(f"/home/osanchezd/Downloads/PF_Henry/Clean_data/clean_table_hours.csv",
                    sep=";",
                    index=False,
                    )

    print(f'table_hours job done!.')

"""