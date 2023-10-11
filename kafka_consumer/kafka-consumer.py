from kafka import KafkaConsumer
import base64
import json
import os 

###################
from pprint import pprint
from google.cloud import bigquery
# from google.cloud import storage
from google.cloud import bigquery


# Setting up logging with timestamp and log level
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'keys.json'
# Construct a BigQuery client object.
client = bigquery.Client()
# Define server with port'
logging.info('Initialized BigQuery Client')

table_info_schema = {
    'fields': [
        {'type': 'FLOAT', 'name': 'acousticness', 'mode': 'NULLABLE'},
        {'type': 'STRING', 'name': 'analysis_url', 'mode': 'NULLABLE'},
        {'type': 'FLOAT', 'name': 'danceability', 'mode': 'NULLABLE'},
        {'type': 'INTEGER', 'name': 'duration_ms', 'mode': 'NULLABLE'},
        {'type': 'FLOAT', 'name': 'energy', 'mode': 'NULLABLE'},
        {'type': 'STRING', 'name': 'genre', 'mode': 'NULLABLE'},
        {'type': 'STRING', 'name': 'id', 'mode': 'NULLABLE'},
        {'type': 'FLOAT', 'name': 'instrumentalness', 'mode': 'NULLABLE'},
        {'type': 'INTEGER', 'name': 'key', 'mode': 'NULLABLE'},
        {'type': 'FLOAT', 'name': 'liveness', 'mode': 'NULLABLE'},
        {'type': 'FLOAT', 'name': 'loudness', 'mode': 'NULLABLE'},
        {'type': 'INTEGER', 'name': 'mode', 'mode': 'NULLABLE'},
        {'type': 'STRING', 'name': 'song_name', 'mode': 'NULLABLE'},
        {'type': 'FLOAT', 'name': 'speechiness', 'mode': 'NULLABLE'},
        {'type': 'FLOAT', 'name': 'tempo', 'mode': 'NULLABLE'},
        {'type': 'INTEGER', 'name': 'time_signature', 'mode': 'NULLABLE'},
        {'type': 'STRING', 'name': 'track_href', 'mode': 'NULLABLE'},
        {'type': 'STRING', 'name': 'type', 'mode': 'NULLABLE'},
        {'type': 'STRING', 'name': 'uri', 'mode': 'NULLABLE'},
        {'type': 'FLOAT', 'name': 'valence', 'mode': 'NULLABLE'}
    ]
}

def load_json_to_bq(table_info_schema, json_data: dict, table_name: str = None, dataset: str = None):
    try:

        table_info = client.get_table(f'{dataset}.{table_name}')
        table_info.schema = table_info_schema['fields']
        client.update_table(table_info, ['schema'])
        job_config = bigquery.LoadJobConfig(
            schema=table_info_schema['fields'],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ],
        )
        job = client.load_table_from_json(
            json_data, f'{dataset}.{table_name}', job_config=job_config
        )
        job.result()
        logging.info(f"Loaded row to {table_name}")
    except Exception as e:
       print(e)
       for error in job.errors:
           logging.info(f"BigQuery Error: {error['message']}")
       return f"Error loading data to {table_name}"



# Read and print message from consumer
def int_check(data):
    return int(data) if not None else None
def float_check(data):
    return float(data) if not None else None
def str_check(data):
    return str(data) if not None else None



def read_consumer(bootstrap_servers,topicName):
    json_data = [] 
    # Initialize consumer variable
    consumer = KafkaConsumer (topicName , 
                            auto_offset_reset='earliest', 
                            bootstrap_servers = bootstrap_servers, 
                            group_id='spotify_stream',
                            #request_timeout_ms =3000000,
                            heartbeat_interval_ms=1000,
                            session_timeout_ms=60000,
                            max_poll_records=100,
                            max_poll_interval_ms=900000)
    for msg in consumer:
        message_str = msg.value
        # Parse the JSON data
        logging.info("PArsing data")
        data = json.loads(message_str)
        row_data = {'acousticness': float_check(data.get('after',None).get('acousticness',None)),
        'analysis_url': str_check(data.get('after',None).get('analysis_url',None)),
        'danceability': float_check(data.get('after',None).get('danceability',None)),
        'duration_ms': int_check(data.get('after',None).get('duration_ms',None)),
        'energy': float_check(data.get('after',None).get('energy',None)),
        'genre': str_check(data.get('after',None).get('genre',None)),
        'id': str_check(data.get('after',None).get('id',None)),
        'instrumentalness': float_check(data.get('after',None).get('instrumentalness',None)),
        'key': int_check(data.get('after',None).get('key',None)),
        'liveness': float_check(data.get('after',None).get('liveness',None)),
        'loudness': float_check(data.get('after',None).get('loudness',None)),
        'mode': int_check(data.get('after',None).get('mode',None)),
        'song_name': str_check(data.get('after',None).get('song_name',None)),
        'speechiness': float_check(data.get('after',None).get('speechiness',None)),
        'tempo': float_check(data.get('after',None).get('tempo',None)),
        'time_signature': int_check(data.get('after',None).get('time_signature',None)),
        'track_href': str_check(data.get('after',None).get('track_href',None)),
        'type': str_check(data.get('after',None).get('type',None)),
        'uri': str_check(data.get('after',None).get('uri',None)),
        'valence': float_check(data.get('after',None).get('valence',None))
        }
        json_data.append(row_data)
        load_json_to_bq(table_info_schema=table_info_schema,json_data=json_data,\
                        table_name='spotify',dataset='streaming')
        consumer.commit()

if __name__ == '__main__':
    bootstrap_servers = ['localhost:9092']
    # Define topic name from where the message will recieve
    topicName = 'postgres.public.spotify_streaming'
    logging.info("+++++++++++++++++++++++++++++++++++++++++++++ starting Kafka to BigQuery Stream +++++++++++++++++++++++++++++++++++++++++++++")
    logging.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    read_consumer(bootstrap_servers=bootstrap_servers,topicName=topicName)
