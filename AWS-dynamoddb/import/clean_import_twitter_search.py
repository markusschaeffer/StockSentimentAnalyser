import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

def clean_data(table, data):
    with table.batch_writer() as batch:
        for entry in data:
            created_at = entry['created_at']
            symbol = entry['symbol']
            id = entry['id']
            full_text = entry['full_text']
            url = entry['url']

            batch.delete_item(
                Key={
                    'id': id
                }
            )

def import_data(table, data):
    with table.batch_writer() as batch:
        for entry in data:
            created_at = entry['created_at']
            symbol = entry['symbol']
            id = entry['id']
            full_text = entry['full_text']
            url = entry['url']

            batch.put_item(
                Item={
                    'created_at': created_at,
                    'symbol': symbol,
                    'id': id,
                    'full_text': full_text,
                    'url':url
                }
            )

if __name__ == "__main__":

    tablename = 'Twitter'
    dynamodb = boto3.resource('dynamodb')
    client = boto3.client('dynamodb')
    table = dynamodb.Table(tablename)

    data_csv = r'data/2019_12_14_twitter.csv'
    df = pd.read_csv(data_csv)
    
    data_json = r'data/2019_12_14_twitter.json'
    df.to_json(data_json, orient='records')

    with open(data_json, 'r') as json_file:
        data = json.load(json_file)
        #clean table
        clean_data(table, data)
        #import in table
        import_data(table, data)