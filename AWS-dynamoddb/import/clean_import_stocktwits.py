import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

def clean_data(table, data):
    with table.batch_writer() as batch:
        for entry in data:
            id = entry['id']

            batch.delete_item(
                Key={
                    'id': id
                }
            )
def insert_update_data(table, data):
    with table.batch_writer() as batch:
        for entry in data:
            createdAt = entry['createdAt']
            symbol = entry['symbol']
            id = entry['id']
            body = entry['body']
            stocktwitsSentiment = entry['stocktwitsSentiment']

            batch.put_item(
                Item={
                    'createdAt': createdAt,
                    'symbol': symbol,
                    'id': id,
                    'body': body,
                    'stocktwitsSentiment':stocktwitsSentiment
                }
            )

def typofix(table, data):
    tobeupdated = []
    for entry in data:
        if(entry['createdAt']) is None:
            createdAt = entry['createdat']
            symbol = entry['symbol']
            id = entry['id']
            body = entry['body']
            stocktwitsSentiment = entry['stocktwitssentiment']
            tobeupdated.append({'id': id, 'createdAt': createdAt, 'symbol': symbol, 'body': body, 'stocktwitsSentiment':stocktwitsSentiment})
    insert_update_data(table, tobeupdated)

if __name__ == "__main__":

    tablename = 'Stocktwits'
    dynamodb = boto3.resource('dynamodb')
    client = boto3.client('dynamodb')
    table = dynamodb.Table(tablename)

    data_csv = r'data/2019_12_26_stocktwits.csv'
    df = pd.read_csv(data_csv)
    
    data_json = r'data/2019_12_26_stocktwits.json'
    df.to_json(data_json, orient='records')

    with open(data_json, 'r') as json_file:
        data = json.load(json_file)
        #typofix(table, data)
        #clean table
        clean_data(table, data)
        #insert_update into table
        insert_update_data(table, data)