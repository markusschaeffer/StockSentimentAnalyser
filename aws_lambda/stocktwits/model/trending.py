import boto3
from datetime import datetime
import pytz

class Trending:

    def __init__(self, dynamoDBTableName):
        '''
        @dynamoDBTableName: name of the dynamodb table (Stocktwits_Trends)
        '''

        self.dynamoDBTableName = dynamoDBTableName

        # specify dynamodb and table
        self.dynamodb = boto3.resource('dynamodb')
        self.client = boto3.client('dynamodb')
        self.table = self.dynamodb.Table(self.dynamoDBTableName)

    def describeTable(self):
        '''
        describes the DynamoDB Table used
        '''
        return self.client.describe_table(TableName=self.dynamoDBTableName)
                
    def store(self, data):
        '''
        @data: a list of trending equities on stocktwits
        '''
        
        tz_NY = pytz.timezone('America/New_York') 
        datetime_NY = datetime.now(tz_NY)
        ts = str(datetime_NY).split('.')[0]
        dat = ts.split(' ')[0]
        tim = ts.split(' ')[1][0:5]

        self.table.put_item(
            Item={
                'timestamp': ts,
                'date': dat,
                'time': tim,
                'symbol1': data[0]['symbol'],
                'symbol2': data[1]['symbol'],
                'symbol3': data[2]['symbol'],
                'symbol4': data[3]['symbol'],
                'symbol5': data[4]['symbol']
            }
        )
