import boto3


class Stocktwits:

    def __init__(self, dynamoDBTableName):
        '''
        @dynamoDBTableName: name of the dynamodb table (Stocktwits)
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

    def batchStore(self, data):
        '''
        stores several twtis in a batch process
        @data: a list of stocktwits twits (for a specifc stock)
        '''

        with self.table.batch_writer() as batch:
            for twit in data:
                id = twit['symbol'] + '-' + str(twit['messageId'])
                symbol = twit['symbol']
                createdAt = twit['createdAt']
                body = twit['body']
                stocktwitsSentiment = str(twit['stocktwitsSentiment'])
                if stocktwitsSentiment == '':
                    stocktwitsSentiment = 'None'

                batch.put_item(
                    Item={
                        'id': id,
                        'symbol': symbol,
                        'createdAt': createdAt,
                        'body': body,
                        'stocktwitsSentiment': stocktwitsSentiment
                    }
                )
                
    def store(self, data):
        '''
        stores several twtis
        @data: a list of stocktwits twits (for a specifc stock)
        '''

        for twit in data:
            id = twit['symbol'] + '-' + str(twit['messageId'])
            symbol = twit['symbol']
            createdAt = twit['createdAt']
            body = twit['body']
            stocktwitsSentiment = str(twit['stocktwitsSentiment'])
            if stocktwitsSentiment == '':
                stocktwitsSentiment = 'None'

            self.table.put_item(
                Item={
                    'id': id,
                    'symbol': symbol,
                    'createdAt': createdAt,
                    'body': body,
                    'stocktwitsSentiment': stocktwitsSentiment
                }
            )
