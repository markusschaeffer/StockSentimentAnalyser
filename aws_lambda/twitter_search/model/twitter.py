import boto3


class Twitter:

    def __init__(self, dynamoDBTableName):
        '''
        @dynamoDBTableName: name of the dynamodb table (Twitter)
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

    def batchStore(self, data, symbol):
        '''
        stores several tweets in a batch process
        @data: a list of tweets for a specifc stock

        for batch write documentation in dynamodb see
        #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.batch_write_item
        '''

        with self.table.batch_writer() as batch:
            for tweet in data:
                id = tweet.id
                symbol = str(symbol)
                created_at = str(tweet.created_at)
                full_text = str(tweet.full_text)
                if(len(tweet.entities['urls']) > 0):
                    url = str(tweet.entities['urls'][0]['expanded_url'])
                    if('https://twitter.com/' in url): #remove twitter replies
                        url = 'None'
                else:
                    url = 'None'

                batch.put_item(
                    Item={
                        'id': id,
                        'symbol': symbol,
                        'created_at': created_at,
                        'full_text': full_text,
                        'url': url
                    }
                )
