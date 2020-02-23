import boto3
import hashlib

class NewsAPI:

    def __init__(self, dynamoDBTableName):
        '''
        @dynamoDBTableName: name of the dynamodb table (NewsAPI)
        '''

        self.dynamoDBTableName = dynamoDBTableName

        # specify dynamodb and table
        self.dynamodb = boto3.resource('dynamodb')
        self.client = boto3.client('dynamodb')
        self.table = self.dynamodb.Table(self.dynamoDBTableName)

    def batchStore(self, data):
        '''
        @data: a list of news articles (for a specifc stock) to be stored
        '''

        for article in data:

            id = article['symbol'] + '-' + hashlib.sha256(str(article['title']).encode('utf-8')).hexdigest()
            symbol = article['symbol']
            date = article['date']
            source = article['source']
            title = article['title']
            description =  article['description']
            content = article['content']
            url = article['url']

            self.table.put_item(
                Item={
                    'id': id,
                    'symbol': symbol,
                    'date': date,
                    'source': source,
                    'title': title,
                    'description': description,
                    'content': content,
                    'url': url
                }
            )
