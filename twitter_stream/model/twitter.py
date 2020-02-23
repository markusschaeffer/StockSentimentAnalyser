import boto3

class Twitter:

    def __init__(self, dynamoDBTableName='Twitter_Stream'):
        """
        @dynamoDBTableName: name of the dynamodb table (Twitter_Stream)
        """
        self.dynamoDBTableName = dynamoDBTableName
        self.dynamodb = boto3.resource('dynamodb')
        self.client = boto3.client('dynamodb')
        self.table = self.dynamodb.Table(self.dynamoDBTableName)

    def putTweet(self, symbol, created_at, text, prediction, probaBull, probaBear):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.put_item
        """
        # put item to dynamodb table
        self.table.put_item(
            Item={
                'symbol': symbol,
                'created_at': created_at,
                'text': text,
                'prediction': prediction,
                'probaBull': probaBull,
                'probaBear': probaBear
            }
        )