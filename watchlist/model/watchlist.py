import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr


class Watchlist:

    # default Expressions for dynamodb table scan
    DEFAULTFILTEREXPRESSION = '#stocktwits = :true OR #stocktwits = :false'
    DEFAULTEXPRESSIONATTRIBUTEVALUES = {':true': True, ':false': False}
    DEFAULTEXPRESSIONATTRIBUTENAMES = {
        '#name': 'name', '#stocktwits': 'stocktwits'}
    DEFAULTPROJECTIONEXPRESSION = 'isin, #name, symbol, stocktwits'

    def __init__(self, watchlistPath = './watchlist/', watchlistFileName = 'watchlist.txt', dynamoDBTableName = 'Watchlist'):
        '''
        @watchlistPath: path to the watchlist without the watchlist name, e.g. ./watchlist/
        @watchlistFileName: name of the watchlist, e.g. watchlist.txt
        @dynamoDBTableName: name of the dynamodb table, e.g. Watchlist
        '''

        self.watchlistPath = watchlistPath
        self.watchlistFileName = watchlistFileName
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

    def addAllStocks(self):
        '''
        persists the watchlist file to self.table
        '''

        # read watchlist from .txt file
        df_watchlist = pd.read_csv(self.watchlistPath+self.watchlistFileName)
        df_watchlist = df_watchlist[['isin', 'name',
                                     'symbol', 'stocktwits']]  # reorder columns

        # pandas data frame --> json
        pathToJsonWatchlist = self.watchlistPath + \
            (self.watchlistFileName.split('.')[0])+'.json'
        df_watchlist.to_json(pathToJsonWatchlist, orient='records')

        # loop through every item in the json file
        with open(pathToJsonWatchlist, 'r') as json_file:
            watchlist = json.load(json_file)
            for stock in watchlist:
                isin = stock['isin']
                name = stock['name']
                symbol = stock['symbol']
                stocktwits = stock['stocktwits']

                print('Adding stock:', isin, name, symbol, stocktwits)

                # put item to dynamodb table
                self.table.put_item(
                    Item={
                        'isin': isin,
                        'name': name,
                        'symbol': symbol,
                        'stocktwits': stocktwits
                    }
                )

    def addStock(self, stock):
        '''
        stores a single stock
        @stock: a dict containing isin, name, symbol, and stocktwits as keys
        '''

        isin = stock['isin']
        name = stock['name']
        symbol = stock['symbol']
        stocktwits = stock['stocktwits']

        print('Adding stock:', isin, name, symbol, stocktwits)

        # put item to dynamodb table
        self.table.put_item(
            Item={
                'isin': isin,
                'name': name,
                'symbol': symbol,
                'stocktwits': stocktwits
            }
        )

    def getStocks(self, filterExpression=DEFAULTFILTEREXPRESSION,
                  projectionExpression=DEFAULTPROJECTIONEXPRESSION,
                  expressionAttributeNames=DEFAULTEXPRESSIONATTRIBUTENAMES,
                  expressionAttributeValues=DEFAULTEXPRESSIONATTRIBUTEVALUES
                  ):
        '''
        @filterExpression:  
            specifies a condition that returns only items that satisfy the condition. All other items are discarded.
            e.g. Key('year').between(1950, 1959)
            or "#order_status = :delivered OR #order_status = :void OR #order_status = :bad",
        @projectionExpression:
            specifies the attributes you want in the scan result.
        @expressionAttributeNames:
            provides name substitution.
            e.g. {'#name': 'name'}
            or {"#order_status": "status"}
        @expressionAttributeValues:
            provides value substitution. You use this because you can't use literals in any expression, including KeyConditionExpression
            e.g. {":delivered": "delivered", ":void": "void", ":bad": "bad"}
        '''

        responseList = []

        # The scan method returns a subset of the items each time, called a page.
        response = self.table.scan(
            FilterExpression=filterExpression,
            ProjectionExpression=projectionExpression,
            ExpressionAttributeNames=expressionAttributeNames,
            ExpressionAttributeValues=expressionAttributeValues
        )

        if int(response['ResponseMetadata']['HTTPStatusCode']) == 200:
            responseList = response['Items']

        # When the last page is returned, LastEvaluatedKey is not part of the response.
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=filterExpression,
                ProjectionExpression=projectionExpression,
                ExpressionAttributeNames=expressionAttributeNames,
                ExpressionAttributeValues=expressionAttributeValues,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            if int(response['ResponseMetadata']['HTTPStatusCode']) == 200:
                responseList = responseList+response['Items']

        return responseList

    def getFieldForStocks(self, field, filterExpression=DEFAULTFILTEREXPRESSION,
                                        projectionExpression=DEFAULTPROJECTIONEXPRESSION,
                                        expressionAttributeNames=DEFAULTEXPRESSIONATTRIBUTENAMES,
                                        expressionAttributeValues=DEFAULTEXPRESSIONATTRIBUTEVALUES):
        '''
        returns all values for a specified field in the watchlist
        '''
        returnList = []
        stocksList = self.getStocks(filterExpression=filterExpression,
                                    projectionExpression=projectionExpression,
                                    expressionAttributeNames=expressionAttributeNames,
                                    expressionAttributeValues=expressionAttributeValues)
        for stock in stocksList:
            returnList.append(stock[field])
        return returnList

    def getSymbols(self, filterExpression=DEFAULTFILTEREXPRESSION,
                         projectionExpression=DEFAULTPROJECTIONEXPRESSION,
                         expressionAttributeNames=DEFAULTEXPRESSIONATTRIBUTENAMES,
                         expressionAttributeValues=DEFAULTEXPRESSIONATTRIBUTEVALUES):
        '''
        returns all stock symbols in the watchlist as a list of strings
        '''
        return self.getFieldForStocks('symbol', filterExpression=filterExpression,
                                                projectionExpression=projectionExpression,
                                                expressionAttributeNames=expressionAttributeNames,
                                                expressionAttributeValues=expressionAttributeValues)

    def getIsins(self, filterExpression=DEFAULTFILTEREXPRESSION, expressionAttributeValues=DEFAULTEXPRESSIONATTRIBUTEVALUES):
        '''
        returns all isins in the watchlist as a list of strings
        '''
        return self.getFieldForStocks('isin', filterExpression=filterExpression, expressionAttributeValues=expressionAttributeValues)

    def getIsinForSymbol(self, symbol):
        '''
        returns the isin for a single specified symbol
        '''

        fe = '#symbol = :sym'
        eav = {':sym': symbol}
        ean = {'#symbol': 'symbol'}
        pe = 'isin, symbol'

        isin = ''
        try:
            isin = self.getStocks(
                filterExpression=fe, projectionExpression=pe, expressionAttributeNames=ean, expressionAttributeValues=eav)[0]['isin']
        except ClientError as e:
            print(e.response['Error']['Message'])

        return isin

    def getNameForSymbol(self, symbol):
        '''
        returns the name for a single specified symbol
        @symbol: e.g. AMZN
        @name: e.g. Amazon
        '''
        name = ''
        try:
            isin = self.getIsinForSymbol(symbol)
            name = self.table.query(KeyConditionExpression=Key('isin').eq(isin))[
                'Items'][0]['name']
        except ClientError as e:
            print(e.response['Error']['Message'])

        return name

    def getLastId(self, symbol, source):
        '''
        returns the lastId for a specified symbol and source (stocktwits or twitter)
        @symbol: e.g. AMZN
        @source: stocktwits, twitter
        '''

        isin = self.getIsinForSymbol(symbol)
        lastId = 0
        try:
            response = self.table.get_item(
                Key={'isin': isin}
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
        except KeyError as e:
            print(e.response['Error']['Message'])
        else:
            if(source == 'stocktwits'):
                if 'stocktwitsLastMessageId' in response['Item']:
                    lastId = response['Item']['stocktwitsLastMessageId']
            if(source == 'twitter'):
                if 'twitterLastId' in response['Item']:
                    lastId = response['Item']['twitterLastId']
        return lastId

    def updateLastId(self, symbol, lastId, source):
        '''
        updates the lastId of the specified symbol for a given source 
        @symbol: e.g. AMZN
        @source: stocktwits, twitter
        '''
        if(lastId != 0):
            isin = self.getIsinForSymbol(symbol)
            if(source == 'stocktwits'):
                response = self.table.update_item(
                    Key={
                        'isin': isin
                    },
                    UpdateExpression="set stocktwitsLastMessageId = :lmid",
                    ExpressionAttributeValues={
                        ':lmid': lastId
                    },
                    # The ReturnValues parameter instructs DynamoDB to return only the updated attributes
                    ReturnValues="UPDATED_NEW"
                )
            if(source == 'twitter'):
                response = self.table.update_item(
                    Key={
                        'isin': isin
                    },
                    UpdateExpression="set twitterLastId = :lmid",
                    ExpressionAttributeValues={
                        ':lmid': lastId
                    },
                    # The ReturnValues parameter instructs DynamoDB to return only the updated attributes
                    ReturnValues="UPDATED_NEW"
                )

            if int(response['ResponseMetadata']['HTTPStatusCode']) == 200:
                return True
            else:
                return False
