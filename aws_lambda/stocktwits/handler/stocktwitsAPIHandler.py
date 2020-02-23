import requests
from requests.exceptions import HTTPError
import json
import re


class StocktwitsAPIHandler:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def getDataForSymbol(self, symbol, lastMessageId=0, limit=30):
        ''' 
        returns up to 30 messages for a given stock symbol/ticker and the last message id
        '''

        messageDataList = []
        maxMessageId = 0

        params = {
            # Ticker symbol, Stock ID, or RIC code of the symbol (Required)
            'id': symbol,
            # Returns results with an ID greater than (more recent than) the specified ID.
            #'since': lastMessageId,
            # 'max':	, #Returns results with an ID less than (older than) or equal to the specified ID.
            # Default and max limit is 30. This limit must be a number under 30.
            'limit': limit
            # 'filter':	, #Filter messages by links, charts, videos, or top. (Optional)
        }

        try:
            resp = requests.get(
                'https://api.stocktwits.com/api/2/streams/symbol/'+symbol+'.json', params=params)

            #print(json.dumps(resp.json(), indent=2, sort_keys=True))

            if(resp.json()['response']['status']) == 429:
                raise Exception('Rate limit exceeded. Client may not make more than 200 requests an hour.')

            if(resp.json()['response']['status']) == 404:
                raise Exception('Symbol not found')

            cursor = resp.json()['cursor']
            maxMessageId = cursor['max'] #max message id in the response

            if maxMessageId is not None:
                messages = resp.json()['messages']
                messagesList = []
                for message in messages:
                    messageId, createdAt, body, stocktwitsSentiment = StocktwitsAPIHandler.preprocess(
                        self, message)
                    messageData = {
                        'symbol': symbol,
                        'messageId': messageId,
                        'createdAt': createdAt,
                        'body': body,
                        'stocktwitsSentiment': stocktwitsSentiment
                    }
                    messageDataList.append(messageData)

            resp.raise_for_status()  # If the response was successful, no Exception will be raised
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')  # Python 3.6
        except Exception as err:
            print(f'Other error occurred: {err}')  # Python 3.6

        return(messageDataList, maxMessageId)

    def preprocess(self, message):
        '''
        extracts relevant information from a stocktwits message
        preprocesses the message
        '''

        messageId = message['id']
        createdAt = message['created_at']
        #transform time format
        #from e.g. 2019-10-25T00:11:11Z to 2019-10-25 00:11:11
        createdAt = createdAt.replace('T',' ').replace('Z','')
        
        # remove urls from the body
        body = re.sub(r'http\S+', '', str(message['body']))

        #remove \n and \t
        body = re.sub('\n', '', body)
        body = re.sub('\t', '', body)

        #remove hastags mentions
        body = re.sub(r'#\S+', '', body)

        stocktwitsSentiment = None
        if ('entities' in message):
            for entity in message['entities']:
                if entity == 'sentiment':
                    if message['entities']['sentiment'] is not None:
                        # 'Bullish' or 'Bearish'
                        stocktwitsSentiment = message['entities']['sentiment']['basic']

        return messageId, createdAt, body, stocktwitsSentiment
    
    def getTrending(self, limit=5):
        '''
        returns trending equities on stocktwits
        '''

        params = {
            # Default and max limit is 30. This limit must be a number under 30.
            'limit': limit
        }

        try:
            resp = requests.get(
                'https://api.stocktwits.com/api/2/trending/symbols/equities.json', params=params)

            if(resp.json()['response']['status']) == 429:
                raise Exception('Rate limit exceeded. Client may not make more than 200 requests an hour.')

            resp.raise_for_status()  # If the response was successful, no Exception will be raised
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')  # Python 3.6
        except Exception as err:
            print(f'Other error occurred: {err}')  # Python 3.6

        return list(resp.json()['symbols'])