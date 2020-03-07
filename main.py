from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import re
import dill
import boto3
import time
import logging

#imports necessary for the model/classifier
import numpy as np
import pandas as pd
import nltk
from nltk.corpus import wordnet

# import self created modules
from watchlist.model.watchlist import Watchlist
from twitter_stream.model.twitter import Twitter
import config

class MyTwitterStreamer():

    def __init__(self):
        pass

    def stream_tweets(self, keyword_list):
        # This handles Twitter authetification and the connection to the Twitter Streaming API
        auth = OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
        auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)
        
        listener = MyStreamListener()
        stream = Stream(auth, listener)

        # This line filters Twitter Streams to capture data by the keywords
        stream.filter(track=keyword_list)

class MyStreamListener(StreamListener):

    def __init__(self):
        """
        load classifier model
        """
        self.classifier = MyClassifier()
        self.model = Twitter()
        self.helper = MyHelper()

    #https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object
    def on_data(self, tweet):
        """
        - extracts relevant information from the tweets
        - classifies the text using a classifier
        - persists the result to a database
        """
        try:
            tweet_json = json.loads(tweet)
            #print(json.dumps(tweet_json, indent=4, sort_keys=True))

            if(tweet_json['lang'] == 'en'):

                #id = tweet_json['id_str']
                created_at = tweet_json['created_at']
                user_followers_count = tweet_json['user']['followers_count']
                
                # filter spam (user has to have more than 50 followers)
                #if(user_followers_count <= 50):
                #    return True

                text = self.helper.getTextFromTweet(tweet_json)

                # filter multi stock symbol messages/statuses
                if(text.count('$') > 1):
                    return True

                # filter multi mentions and hastags
                if(text.count('@') > 5 or text.count('#') > 5):
                    return True

                # get associated symbols/stocks
                symbols = self.helper.getSymbolsFromText(text)
                if(len(symbols) == 0):
                    return True

                prediction = self.classifier.getPrediction(text)
                probaBull = int(self.classifier.getBullishPropa(text) * 100)
                probaBear = int(self.classifier.getBearishPropa(text) * 100)

                for symbol in symbols:
                    print('**********************************************************************')
                    print(symbol)
                    print(created_at)
                    print(text)
                    print(prediction)
                    print('Bullish: ' + str(probaBull))
                    print('Bearish: ' + str(probaBear))
                    print('**********************************************************************')
                    self.model.putTweet(symbol, created_at, text, prediction, probaBull, probaBear)
            return True
        except BaseException as e:
            logging.error("Error on_data %s" % str(e))
        return True
          
    def on_error(self, status):
        logging.error(str(status))

class MyHelper():
    
    def __init__(self):
        self.wl = Watchlist()

    def getTextFromTweet(self, tweet_json):
        """
        extracts the text from a tweet
        """
        text = ''

        if('retweeted_status' in tweet_json):
            #retweet
            if(tweet_json['retweeted_status']['truncated'] == True):
                #truncated
                text = tweet_json['retweeted_status']['extended_tweet']['full_text']
            else:
                #not truncated
                text = tweet_json['retweeted_status']['text']
        else:
            #no retweet
            if(tweet_json['truncated'] == True):
                #truncated
                text = tweet_json['extended_tweet']['full_text']
            else:
                #not truncated
                text = tweet_json['text']

        #remove \n\t from text
        text = re.sub('\t', '', text)
        text = re.sub('\n', '', text)

        return text
    
    def getSymbolsFromText(self, text):
        """
        returns all symbols/tickers found in a given text
        @return: a list of all stock symbols/tickers found (e.g. ['$AMZN', '$AAPL'])
                 which are on the global watchlist
        """
        regex = r'\$[\S]+'
        matches = re.findall(regex, text)
        matches = [symbol.upper() for symbol in matches]

        #get all symbols of stocks on the watchlist which are active for twitter_stream
        activeSymbols = list(map(lambda stock: '$' + stock, self.wl.getSymbols(
            filterExpression='#twitter_stream_active = :true', 
            expressionAttributeValues={':true': True}, 
            expressionAttributeNames = {'#name': 'name', '#twitter_stream_active': 'twitter_stream_active'}
        )))
        
        #filter out symbols which are not on the watchlist
        matches = [symbol for symbol in matches if symbol in activeSymbols]

        return matches

class MyClassifier():

    def __init__(self):
        """
        - Downloads the classifier model from a S3 Bucket
        - Deserializes and initiates the classifier model
        """
        # dill deserialisation hack
        # https://stackoverflow.com/questions/42960637/python-3-5-dill-pickling-unpickling-on-different-servers-keyerror-classtype
        dill._dill._reverse_typemap['ClassType'] = type

        #Download the latest model from a S3 bucket
        s3_client = boto3.client('s3')
        s3_client.download_file(config.MODEL_BUCKET_NAME, config.MODEL_FILE_NAME, config.MODEL_FILE_NAME) 

        #load model
        self.model = dill.load(open(config.MODEL_FILE_NAME, 'rb'))['model']

    def getPrediction(self, text):
        """
        @return: 'Bullish' or 'Bearish'
        """
        return self.model.predict([text])[0]

    def getBullishPropa(self, text):
        """
        @return: the propability of the text being Bullish
        """
        return self.model.predict_proba([text])[0][1]

    def getBearishPropa(self, text):
        """
        @return: the propability of the text being Baerish
        """
        return self.model.predict_proba([text])[0][0]

def main():
    logging.basicConfig(filename='mylog_' + time.strftime("%Y-%m-%d-%H-%M-%S") + '.log', level=logging.INFO, format='%(asctime)s : %(message)s')
    logging.info('Starting Application')

    #get symbols of stocks on the watchlist
    wl = Watchlist()
    symbols = list(map(lambda stock: '$' + stock, wl.getSymbols()))

    # start Twitter stream
    twitter_streamer = MyTwitterStreamer()
    twitter_streamer.stream_tweets(symbols)

if __name__ == '__main__':
    sleep_delay_counter = 1
    while True:
        try:
            main()
        except Exception:
            logging.exception("Fatal error in main loop")
            time.sleep(60 * sleep_delay_counter) # time to sleep in minutes
            sleep_delay_counter += 1
            logging.info('Restarting')
            continue