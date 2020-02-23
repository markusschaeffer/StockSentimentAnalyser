import tweepy  # http://docs.tweepy.org/en/latest/getting_started.html
import json
import aws_lambda.twitter_search.handler.config as config
import aws_lambda.twitter_search.handler.blacklist as blacklist
import re


class TwitterSearchHandler:

    def __init__(self):
        #use OAuthHandler (18,000 tweets/15-min)
        #self.auth = tweepy.OAuthHandler(config.twitter_consumerKey, config.twitter_consumerSecret)
        #self.auth.set_access_token(config.twitter_accessToken, config.twitter_accessTokenSecret)

        # use AppAuthHandler (45,000 tweets/15-min)
        self.auth = tweepy.AppAuthHandler(
            config.twitter_consumerKey, config.twitter_consumerSecret)
        self.api = tweepy.API(self.auth, wait_on_rate_limit=False)
        self.remainingAPICalls = self.getRemainingCalls()

    def getRemainingCalls(self):
        data = self.api.rate_limit_status()
        remainingCalls = int(data['resources']['search']
                             ['/search/tweets']['remaining'])
        return remainingCalls

    def preprocess(self, tweets, symbol):

        tweets = list(tweets)
        #print('#Tweets raw: ' + str(len(tweets)))

        # filter tweets from users on the blacklist
        tweets = list(filter(lambda tweet: str(tweet.user.screen_name) not in blacklist.twitter_user_blacklist, tweets))
        #print('#Tweets after user blacklist filter: ' + str(len(tweets)))

        # filter spam (user has to have more than 50 followers)
        tweets = list(filter(lambda tweet: int(
            tweet.user.followers_count) >= 50, tweets))
        #print('#Tweets after spam filter: ' + str(len(tweets)))

        # filter not containing symbol
        tweets = list(filter(lambda tweet: int(
            tweet.full_text.count(symbol)) >= 1, tweets))
        #print('#Tweets after not containing symbol filter: ' + str(len(tweets)))

        # filter multi stock symbol messages/statuses
        tweets = list(filter(lambda tweet: int(
            tweet.full_text.count('$')) == 1, tweets))
        #print('#Tweets after multi stock filter: ' + str(len(tweets)))

        for tweet in tweets:
            #remove URLS from text
            tweet.full_text = re.sub(r'http\S+', '', tweet.full_text)
            #remove \n and \t
            tweet.full_text = re.sub('\t', '', tweet.full_text)
            tweet.full_text = re.sub('\n', '', tweet.full_text)
            #remove all hastag mentions
            tweet.full_text = re.sub(r'#\S+', '', tweet.full_text)

        #remove duplicate entries
        fulltexts = []
        idsToBeRemoved = []
        for tweet in tweets:
            if(tweet.full_text.strip() in fulltexts):
                idsToBeRemoved.append(tweet.id)
            else:
                fulltexts.append(tweet.full_text.strip())
        tweets = list(filter(lambda tweet: tweet.id not in idsToBeRemoved, tweets))

        return tweets

    def getDataForSymbol(self, symbol, since=0, items=500):
        tweets = []
        maxId = 0
        symbol = '$' + symbol
        query = symbol + ' -filter:retweets'  # remove retweets
        if(self.remainingAPICalls >= 0):
            # searching for tweets
            # https://developer.twitter.com/en/docs/tweets/search/api-reference/get-search-tweets
            tweets = tweepy.Cursor(self.api.search, q=query, tweet_mode='extended',
                                   lang="en", count=100, since_id=since).items(items)
            self.remainingAPICalls = self.getRemainingCalls()
            tweets = self.preprocess(tweets, symbol)
            if(len(tweets) > 0):
                maxId = tweets[0].id
        else:
            print('>>>Rate Limit reached!')

        return maxId, tweets
