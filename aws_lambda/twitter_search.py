"""
Reads Data from Twitter Search API and stores it in a AWS DynamodDb Table
"""

from watchlist.model.watchlist import Watchlist
from aws_lambda.twitter_search.handler.twitterHandler import TwitterSearchHandler
from aws_lambda.twitter_search.model.twitter import Twitter

if __name__ == "__main__":
    wl = Watchlist('./watchlist/', 'watchlist.txt', 'Watchlist')

    handler = TwitterSearchHandler()
    model = Twitter('Twitter')

    # get symbols for stocks in watchlist
    symbols = wl.getSymbols()

    for symbol in symbols:
        # get lastId to pull only new data
        lmId = wl.getLastId(symbol, 'twitter')

        # get data for symbol
        maxId, data = handler.getDataForSymbol(symbol, since=lmId)

        # store all new twitter statuses for symbol
        model.batchStore(data, symbol)

        # update lasId for symbol
        wl.updateLastId(symbol, maxId, 'twitter')

        print('##### ' + symbol + ': ' + str(len(data)) + ' New Tweets')