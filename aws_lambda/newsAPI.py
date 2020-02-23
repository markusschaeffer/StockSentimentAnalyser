"""
Reads Data from various News APIs and stores it in a AWS DynamodDb Table
"""

from watchlist.model.watchlist import Watchlist
from aws_lambda.news.handler.newsAPIHandler import NewsAPIHandler
from aws_lambda.news.model.newsAPI import NewsAPI

import json
from datetime import date

if __name__ == "__main__":
    wl = Watchlist('./watchlist/', 'watchlist.txt', 'Watchlist')
    # persist the Watchlist
    # wl.addAllStocks()

    handler = NewsAPIHandler()
    model = NewsAPI('NewsAPI')

    today = date.today()

    # get symbols for stocks in watchlist
    symbols = wl.getSymbols()

    for symbol in symbols:
        print('##### Processing: ' + symbol)

        # get name for symbol
        stockName = wl.getNameForSymbol(symbol)

        # get newsAPI data for symbol
        data = handler.getDataForSymbol(symbol=symbol, stockName=stockName, from_param=today, to_param=today)

        # store newsAPI articles for symbol
        model.batchStore(data)