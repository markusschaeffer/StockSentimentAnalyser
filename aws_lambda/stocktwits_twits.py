"""
Reads Data from Stocktwits Search API and stores it in a AWS DynamodDb Table
"""

from watchlist.model.watchlist import Watchlist
from aws_lambda.stocktwits.handler.stocktwitsAPIHandler import StocktwitsAPIHandler
from aws_lambda.stocktwits.model.stocktwits import Stocktwits
import json

if __name__ == "__main__":
    wl = Watchlist('./watchlist/', 'watchlist.txt', 'Watchlist')

    handler = StocktwitsAPIHandler()
    model = Stocktwits('Stocktwits')

    # get symbols for stocktwits relevant stocks
    symbols = wl.getSymbols(
        filterExpression='#stocktwits = :true', expressionAttributeValues={':true': True})

    for symbol in symbols:
        print('##### Processing: ' + symbol)

        # get lastMessageId to pull only new messages from stocktwits
        #lmId = wl.getLastId(symbol, 'stocktwits')

        # get stocktwits data for symbol
        #data, maxMessageId = handler.getDataForSymbol(symbol, lmId)
        data, maxMessageId = handler.getDataForSymbol(symbol)

        #print(json.dumps(data, indent=4, sort_keys=True))
        # store all new twits for symbol
        model.store(data)

        # update lastMessageId for symbol
        #wl.updateLastId(symbol, maxMessageId, 'stocktwits')