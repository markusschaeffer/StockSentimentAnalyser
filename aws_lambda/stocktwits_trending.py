"""
Reads Data from Stocktwits Trending API and stores it in a AWS DynamodDb Table
"""

from watchlist.model.watchlist import Watchlist
from aws_lambda.stocktwits.handler.stocktwitsAPIHandler import StocktwitsAPIHandler
from aws_lambda.stocktwits.model.trending import Trending
import json

if __name__ == "__main__":
    
    handler = StocktwitsAPIHandler()
    model = Trending('Stocktwits_Trending')

    # get trending equities
    data = handler.getTrending()

    #print(json.dumps(data, indent=4, sort_keys=True))
    
    #store data
    model.store(data)