import requests
import json
from . import const
from . import config

class NewsAPIHandler:
    '''
    see https://newsapi.org/docs for documentation
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_everything(
        self,
        q=None,
        qintitle=None,
        sources=const.SOURCES,
        from_param=None,
        to=None,
        language='en',
        sort_by='relevancy',
        page=1,
        page_size=100,
    ):
        '''
        Calls the /everything endpoint.
        '''

        payload = {}
        payload["apiKey"] = config.newsapi_key  # apiKey
        payload["q"] = q  # keyword/phrase
        payload["qintitle"] = qintitle  # keyword/phrase in title
        payload["sources"] = sources  # sources
        payload["from"] = from_param  # from-date
        payload["to"] = to  # to-date
        payload["language"] = language  # language
        payload["sortBy"] = sort_by  # sort method
        payload["pageSize"] = page_size  # page size
        payload["page"] = page  # page

        # send request
        response = requests.get(const.EVERYTHING_URL, timeout=30, params=payload)

        # check response status
        if response.status_code != requests.codes.ok:
            raise Exception(response.json())

        return response.json()['articles']

    def getDataForSymbol(self, symbol, stockName, from_param, to_param):

        articles = self.get_everything(q=stockName, from_param=from_param, to=to_param)

        dataList = []
        for article in articles:
            data = {
                'symbol': symbol,
                'date': str(from_param),
                'source': article['source']['id'],
                'title': article['title'],
                'description': str(article['description']),
                'content': str(article['content']),
                'url': str(article['url'])
            }
            dataList.append(data)
        return dataList
