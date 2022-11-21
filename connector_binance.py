import requests
import pandas as pd
import json
from datetime import datetime
from pprint import pprint
import random


import conn_binance_stream as stream


class BinanceConnector:
    def __init__(self, market):
        self.proxies = [
            'neppaque5766:fac948@193.23.50.223:10177',
            'neppaque5766:fac948@109.248.7.92:10228',
            'neppaque5766:fac948@109.248.7.192:10239',
            'neppaque5766:fac948@213.108.196.235:10163',
            'neppaque5766:fac948@109.248.7.172:10196',
            'neppaque5766:fac948@213.108.196.206:10120',
            'neppaque5766:fac948@193.23.50.91:10351'
        ]
        if market == 'future':
            self.endpoint = 'https://fapi.binance.com/fapi/v1/'
            self.metrics = [
                self.add_fundings,
                self.add_open_interest,
                self.add_volumes
                ]
        elif market == 'spot':
            self.endpoint = 'https://api.binance.com/api/v3/'
            self.metrics = [
                self.add_volumes
                ]


    # getting all quotes from binance. 
    # First get all USDT quotes, then all missing BUSD's
    def add_all_quotes(self):
        r = requests.get(self.endpoint + 'exchangeInfo?permissions=["SPOT"]')
        r_json = json.loads(r.text)

        symbols = r_json['symbols']

        quotes_USDT = [x['baseAsset'] for x in symbols if 'USDT' in x['quoteAsset']]

        quotes_BUSD = [x['baseAsset'] for x in symbols if 'BUSD' in x['quoteAsset'] and 
                       x['baseAsset'] not in quotes_USDT]

        quotes_all = [x + 'USDT' for x in quotes_USDT] + \
                     [x + 'BUSD' for x in quotes_BUSD]

        quotes_df = pd.DataFrame({'quotation': quotes_all})

        # thit quotes are delisting
        drop = ['DNTUSDT', 'DNTBUSD', 'DNTBTC', 'NBSUSDT', 'BTGUSDT', 'BTGBUSD', 'BTGBTC', 'TCTUSDT', 'TCTBTC', 'VIDTUSDT']
        quotes_df = quotes_df[~quotes_df['quotation'].isin(drop)]

        #print(quotes_df.drop_duplicates().sort_values(by='quotation', ascending=True)[30:50])
        return quotes_df.drop_duplicates()


    def add_volumes(self, df_inn):
        df_ = df_inn.copy()
        df_ = df_.sort_values(by='quotation', ascending=True)
        quotes = df_['quotation'].to_list()

        r = requests.get(self.endpoint + 'ticker/24hr')
        ticker_24h_json = json.loads(r.text)
        volumes = [x['quoteVolume'] for x in ticker_24h_json 
                   if x['symbol'] in quotes]
        quotes_ = [x['symbol'] for x in ticker_24h_json 
                   if x['symbol'] in quotes]

        quotes_sorted, volumes_sorted = zip(*sorted(zip(quotes_, volumes)))
        with_volumes = pd.DataFrame({
            'quotation': quotes_sorted,
            'turnover_24h': volumes_sorted
        })

        result = df_.merge(with_volumes, how='inner', on='quotation')
        return result


    def add_fundings(self, df_inn):
        df_ = df_inn.copy()
        df_ = df_.sort_values(by='quotation', ascending=True)
        quotes = df_['quotation'].to_list()

        #print(df_quotes)
        quotes = df_['quotation'].to_list()
        quotes_, funding_rate, funding_time = \
            stream.get_last_fundings(quotes)

        with_fundings = pd.DataFrame({
            'quotation': quotes_,
            'funding_rate': funding_rate,
            'next_funding_time': funding_time
        })

        result = df_.merge(with_fundings, how='inner', on='quotation')
        return result
    

    def add_open_interest(self, df_inn):
        df_ = df_inn.copy()
        df_ = df_.sort_values(by='quotation', ascending=True)
        quotes = df_['quotation'].to_list()

        oi_list = []
        for q in quotes:
            endpoint = self.endpoint + 'openInterest'
            payload = {
                'symbol': q
            }
            try:
                r = requests.get(endpoint, params=payload)
                if r.status_code == 200:
                    oi_list.append(json.loads(r.text))
                else:
                    print('>>> error OI request:', r.status_code)
            except Exception as e:
                print(e)

        quotes_ = [x['symbol'] for x in oi_list 
                   if x['symbol'] in quotes]

        oi = [x['openInterest'] for x in oi_list 
                   if x['symbol'] in quotes]
        
        with_OI = pd.DataFrame({
            'quotation': quotes_,
            'open_interest': oi
        })

        result = df_.merge(with_OI, how='inner', on='quotation')
        return result


    def get_server_time(self):
        r = requests.get(self.endpoint + 'time')
        server_time = json.loads(r.text)['serverTime']
        return server_time


    def get_kline(self, quotation, proxy, interval, limit=15):
        endpoint = self.endpoint + 'klines'
        payload = {
            'symbol': quotation,
            'interval': interval,
            'limit': limit
        }

        df = 0
        try:
            #proxy = random.randint(0, len(self.proxies) - 1)
            proxies = {'http': proxy}
            r = requests.get(endpoint, params=payload,proxies=proxies)
            if r.status_code == 200:
                data = json.loads(r.text)
                columns = [
                    'Open time', 'Open', 'High', 'Low', 'Close',
                    'Volume', 'Close time', 'Quote asset volume',
                    'Number of trades', 'Taker buy base asset volume',
                    'Taker buy quote asset volume', 'Ignore'
                    ]
                df = pd.DataFrame(data, columns=columns)
                #print(df['Volume'], df['Quote asset volume'])
            else:
                print('>>> error OI request:', r.status_code)
        except Exception as e:
            print(e)

        result = df[[
            'Open', 'High', 'Low', 'Close', 
            'Volume', 'Quote asset volume']].astype(float)

        return result


    def get_volume_4h(self, quotation, proxy, interval = '4h', limit=1):
        return (self.get_kline(quotation, proxy, interval, limit)
                .iloc[0]['Quote asset volume'])


    def get_market_data(self):
        quotes_df = self.add_all_quotes()

        for function in self.metrics:
            result_df = function(quotes_df)
            quotes_df = result_df
        
        return result_df


if __name__ == '__main__':
    connector = BinanceConnector('spot')
    print(connector.get_kline('TCTUSDT', '5m'))
    #qqq = connector.add_all_quotes()
    #print(connector.add_volumes(qqq))
    #print(connector.get_volume_4h('BTCUSDT'))