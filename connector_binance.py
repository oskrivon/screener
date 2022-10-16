import requests
import pandas as pd
import json
from datetime import datetime
from pprint import pprint


import conn_binance_stream as stream


class BinanceConnector:
    def __init__(self):
        self.endpoint = 'https://fapi.binance.com/'


    # getting all quotes from binance. 
    # First get all USDT quotes, then all missing BUSD's
    def add_all_quotes(self):
        r = requests.get(self.endpoint + 'fapi/v1/exchangeInfo')
        r_json = json.loads(r.text)

        symbols = r_json['symbols']
        quotes_USDT = [x['baseAsset'] for x in symbols if 'USDT' in x['quoteAsset']]

        quotes_BUSD = [x['baseAsset'] for x in symbols if 'BUSD' in x['quoteAsset'] and 
                       x['baseAsset'] not in quotes_USDT]

        quotes_all = [x + 'USDT' for x in quotes_USDT] + \
                     [x + 'BUSD' for x in quotes_BUSD]

        quotes_df = pd.DataFrame({'quotation': quotes_all})
        #print(quotes_df.drop_duplicates().sort_values(by='quotation', ascending=True)[30:50])
        return quotes_df.drop_duplicates()


    def add_volumes(self, df_inn):
        df_ = df_inn.copy()
        df_ = df_.sort_values(by='quotation', ascending=True)
        quotes = df_['quotation'].to_list()

        r = requests.get(self.endpoint + 'fapi/v1/ticker/24hr')
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
            endpoint = self.endpoint + 'fapi/v1/openInterest'
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
        r = requests.get(self.endpoint + 'fapi/v1/time')
        server_time = json.loads(r.text)['serverTime']
        return server_time


    def get_kline(self, quotation, interval):
        endpoint = self.endpoint + 'fapi/v1/klines'
        payload = {
            'symbol': quotation,
            'interval': interval,
            'limit': 15
        }

        df = 0
        try:
            r = requests.get(endpoint, params=payload)
            if r.status_code == 200:
                data = json.loads(r.text)
                columns = [
                    'Open time', 'Open', 'High', 'Low', 'Close',
                    'Volume', 'Close time', 'Quote asset volume',
                    'Number of trades', 'Taker buy base asset volume',
                    'Taker buy quote asset volume', 'Ignore'
                    ]
                df = pd.DataFrame(data, columns=columns)
            else:
                print('>>> error OI request:', r.status_code)
        except Exception as e:
            print(e)
        
        result = df[['Open', 'High', 'Low', 'Close', 'Volume']].astype(float)
        return result


    def get_market_data(self):
        quotes_df = self.add_all_quotes()
        metrics = [
            self.add_fundings,
            self.add_open_interest,
            self.add_volumes
        ]

        for function in metrics:
            result_df = function(quotes_df)
            quotes_df = result_df
        
        return result_df


if __name__ == '__main__':
    connector = BinanceConnector()
    print(connector.get_kline('BTCUSDT'))
    #df = pd.DataFrame({'quotation': quotation})
    #print(connector.get_fundings(df))