import numpy as np
import pandas as pd
from datetime import datetime
import multiprocessing

import talib
from pybit import inverse_perpetual

import data_preparer
import connector_binance as binance

class Screener:
    def __init__(self, exchange, market):
        self.exchange = exchange
        self.market = market

        if self.exchange == 'bybit':
            self.session = inverse_perpetual.HTTP(
                    endpoint="https://api.bybit.com"
                    )

            # get all quotes from bybit
            self.quotation = []
            result = self.session.query_symbol()['result']
            for r in result:
                name = r['name']
                if name[-1] == 'T': self.quotation.append(r['name'])

            self.df = pd.DataFrame({'quotation': self.quotation})
            
        if self.exchange == 'binance':
            self.proxies = [
                'vHqe1yNDmpfIcr:neppaque@185.252.27.202:54168',
                'vHqe1yNDmpfIcr:neppaque@176.114.6.251:54168',
                'vHqe1yNDmpfIcr:neppaque@193.42.105.50:54168',
                'vHqe1yNDmpfIcr:neppaque@185.230.91.69:54168',
                #'neppaque5766:fac948@109.248.7.172:10196',
                #'neppaque5766:fac948@213.108.196.206:10120',
                #'neppaque5766:fac948@193.23.50.91:10351'
            ]

            self.connector = binance.BinanceConnector(self.market)

        print('>>> Screener OK')


    def get_quotes_from_ex(self, connector):
        return connector.get_all_quotes()


    def get_market_metrics(self):
        if self.exchange == 'bybit':
            df_ = self.df.copy()
            df_ = df_.sort_values(by='quotation', ascending=True)
            quotes = df_['quotation'].to_list()

            data = []
            for q in quotes:
                try:
                    data.append(
                        self.session.latest_information_for_symbol(
                        symbol=q)['result'][0])
                except Exception as e:
                    print(e)

            quotes_ = [x['symbol'] for x in data if x['symbol'] in quotes]
            turnover_24h = [x['turnover_24h'] for x in data if x['symbol'] in quotes]
            open_interest = [x['open_interest'] for x in data if x['symbol'] in quotes]
            funding_rate = [x['funding_rate'] for x in data if x['symbol'] in quotes]
            next_funding_time = [x['next_funding_time'] for x in data if x['symbol'] in quotes]

            df = pd.DataFrame({
                'quotation': quotes_,
                'turnover_24h': turnover_24h,
                'open_interest': open_interest,
                'funding_rate': funding_rate,
                'next_funding_time': next_funding_time
            })
        
        if self.exchange == 'binance':
            df = self.connector.get_market_data()

        # for spot market funding rate and open interest are not exist
        if 'funding_rate' in df:
            df[['turnover_24h', 'open_interest', 'funding_rate']] = \
                df[['turnover_24h', 'open_interest', 'funding_rate']].astype(float)
        else:
            df[['turnover_24h']] = df[['turnover_24h']].astype(float)

        # drop all coins with 24h volume less that $1k 
        df = df[df.turnover_24h > 1000]
        return df


    # return 14x5 and 30x1 natres
    def get_natr(self, quotation, proxy):
        natr_14x5 = 0
        natr_30x1 = 0

        if self.exchange == 'bybit':
            df = data_preparer.data_preparation(quotation, '15m')
        
        if self.exchange == 'binance':
            df = self.connector.get_kline(quotation, proxy, '5m')

            x = 100 # magick number, if < natr be worst
            timeperiod = 14
            
            high = np.array(df['High'])[-x:]
            low = np.array(df['Low'])[-x:]
            close = np.array(df['Close'])[-x:]

            natr_14x5 = talib.NATR(high, low, close, timeperiod)[-1]
            

            df = self.connector.get_kline(quotation, proxy, '1m', limit=31)

            x = 100 # magick number, if < natr be worst
            timeperiod = 30
            
            high = np.array(df['High'])[-x:]
            low = np.array(df['Low'])[-x:]
            close = np.array(df['Close'])[-x:]

            natr_30x1 = talib.NATR(high, low, close, timeperiod)[-1]

        return natr_14x5, natr_30x1


    # in addition to the natr, add the 4h volumes
    def add_natr(self, metrics, proxy):
        metrics_ = metrics.copy()

        natr_14x5 = []
        natr_30x1 = []
        vol_4h = []

        for row in metrics_.itertuples():
            quotation = row.quotation
            natr_14x5.append(self.get_natr(quotation, proxy)[0])
            natr_30x1.append(self.get_natr(quotation, proxy)[1])
            vol_4h.append(self.connector.get_volume_4h(quotation, proxy))
        
        metrics_['natr_14x5'] = natr_14x5
        metrics_['natr_30x1'] = natr_30x1
        metrics_['vol_4h'] = vol_4h
        return metrics_

    
    def get_top_natr(self, num=10):
        market_metrics = self.get_market_metrics()
        all_market_natr = self.add_natr(market_metrics)
        sorted_df = all_market_natr.sort_values(by='natr', 
                                                ascending=False)
        top_10_natr = sorted_df[:num]

        analysis_time = datetime.utcnow()
        return top_10_natr, analysis_time

    def get_upcoming_fundings(self, num=10):
        market_metrics = self.get_market_metrics()
        upcoming_time_row = market_metrics['next_funding_time'].min()
        upcoming_fundings = \
            market_metrics[market_metrics['next_funding_time'] == upcoming_time_row]
        sorted_df = upcoming_fundings.sort_values(
            by=['funding_rate'],
            key=abs,
            ascending=False
            )
        top_10_fund = sorted_df[:num]

        upcoming_time = datetime.fromtimestamp(int(upcoming_time_row)/1000).strftime('%Y-%m-%d %H:%M:%S')
        return self.add_natr(top_10_fund), upcoming_time

    
    def get_screening(self):
        market_metrics = self.get_market_metrics()

        # splitting a dataframe into chunk
        chunk_size = len(market_metrics) // len(self.proxies)
        dfs = []
        i = 0
        while i < len(market_metrics):
            dfs.append(market_metrics[i:i + chunk_size])
            i += chunk_size

        pool = multiprocessing.Pool(len(self.proxies))
        result = pool.starmap(self.add_natr, zip(dfs, self.proxies))

        mm_with_natres = pd.concat(result, sort=False, axis=0)

        return mm_with_natres


if __name__ == '__main__':
    screener = Screener('binance', 'spot')
    #mmm = screener.get_market_metrics()
    #print(mmm)
    #print(screener.get_screening())
    #print(screener.get_natr('BTCUSDT'))
    print(screener.get_screening())