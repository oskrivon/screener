import numpy as np
import pandas as pd
from datetime import datetime

import talib
from pybit import inverse_perpetual

import data_preparer
import connector_binance as binance

test_list = \
    [['BTCUSD', 10943347813600.0, 451577968, '2022-09-17T16:00:00Z'], 
    ['ETHUSD', 449309338887.8, 226872792, '2022-09-17T16:00:00Z'], 
    ['BTCUSDT', 3008518839.85, 48759.57, '2022-09-17T16:00:00Z'], 
    ['ETHUSDT', 2501056220.6879997, 341516.89, '2022-09-17T16:00:00Z'], 
    ['ATOMUSDT', 263943453.675, 2127719.2, '2022-09-17T16:00:00Z'], 
    ['LUNA2USDT', 217587574.774, 4068263, '2022-09-17T16:00:00Z'], 
    ['ETCUSDT', 177998703.021, 976940.6, '2022-09-17T16:00:00Z'], 
    ['LTCUSD', 126658627.55, 5729037, '2022-09-17T16:00:00Z'], 
    ['1000LUNCUSDT', 124985615.55559999, 39915343, '2022-09-17T16:00:00Z'],
    ['SOLUSD', 123247447.18, 2714407, '2022-09-17T16:00:00Z']]

class Screener:
    def __init__(self, ex_flag):
        self.exchange = ex_flag

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
            self.connector = binance.BinanceConnector()            

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

        df[['turnover_24h', 'open_interest', 'funding_rate']] = \
            df[['turnover_24h', 'open_interest', 'funding_rate']].astype(float)
        #df[['next_funding_time']] = df[['next_funding_time']].astype(int)

        #date = datetime.fromisoformat(data['next_funding_time'][:-1])

        return df


    def get_natr(self, quotation):
        if self.exchange == 'bybit':
            df = data_preparer.data_preparation(quotation, '15m')
        
        if self.exchange == 'binance':
            df = self.connector.get_kline(quotation, '5m')

            x = 100 # magick number, if < natr be worst
            timeperiod = 14
            
            high = np.array(df['High'])[-x:]
            low = np.array(df['Low'])[-x:]
            close = np.array(df['Close'])[-x:]


        natr = talib.NATR(high, low, close, timeperiod)[-1]

        return natr


    def add_natr(self, metrics):
        metrics_ = metrics.copy()

        natr = []
        for row in metrics_.itertuples():
            quotation = row.quotation
            natr.append(self.get_natr(quotation))
        
        metrics_['natr'] = natr
        return metrics_

    
    def get_screening(self, num=10):
        market_metrics = self.get_market_metrics()
        sorted_df = market_metrics.sort_values(by=['turnover_24h'], 
                                               ascending=False)
        top_10_vol = sorted_df[:num]

        analysis_time = datetime.utcnow()
        return self.add_natr(top_10_vol), analysis_time

    
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
        sorted_df = upcoming_fundings.sort_values(by=['funding_rate'], 
                                                  ascending=False)
        top_10_fund = sorted_df[:num]

        upcoming_time = datetime.fromtimestamp(int(upcoming_time_row)/1000).strftime('%Y-%m-%d %H:%M:%S')
        return self.add_natr(top_10_fund), upcoming_time


if __name__ == '__main__':
    screener = Screener('binance')
    print(screener.get_top_natr())
    #metrics = screener.get_market_metrics()
    #print(screener.get_upcoming_fundings())
    #print(screener.sorting(metrics, False, param=4))
    #top_10_vol = screener.get_top()
    #print(screener.add_natr(test_list))
    #print(screener.get_screening())