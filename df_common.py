import pandas as pd
import datetime

from pybit import usdt_perpetual

def dataframe_create(df, **kwargs):
    for i in kwargs.items():
        if i[0] == 'drop':
            df_total = df.drop(i[1], axis=1)
        elif i[0] == 'timestamp':
            df_total['timestamp'] = pd.to_datetime(df_total['timestamp'], unit=i[1])
    return df_total

def grouping_by_time(df, frequency = '1m', round = 5):
    grouped_price = df.groupby([pd.Grouper(
    key='timestamp', freq=frequency)]).agg(
        Open = ('price', 'first'),
        High = ('price', 'max'),
        Low = ('price', 'min'),
        Close = ('price', 'last'),
        Volume = ('size', 'sum'), ).round(round)

    # clearing nan-values
    grouped_price = grouped_price.fillna(method="ffill")
    grouped_price = grouped_price.fillna(method="bfill")
    return grouped_price

def latest_data_update(df, quotation, interval = 1):
    last_df_date = df.index[-1]
    date_end = datetime.datetime.now()

    # UTC correction
    date_start = last_df_date + datetime.timedelta(hours=3, minutes=1)

    start_unix = int(datetime.datetime.timestamp(date_start))
    now_unix = int(datetime.datetime.timestamp(date_end))

    # get last data from bybit
    session_unauth = usdt_perpetual.HTTP(endpoint="https://api.bybit.com")

    try:
        data = session_unauth.query_kline(
            symbol=quotation,
            interval=interval,
            #limit=200,
            from_time=start_unix
            )

        # raw data from bybit -> df
        # see more https://bybit-exchange.github.io/docs/linear/#t-querykline
        df_new = pd.DataFrame(data['result'])

        df_new = df_new.drop(['id', 'symbol', 'period', 
                      'interval', 'turnover', 
                      'open_time'], 
                      axis=1)
        
        df_new = df_new[['start_at', 'open','high','low','close', 'volume']]

        df_new.rename(columns={'start_at': 'timestamp', 
                           'open': 'Open', 'high': 'High', 
                           'low': 'Low', 'close': 'Close', 
                           'volume': 'Volume'}, inplace=True)

        df_new['timestamp'] = pd.to_datetime(df_new['timestamp'], unit='s')
        df_new = df_new.set_index('timestamp')

        df_new = df_new[['Open', 'Close', 'High', 'Low', 'Volume']].astype(float)

        # clearing nan-values
        df_new = df_new.fillna(method="ffill")
        df_new = df_new.fillna(method="bfill")

        df_concat = pd.concat([df, df_new])
        
    except Exception as e:
        #print('from "df_common", for ', quotation, ': ', e)
        df_concat = df
        raise

    return df_concat

def data_update(df, quotation, interval):
    last_df_date = df.index[-1]
    date_end = datetime.datetime.utcnow()

    # UTC correction
    date_start = last_df_date + datetime.timedelta(hours=3, minutes=1)

    start_unix = int(datetime.datetime.timestamp(date_start))
    now_unix = int(datetime.datetime.timestamp(date_end))

    delta = now_unix - start_unix
    th = 100

    while delta > th:
        try:
            df = latest_data_update(df, quotation, interval)

            start_unix = int(datetime.datetime.timestamp(df.index[-1]))
            delta = now_unix - start_unix
        except Exception as e:
            #print('from "df_common", func "data_update" - break')
            break
    
    return df