from datetime import datetime
import pandas as pd


def msg_formatter(screening, header, funding_flag=False):
    msg = header + '\n'
    if funding_flag: # True - if need return the time of funding
        msg += 'upcoming funding: ' + \
            str(screening[1]) + '\n'
            #str(pd.to_datetime(screening[1], format='%H:%M:%S')) + '\n'
    msg += 'quotation: 24h vol | OI | funding rate | natr' + '\n'
    for row in screening[0].itertuples():
        quot = row.quotation
        volume = num_formatter(row.turnover_24h)
        oi = num_formatter(row.open_interest)
        funding_rate = '{:.5f}'.format(round(row.funding_rate, 5))
        natr = round(row.natr, 2)

        msg += quot + ': $' + str(volume) + ' | ' + str(oi) + ' | ' + \
            str(funding_rate) + ' | ' + str(natr) + '\n'

    return msg


def msg_copy_tickers_formatter(screening):
    msg = 'click\/tap on ticker to copy' + '\n' + '\n'
    for row in screening.itertuples():
        msg += '`' + str(row.quotation) + '`' + '   '

    return msg


def df_formatter(df):
    df_format = df.copy()
    if 'turnover_24h' in df_format:
        df_format['turnover_24h'] = \
            df_format['turnover_24h'].apply(lambda x: num_formatter(x))
    if 'vol_4h' in df_format:
        df_format['vol_4h'] = \
            df_format['vol_4h'].apply(lambda x: num_formatter(x))
    if 'open_interest' in df_format:
        df_format['open_interest'] = \
            df_format['open_interest'].apply(lambda x: num_formatter(x))
    if 'funding_rate' in df_format:
        df_format['funding_rate'] = \
            df_format['funding_rate'].apply(lambda x: x*100)
    if 'natr_14x5' in df_format:
        df_format['natr_14x5'] = \
            df_format['natr_14x5'].apply(lambda x: x)
    if 'natr_30x1' in df_format:
        df_format['natr_30x1'] = \
            df_format['natr_30x1'].apply(lambda x: x)
    #print(df_format)
    return df_format


def num_formatter(num):
    num = float('{:.3g}'.format(num))
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'),
                         ['', 'K', 'M', 'B', 'T'][magnitude])


def date_formatter(date_time_str):
    date = datetime.fromisoformat(date_time_str[:-1])
    return date.strftime('%m-%d %H:%M:%S')