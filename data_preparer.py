import pandas as pd

import df_common as dfc


# interval example: '15m'
def data_preparation(quotation, interval):
    df_path = 'market_history/' + quotation + '.csv'
    df_raw = pd.read_csv(df_path)

    # open datasets and create dataframe
    df = dfc.dataframe_create(df=df_raw,
                              drop=['symbol', 'tickDirection', 'trdMatchID', 
                                    'side', 'grossValue', 'homeNotional', 
                                     'foreignNotional'
                                     ],
                              timestamp = 's'
                              )
    
    minutes_per_unit = {"m": 1, "h": 60, "d": 1440}

    def convert_to_minutes(s):
        return int(s[:-1]) * minutes_per_unit[s[-1]]

    interval_ = convert_to_minutes(interval)

    # gpouping data to tameframe
    grouped_price = dfc.grouping_by_time(df, str(interval_)+'min')

    # update
    grouped_price_update = dfc.data_update(grouped_price, quotation, interval_)

    return grouped_price_update