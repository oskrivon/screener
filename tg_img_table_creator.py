import pandas as pd 
import dataframe_image as dfi


def img_table_creator(df_inn, column_name):
    df = df_inn.copy()
    path = 'screener_results/'

    if 'funding_rate' in df: # it is futures market
        df_ = df.loc[:, ['quotation', 'natr_14x5', 'natr_30x1',
                        'vol_4h', 'turnover_24h',
                        'funding_rate', 'open_interest']]
        df_.rename(columns = {'turnover_24h' : 'vol 24h', 
                            'open_interest' : 'OI', 
                            'funding_rate' : 'FR',
                            'natr_14x5': 'natr 14x5m',
                            'natr_30x1': 'natr 30x1m',
                            'vol_4h': 'vol 4h'}, inplace = True)
        
        df_.reset_index(drop=True, inplace=True)
    
        df_img = df_.style.set_properties(
            **{
                'background-color': 'magenta',
                'subset': column_name
            }
        ).format(formatter={('natr 14x5m'): '{:.2f}',
                            ('natr 30x1m'): '{:.2f}',
                            ('funding rate'): '{:.4f}'})

        if column_name == 'vol 4h':metric_name = 'vol 4h'
        if column_name == 'vol 24h':metric_name = 'vol 24h'
        if column_name == 'natr 14x5m': metric_name = 'natr 14x5m'
        if column_name == 'natr 30x1m': metric_name = 'natr 30x1m'
        if column_name == 'FR': metric_name = 'fundings'

    else:
        df_ = df.loc[:, ['quotation', 'natr_14x5', 'natr_30x1', 'vol_4h', 'turnover_24h']]
        df_.rename(columns = {'turnover_24h': 'vol 24h',
                              'vol_4h': 'vol 4h',
                              'natr_14x5': 'natr 14x5m',
                              'natr_30x1': 'natr 30x1m',}, inplace = True)

        df_.reset_index(drop=True, inplace=True)
        
        df_img = df_.style.set_properties(
            **{
                'background-color': 'magenta',
                'subset': column_name
            }
        ).format(formatter={('natr 14x5m'): '{:.2f}',
                            ('natr 30x1m'): '{:.2f}'})

        if column_name == 'vol 4h':metric_name = 'vol 4h spot'
        if column_name == 'vol 24h':metric_name = 'vol 24h spot'
        if column_name == 'natr 14x5m': metric_name = 'natr 14x5m spot'
        if column_name == 'natr 30x1m': metric_name = 'natr 30x1m spot'

    name = path + metric_name + '.png'

    dfi.export(df_img, name)

if __name__ == '__main__':
    df = pd.DataFrame([('BTCUSDT', 100000, 20000000, 0.025, 2.58, 36)],
                      columns=['quotation', 'turnover_24h', 'open_interest', 'funding_rate', 'natr', 'opopo'])
    img_table_creator(df, 'volume')

    df = pd.DataFrame([('BTCUSDT', 20000000, 2.58)],
                      columns=['quotation', 'turnover_24h', 'natr'])
    img_table_creator(df, 'natr')