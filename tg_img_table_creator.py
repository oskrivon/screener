import pandas as pd 
import dataframe_image as dfi


def img_table_creator(df_inn, column_name):
    df = df_inn.copy()
    path = 'screener_results/'

    if df.shape[1] == 7: # it is futures market
        df_ = df.loc[:, ['quotation', 'turnover_24h', 'open_interest', 'funding_rate', 'natr']]
        df_.rename(columns = {'turnover_24h' : 'volume', 
                            'open_interest' : 'OI', 
                            'funding_rate' : 'funding rate'}, inplace = True)
        
        df_.reset_index(drop=True, inplace=True)
    
        df_img = df_.style.set_properties(
            **{
                'background-color': 'magenta',
                'subset': column_name
            }
        ).format(formatter={('natr'): '{:.2f}',
                            ('funding rate'): '{:.4f}'})

        if column_name == 'volume':metric_name = 'screening'
        if column_name == 'natr':
                metric_name = 'natr'
        if column_name == 'funding rate': metric_name = 'fundings'

    elif df.shape[1] == 4:
        
        df_ = df.loc[:, ['quotation', 'natr', 'turnover_24h', 'vol_4h']]
        df_.rename(columns = {'turnover_24h' : 'volume',
                              'vol_4h': 'volume 4h'}, inplace = True)

        df_.reset_index(drop=True, inplace=True)
        
        df_img = df_.style.set_properties(
            **{
                'background-color': 'magenta',
                'subset': column_name
            }
        ).format(formatter={('natr'): '{:.2f}'})

        metric_name = 'natr_spot'
        print(df_)
        print(df_.columns)

    name = path + metric_name + '.png'

    dfi.export(df_img, name)

if __name__ == '__main__':
    df = pd.DataFrame([('BTCUSDT', 100000, 20000000, 0.025, 2.58, 36)],
                      columns=['quotation', 'turnover_24h', 'open_interest', 'funding_rate', 'natr', 'opopo'])
    img_table_creator(df, 'volume')

    df = pd.DataFrame([('BTCUSDT', 20000000, 2.58)],
                      columns=['quotation', 'turnover_24h', 'natr'])
    img_table_creator(df, 'natr')