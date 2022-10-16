import pandas as pd 
import dataframe_image as dfi


def img_table_creator(df_inn, column_name):
    df = df_inn.copy()
    path = 'screener_results/'

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
    )

    if column_name == 'volume':metric_name = 'screening'
    if column_name == 'natr': metric_name = 'natr'
    if column_name == 'funding rate': metric_name = 'fundings'

    name = path + metric_name + '.png'

    dfi.export(df_img, name)