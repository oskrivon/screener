import json
import pandas as pd


def json_save(spot_5, spot_1, future_5, future_1):
    result_json = {}

    spot_5_dict = spot_5.to_dict('records')
    spot_1_dict = spot_1.to_dict('records')
    future_5_dict = future_5.to_dict('records')
    future_1_dict = future_1.to_dict('records')

    result_json['spot_5'] = spot_5_dict
    result_json['spot_1'] = spot_1_dict
    result_json['future_5'] = future_5_dict
    result_json['future_1'] = future_1_dict

    print(result_json)

    try:
        with open('test.json', 'w') as outfile:
            json.dump(result_json, outfile, indent=4)
    except Exception as e:
        print(e)

if __name__ == '__main__':
    df = pd.DataFrame(
        {'quotation': ['BTCUSDT', 'EHTh'],
        'natr': [0.5, 0.75]},
        index=[0, 1])
    df_empty = pd.DataFrame()

    json_save(df, df, df, df_empty)
