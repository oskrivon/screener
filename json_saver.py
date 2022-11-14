import json

def json_save(df):
    print(df)

    result_json = {}

    df_dict = df.to_dict('records')
    result_json['spot'] = df_dict

    print(result_json)

    try:
        with open('test.json', 'w') as outfile:
            json.dump(result_json, outfile, indent=4)
    except Exception as e:
        print(e)