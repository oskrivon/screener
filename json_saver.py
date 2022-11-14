import pandas as pd
import json


df = pd.DataFrame(
    {'quotation': ['BTCUSDT', 'EHTh'],
    'natr': [0.5, 0.75]},
    index=[0, 1])

print(df)

big_json = {}

result = df.to_dict('records')
print(type(result))
print(result)
big_json['spot'] = result
big_json['future'] = result

print(big_json)

#parsed = json.dumps(big_json)
#print(type(parsed))
#print(parsed)

with open('test.json', 'w') as outfile:
    json.dump(big_json, outfile, indent=4)

#dump = json.dumps(parsed, indent=4)
#print(dump)