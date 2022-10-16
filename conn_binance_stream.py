import websocket
import json

count = 0

def on_message(ws, message):
    fundings_json = json.loads(message)
    fundings = [x['r'] for x in fundings_json if x['s'] == 'BTCUSDT']
    global count
    count += 1
    #print(fundings)
    print(count)
    if count >= 1: 
        ws.close()

def get_last_fundings(quotes):
    ws = websocket.create_connection("wss://fstream.binance.com/ws/!markPrice@arr@1s")
    fundings_json = json.loads(ws.recv())
    #print(fundings_json)

    quotes = [x['s'] for x in fundings_json if x['s'] in quotes]
    funding_rate = [x['r'] for x in fundings_json if x['s'] in quotes]
    funding_time = [x['T'] for x in fundings_json if x['s'] in quotes]

    ws.close()
    return quotes, funding_rate, funding_time

if __name__ == '__main__':
    fff, qqq = get_last_fundings()
    print(qqq, fff)
    print(len(fff), len(qqq))