from email import message_from_binary_file
import requests
import pandas as pd
import json
import yaml
import time
import threading
from multiprocessing import Process, Pipe, Queue
import schedule
from datetime import datetime

import market_screener as ms
import tg_msg_preparer as msg_preparer
import tg_msg_sender as msg_sender
import tg_img_table_creator as img_creator
import json_saver as saver


class ScreenerBot:
    def __init__(self):
        path = 'screener_token.txt'
        self.TOKEN = open(path, 'r').read()
        self.URL = 'https://api.telegram.org/bot'
        self.users_list = 'users.yaml'

        self.thread_go = True

        self.proxies = {'http': 'http://neppaque5766:fac948@193.23.50.40:10251'}

        self.users = []
        with open('users.yaml') as f:
            self.users = yaml.load(f, Loader=yaml.SafeLoader)
        f.close()

        self.screener_future = ms.Screener('binance', 'future')
        self.screener_spot = ms.Screener('binance', 'spot')
        self.sender = msg_sender.TgSender(self.TOKEN, self.URL)

        gag_later = 'creating an up\-to\-date screening in progress'

        self.msg_screening = gag_later
        self.msg_natrs = gag_later
        self.msg_fundings = gag_later
        self.msg_natrs_spot = gag_later
        self.funding_time = ''

        self.tickers_fut_vol_4h = gag_later
        self.tickers_fut_vol_24h = gag_later
        self.tickers_fut_natr_14x5 = gag_later
        self.tickers_fut_natr_30x1 = gag_later
        self.tickers_fut_fund = gag_later

        self.tickers_spot_vol_4h = gag_later
        self.tickers_spot_vol_24h = gag_later
        self.tickers_spot_natr_14x5 = gag_later
        self.tickers_spot_natr_30x1 = gag_later

        self.msg_log = 'msg_log.csv'


    def get_updates(self, offset=0):
        result = requests.get(f'{self.URL}{self.TOKEN}/getUpdates?offset={offset}',
            proxies=self.proxies).json()
        #print(len(result))
        return result['result']


    def funding_reaction(self, chat_id):
        if (len(str(self.funding_time)) > 0):
            chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
            time = str(self.funding_time)
            escaping_list = []
            final_str = ''

            time_list = list(time)
            for val in time_list:
                if val in chars:
                    escaping_list.append('\\')
                    escaping_list.append(val)
                else:
                    escaping_list.append(val)

            escaping_time = final_str.join(escaping_list)

        else:
            escaping_time = ''

        caption = 'next funding time\\: ' + escaping_time + '\n' + '\n' + self.tickers_fut_fund
        self.sender.send_photo(chat_id, 'screener_results/' + 'fundings' + '.png', caption)

    
    def check_message(self, msg, chat_id):
        global thread_go

        if msg == '/start':
            self.users_update(chat_id)
            self.reply_keyboard(chat_id)
        
        if msg == 'help':
            msg_ = ('buttons description' + '\n' + '\n' +
                    'spot: show spot market metrics' + '\n' +
                    'future: show futures market metrics' + '\n' + '\n' +
                    'VOL 4h: get the top 10 quotes with the highest volumes (last 4 hours, $)' + '\n' + 
                    'VOL 44h: get the top 10 quotes with the highest volumes (last 24 hours, $)' + '\n' + '\n' + 
                    'NATR 14x5m: get the top 10 quotes with the highest NATR for the current market (14 5-minute candles)' + '\n' +
                    'NATR 30x1m: get the top 10 quotes with the highest NATR for the current market (30 1-minute candles)' + '\n' + '\n' +
                    'FUNDING: get the top quotes with the highest fundings rate' + '\n' + '\n' + 
                    'data source: Binance' + '\n' +
                    '________________' + '\n' +
                    'описание кнопок' + '\n' + '\n' +
                    'spot: показать метрики спотового рынка' + '\n' + 
                    'future: показать метрики фьючерсного рынка' + '\n' + '\n' +
                    'VOL 4h: получить топ-10 котировок с самыми высокими объемами(последние 4 часа, $)' + '\n' +
                    'VOL 24h: получить топ-10 котировок с самыми высокими объемами(последние 24 часа, $)' + '\n' + '\n' + 
                    'NATR 14x5m: получить топ-10 котировок с самым высоким NATR для текущего рынка (14 5-минутных свечей)' + '\n' +
                    'NATR 30x1m: получить топ-10 котировок с самым высоким NATR для текущего рынка (30 минутных свечей)' + '\n' + '\n' +
                    'FUNDING: получить топ котировок с самой высокой ставкой финансирования' + '\n' + '\n' +
                    'источник данных: Binance' 
                )
            self.sender.send_message(chat_id, msg_)

        if msg == 'roadmap':
            msg_ = 'project roadmap:' + '\n'  + \
                '1. screening module for three metrics: funding, volumes, natr' + '\n' + \
                '2. telegram bot, user interface' + '\n' + \
                '3. server start' + '\n' + \
                '4. closed test' + '\n' + \
                '5. functionality improvements' + '\n' + \
                '6. open test' + '\n' + \
                '7. audience interest analysis' + '\n' + \
                '8. add spot market' + '\n' + \
                '9. add several NATRes and volumes' + '\n' + \
                '10. site development' + '\n' + \
                '11. site launch' + '\n' + \
                '12. signals within the bot' + '\n' + \
                '13. module for analyzing support and resistance levels' + '\n' + \
                '14. integration of levels into the bot' + '\n' + \
                '15. app development' + '\n' + \
                '16. app launch'
            self.sender.send_message(chat_id, msg_)
        
        if msg == 'feedback':
            msg_ = 'Group for discussions and suggestions' + '\n' + '\n' + \
                '[Discuss](https://t.me/stockSlicerScreener)'
            
            self.sender.send_message(chat_id, msg_, True)

        if msg == 'uc':
            self.sender.send_message(chat_id, str(len(self.users)))

        if msg == 'tc':
            try:
                df_log = pd.read_csv(self.msg_log)
                self.sender.send_message(chat_id, str(len(df_log)))
                df_log = pd.DataFrame()
            except Exception as e:
                self.sender.send_message(1109752742, 'read msg log error: ' + str(e))
        
        if msg == 'go':
            self.thread_go = True
        
        if msg.startswith('bot update 235813'):
            msg = msg.replace('bot update 235813', '')

            def broadcast():
                for user in self.users:
                    self.sender.send_message(user, msg)
                    time.sleep(0.05)
            
            th_connection = threading.Thread(
                target=broadcast
            )
            th_connection.daemon = True
            th_connection.start()

        if msg == 'VOL 4h FUT':
            caption = 'top quotes by volume for the last 4 hours' + '\n' + '\n' + \
                self.tickers_fut_vol_4h
            self.sender.send_photo(chat_id, 'screener_results/' + 'vol 4h' + '.png', caption)
            self.reply_keyboard(chat_id)

        if msg == 'VOL 24h FUT':
            caption = 'top quotes by volume for the last 24 hours' + '\n' + '\n' + \
                self.tickers_fut_vol_24h
            self.sender.send_photo(chat_id, 'screener_results/' + 'vol 24h' + '.png', caption)
            self.reply_keyboard(chat_id)
        
        if msg == 'NATR 14x5m FUT':
            caption = 'top qoutes by NATR \(14 five minute candles\) for the futures market' + '\n' + '\n' + \
                self.tickers_fut_natr_14x5
            self.sender.send_photo(chat_id, 'screener_results/' + 'natr 14x5m' + '.png', caption)
            self.reply_keyboard(chat_id)
        
        if msg == 'NATR 30x1m FUT':
            caption = 'top qoutes by NATR \(30 one minute candles\) for the futures market' + '\n' + '\n' + \
                self.tickers_fut_natr_30x1
            self.sender.send_photo(chat_id, 'screener_results/' + 'natr 30x1m' + '.png', caption)
            self.reply_keyboard(chat_id)

        if msg == 'FUNDING':
            self.funding_reaction(chat_id)
            self.reply_keyboard(chat_id)

        if msg == 'VOL 4h SPOT':
            caption = 'top quotes by volume for the last 4 hours' + '\n' + '\n' + \
                self.tickers_spot_vol_4h
            self.sender.send_photo(chat_id, 'screener_results/' + 'vol 4h spot' + '.png', caption)
            self.reply_spot(chat_id)
        
        if msg == 'VOL 24h SPOT':
            caption = 'top quotes by volume for the last 24 hours' + '\n' + '\n' + \
                self.tickers_spot_vol_24h
            self.sender.send_photo(chat_id, 'screener_results/' + 'vol 24h spot' + '.png', caption)
            self.reply_spot(chat_id)

        if msg == 'NATR 14x5m SPOT':
            caption = 'top qoutes by NATR \(14 five minute candles\) for the spot market' + '\n' + '\n' + \
                self.tickers_spot_natr_14x5
            self.sender.send_photo(chat_id, 'screener_results/' + 'natr 14x5m spot' + '.png', caption)
            self.reply_spot(chat_id)

        if msg == 'NATR 30x1m SPOT':
            caption = 'top qoutes by NATR \(30 one minute candles\) for the spot market' + '\n' + '\n' + \
                self.tickers_spot_natr_30x1
            self.sender.send_photo(chat_id, 'screener_results/' + 'natr 30x1m spot' + '.png', caption)
            self.reply_spot(chat_id)

        if msg == 'spot':
            self.reply_spot(chat_id)

        if msg == 'future':
            self.reply_keyboard(chat_id)
        
        #self.reply_keyboard(chat_id)


    def reply_keyboard(self, chat_id, text='select metric'):
        reply_markup = { 
            "keyboard": [
                ['spot'],
                #['NATR 5 FUT', 'NATR 1 FUT', 'VOL 4h FUT', 'VOL 24h FUT', 'FUNDING'], 
                ['NATR 14x5m FUT', 'NATR 30x1m FUT'],
                ['VOL 4h FUT', 'VOL 24h FUT', 'FUNDING'],
                ['help', 'roadmap', 'feedback']
            ], 
            "resize_keyboard": True, 
            "one_time_keyboard": True
        }

        #reply_markup['keyboard'][1]['feedback'].url = 'https://www.google.ru/'

        data = {
            'chat_id': chat_id, 
            'text': text, 
            'reply_markup': json.dumps(reply_markup)
        }
        requests.post(f'{self.URL}{self.TOKEN}/sendMessage',
            proxies=self.proxies, data=data)

    def reply_spot(self, chat_id, text='select metric'):
        reply_markup = { 
            "keyboard": [
                ['future'],
                ['NATR 14x5m SPOT', 'NATR 30x1m SPOT'], 
                ['VOL 4h SPOT', 'VOL 24h SPOT'],
                ['help', 'roadmap', 'feedback']
            ], 
            "resize_keyboard": True, 
            "one_time_keyboard": True
        }

        #reply_markup['keyboard'][1]['feedback'].url = 'https://www.google.ru/'

        data = {
            'chat_id': chat_id, 
            'text': text, 
            'reply_markup': json.dumps(reply_markup)
        }
        requests.post(f'{self.URL}{self.TOKEN}/sendMessage',
            proxies=self.proxies, data=data)


    def users_update(self, chat_id):
        #file = open('users.yaml', 'a+')
        if chat_id in self.users:
            print('user are exist')
        else:
            try:
                self.users.append(chat_id)
                file = open(self.users_list, 'w')
                yaml.dump(self.users, file)
                file.close()
            except Exception as e:
                self.sender.send_message(1109752742, 'user add error' + str(e))

            try:
                with open('users.yaml') as f:
                    self.users = yaml.load(f, Loader=yaml.SafeLoader)
                    print(self.users)
                f.close()
            except Exception as e:
                self.sender.send_message(1109752742, 'open file after dump error' + str(e))

    
    def receiving_messages(self):
        self.sender.send_message(1109752742, '>>> bot is running') # debug

        try:
            update_id = self.get_updates()[-1]['update_id']
        except Exception as e:
            self.sender.send_message(1109752742, 'get last msg error: ' + str(e))
        while True:
            try:
                messages = self.get_updates(update_id)
            except Exception as e:
                self.sender.send_message(1109752742, 'get updates for msg error: ' + str(e))
            #print(messages)
            try:
                for message in messages:
                    if update_id < message['update_id']:
                        # save new msg in csv
                        msg_df = pd.DataFrame([[datetime.now(), message]])
                        try:
                            msg_df.to_csv(self.msg_log, mode='a', header=None, index=False)
                        except Exception as e:
                            self.sender.send_message(1109752742, 'add to msg log error: ' + str(e))
                        # save end

                        if 'message' in message:
                            chat_id = message['message']['chat']['id']
                            try:
                                msg = message['message']['text']
                                print(f"user id: {chat_id}, message: {msg}")
                                
                            except Exception as e:
                                self.sender.send_message(1109752742, 'was sender no msg') # debug
                                msg = 'no message'

                            self.check_message(msg, chat_id)
                            try:
                                update_id = message['update_id']
                            except Exception as e:
                                self.sender.send_message(1109752742, 'msg is have not update_id') # debug
                        else:
                            update_id = message['update_id']
                            self.check_message('wtf', chat_id)
            except Exception as e:
                print(e)
                
            time.sleep(2)
    

    def get_img_name(self, quotation):
        return quotation[:-4] + '.png'


    def screening_preparer(self, q, screening_type, delay):
        while self.thread_go:
            try:
                if screening_type == self.screener_future.get_screening:
                    #screening = msg_preparer.df_formatter(screening_type())
                    screening = screening_type()
                    upcoming_time_row = screening['next_funding_time'].min()

                    num = 10
                    
                    top_natr_14x5 = screening.sort_values(by='natr_14x5', ascending=False)[:num]
                    tickers_fut_natr_14x5 = msg_preparer.msg_copy_tickers_formatter(top_natr_14x5)
                    top_natr_14x5 = msg_preparer.df_formatter(top_natr_14x5)
                    img_creator.img_table_creator(top_natr_14x5, 'natr 14x5m')

                    top_natr_30x1 = screening.sort_values(by='natr_30x1', ascending=False)[:num]
                    tickers_fut_natr_30x1 = msg_preparer.msg_copy_tickers_formatter(top_natr_30x1)
                    top_natr_30x1 = msg_preparer.df_formatter(top_natr_30x1)
                    img_creator.img_table_creator(top_natr_30x1, 'natr 30x1m')

                    top_vol_4h = screening.sort_values(by='vol_4h', ascending=False)[:num]
                    tickers_fut_vol_4h = msg_preparer.msg_copy_tickers_formatter(top_vol_4h)
                    top_vol_4h = msg_preparer.df_formatter(top_vol_4h)
                    img_creator.img_table_creator(top_vol_4h, 'vol 4h')

                    top_vol_24h = screening.sort_values(by='turnover_24h', ascending=False)[:num]
                    tickers_fut_vol_24h = msg_preparer.msg_copy_tickers_formatter(top_vol_24h)
                    top_vol_24h = msg_preparer.df_formatter(top_vol_24h)
                    img_creator.img_table_creator(top_vol_24h, 'vol 24h')

                    upcoming_fundings = \
                        screening[screening['next_funding_time'] == upcoming_time_row]
                    top_fund = upcoming_fundings.sort_values(
                        by=['funding_rate'],
                        key=abs,
                        ascending=False
                        )[:num]
                    funding_time = datetime.fromtimestamp(int(upcoming_time_row)/1000).strftime('%Y-%m-%d %H:%M:%S')
                    tickers_fut_fund = msg_preparer.msg_copy_tickers_formatter(top_fund)
                    top_fund = msg_preparer.df_formatter(top_fund)
                    img_creator.img_table_creator(top_fund, 'FR')

                    # scetch saving json for rest server
                    df_for_json = top_natr_14x5.copy()
                    df_for_json = df_for_json.loc[:, ['quotation', 'natr_14x5', 'turnover_24h', 'vol_4h']]
                    df_for_json['Picture'] = df_for_json['quotation'].apply(self.get_img_name)
                    df_for_json['natr_14x5'] = df_for_json['natr_14x5'].round(2)
                    df_rename = df_for_json.rename(columns = {
                        'quotation': 'Quotation',
                        'natr_14x5': 'NATR_14x5m',
                        'turnover_24h': 'Vol_24h',
                        'vol_4h': 'Vol_4h'
                    })

                    df_dict = df_rename.to_dict('records')
                    try:
                        with open('future_14x5m.json', 'w') as outfile:
                            json.dump(df_dict, outfile, indent=4)
                    except Exception as e:
                        print(e)
                    # end of the scetch

                    # scetch saving json for rest server
                    df_for_json = top_natr_30x1.copy()
                    df_for_json = df_for_json.loc[:, ['quotation', 'natr_30x1', 'turnover_24h', 'vol_4h']]
                    df_for_json['Picture'] = df_for_json['quotation'].apply(self.get_img_name)
                    df_for_json['natr_30x1'] = df_for_json['natr_30x1'].round(2)
                    df_rename = df_for_json.rename(columns = {
                        'quotation': 'Quotation',
                        'natr_30x1': 'NATR_30x1m',
                        'turnover_24h': 'Vol_24h',
                        'vol_4h': 'Vol_4h'
                    })

                    df_dict = df_rename.to_dict('records')
                    try:
                        with open('future_30x1m.json', 'w') as outfile:
                            json.dump(df_dict, outfile, indent=4)
                    except Exception as e:
                        print(e)
                    # end of the scetch

                    q.put([
                        tickers_fut_natr_14x5, tickers_fut_natr_30x1,
                        tickers_fut_vol_4h, tickers_fut_vol_24h,
                        tickers_fut_fund, funding_time
                    ])

                elif screening_type == self.screener_spot.get_screening:
                    #screening = msg_preparer.df_formatter(screening_type())
                    screening = screening_type()

                    num = 10

                    top_natr_14x5 = screening.sort_values(by='natr_14x5', ascending=False)[:num]
                    tickers_spot_natr_14x5 = msg_preparer.msg_copy_tickers_formatter(top_natr_14x5)
                    top_natr_14x5 = msg_preparer.df_formatter(top_natr_14x5)
                    img_creator.img_table_creator(top_natr_14x5, 'natr 14x5m')

                    top_natr_30x1 = screening.sort_values(by='natr_30x1', ascending=False)[:num]
                    tickers_spot_natr_30x1= msg_preparer.msg_copy_tickers_formatter(top_natr_30x1)
                    top_natr_30x1 = msg_preparer.df_formatter(top_natr_30x1)
                    img_creator.img_table_creator(top_natr_30x1, 'natr 30x1m')

                    top_vol_4h = screening.sort_values(by='vol_4h', ascending=False)[:num]
                    tickers_spot_vol_4h = msg_preparer.msg_copy_tickers_formatter(top_vol_4h)
                    top_vol_4h = msg_preparer.df_formatter(top_vol_4h)
                    img_creator.img_table_creator(top_vol_4h, 'vol 4h')

                    top_vol_24h = screening.sort_values(by='turnover_24h', ascending=False)[:num]
                    tickers_fut_vol_24h = msg_preparer.msg_copy_tickers_formatter(top_vol_24h)
                    top_vol_24h = msg_preparer.df_formatter(top_vol_24h)
                    img_creator.img_table_creator(top_vol_24h, 'vol 24h')

                    # scetch saving json for rest server
                    df_for_json = top_natr_14x5.copy()
                    df_for_json = df_for_json.loc[:, ['quotation', 'natr_14x5', 'turnover_24h', 'vol_4h']]
                    df_for_json['Picture'] = df_for_json['quotation'].apply(self.get_img_name)
                    df_for_json['natr_14x5'] = df_for_json['natr_14x5'].round(2)
                    df_rename = df_for_json.rename(columns = {
                        'quotation': 'Quotation',
                        'natr_14x5': 'NATR_14x5m',
                        'turnover_24h': 'Vol_24h',
                        'vol_4h': 'Vol_4h'
                    })
                    
                    df_dict = df_rename.to_dict('records')
                    try:
                        with open('spot_14x5m.json', 'w') as outfile:
                            json.dump(df_dict, outfile, indent=4)
                    except Exception as e:
                        print(e)
                    # end of the scetch

                    # scetch saving json for rest server
                    df_for_json = top_natr_30x1.copy()
                    df_for_json = df_for_json.loc[:, ['quotation', 'natr_30x1', 'turnover_24h', 'vol_4h']]
                    df_for_json['Picture'] = df_for_json['quotation'].apply(self.get_img_name)
                    df_for_json['natr_30x1'] = df_for_json['natr_30x1'].round(2)
                    df_rename = df_for_json.rename(columns = {
                        'quotation': 'Quotation',
                        'natr_30x1': 'NATR_30x1m',
                        'turnover_24h': 'Vol_24h',
                        'vol_4h': 'Vol_4h'
                    })

                    df_dict = df_rename.to_dict('records')
                    try:
                        with open('spot_30x1m.json', 'w') as outfile:
                            json.dump(df_dict, outfile, indent=4)
                    except Exception as e:
                        print(e)
                    # end of the scetch

                    q.put([
                        tickers_spot_natr_14x5, tickers_spot_natr_30x1,
                        tickers_spot_vol_4h, tickers_fut_vol_24h
                    ])

                time.sleep(delay)
            except Exception as e:
                print(e, screening_type)
                self.sender.send_message(1109752742, 'screening preparing error: ' + str(e))


    def alert(self):
        for user in self.users:
            self.funding_reaction(user)
            time.sleep(0.05)

    
    def alert_schedule(self):
        print('>>> schedule loaded')
        schedule.every().day.at("02:54:00").do(self.alert)
        schedule.every().day.at("10:54:00").do(self.alert)
        schedule.every().day.at("18:54:00").do(self.alert)

        while True:
            schedule.run_pending()

    
    def upload_market_data(self):
        while True:
            (self.tickers_fut_natr_14x5, self.tickers_fut_natr_30x1,
            self.tickers_fut_vol_4h, self.tickers_fut_vol_24h,
            self.tickers_fut_fund, self.funding_time) = self.q1.get()

            (self.tickers_spot_natr_14x5, self.tickers_spot_natr_30x1,
            self.tickers_spot_vol_4h, self.tickers_spot_vol_24h) = self.q2.get()


    def run(self):
        self.q1 = Queue()
        th_screening_future = Process(
            target=self.screening_preparer,
            args=(
                self.q1, self.screener_future.get_screening, 1
            )
        )
        #th_screening_future.daemon = True
        th_screening_future.start()

        self.q2 = Queue()
        th_screening_spot = Process(
            target=self.screening_preparer,
            args = (
                self.q2, self.screener_spot.get_screening, 1
            )
        )
        #th_screening_spot.daemon = True
        th_screening_spot.start()

        th_connection = threading.Thread(
            target=self.upload_market_data
        )
        th_connection.daemon = True
        th_connection.start()

        th_alert = threading.Thread(
            target=self.alert_schedule
        )
        th_alert.daemon = True
        th_alert.start()

        try:
            self.receiving_messages()
        except Exception as e:
            print(e)
            self.sender.send_message(1109752742, 'receiving msg error: ' + str(e))

        
if __name__ == '__main__':
    bot = ScreenerBot()
    print(bot.run())
