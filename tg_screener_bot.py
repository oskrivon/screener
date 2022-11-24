from email import message_from_binary_file
import requests
import pandas as pd
import json
import yaml
import time
import threading
from multiprocessing import Process, Pipe
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
            print(self.users)
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
        
        if msg == 'help':
            msg_ = ('"VOLUME" to get the top 10 quotes with the highest volumes at the moment (last 24 hours, $)' + '\n'+
                    '"NATR" to get the top 10 quotes with the highest NATR' + '\n' +
                    '"FUNDING" to get the top quotes with the highest fundings rate' + '\n' +
                    'data source: Binance')
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
                '8. signals within the bot' + '\n' + \
                '9. module for analyzing support and resistance levels' + '\n' + \
                '10. integration of levels into the bot' + '\n' + \
                '11. site development' + '\n' + \
                '12. site launch' + '\n' + \
                '13. app development' + '\n' + \
                '14. add launch'
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

        if msg == 'VOL 4h FUT':
            caption = 'top qoutes by volume' + '\n' + '\n' + \
                self.tickers_fut_vol_4h
            self.sender.send_photo(chat_id, 'screener_results/' + 'vol 4h' + '.png', caption)

        if msg == 'VOL 24h FUT':
            caption = 'top qoutes by volume' + '\n' + '\n' + \
                self.tickers_fut_vol_24h
            self.sender.send_photo(chat_id, 'screener_results/' + 'vol 24h' + '.png', caption)
        
        if msg == 'NATR 14x5m FUT':
            caption = 'top qoutes by NATR' + '\n' + '\n' + \
                self.tickers_fut_natr_14x5
            self.sender.send_photo(chat_id, 'screener_results/' + 'natr 14x5m' + '.png', caption)
        
        if msg == 'NATR 30x1m FUT':
            caption = 'top qoutes by NATR' + '\n' + '\n' + \
                self.tickers_fut_natr_30x1
            self.sender.send_photo(chat_id, 'screener_results/' + 'natr 30x1m' + '.png', caption)

        if msg == 'FUNDING':
            self.funding_reaction(chat_id)
        
        self.reply_keyboard(chat_id)

        if msg == 'VOL 4h SPOT':
            caption = 'top qoutes by NATR' + '\n' + '\n' + \
                self.tickers_spot_vol_4h
            self.sender.send_photo(chat_id, 'screener_results/' + 'vol 4h spot' + '.png', caption)
            self.reply_spot(chat_id)
        
        if msg == 'VOL 24h SPOT':
            caption = 'top qoutes by NATR' + '\n' + '\n' + \
                self.tickers_spot_vol_24h
            self.sender.send_photo(chat_id, 'screener_results/' + 'vol 24h spot' + '.png', caption)
            self.reply_spot(chat_id)

        if msg == 'NATR 14x5m SPOT':
            caption = 'top qoutes by NATR' + '\n' + '\n' + \
                self.tickers_spot_natr_14x5
            self.sender.send_photo(chat_id, 'screener_results/' + 'natr 14x5m spot' + '.png', caption)
            self.reply_spot(chat_id)

        if msg == 'NATR 30x1m SPOT':
            caption = 'top qoutes by NATR' + '\n' + '\n' + \
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
                
            time.sleep(3)


    def screening_preparer(self, screening_type, conn, delay):
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

                    conn.send([
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
                    #df_for_json = screening[0][['quotation','natr','turnover_24h']].copy()
                    #saver.json_save(df_for_json)
                    # end of the scetch

                    conn.send([
                        tickers_spot_natr_14x5, tickers_spot_natr_30x1,
                        tickers_spot_vol_4h, tickers_fut_vol_24h
                    ])

                time.sleep(delay)
            except Exception as e:
                print(e, screening_type)
                self.sender.send_message(1109752742, 'screening preparing error: ' + str(e))


    def alert(self):
        print(self.users)
        for user in self.users:
            self.funding_reaction(user)

    
    def alert_schedule(self):
        print('>>> schedule loaded')
        schedule.every().day.at("02:54:00").do(self.alert)
        schedule.every().day.at("10:54:00").do(self.alert)
        schedule.every().day.at("18:54:00").do(self.alert)

        while True:
            schedule.run_pending()


    def upload_market_data(self, conn_fut, conn_spot):
        while True:
            future_tickers = conn_fut.recv()
            spot_tickers = conn_spot.recv()

            (self.tickers_fut_natr_14x5, self.tickers_fut_natr_30x1,
            self.tickers_fut_vol_4h, self.tickers_fut_vol_24h,
             self.tickers_fut_fund, self.funding_time) = future_tickers

            (self.tickers_spot_natr_14x5, self.tickers_spot_natr_30x1,
            self.tickers_spot_vol_4h, self.tickers_spot_vol_24h) = spot_tickers


    def run(self):
        parent_future, child_future = Pipe()
        parent_spot, child_spot = Pipe()

        th_screening_future = Process(
            target=self.screening_preparer,
            args=(
                self.screener_future.get_screening, child_future, 1
            )
        )
        #th_screening_future.daemon = True
        th_screening_future.start()

        th_screening_spot = Process(
            target=self.screening_preparer,
            args = (
                self.screener_spot.get_screening, child_spot, 1
            )
        )
        #th_screening_spot.daemon = True
        th_screening_spot.start()

        th_connection = threading.Thread(
            target=self.upload_market_data,
            args= (parent_future, parent_spot)
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
