from email import message_from_binary_file
import requests
import pandas as pd
import json
import yaml
import time
import threading
import schedule
import datetime

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

        # creating file with users id (if not exist)
        #file = open(self.users_list, 'a')
        #file.close()

        self.users = []
        with open('users.yaml') as f:
            self.users = yaml.load(f, Loader=yaml.SafeLoader)
            print(self.users)
        f.close()

        self.screener = ms.Screener('binance', 'future')
        self.sender = msg_sender.TgSender(self.TOKEN, self.URL)

        gag_later = 'creating an up\-to\-date screening in progress'

        self.msg_screening = gag_later
        self.msg_natrs = gag_later
        self.msg_fundings = gag_later
        self.funding_time = ''

        self.tickers_screening = gag_later
        self.tickers_natrs = gag_later
        self.tickers_fundings = gag_later

        self.msg_log = 'msg_log.csv'


    def get_updates(self, offset=0):
        result = requests.get(f'{self.URL}{self.TOKEN}/getUpdates?offset={offset}').json()
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

        caption = 'next funding time\\: ' + escaping_time + '\n' + '\n' + \
            self.tickers_fundings
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
        
        #if msg == 'stop': 
            #self.thread_go = False
        
        if msg == 'go':
            self.thread_go = True

        if msg == 'VOLUME':
            caption = 'top qoutes by volume' + '\n' + '\n' + \
                self.tickers_screening
            self.sender.send_photo(chat_id, 'screener_results/' + 'screening' + '.png', caption)
        
        if msg == 'NATR':
            caption = 'top qoutes by NATR' + '\n' + '\n' + \
                self.tickers_natrs
            self.sender.send_photo(chat_id, 'screener_results/' + 'natr' + '.png', caption)

        if msg == 'FUNDING':
            self.funding_reaction(chat_id)
        
        self.reply_keyboard(chat_id)


    def reply_keyboard(self, chat_id, text='select metric'):
        reply_markup = { 
            "keyboard": [
                ['VOLUME', 'NATR', 'FUNDING'], 
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
        requests.post(f'{self.URL}{self.TOKEN}/sendMessage', data=data)


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
                        msg_df = pd.DataFrame([[datetime.datetime.now(), message]])
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
                
            time.sleep(1)


    def screening_preparer(self, screening_type, delay):
        while self.thread_go:
            try:
                if screening_type == self.screener.get_screening:
                    screening = screening_type(num=10)
                    
                    header = 'top qoutes by volume'
                    self.msg_screening = msg_preparer.msg_formatter(
                        screening, header)

                    self.tickers_screening = msg_preparer.msg_copy_tickers_formatter(
                        screening)
                    
                    column_to_highlight = 'volume'

                if screening_type == self.screener.get_top_natr:
                    screening = screening_type(num=10)

                    # scetch saving json for rest server
                    df_for_json = screening[0][['quotation','natr','turnover_24h']].copy()
                    saver.json_save(df_for_json)
                    # end of the scetch
                    
                    header = 'top qoutes by natr'
                    self.msg_natrs = msg_preparer.msg_formatter(
                        screening, header)

                    self.tickers_natrs = msg_preparer.msg_copy_tickers_formatter(
                        screening)
                    
                    column_to_highlight = 'natr'

                if screening_type == self.screener.get_upcoming_fundings:
                    screening = screening_type(num=10)

                    header = 'top qoutes by funding rates'
                    self.msg_fundings = msg_preparer.msg_formatter(
                        screening, header, funding_flag=True)

                    self.tickers_fundings = msg_preparer.msg_copy_tickers_formatter(
                        screening)

                    self.funding_time = screening[1]
                    
                    column_to_highlight = 'funding rate'

                screening_formated = msg_preparer.df_formatter(screening[0])
                img_creator.img_table_creator(screening_formated, column_to_highlight)

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


    def run(self):
        th_volume = threading.Thread(
            target=self.screening_preparer,
            args=(
                self.screener.get_screening, 60
            )
        )
        th_volume.daemon = True
        th_volume.start()

        th_natr = threading.Thread(
            target=self.screening_preparer,
            args=(
                self.screener.get_top_natr, 1
            )
        )
        th_natr.daemon = True
        th_natr.start()

        th_funding = threading.Thread(
            target=self.screening_preparer,
            args=(
                self.screener.get_upcoming_fundings, 60
            )
        )
        th_funding.daemon = True
        th_funding.start()

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
