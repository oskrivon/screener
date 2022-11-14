import time

import tg_screener_bot


if __name__ == '__main__':
    while True:
        try:
            bot = tg_screener_bot.ScreenerBot()
            bot.run()
        except Exception as e:
            print(e)
            print('try again')
            time.sleep(1)