import tg_screener_bot 


if __name__ == '__main__':
    while True:
        try:
            bot = tg_screener_bot.ScreenerBot()
            bot.run()
        except Exception as s:
            print('try again')