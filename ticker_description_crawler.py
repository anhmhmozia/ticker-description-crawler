from yahooquery import Ticker
from datetime import datetime
import mysql_connection
from configs import cfg
cfg.run()

def ticker_crawler():
    items = []
    today = datetime.today().strftime("%Y-%m-%d")
    symbols = mysql_connection.select_normal('tickers', ['ticker'], 'WHERE description_crawl_at is null', 'scalemarketcap desc', 100)
    for s in symbols:
        symbol = s[0]
        print(symbol)
        try:
            ticker = Ticker(symbol)
            tickerInfo = ticker.asset_profile[symbol]
            description = tickerInfo['longBusinessSummary'] or ''
            website = tickerInfo['website'] or ''
            items.append((symbol, description, website, today))
        except Exception as e:
            print('Error')
            items.append((symbol, '', '', today))
            continue

    mysql_connection.insert_or_update('tickers', [
                                          'ticker', 'description', 'website', 'description_crawl_at'],
                                          ['ticker'], items)

if __name__ == '__main__':
    ticker_crawler()
    mysql_connection.disconnection()