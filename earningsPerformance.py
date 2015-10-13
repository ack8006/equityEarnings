import sys
sys.path.append('../13F')
from keys import quandl_api_key
from dbconnection import start_db_connection
from contextlib import closing
import datetime
import time
import pandas.io.data as web
import quandlpy as qp


DB_CONNECTION_TYPE=('local')
#DB_CONNECTION_TYPE=('AWS')

class EarningsPerformance(object):
    def __init__(self, ticker, quarter_date):
        self.ticker = ticker
        self.quarter_date = quarter_date

    def analyze_earnings_performance(self):
        earnings_details = self.get_earnings_date_information()
        if earnings_details:
            earnings_details = self.clean_earnings_details(earnings_details)
        else:
            print 'NO EARNINGS DETAILS'
            return
        performance = self.get_earnings_performance(earnings_details)
        return {'previous_day': performance[0],
                'earnings_change': performance[1],
                'earnings_day': performance[2]}

    #This Method is making an assumption that the first earnings after the
    #quarter date is the correct one
    def get_earnings_date_information(self):
        conn = start_db_connection(DB_CONNECTION_TYPE)
        with closing(conn.cursor()) as cur:
            cur.execute('''SELECT ticker, earningsdate, time
                        FROM equityearningsdates WHERE
                        earningsdate > %s AND ticker=%s
                        ORDER BY earningsdate ASC''',
                        (self.quarter_date, self.ticker))
            earnings_details = cur.fetchone()
        conn.close()
        return earnings_details

    def clean_earnings_details(self, earnings_details):
        ticker = earnings_details[0]
        earnings_date = earnings_details[1]
        earnings_time = self.parse_earnings_time(earnings_details[2])
        return (ticker, earnings_date, earnings_time)

    def parse_earnings_time(self, earnings_time):
        #***ASSUMPTIONS ABOUND
        if earnings_time == 'Time Not Supplied':
            earnings_time == 'After Market Close'
        elif 'am' in earnings_time:
            earnings_time = 'Before Market Open'
        elif 'pm' in earnings_time:
            earnings_time = 'After Market Close'
        return earnings_time

    #IMMEDIATE, EOD
    def get_earnings_performance(self, earnings_details):
        ticker, earnings_date, earnings_time = earnings_details
        print ticker, earnings_date, earnings_time
        periods = ['Close','Open','Close']
        if earnings_time == 'Before Market Open':
            earnings_date = earnings_date- datetime.timedelta(days=1)
        price_data = self.get_price_data(ticker, earnings_date)
        price_points = self.get_surrounding_prices(price_data, earnings_date)
        return self.calculate_performance(price_points)

    def get_price_data(self, ticker, earnings_date):
        start_date = earnings_date - datetime.timedelta(days=5)
        end_date = earnings_date + datetime.timedelta(days=5)
        price_data = None
        try:
            price_data = web.DataReader(ticker, 'yahoo', start_date, end_date)
        except IOError:
            try:
                price_data = web.DataReader(ticker, 'google', start_date, end_date)
            except IOError:
                try:
                    price_data = qp.get('WIKI', ticker, api_key=quandl_api_key,
                                        start_date = start_date,
                                        end_date = end_date)
                    price_data.set_index('Date', inplace=True)
                except KeyError:
                    pass
        return price_data

    def get_surrounding_prices(self, price_data, earnings_date):
        previous_open = price_data.ix[price_data.index <= str(earnings_date)].tail(1)['Open'][0]
        previous_close = price_data.ix[price_data.index <= str(earnings_date)].tail(1)['Close'][0]
        after_open = price_data.ix[price_data.index > str(earnings_date)].head(1)['Open'][0]
        after_close = price_data.ix[price_data.index > str(earnings_date)].head(1)['Close'][0]
        return (previous_open, previous_close, after_open, after_close)

    def calculate_performance(self, prices):
        previous_pct = (prices[1]-prices[0])/prices[0]
        earnings_pct = (prices[2]-prices[1])/prices[1]
        after_pct = (prices[3]-prices[2])/prices[2]
        return previous_pct, earnings_pct, after_pct


if __name__ == '__main__':
    #performance = EarningsPerformance('NFLX', '2015-03-31').analyze_earnings_performance()
    #performance = EarningsPerformance('CHEF', '2015-03-31').analyze_earnings_performance()
    #performance = EarningsPerformance('CEMI', '2015-03-31').analyze_earnings_performance()
    performance = EarningsPerformance('COO', '2015-03-31').analyze_earnings_performance()

    print performance

