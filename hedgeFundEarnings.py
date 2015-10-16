import sys
sys.path.append('../13F')
from dbconnection import start_db_connection
from contextlib import closing
from earningsPerformance import EarningsPerformance

DB_CONNECTION_TYPE = 'local'


class EarningsAnalysis(object):
    def __init__(self, cik, quarter_date):
        self.cik = cik
        self.quarter_date = quarter_date

    def earnings_performance(self):
        tickers = self.get_fund_tickers()
        for ticker in tickers:
            perf = EarningsPerformance(ticker, self.quarter_date).analyze_earnings_performance()
            print perf


    def get_fund_tickers(self):
        conn = start_db_connection(DB_CONNECTION_TYPE)
        with closing(conn.cursor()) as cur:
            cur.execute('''SELECT DISTINCT c.ticker
                        FROM form13flist b
                        INNER JOIN (SELECT DISTINCT accessionnunber, cusip
                        FROM form13fholdings) a
                        ON a.accessionnunber=b.accessionnunber
                        INNER JOIN cusiplist c
                        ON a.cusip=c.cusip
                        WHERE b.quarterdate = %s AND
                        b.cik =%s''', (self.quarter_date, self.cik))
            tickers = [x[0] for x in cur.fetchall()]
        conn.close()
        return tickers



if __name__ == '__main__':
    EarningsAnalysis('1159159', '2015-06-30').earnings_performance()
