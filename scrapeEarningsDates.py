from lxml import html
import requests
import sys
sys.path.append('../13F')
from dbconnection import start_db_connection
from contextlib import closing
import datetime
import psycopg2


DB_CONNECTION_TYPE = 'local'
#DB_CONNECTION_TYPE = 'AWS'


def scrape_earnings_page(last_date):
    string_date = calculate_string_date(last_date)
    url = 'http://biz.yahoo.com/research/earncal/{}.html'.format(string_date)
    page = requests.get(url)
    tree = html.fromstring(page.text)
    earnings_lines_count = len(tree.xpath('//*/td/table[1]/tr'))
    lines = []
    for x in xrange(3, earnings_lines_count):
        line = tree.xpath('//*/td/table[1]/tr[{}]/td//text()'.format(str(x)))
        if len(line) == 1: continue
        lines.append(prepare_line(line))
    upload_earnings_information(last_date, lines)

def prepare_line(line):
    #If no ticker
    if len(line) == 2: line.append(None)
    #elif line[2] == 'Time Not Supplied': line[2] = None
    #Sometimes unndeeded fourth entry
    if parse_eps_field(line[2]):
        line.pop(2)
    return line[:3]

def parse_eps_field(s):
    if s == 'N/A':
        return True
    try:
        float(s)
        return True
    except ValueError, TypeError:
        return False

def upload_earnings_information(date, lines):
    conn = start_db_connection(DB_CONNECTION_TYPE)
    with closing(conn.cursor()) as cur:
        cur.execute('''SELECT ticker FROM cusiplist''')
        tickers = [x[0] for x in cur.fetchall()]
        for line in lines:
            name, ticker, time = line
            if ticker in tickers:
                try:
                    cur.execute('''INSERT INTO equityearningsdates
                                (ticker, earningsdate, time) VALUES
                                (%s,%s,%s)''',(ticker, date, time))
                except psycopg2.IntegrityError:
                    conn.rollback()
    conn.commit()
    conn.close()

def calculate_string_date(last_date):
    return last_date.strftime("%Y%m%d")

def get_min_db_date():
    conn = start_db_connection(DB_CONNECTION_TYPE)
    with closing(conn.cursor()) as cur:
        cur.execute('SELECT MAX(earningsdate) FROM equityearningsdates')
        last_date = cur.fetchone()[0]
    conn.close()
    return last_date

if __name__ == '__main__':
    last_date = get_min_db_date()
    to_date = datetime.datetime.now().date()+datetime.timedelta(days=14)
    while last_date < to_date:
        scrape_earnings_page(last_date)
        last_date += datetime.timedelta(days=1)

