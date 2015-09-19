#!/usr/bin/env python

__version__ = '0.0.1'

try:
    # py3
    from urllib.request import Request, urlopen 
    from urllib.parse import urlencode 
except ImportError: 
    # py2 
    from urllib2 import Request, urlopen 
    from urllib import urlencode

import re
import sys
import math
import time
import datetime
import sqlite3 as sql

# all dates starting from 1950, when SP500 starts
START_YEAR = 1950
START_DATE = '1950-01-01'
TODAY_YMD, TODAY_YM, LAST_TRADE_DAY = '', '', ''

# LOCAL | yahoofinance
DATA_SOURCE = 'yahoofinance'

BASE_DIR_PATH = './'

SQL_DIR_PATH = '%s/SQL' % BASE_DIR_PATH
YF_DB_FILE = '%s/yahoofinance.db' % SQL_DIR_PATH

MY_LOG_FILE_NAME = '%s/yahoofinance.log' % BASE_DIR_PATH

# regexp for date - YYYY-MM-DD
RE_DATE = re.compile('^(\d\d\d\d)-(\d\d)-(\d\d)')

# --------------------------------------------------------------------------- #
# Shared functions
# --------------------------------------------------------------------------- #
# def unit_to_number(unit): k/m/b to number
# def convert_int(num_str, unit=''): '100,000K' -> 100000000
# def convert_float(num_str): '1,999.99' -> 1999.99
# def month_atoi(month_str): Jan -> 1, Feb -> 2
# def date_atoymd(date_): 'Feb 20, 2012' -> 2012-02-02
# --------------------------------------------------------------------------- #

#
# test shared functions
#
# --------------------------------------------------------------------------- #
def test_shared_func(*args):
    if len(args) < 1 or args[0] == 'usage':
        print """yahoofinance.py test-shared <command> <args>
        all                run all pre-defined tests
        correlation
        unit_to_number     <K|M|B>
        convert_int        <number_str> [<unit:K|M|B>]
        convert_float      <float_str>
        month_atoi         <month: Jan|march>
        date_atoymd        <date: Feb 20, 2012>
        range_month        <start_ym: 2012-01> <number> [forward|backward]
        range_day          <start_date> <end_date>
        range_quarter      <end_quarter: 2012Q1> <number> <include_this=1>
        pprint_name_value  <"name1:name2.." "value1:valu2..."
        """
    else:
        try:
            if args[0] == 'all':
                list_title, list_value = [], []
                list_title.append('=================>')
                list_value.append('unit_to_number(*)')
                list_title.append('unit_to_number(K)')
                list_value.append(unit_to_number('K'))
                list_title.append('unit_to_number(b)')
                list_value.append(unit_to_number('b'))

                list_title.append('=================>')
                list_value.append('convert_date(*)')
                list_title.append('convert_date("20110101")')
                list_value.append(convert_date("20110101"))
                list_title.append('convert_date("201101011")')
                list_value.append(convert_date("201101011"))

                list_title.append('=================>')
                list_value.append('convert_int(*)')
                list_title.append('convert_int("1000K")')
                list_value.append(convert_int("1000K"))
                list_title.append('convert_int("1000K", "m")')
                list_value.append(convert_int("1000K", "m"))
                list_title.append('convert_int("10")')
                list_value.append(convert_int(10))
                list_title.append('convert_int("N/A")')
                list_value.append(convert_int('N/A')/10)
                list_title.append('convert_int(-10.9)')
                list_value.append(convert_int(-10.9))

                list_title.append('=================>')
                list_value.append('convert_float(*)')
                list_title.append('convert_float("2,221.39")')
                list_value.append(convert_float("2,221.39"))
                list_title.append('convert_float("N/A")')
                list_value.append(convert_int("N/A"))

                list_title.append('=================>')
                list_value.append('month_atoi()')
                list_title.append('month_atoi("Jan")')
                list_value.append(month_atoi("Jan"))
                list_title.append('month_atoi("XXX")')
                list_value.append(month_atoi("XXX"))

                list_title.append('=================>')
                list_value.append('date_atoymd()')
                list_title.append('date_atoymd("Sep 15")')
                list_value.append(date_atoymd("Sep 15"))
                list_title.append('date_atoymd("Sep 15,2015")')
                list_value.append(date_atoymd("Sep 15,2015"))

                list_title.append('=================>')
                list_value.append('range_month()')
                list_title.append('range_month("2012-01", 3)')
                list_value.append(range_month("2012-01", 3))
                list_title.append('range_month("2012-01", 3, "backward", 1)')
                list_value.append(range_month("2012-01", 3, "backward", 1))

                list_title.append('=================>')
                list_value.append('range_quarter()')
                list_title.append('range_quarter("2012Q1", 4)')
                list_value.append(range_quarter("2012Q1", 4))
                list_title.append('range_quarter("2015Q4", 4, 1)')
                list_value.append(range_quarter("2015Q4", 4, 1))

                list_title.append('=================>')
                list_value.append('YFQuota.static_get_daily()')
                list_title.append('YFQuota.static_get_daily()')
                list_value.append('%d:%s-%s' % (
                    len(YFQuota.static_get_daily()),
                    YFQuota.static_get_daily()[0][1],  
                    YFQuota.static_get_daily()[-1][1]))
                
                pprint_name_value(list_title, list_value)
            elif args[0] == 'correlation': 
                #def correltion(list1, list2):
                list1=[[1,2,3,4],[1.1,2.5,3.9,4.0],[1.8,-2.6,-8.1,4.5],[1.1,2.5,3.9,4.0,0,0],[1.1,2.5,3.9,4.0,0,0,0],[4.64,-6.45,1.80,23.52,13.49,9.26,-3.03,5.31,12.23,3.10,-6.09,-3.14,4.07,6.35,0.61,5.45,-2.01,0.80,-2.03,5.99,-3.01,4.51,2.39,1.74,9.84,4.04,4.31,2.60,-0.07,-0.24,-3.61,-4.82,2.70,0.09,8.46,-0.28,-1.13,5.57,0.96,4.58,-2.33,-1.10,1.87,0.32,-0.56]]
                list2=[[10,20,30,40],[1.8,-2.6,-8.1,4.5],[1.1,2.5,3.9,4.0],[1.8,-2.6,-8.1,4.5,0,0],[1.8,-2.6,-8.1,4.5,0,0,0],[0.78,-8.57,-10.99,8.54,9.39,5.31,0.02,7.41,3.36,3.57,-1.98,5.74,1.78,-3.70,2.85,5.88,1.48,-8.20,-5.39,6.88,-4.74,8.76,3.69,-0.23,6.53,2.26,3.20,-0.10,2.85,-1.35,-1.83,-2.15,-5.68,-7.18,10.77,-0.51,0.85,4.36,4.06,3.13,-0.75,-6.27,3.96,1.26,1.98]]

                for x,y in zip(list1,list2):
                    print ["%2.2f" % f for f in x]
                    print ["%2.2f" % f for f in y]
                    print correlation(x,y)
                    print '-------------'

            elif args[0] == 'unit_to_number':
                print unit_to_number(*args[1:])
            elif args[0] == 'convert_int':
                print convert_int(*args[1:])
            elif args[0] == 'convert_float':
                print convert_float(*args[1:])
            elif args[0] == 'month_atoi':
                print month_atoi(*args[1:])
            elif args[0] == 'date_atoymd':
                print date_atoymd(*args[1:])
            elif args[0] == 'range_month':
                print range_month(*args[1:])
            elif args[0] == 'range_day':
                print range_day(*args[1:])
            elif args[0] == 'range_quarter':
                print range_quarter(*args[1:])
            elif args[0] == 'pprint_name_value':
                pprint_name_value(args[1].split(':'), args[2].split(':'))
            else: 
                test_shared_func('usage')
        except:
            raise
            test_shared_func('usage')

#
# unit to number, k/b/m
#
# --------------------------------------------------------------------------- #
def unit_to_number(unit):
    if unit.lower() == 'k':
        return 1000
    elif unit.lower() == 'm':
        return 1000000
    elif unit.lower() == 'b':
        return 1000000000
    else:
        return 1

# --------------------------------------------------------------------------- #
def convert_date(input_):
    """
    1) convert date string to yyyy-mm-dd format, e.g.
       ('20160101') --> '2016-01-01'
       for class StockER
    """
    if type(input_) is str:
        input_ = re.sub(',', '', input_)
        a = re.search('^(\d{4})(\d{2})(\d{2})$', input_)
   
        if a:
            return '-'.join(a.groups())
        else:
            return input_
    return ''

# --------------------------------------------------------------------------- #
def convert_int(input_, unit=''):
    """
    1) convert number string to number string in unit like K/M/B, like
       ('100,000K'), ('1.2M'), ('10000', 'k')
    2) input may be an integer itself, just return it. This is some
       functions argument
    """
    if type(input_) is int:
        return input_
       
    if type(input_) is float:
        adj = 0.5
        if input_ < 0:
            adj = -0.5
        return int(adj + input_)

    if type(input_) is str:
        input_ = re.sub(',', '', input_)
        a = re.search('([\d|\.]+)([kmb]{0,1})', input_.lower())
    
        i = 0
        if a:
            try: 
                i = 0.5 + (float(a.group(1)) * unit_to_number(a.group(2)) \
                    / unit_to_number(unit)) 
                return int(i)
            except:
                return 0
        else:
            return 0
    # if reach here, either not string, or corrupt string
    return 0

# --------------------------------------------------------------------------- #
def convert_float(float_str, default_value=-1.0):
    """
    convert float string to float after removing ','
    """

    float_str = re.sub(',', '', float_str)

    try:
        return float(float_str)
    except:
        return default_value

# --------------------------------------------------------------------------- #
def month_atoi(month_str):
    """
    convert month string to digital
    """
    list_months  = [
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
    ]

    for i, month in enumerate(list_months):
        if re.match('^%s' % month.lower(), month_str.lower()):
            return i + 1
    return 0
    
# --------------------------------------------------------------------------- #
def date_atoymd(date_, default_value='0000-00-00'):
    """
    convert date string to YYYY-MM-DD or MM-DD formate, like
    Feb 2, 2012 -> '2012-02-02'
    Oct 30      -> '10-30', for Stock FY Ends
    """
    a = re.search('^(\w+)\s+(\d+)[\s|,]+(\d{4})', date_)
    if a: 
        return '%04d-%02d-%02d' % ( 
            int(a.group(3)), 
            month_atoi(a.group(1)), 
            int(a.group(2))
            )
    
    b = re.search('^(\w+)\s+(\d+)', date_)
    if b:
        return '%02d-%02d' % ( 
            month_atoi(b.group(1)), 
            int(b.group(2))
            )
    
    return default_value

# --------------------------------------------------------------------------- #
def range_day(start_date, end_date):
    '''
    return list of date between start_date and end_day
    '''
    days_in_month = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    list_days = []

    year, month, day = map(int, start_date.split('-'))

    date_ = '%04d-%02d-%02d' % (year, month, day)

    while date_ <= end_date:
        list_days.append(date_)

        day += 1

        if day > days_in_month[month - 1]:
            day = 1
            month += 1

        if month > 12:
            month = 1
            year += 1

        date_ = '%04d-%02d-%02d' % (year, month, day)

    return list_days

# --------------------------------------------------------------------------- #
def range_month(start_ym, number, mode='forward', include_this=1):
    '''
    get range of months, e.g, 
    (2011-01, 12) -> [2011-01, 2011-02,.....]
    (2011-01, 12, 'backward', 1) -> [2009-01, 2009-02,.....2011-01]
    mode: incremental|reverse
    '''

    number = convert_int(number)
    include_this = convert_int(include_this)

    # split start_ym to year, month, if failed, return [] since invalid input
    try: 
        [year, month] = map(int, start_ym.split('-'))
    except:
        return []

    if include_this: 
        list_month = [start_ym]
        number -= 1

    for i in range(number): 
        if mode == 'forward': 
            month += 1 
        else: 
            month -= 1

        if month > 12: 
            year += 1
            month = 1

        if month < 1:
            year -= 1
            month = 12

        list_month.append('%04d-%02d' % (year, month))

    return sorted(list_month)

# --------------------------------------------------------------------------- #
def range_quarter(end_quarter, number, include_this=1):
    '''
    generate list of quarters, like od.range_quarter(2012Q1,12)
    include_this = 1, means staring from the end_quarter
    '''

    number = convert_int(number)
    include_this = convert_int(include_this)
    
    # init var
    list_quarters = []
    year = int(end_quarter[0:4])
    quarter = int(end_quarter[5])
    
    # if not include this quarter, quarter -1
    if include_this: 
        list_quarters.append(end_quarter)
        number -= 1

    for i in range(number):
        quarter -= 1
        if quarter <= 0: 
            quarter = 4
            year -= 1

        list_quarters.append('%04dQ%d' % (year, quarter))

    return sorted(list_quarters)

# --------------------------------------------------------------------------- #
def pprint_name_value(list_name, list_value):
    max_len = len(max(list_name, key=len))
    
    for name, value in zip(list_name, list_value): 
        try: 
            print '%*s : [%s]' % (max_len, name, value) 
        except: 
            print '%*s : [%s]' % (max_len, name, 'corrupted') 
        
# --------------------------------------------------------------------------- #
def get_today_dates():
    '''
    get today dates YYYY-MM-DD and YYYY-MM formats
    '''
    date_time = datetime.datetime.now() 

    year = date_time.year
    month = date_time.month
    day = date_time.day

    ymd = '%04d-%02d-%02d' % (year, month, day)

    ym = '%04d-%02d' % (year, month)

    return (ymd, ym)

# --------------------------------------------------------------------------- #
def average(list_float): 
    """
    calculate the average of list of float
    """
    assert len(list_float) > 0 
    return float(sum(list_float)) / len(list_float)

def correlation(list1, list2):
    if len(list1) != len(list2):
        print 'Error: correlation(), 2 different length list'
        return 0.0
            
    len_ = len(list1)
    if len_ == 0:
        print 'Error: correlation(), zero length list'
        return 0.0

    avg_list1 = average(list1)
    avg_list2 = average(list2)

    diff_prod = 0
    list1_diff2 = 0
    list2_diff2 = 0
    for i in range(len_): 
        list1_diff = list1[i] - avg_list1 
        list2_diff = list2[i] - avg_list2 
        
        diff_prod += list1_diff * list2_diff 

        list1_diff2 += list1_diff * list1_diff 
        list2_diff2 += list2_diff * list2_diff 

    if list1_diff2 * list2_diff2 == 0: 
        return 0.0 

    return diff_prod / math.sqrt(list1_diff2 * list2_diff2)


# --------------------------------------------------------------------------- #
# end of Shared functions
# --------------------------------------------------------------------------- #

YF_INSIDER = 1
NONEWLINE = 2

class SimpleHTMLParser(object):
    """
    A very simple HTML parser on top of urllib, flags:
    * YF_INISDER: reading yahoofinance insider info to get url of form 4
    * NONEWLINE:  not used now
    """
    # ----------------------------------------------------------------------- #
    def __init__(self, url, flag=NONEWLINE):
        self.NONEWLINE = 2

        self.html_text = ''
        self.url = url
        self.flag = flag
        self.list_url = []
        self.read_and_parse()

    # ----------------------------------------------------------------------- #
    def read_and_parse(self): 
        req = Request(self.url) 
        
        try: 
            response = urlopen(req) 
        except: 
            self.html_text = 'Error'
            return
        else: 
            #self.raw_data = str(response.read().decode('utf-8', 'replace').strip())
            self.raw_data = response.read().strip()
        
        if self.flag & self.NONEWLINE:
            self.raw_data = re.sub('[\n|\r]', ' ', self.raw_data) 
       
        # remove <sup>6</sup>
        self.raw_data = re.sub('<sup>\d+</sup>', '', self.raw_data)

        # -------------------------------------- #
        # tr|br --> new line
        # td|th --> |
        # url   --> remove
        # -------------------------------------- #
        wait = 0
        for line in self.raw_data.split('<'):
            line = line.strip()

            # <tr>|<th> --> \n
            if re.search('^tr|^th|^table', line):

                # for reading yf_insider, need the insider url
                # so print out the last url
                if self.flag & YF_INSIDER and len(self.list_url):
                    self.html_text += '|%s' % self.list_url[-1]
                self.html_text += '\n'

            # <td> --> |
            if re.search('^td', line):
                self.html_text += '|'

            # <span> --> ' '
            if re.search('^span', line):
                self.html_text += ' '

            a = re.search('^a.*href="*([^>"]+)"*>', line)
            if a:
                self.html_text += ' '
                self.list_url.append(a.group(1))

            # remove the <tag>
            line = re.sub('[^\>]*>\s*', '', line)
            line = line.strip()
       
            if not line == '': 
                self.html_text += line

class Log(object):
    """
    Write log entry to file
    """
    # ----------------------------------------------------------------------- #
    def __init__(self):
        self.log_file_name = MY_LOG_FILE_NAME

    # ----------------------------------------------------------------------- #
    def write(self, message):
        with open(self.log_file_name, 'w+') as f:
            f.write('%s: %s\n' % (datetime.datetime.now(), message))
            f.close()
         
class YFDB(object):
    """
    super class for yahoo finance database. 
    self.table is the default table for various class. 
    * class Stock: table Stock
    """

    # ----------------------------------------------------------------------- #
    def __init__(self, table="Stock"):
        self.conn = sql.connect(YF_DB_FILE)
        self.cursor = self.conn.cursor()
        self.table = table
        self.debug = 1

    # ----------------------------------------------------------------------- #
    def pprint(self):
        sql_cmd = "SELECT * FROM %s" % self.table
        self.cursor.execute(sql_cmd)
        for row in self.cursor.fetchall():
            print row

    # ----------------------------------------------------------------------- #
    def fetch_one_row(self, sql_code='', exit_if_none=False, error_msg=None):
        """
        Just fetch one row from sql code
        """
        self.cursor.execute(sql_code)
        
        row = self.cursor.fetchone() 
        
        if self.debug > 5:
            print 'YFDB.fetch_one_row():', row

        if not row and error_msg:
            print error_msg

        if not row and exit_if_none:
            sys.exit(1)
        
        return row

    # ----------------------------------------------------------------------- #
    def fetch_many_rows(self, sql_code, exit_if_none=False, error_msg=None):
        """
        Just fetch one row from sql code
        """
        self.cursor.execute(sql_code)
        
        rows = self.cursor.fetchall() 

        if not rows and error_msg:
            print error_msg

        if not rows and exit_if_none:
            sys.exit(1)
        
        return rows
    # ----------------------------------------------------------------------- #
    def fetch_one(self, sql_code, default_value=None):
        """
        Just fetch one column and one row from sql code, like from id to name,
        name to id, etc
        """
        self.cursor.execute(sql_code)
        
        row = self.cursor.fetchone()
        if row:
            return row[0]
        
        return default_value

    def fetch_many(self, sql_code, default_list_value=[]):
        """
        Just fetch many row of one column from sql code, like from sectorid to
        all stock tickers
        """
        if self.debug:
            print 'YFDB.fetch_many() - SQL code ==>', sql_code

        self.cursor.execute(sql_code)
        
        row = self.cursor.fetchall()
        if row:
            list_value = list(zip(*row)[0])
        else:
            list_value =[]
      
        len1 = len(list_value)
        len2 = len(default_list_value)

        if len1 < len2:
            list_value = default_list_value[:(len2-len1)] + list_value
        
        return list_value

class YFStock(YFDB):
    """
    Class YFStock: get stock info from finance.yahoo.com
    """

    # ----------------------------------------------------------------------- #
    def __init__(self):
        YFDB.__init__(self, 'Stock')
        self.debug = 0

        self.list_field_wget = ['Ticker', 'Name', 'FYEnds', 'Beta', 'AvgVol',
            'Shares', 'Floating', 'MarketCap', 'Active']

        self.list_field_all = ['StockID', 'Ticker', 'Active', 'Name', 'FYEnds', 
            'Beta', 'HasOption', 'Close', 'AvgVol', 'Shares', 'Floating', 
            'MarketCap', 'Start', 'End']

    # ----------------------------------------------------------------------- #
    def test(self, *args):
        if len(args) < 1 or re.search('usage|help', args[0]):
            print """yahoofinance.py test_yfstock <method> <args>
            
            get_stock_id      <ticker> e.g. get_stock_id YHOO
            get_stock_info    <ticker> e.g. get_stock_info TSLA
            get_stock_info_dict    <ticker> e.g. get_stock_info TSLA
            wget_stock_info   <ticker> e.g. wget_stock_info OCLR
            upsert_stock_info <ticker> e.g. upsert_stock_info OCLR
            aget_stock_id     <ticker> e.g. aget_stock_id OCLR
            adjust_active              
            """
        else: 
            try: 
                if args[0] == 'get_stock_id': 
                    print self.get_stock_id(*args[1:])
                elif args[0] == 'get_stock_info_dict': 
                    print self.get_stock_info_dict(*args[1:])
                elif args[0] == 'get_stock_info': 
                    stock_info = self.get_stock_info(*args[1:])
                    if stock_info: 
                        pprint_name_value(self.list_field_all, stock_info)
                    else:
                        print stock_info
                elif args[0] == 'wget_stock_info': 
                    print self.wget_stock_info(*args[1:])
                elif args[0] == 'upsert_stock_info':
                    print self.upsert_stock_info(*args[1:])
                elif args[0] == 'aget_stock_id':
                    print self.aget_stock_id(*args[1:])
                elif args[0] == 'adjust_active':
                    print 'here'
                    self.adjust_active()
                else:
                    self.test('usage')
            except:
                raise
                self.test('usage')

    # ----------------------------------------------------------------------- #
    def get_stock_id(self, ticker, active=0):
        """
        get stock id
        """
        
        # skip NAME, don't know why this ticker always breaks stockid
        if ticker == "NAME":
            return None

        return self.fetch_one("""
            SELECT StockID FROM Stock
            WHERE Ticker="%s" AND active>=%d
            """ % (ticker, active)
            )

    # ----------------------------------------------------------------------- #
    def aget_stock_id(self, ticker):
        """
        Get StockID of ticker. If not existing, add it to DB
        """
        id = self.get_stock_id(ticker)

        # if ticker not in stock table, 1) if index, just insert; 2) if stock
        # try to wget stock info, if not valid, return None
        if id == None:
            if ticker[0] == '^':
                self.insert_index_info(ticker)
            else: 
                self.upsert_stock_info(ticker)
            id = self.get_stock_id(ticker)

        return id

    # ----------------------------------------------------------------------- #
    def re(self, re_str, line, default=0):
        found = re.match(re_str, line)
        if found:
            return found.group(1)
        else:
            return default

    # ----------------------------------------------------------------------- #
    def get_stock_info_dict(self, ticker, active=1):
        """
        fetch stock info in dictionary format, so no need to worry about 
        table structure change in future

        # StockID   integer primary key NOT NULL,   1)       
        # Ticker    char(10) DEFAULT 'NA',          2)       x
        # Active    integer  DEFAULT 0,             3)      
        # Name      text     DEFAULT 'NA',          4)       x
        # FYEnds    text     DEFAULT '12-31',       5)       x
        # Beta      real     DEFAULT '-1.0',        6)       x
        # HasOption integer  DEFAULT 0,             7)
        # Close     real     DEFAULT 0.0,           8)
        # AvgVol    integer  DEFAULT 0,             9)       x
        # Shares    integer  DEFAULT 0,            10)       x
        # Floating  integer  DEFAULT 0,            10)       x
        # MarketCap integer  DEFAULT 0,            10)       x
        # Start     text     DEFAULT '0000-00-00', 13)
        # End       text     DEFAULT '0000-00-00'  14)
        """

        list_value = self.get_stock_info(ticker, active)

        if list_value:
            return dict(self.list_field_all, list_value)
        else:
            return {}

    # ----------------------------------------------------------------------- #
    def get_stock_info(self, ticker, active=1):
        #SELECT StockID, Ticker, Name, FYEnds, Beta, 
        #HasOption, AvgVol, MarketCap, Start, End 
        self.cursor.execute("""
        SELECT * FROM Stock WHERE Ticker=? AND Active>?
        """, (ticker, active,))

        return self.cursor.fetchone() 

    # ----------------------------------------------------------------------- #
    def insert_index_info(self, ticker):
        r = self.cursor.execute("""
            INSERT INTO Stock 
            (Ticker, Active, Name, FYEnds,  Beta) VALUES 
            (?,      9,      ?,    '12-31', 1)
            """, (ticker, ticker)
            ) 
        self.conn.commit() 
        return r.rowcount

    # ----------------------------------------------------------------------- #
    def upsert_stock_info(self, ticker):
        #ticker, name, fy_ends, beta, avg_vol, shares, floating, mkt_cap, 
        #active
        list_value = self.wget_stock_info(ticker)

        if list_value[1] == None or list_value[1] == 'NA':
            #and list_value[-2] == 0):
            if self.debug:
                print 'failed to get stock info or invalid ticker - %s' % ticker
            return 0

        stock_id = self.get_stock_id(ticker)

        # if stock_id == None, just insert
        if stock_id == None:
            r = self.cursor.execute("""
            INSERT INTO Stock 
            (%s) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """ % ','.join(self.list_field_wget), list_value
            )
        else:
            #ticker, name, fy_ends, beta, avg_vol, shares, floating, mkt_cap
            r = self.cursor.execute("""
            UPDATE Stock SET
            Name=?, FYEnds=?, Beta=?, AvgVol=?, Shares=?, Floating=?, 
            MarketCap=?, Active=?
            WHERE StockID=?""", 
            (tuple(list_value[1:]) + tuple([stock_id]))
            )

        self.conn.commit()

        return r.rowcount

    # ----------------------------------------------------------------------- #
    def active_level(self, average_volume, market_cap, amount, has_option,
        price):
        '''
        0: not active/delisted, not decided by this func but when changed
        1: listed/valid, but not active
        9: listed and active, (avgvol>200K & mktcap>100M) or (avg_vol)>500K
        '''
        level = 0

        if has_option:
            level = 2

        mktcap_factor = convert_int(market_cap)/250
        if mktcap_factor > 4:
            level += 5
        else:
            level += mktcap_factor

        vol_factor = convert_int(average_volume)/250000
        if vol_factor > 4:
            level += 5
        else:
            level += vol_factor

        # amount < 100K, it is 0
        if amount != -1 and amount < 500000:
            level = 0

        # price < 1, forget 
        if price != -1 and price < 1:
            level = 0
                
        return level
                
    # ----------------------------------------------------------------------- #
    def wget_stock_info(self, ticker):
        """
        Get all stock finance info from finance.yahoo.com, including
        name, FY ends, beta, Market Cap, etc.
        # StockID   integer primary key NOT NULL,   1)       
        # Ticker    char(10) DEFAULT 'NA',          2)       x
        # Active    integer  DEFAULT 0,             3)      
        # Name      text     DEFAULT 'NA',          4)       x
        # FYEnds    text     DEFAULT '12-31',       5)       x
        # Beta      real     DEFAULT '-1.0',        6)       x
        # HasOption integer  DEFAULT 0,             7)
        # Close     real     DEFAULT 0.0,           8)
        # AvgVol    integer  DEFAULT 0,             9)       x
        # Shares    integer  DEFAULT 0,            10)       x
        # Floating  integer  DEFAULT 0,            10)       x
        # MarketCap integer  DEFAULT 0,            10)       x
        # Start     text     DEFAULT '0000-00-00', 13)
        # End       text     DEFAULT '0000-00-00'  14)
        """

        # assign the default values
        name = 'NA'

        _avg_vol = '0'
        _fy_ends = 'Dec 31'
        _beta = '-1.0'
        _shares = '0'
        _mkt_cap = '0'
        _floating = '0'
        
        #
        #(1, u'BRCD', u'Brocade Communications Systems, Inc.', u'Nov 1', 1.23,\
        # 0, 5189210, 5140, u'0000-00-00', u'0000-00-00')
        #

        url = 'http://finance.yahoo.com/q/ks?s=%s+Key+Statistics' % ticker
        p = SimpleHTMLParser(url)

        # if not found ticker, reply [ticker + None * 9]
        if re.search('no results for the given search term', 
            p.html_text):
            return [ticker] + [None] * 7

        for line in p.html_text.split('\n'):
            # |Fiscal Year Ends:|Dec 31
            _fy_ends = self.re('^\|Fiscal Year Ends:\|(.*)', line, _fy_ends)

            _avg_vol = self.re('^\|Avg Vol.*3 month.*\|(\S*)', line, _avg_vol)
            _mkt_cap = self.re('^\|Market Cap.*:\|\s*(\S+)', line, _mkt_cap)
            _shares = self.re('^\|Shares Outs.*:\|\s*(\S+)', line, _shares)
            _floating = self.re('^\|Float:\|\s*(\S+)', line, _floating)

            # |Beta:|1.47, N/A
            _beta = self.re('^\|Beta:\|\s*(\S+)', line, _beta)

            # <h2>Diana Shipping Inc. (DSX)</h2> 
            name = self.re('^\|(.*) \(%s\)' % ticker, line, name)
          
        # convert info string to correct format
        avg_vol = convert_int(re.sub('\,', '', _avg_vol))
        fy_ends = date_atoymd(_fy_ends, '12-31')
        mkt_cap = convert_int(_mkt_cap, 'M')
        shares = convert_int(_shares)
        floating = convert_int(_floating)
        beta = convert_float(_beta, -1.0)

        #active_level(average_volume, market_cap, amount, has_option, price):
        active = self.active_level(avg_vol, mkt_cap, -1, 0, -1)
       
        name = unicode(name, errors='replace').strip()

        if self.debug:
            print 'wget_stock_info, url - ', url 
            pprint_name_value(self.list_field_wget, [ticker, name, fy_ends, \
                beta, avg_vol, shares, floating, mkt_cap, active])

        return ticker, name, fy_ends, beta, avg_vol, shares, floating,\
            mkt_cap, active

    # ----------------------------------------------------------------------- #
    def adjust_active(self): 
        '''
        Go thru all stock info, adjust active field
        #if mkt_cap > 100 and avg_vol > 500000:
        * avgvol > 
        '''

        self.cursor.execute("""
            SELECT * FROM Stock WHERE Active!=-1
            """)

        for row in self.cursor.fetchall():
            print row[1]

class StockER(YFDB):
    """
    Class: Stock ER info
    """
    # ----------------------------------------------------------------------- #
    def __init__(self):
        YFDB.__init__(self, "StockER")
        self.debug = 0
        self.stock_obj = YFStock()
        self.sector_obj = YFSector()

    # ----------------------------------------------------------------------- #
    def test(self, *args):
        if len(args) < 1 or re.search('usage|help', args[0]):
            print """yahoofinance.py test_yfstocker <method> <args>
            
            wget_yfer_day         day (ike '2015-10-27' or '20151027')
            wget_yfer_range       start_day end_day
            wget_yfer_next_months start_day end_day
            upsert_er_record      ticker, source, day, time
            get_er_record         ticker, source, day
            get_cy_quarter        ticker, date
            """
        else: 
            if args[0] == 'all': 
                self.debug = 1
                print '!!!!!! wget_yfer_day("2016-04-26") !!!!'
                self.wget_yfer_day("2016-04-26")
                print '!!!!!! get_er_record("BRX", "YF", "2016-04-26") !!!!'
                print self.get_er_record(ticker="BRX", source="YF", rawdate="2016-04-26")
            elif args[0] == 'wget_yfer_day': 
                n = self.wget_yfer_day(*args[1:])
                print 'er records in day:', n
            elif args[0] == 'wget_yfer_range': 
                n = self.wget_yfer_range(*args[1:])
                print 'total ER records in range:', n
            elif args[0] == 'wget_yfer_next_months': 
                if len(args) > 1: 
                    m = int(args[1])
                    n = self.wget_yfer_next_months(m)
                else:
                    m = 3
                    n = self.wget_yfer_next_months()
                print 'total ER records in next %d months:' % m, n
            elif args[0] == 'upsert_er_record': 
                i = self.upsert_er_record(*args[1:])
                print '%d records inserted' % i
            elif args[0] == 'get_er_record': 
                for row in self.get_er_record(
                    ticker=args[1], 
                    source=args[2], 
                    rawdate=args[3],
                    erdate=args[4]):
                    print row
            elif args[0] == 'get_cy_quarter': 
                print self.get_cy_quarter(*args[1:])
            else: 
                self.test('usage')
    
    # ----------------------------------------------------------------------- #
    def get_cy_quarter(self, ticker, date_):
        """
        based on ticker, have the FY-end-day, and calculate CY quarter, like
        YHOO, 2015-09
        """

        fy_ends = self.fetch_one_row(sql_code="""
            SELECT FYEnds from Stock
            WHERE Ticker="%s" and Active>0
            """ % ticker
            )

        if fy_ends == None:
            return None

        print fy_ends
    # ----------------------------------------------------------------------- #
    def get_er_record(self, ticker, source='YF', rawdate='', erdate='', 
        fy_quarter='', cy_quarter=''):
    
        sql_code="""
            SELECT * from StockERView
            WHERE Ticker="%s" AND Source="%s"
            """ % (ticker, source)

        if not rawdate == '':
            sql_code += """
            AND ERRawDate="%s"
            """ % rawdate

        if not erdate == '':
            sql_code += """
            AND ERDate="%s"
            """ % erdate

        return self.fetch_many_rows(sql_code=sql_code)
    
    # ----------------------------------------------------------------------- #
    #CREATE TABLE StockER (
    #StockID   integer NOT NULL,
    #SourceID  integer NOT NULL,
    #RawDate   char(10),
    #RawTime   char(20),
    #FYQuarter char(6),
    #CYQuarter char(6),
    #ERDate    char(10),
    #FOREIGN KEY(StockID)  REFERENCES Stock(StockID),
    #FOREIGN KEY(SourceID) REFERENCES Source(SourceID)
    #);
    def upsert_er_record(self, ticker, source, date_, time):
        stock_id = self.stock_obj.get_stock_id(ticker)
        source_id = self.sector_obj.get_source_id(source)

        # if stock_id == None, invalid ticker, report it the return
        if stock_id == None:
            print 'Error StockER.upsert_er_record(): unknown ticker - %s' % ticker, date_, time
            return 0

        # if source_id == None, invalid source, report it the return
        if source_id == None:
            print 'Error StockER.upsert_er_record(): invalid source - %s' % source
            print ticker, source, date_, time
            return 0

        if self.debug > 3: 
            print "er info", ticker, source, date_, time 

        if self.get_er_record(ticker, source, date_):
            r = self.cursor.execute("""
                UPDATE StockER 
                SET RawTime=?
                WHERE 
                    StockID=? AND
                    SourceID=? AND
                    RawDate=?
                """, (time, stock_id, source_id, date_)
            )
            if self.debug > 3: 
                print "update er recode"
        else:
            r = self.cursor.execute("""
                INSERT INTO StockER 
                (StockID, SourceID, RawDate, RawTime)
                VALUES (?, ?, ?, ?) 
                """, (stock_id, source_id, date_, time)
            )
            if self.debug > 3: 
                print "insert er recode"
                print """
                INSERT INTO StockER 
                (StockID, SourceID, RawDate, RawTime)
                VALUES (%d, %d, %s, %s) 
                """ %  (stock_id, source_id, date_, time) 
                print "insert er recode"
        
        self.conn.commit() 
        return r.rowcount

    # ----------------------------------------------------------------------- #
    def wget_yfer_next_months(self, num_months=3):
        """
        Get all ER data in next # days from today
        """
        today = YFDate().today_ymd
        y,m,d = map(int, today.split('-'))

        m += num_months
        if m > 12:
            m = m - 12
            y += 1

        end = '%04d-%02d-%02d' % (y, m, d)
        
        return self.wget_yfer_range(today, end)

    # ----------------------------------------------------------------------- #
    def wget_yfer_range(self, start, end):
        """
        Get all ER data in range
        """

        if self.debug:
            print "StockER.wget_yfer_range(): %d days to wget_yfer_day()" %\
            len(range_day(start, end))
       
        num_record = 0
        num_day = 0

        for date_ in range_day(start, end):
            num_day += 1
            num_record += self.wget_yfer_day(date_)

        return num_record
                
    # ----------------------------------------------------------------------- #
    def wget_yfer_day(self, date_):
        """
        Get all ER stock on the day from url:
        http://biz.yahoo.com/research/earncal/20150612.html
        """

        # date_ is the yyyy-mm-dd, convert it to yyyymmdd
        day = re.sub('-', '', date_)

        url = 'http://biz.yahoo.com/research/earncal/%s.html' % day

        p = SimpleHTMLParser(url)

        rows = []

        # by default, not to parse
        start = 0

        for line in p.html_text.split('\n'):
            if start:
                items = map(str.strip, line.split('|'))

                # if not 3 or 4 columns table, skip
                if len(items) < 4:
                    continue
                elif len(items) == 4:
                    second_time = ''
                else:
                    second_time = items[4]

                # if not valid ticker, skip
                ticker = self.is_ticker(items[2])
                if not ticker: 
                    continue
                    
                # There is 2 format in yahoo finance ER cal, so we try both
                # 4 columns: |Company Name| TICK|EPS|Time|
                # 3 columns: |Company Name| TICK|Time|
                time_ = self.get_er_time_from_2items(items[3], second_time,
                    ticker)

                # if not valid time string, skip
                if not time_:
                    continue
                
                # ok, reach here it is a valid ER record 
                rows.append((ticker, 'YF', date_, time_))

            # start to parse if seen "Earning Announcment'
            elif re.search('Earnings Announcements for', line):
                start = 1
        
        for row in rows:
            ticker, source, date_, time_ = row
            self.upsert_er_record(ticker, source, convert_date(date_), time_)

        print 'StockER.wget_yfer_day(): ER data on !!! %s !!!, %d records' %\
            (date_, len(rows))

        return len(rows)

    # ----------------------------------------------------------------------- #
    def get_er_time_from_2items(self, str1, str2, ticker=''):
        """
        if str1 is not valid er time, try str2, otherwise return 'tns'
        """
        er_time = self.get_er_time(str1)
        if er_time:
            return er_time
        
        er_time = self.get_er_time(str2)
        if er_time:
            return er_time
       
        if self.debug > 3:
            print 'StockER().get_er_time_from_2items(): unrecognized er time'
            print ticker, str1, str2
        return 'tns'

    # ----------------------------------------------------------------------- #
    def get_er_time(self, time_str):
        """
        get ER time, 3 results: BMO|AMC|TNS
        if time supplied, <=
        """
        if re.match('Before Market Open', time_str):
            return 'bmo'
        elif re.match('After Market Close', time_str):
            return 'amc'
        elif re.match('Time Not Supplied', time_str):
            return 'tns'

        time_re = re.match('(\d+)\:(\d+)\s+([p|a]m)\s+et', time_str.lower())

        if time_re:
            hour, minute, apm = map(convert_int, time_re.groups())

            if time_re.group(3).lower()== 'pm':
                hour += 12

            time_int_hhmm = hour * 100 + minute

            if time_int_hhmm <= 900:
                return 'bmo'
            elif time_int_hhmm >= 1600:
                return 'amc'
            else:
                return 'tns'

        return None

    # ----------------------------------------------------------------------- #
    def is_ticker(self, ticker):
        """
        must be ^[A-Z]+$
        """
        if re.match('^[A-Z]+$', ticker.strip()):
            return ticker.strip()

        return None

class YFInsider(YFDB):
    """
    Class for Insider info in yahoo finance
    """
    # ----------------------------------------------------------------------- #
    def __init__(self):
        YFDB.__init__(self, "Insider")
        self.debug = 0

    def test(self):
        print self.get_insider_id('WARREN KELCY L', 'http://biz.yahoo.com/t/69/6162.html')

    # ----------------------------------------------------------------------- #
    def get_or_add_insider_id(self, name, form_url): 
        id = self.get_insider_id(name, form_url)

        if id == None:
            self.add_insider_id(name, form_url)
            id = self.get_insider_id(name, form_url)
        return id

    # ----------------------------------------------------------------------- #
    def add_insider_id(self, name, form_url): 
        if not self.get_insider_id(name, form_url): 
            self.cursor.execute("""
                INSERT INTO Insider(Name, Form4Url)
                values (?, ?)
                """, 
                (name, form_url)
                ) 
            self.conn.commit()
   
    # ----------------------------------------------------------------------- #
    def get_insider_id(self, name, form_url): 
        return self.fetch_one("""
            SELECT InsiderID FROM Insider 
            WHERE Name='%s' and Form4Url='%s'
            """ % (name, form_url)
            )
       
    # ----------------------------------------------------------------------- #
    def delete_insider_by_id(self, id):
        if id == None:
            self.cursor.execute("""
                DELETE FROM Insider
                """)
        else:
            self.cursor.execute("""
                DELETE FROM Insider
                WHERE InsiderID=?
                """, (id,)
                )
        self.conn.commit()

class YFInsiderTransaction(YFDB):
    # ----------------------------------------------------------------------- #
    def __init__(self):
        YFDB.__init__(self, "InsiderTrans")
        self.debug = 1

    def test(self, *args):
        print args
        if len(args) < 1 or re.search('usage|help', args[0]):
            print """yahoofinance.py test-yfinsidertrans <method> <args> 
            wget_insider_transaction ticker  : wget ticker's insider trans 
            """
        else:
            if args[0] == 'wget_insider_transaction':
                self.wget_insider_transaction(*args[1:])
        
    # ----------------------------------------------------------------------- #
    def parse_transaction(self, transaction): 
        """
        Parse transactions from yahoo finance, examples: 
        - Sale at $10.75 - $10.93 per share 
        - Purchase at $8.97 per share 

        Following are not counted, which are director's share payment
        - Disposition (Non Open Market) at $23.01 per share 
        - Option Exercise at $19.59 per share 
        - Automatic Sale at $23 per share 
        """

        price = '0.0' 
        trans = 'NA'

        m = re.search('(Purchase|Sale).*at \$([\d\.]+).*per share', \
            transaction, flags=re.IGNORECASE)

        if m:
            if m.group(1) == 'Purchase': 
                trans = 'B' 
            else:
                trans = 'S' 
            
            price = m.group(2)
        
        if self.debug > 2:
            print 'transaction: %s -> %s, %s' % (transaction, trans, price)
        
        # return trans and price in string formate
        return [trans, price]

    # ----------------------------------------------------------------------- #
    # WAECHTER THOMAS H ; Officer 
    def parse_insider(self, insider): 
        [name, title] = [insider, 'N/A'] 
        
        m = re.match('^(.*)\s+(Director|Beneficial Owner|Officer)$', \
            insider, re.IGNORECASE) 
        
        if m: 
            [name, title] = [m.group(1), m.group(2)]

        if self.debug >= 2:
            print '[%s] -> [%s], [%s]' % (insider, name, title) 
        return [name, title]

    # ----------------------------------------------------------------------- #
    def wget_insider_transaction(self, ticker): 
        stock_id = YFStock().aget_stock_id(ticker)

        if stock_id == None:
            if self.debug:
                print 'failed to get stock id for %s' % ticker
            return 
        
        insider = YFInsider()
        url = 'http://finance.yahoo.com/q/it?s=%s+Insider+Transactions' \
            % ticker

        rows_to_add = []
        p = SimpleHTMLParser(url, YF_INSIDER)
        for line in p.html_text.split('\n'): 
            # skip those undirect transaction
            if not re.search("\|.*irect\|.*[0-9]+", line, re.IGNORECASE): 
                continue

            print line
            [dummy, date, who, shares, dummy, transaction, value, \
                url] = map(str.strip, line.split('|'))
    
            [trans, price] = self.parse_transaction(transaction)

            # only record Purchase|Sale records with price, value
            if trans == 'NA' or float(price) == 0.0 or value == 'N/A':
                continue
    
            # get date, ignore old ones
            date = date_atoymd(date)
            
            [insider_name, role] = self.parse_insider(who)
    
            shares = int(re.sub(',', '', shares))
            value = int(re.sub(',', '', value))

            insider_id = insider.get_or_add_insider_id(insider_name, url)

            rows_to_add.append((stock_id, insider_id, role, date, trans, price, 
                shares, value))
            
        if self.debug: 
            for row in rows_to_add:
                print 'rows to add into Insider Trans:' 
                print row

        self.update_insider_trans(rows_to_add)

    # ----------------------------------------------------------------------- #
    # CREATE TABLE InsiderTrans(
    # StockID     integer NOT NULL,
    # InsiderID   integer NOT NULL,
    # Title       text,
    # Date        char(10),
    # Type        char(10),
    # BuySell     char(1),
    # Price       real,
    # Shares      integer,
    # Amount      real,
    # FOREIGN KEY(StockID) REFERENCES Stock(StockID),
    # FOREIGN KEY(InsiderID) REFERENCES Insider(InsiderID));
    #
    # rows_to_add.append((stock_id, insider_id, role, date, trans, price, 
    #            shares, value))
    # ----------------------------------------------------------------------- #
    def update_insider_trans(self, rows_to_add):

        _rows_to_add = []
        for row in rows_to_add:
            (stock_id, insider_id, role, date, trans, price, shares, value) \
            = row

            record = self.fetch_one("""
            SELECT * FROM InsiderTrans 
            WHERE StockID=\"%s\" AND InsiderID=\"%s\" AND Date=\"%s\" AND
                  BuySell=\"%s\" AND Shares=\"%s\"
            """ % (stock_id, insider_id, date, trans, shares)
            )
      
            if record == None:
                _rows_to_add.append(row)

        if len(_rows_to_add):
            self.cursor.executemany(""" 
                INSERT INTO InsiderTrans 
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """, 
                _rows_to_add
                )
            self.conn.commit()

class Sector(YFDB):
    '''
    Base class - Sector
    '''

    def __init__(self):
        YFDB.__init__(self, "StockSector")
        self.debug = 1

        # --------------------------- dictionaries -------------------------- #
        # ticker to sector id, industryid, AAPL->'YF',
        self.ticker_source_sector_industry = {}
        self.tickers_in_source_sector_industry = {}

    def test(self, *args): 
        if len(args) < 1 or re.search('usage|help', args[0]):
            print """yahoofinance.py test_sector <method> <args>
            
            all  - all tests
            convert - conver_all_sector_info()
            """
        else: 
            if args[0] == 'convert': 
                self.convert_old_sector_info()
            else:
                list_title, list_value = [], []
        
                list_title.append("SouceID:1")
                list_value.append(self.get_source_name(1))
                list_title.append("SectorID:2")
                list_value.append(self.get_sector_name(2))
                list_title.append("IndustryID:3")
                list_value.append(self.get_industry_name(3))
                list_title.append("YF")
                list_value.append(self.get_source_id("YF"))
                list_title.append("Technology")
                list_value.append(self.get_sector_id("Technology"))
                list_title.append("Computer Peripherals")
                list_value.append(self.aget_industry_id("Computer Peripherals")) 
                list_title.append("Stocks:Technology")
                list_value.append(len(self.get_stocks_in_source_sector_industry('YF', 
                    'Technology', "all")))
                list_title.append("Stocks:Computer Peripherals")
                list_value.append(';'.join(self.get_stocks_in_source_sector_industry('YF', 
                    'Technology', "Computer Peripherals")))
                list_title.append("YHOO Sector Info")
                list_value.append(self.get_stock_sector_info_all('YHOO'))
                list_title.append("TSLA Sector Info")
                list_value.append(self.get_stock_sector_info_all('TSLA'))
                list_title.append("NONE Sector Info")
                list_value.append(self.get_stock_sector_info('NONE'))
               
                list_title.append("upsert_ind('TEST', 'test', 'test', ['YHOO', 'AMZN', 'GOOGL'])")
                list_value.append(self.upsert_industry_stocks('TEST', 'test', 'test', ['YHOO', 'AMZN', 'GOOGL']))
                #upsert_industry_stocks(self, source='YF', sector='', industry='', stocks=[]):
                pprint_name_value(list_title, list_value)

    # ----------------------------------------------------------------------- #
    def _get_name_by_id(self, id, table_name='Source'):
        """
        get name by id, from 3 tables, Source, Sector, Indutry
        """
        id_name = 'SourceID'

        if table_name == 'Sector':
            table_name = 'Sector'
            id_name = 'SectorID'
        elif table_name == 'Industry':
            table_name = 'Industry'
            id_name = 'IndustryID'

        return self.fetch_one("""
            SELECT Name FROM %s WHERE %s='%s' 
            """ % (table_name, id_name, id)
            )

    # ----------------------------------------------------------------------- #
    def get_source_name(self, source_id):
        return self._get_name_by_id(source_id, 'Source')
    
    # ----------------------------------------------------------------------- #
    def get_sector_name(self, sector_id):
        return self._get_name_by_id(sector_id, 'Sector')
    
    # ----------------------------------------------------------------------- #
    def get_industry_name(self, industry_id):
        return self._get_name_by_id(industry_id, 'Industry')
    
    # ----------------------------------------------------------------------- #
    def _get_id_by_name(self, name, table_name='Source'):
        """
        get id by name, from 3 tables, Source, Sector, Indutry
        """

        if table_name == 'Source':
            id_name = 'SourceID'
        elif table_name == 'Sector':
            id_name = 'SectorID'
        elif table_name == 'Industry':
            id_name = 'IndustryID'
        else:
            print 'Incorrect table name - %s' % table_name
            sys.exit(0)

        return self.fetch_one("""
            SELECT %s FROM %s WHERE Name='%s' 
            """ % (id_name, table_name, name)
            )

    def aget_source_id(self, name):
        id = self.get_source_id(name)

        if not id:
            self.add_source(name, name)
            id = self.get_source_id(name)

        return id

    # ----------------------------------------------------------------------- #
    def get_source_id(self, name):
        return self._get_id_by_name(name, table_name='Source')

    # ----------------------------------------------------------------------- #
    def add_source(self, name, description=''):
        self.cursor.execute("""
            INSERT INTO Source(Name, Description) 
            values (?, ?) 
            """, 
            (name, description)
            ) 
        self.conn.commit()
   
    # ----------------------------------------------------------------------- #
    def aget_sector_id(self, name):
        id = self.get_sector_id(name)
        
        if not id:
            self.add_sector(name, name)
            id = self.get_sector_id(name)
        
        return id

    # ----------------------------------------------------------------------- #
    def get_sector_id(self, name):
        return self._get_id_by_name(name, table_name='Sector')

    # ----------------------------------------------------------------------- #
    def add_sector(self, name, description=''):
        self.cursor.execute("""
            INSERT INTO Sector(Name, Description) 
            values (?, ?) 
            """, 
            (name, description)
            ) 
        self.conn.commit()
   
    # ----------------------------------------------------------------------- #
    def aget_industry_id(self, name):
        """
        get or add industry id by name
        """
        id = self.get_industry_id(name)

        if not id:
            self.add_industry(name, name)
            id = self.get_industry_id(name)

        return id

    # ----------------------------------------------------------------------- #
    def get_industry_id(self, name):
        return self._get_id_by_name(name, table_name='Industry')

    # ----------------------------------------------------------------------- #
    def add_industry(self, name, description=''):
        self.cursor.execute("""
            INSERT INTO Industry(Name, Description) 
            values (?, ?) 
            """, 
            (name, description)
            ) 
        self.conn.commit()
   
    # ----------------------------------------------------------------------- #
    def get_all_stock(self, source='YF', sector='', industry='', min_active=0,
        order_by='MarketCap', order='DESC'):
        '''
        find all stickers with inputed conditions from VIEW of StockView
        '''
        sql_code = """
            SELECT Ticker
            FROM StockView
            WHERE Source="%s" AND Active>=%d
            """ % (source, min_active)

        if sector != '':
            sql_code += """
            AND Sector="%s"
            """ % sector

        if industry != '':
            sql_code += """
            AND Industry="%s"
            """ % industry

        if order_by != '':
            sql_code += """
            ORDER BY %s %s
            """ % (order_by, order)

        return self.fetch_many(sql_code)

    # ----------------------------------------------------------------------- #
    def get_stock_sector_info_all(self, stock, source="YF"): 
        """
        based on stock ticker, return a list of stock sector info 
        get_stock_sector_info + list_industry_stocks 
        """

        rows = []
        for row in self.get_stock_sector_info(stock, source):
            (d, d, d, source, d, sector, d, industry) = row
            rows.append(list(row) + [\
                list(self.get_stocks_in_source_sector_industry(source, sector, \
                industry))])

        return rows

    # ----------------------------------------------------------------------- #
    def get_stock_sector_info(self, stock, source="YF"): 
        """
        based on stock ticker, return a list of stock sector info 
        [stockid, stock, source, sourceid, sector, sectorid, 
        industry, industryid]
        """
        sql_cmd = """
            SELECT 
                StockSector.StockID,
                Stock.Ticker,
                StockSector.SourceID,
                Source.Name,
                StockSector.SectorID,
                Sector.Name,
                StockSector.IndustryID,
                Industry.Name
            FROM 
                StockSector
                LEFT JOIN Stock ON Stock.StockID=StockSector.StockID
                LEFT JOIN Source ON Source.SourceID=StockSector.SourceID
                LEFT JOIN Sector ON Sector.SectorID=StockSector.SectorID
                LEFT JOIN Industry ON Industry.IndustryID=StockSector.IndustryID
            WHERE 
                Stock.Ticker="%s" and Stock.Active>=0
        """ % (stock)

        if source.lower() != 'all':
            sql_cmd += 'AND Source.Name="%s"' % source

        self.cursor.execute(sql_cmd) 

        return self.cursor.fetchall()

    # ----------------------------------------------------------------------- #
    def get_stocks_in_source_sector_industry(self, source, sector, industry):
        """
        based source/sector, get all tickers
        """
        sql_cmd = """
            SELECT 
                Stock.Ticker
            FROM 
                StockSector
                LEFT JOIN Stock ON Stock.StockID=StockSector.StockID
                LEFT JOIN Source ON Source.SourceID=StockSector.SourceID
                LEFT JOIN Sector ON Sector.SectorID=StockSector.SectorID
                LEFT JOIN Industry ON Industry.IndustryID=StockSector.IndustryID
            WHERE 
                Stock.Active>=0
        """
        if source.lower() != 'all':
            sql_cmd += 'AND Source.Name="%s"' % source
            
        if sector.lower() != 'all':
            sql_cmd += 'AND Sector.Name="%s"' % sector

        if industry.lower() != 'all':
            sql_cmd += 'AND Industry.Name="%s"' % industry
       
        return self.fetch_many(sql_cmd)
   
    #WORK
    # CREATE TABLE StockSector (
    # StockID    integer NOT NULL,
    # SourceID   integer NOT NULL,
    # SectorID   integer NOT NULL,
    # IndustryID integer NOT NULL,
    # UNIQUE(StockID, SourceID, SectorID, IndustryID) ON CONFLICT REPLACE,
    # FOREIGN KEY(StockID)    REFERENCES Stock(StockID),
    # FOREIGN KEY(SourceID)   REFERENCES Source(SourceID),
    # FOREIGN KEY(SectorID)   REFERENCES Sector(SectorID),
    # FOREIGN KEY(IndustryID) REFERENCES Industry(IndustryID)
    # );

    # ----------------------------------------------------------------------- #
    def upsert_industry_stocks(self, source='YF', sector='', industry='', 
        stocks=[]):
        """
        update or insert source/sector/industry information with list of stocks
        """

        source_id = self.aget_source_id(source)
        sector_id = self.aget_sector_id(sector)
        industry_id = self.aget_industry_id(industry)
    
        num_record = 0

        for stock in stocks:
            stock_id = YFStock().aget_stock_id(stock)

            if not stock_id:
                print 'error ticker - %s' % stock
                continue

            sql_cmd = """
            INSERT or REPLACE INTO StockSector 
            (StockID, SourceID, SectorID, IndustryID)
            VALUES (?, ?, ?, ?)
            """
   
            self.cursor.execute(sql_cmd, 
            (stock_id, source_id, sector_id, industry_id)
            )

            self.conn.commit()
            num_record += 1

        return num_record

    # ----------------------------------------------------------------------- #
    def convert_old_sector_info(self):
        """
        convert old sector data into sqlite DB
        """
        lines = []

        with open('/home1/uptrendc/tools/data/sector/sector.dat', 'r') as f:
            lines = f.readlines() 
        
        f.close()

        #good - watch::Finance - Banks::Big Banks::GS:JPM:BAC:C:WFC
        for line in lines:
            items = line.strip().split('::')

            if len(items) < 4:
                continue

            # skip YF entries
            if items[0] == 'yahooFin':
                continue
            
            cnt = self.upsert_industry_stocks(
                source=items[0], 
                sector=items[1], 
                industry=items[2], 
                stocks=items[3].strip().upper().split(':')
                )

            if self.debug:
                print items
                #print '{0:10} {1:20} {2:20} {3}'.format(items)
        
class YFSector(Sector):
    """
    class YFSector: download and api to access yahoo finance sector info
    """

    # ----------------------------------------------------------------------- #
    def __init__(self):
        Sector.__init__(self)
        self.source = 'YF'
        self.source_id = self.get_source_id(self.source)
        self.debug = 0

    def test(self, *args):
        if len(args) < 1 or re.search('usage|help', args[0]):
            print """yahoofinance.py test-yfsector <method> <args>
            wget_all <1st # code> : get all yahoo finance of 1st code if present
            wget_industry_summary : get list of yhaoo finance ind codes
            wget_industry  <code> : get yahoo finance industry info
            get_all_stock <active>: source sector industry minactive orderby, order
            """
        else: 
            try: 
                if args[0] == 'sector':
                    super(YFSector, self).test()
                elif args[0] == 'wget_industry_summary': 
                    print '\n'.join(self.wget_industry_summary(*args[1:]))
                elif args[0] == 'wget_industry': 
                    self.wget_industry(*args[1:])
                elif args[0] == 'wget_all': 
                    self.wget_all(*args[1:])
                elif args[0] == 'get_all_stock': 
                    all_stock = self.get_all_stock(
                        source=args[1],
                        sector=args[2],
                        industry=args[3],
                        min_active=int(args[4])
                        )
                    print all_stock
                else:
                    self.test('usage')
            except:
                raise
                self.test('usage')
        pass

    # ----------------------------------------------------------------------- #
    def re(self, re_str, line, default=0):
        found = re.match(re_str, line)
        if found:
            return found.group(1).strip()
        else:
            return default

    # ----------------------------------------------------------------------- #
    def wget_all(self, first_digit='all'):
        """
        1) get list of ind codes from yahoo finance
        2) for each ind code, aget sotck id/info
        3) update StockSector table
        """
        codes = self.wget_industry_summary()

        for code in sorted(codes):
            if first_digit == 'all' or code[0] == first_digit:
                self.wget_industry(code)
    # ----------------------------------------------------------------------- #
    def wget_industry_summary(self, sector_1st_num='all'): 
        """
        Read all industry code from Yahoo Finance Industry Summary Page at 
        http://biz.yahoo.com/p/sum_conameu.html
        """

        list_code = []

        url = 'http://biz.yahoo.com/p/sum_conameu.html' 
        p = SimpleHTMLParser(url) 
       
        if p.html_text == 'Error':
            return 

        for url_ in p.list_url: 
            a = re.search('^(\d+)conameu.html', url_)

            if a:
                list_code.append(a.group(1))

        return sorted(list_code)
    
    # ----------------------------------------------------------------------- #
    def wget_industry(self, code): 
        url = 'http://biz.yahoo.com/p/%dconameu.html' % convert_int(code)

        if self.debug > 3: 
            print 'wget_industry: url=', url

        sector, industry = '', ''
        stock_list = []
        
        p = SimpleHTMLParser(url) 
       
        if p.html_text == 'Error':
            return 
        
        for line in p.html_text.split('\n'): 
            valid_ticker_found = False

            sector = self.re('^\|.*Sector:\s*([^||^\(]+)', line, sector)
            industry = self.re('^\|.*Industry:\s*([^|(]+)', line, industry)
            
            # find valid ticker, w/o '.' AND upper char
            # need to deal with a corner case:
            # line = '| BBR Holdings (S) Ltd( KJ5.SI)|'

            # this a greedy search, so get the last ()
            re_ticker = re.search('^\|.*[^|]+\((.*)\).*\|', line)
            if re_ticker:
                stock = re_ticker.group(1).strip() 
                valid_ticker_found = re.match('^([A-Z]+)$', stock)

            # if no valid ticker found, continue to next line
            if not valid_ticker_found:
                continue

            if stock not in stock_list: 
                stock_list.append(stock)
                
        if self.debug: 
            pprint_name_value(['Code', 'Sector', 'Industry', 'URL', 'Stocks'], 
                [code, sector, industry, url, ';'.join(stock_list)])

        if sector != '' and industry != '' and len(stock_list):
            rows = []

            sector_id = self.aget_sector_id(sector)
            industry_id = self.aget_industry_id(industry)

            for stock in stock_list:
                stock_id = YFStock().aget_stock_id(stock)

                # even stock_id = get_or_add, still have the chance
                # stock not in yahoofinance.
                if stock_id: 
                    rows.append((stock_id, self.source_id, sector_id, 
                        industry_id))
                else:
                    print "ERROR: wget_industry, url:", url, " sector:", \
                        sector, " industry:", industry, \
                        " REASON: can't get stockid - %s" % stock
           
            if len(rows): 
                if self.debug > 3:
                    print "rows add into StockSector table:" 
                    print rows

                self.cursor.executemany(""" 
                    INSERT INTO StockSector VALUES(?, ?, ?, ?) 
                    """, tuple(rows)
                    )

                self.conn.commit()

                print "Sector:", sector, " Indus:",  industry, "# stocks:",\
                    len(rows)
            else:
                print "ERROR: No rows to insert StockSector Table"
                pprint_name_value(
                    ['Code', 'Sector', 'Industry', 'URL', 'Stocks'], 
                    [code, sector, industry, url, ';'.join(stock_list)]
                    )

        else:
            print "ERROR: Incorrect data when wget_industry"
            pprint_name_value(
                ['Code', 'Sector', 'Industry', 'URL', 'Stocks'], 
                [code, sector, industry, url, ';'.join(stock_list)]
                )

class YFQuota(YFDB):
    '''
    Yahoofinance Historic Date Class, download, store and read stock
    daliy historic data from yahoofinance.com
    '''
    debug = 1
    # ----------------------------------------------------------------------- #
    def __init__(self):
        YFDB.__init__(self, 'DailyQuota')

        self.stock_obj = YFStock()
        self.debug = 3
        self.yfdate = YFDate()
    
    # ----------------------------------------------------------------------- #
    def run(self, *args):
        if len(args) < 1 or re.search('usage|help', args[0]):
            print """yahoofinance.py test-yfquota <method> <args>
            wget_daily      <ticker>
            get_daily       <ticker> [<end_ymd>] [<start_ymd>]
            delete_daily    <ticker> [<end_ymd>] [<start_ymd>]
            wget_daily_all  get all yahoo finance of 1st code if present
            do_calculation  <ticker> : do calcuation of ticker
            """

        else:
            try: 
                if args[0] == 'wget_daily': 
                    print self.wget_daily(*args[1:])
                elif args[0] == '_wget_daily': 
                    print self._wget_daily(ticker=args[1])
                elif args[0] == 'get_daily': 
                    rows = self.get_daily(*args[1:])
                    if rows and len(rows): 
                        pprint_name_value(['#', 'start', 'end'], 
                        [len(rows), rows[0], rows[-1]])
                    else:
                        print 'no rows retrieved'
                elif args[0] == 'delete': 
                    self.delete(*args[1:])
                elif args[0] == 'wget_daily_all': 
                    self.wget_daily_all()
                elif args[0] == 'do_calculation': 
                    self.do_calculation(*args[1:])
                else:
                    self.test('help')
            except:
                raise
                #self.test('help')
    
    # ----------------------------------------------------------------------- #
    # get_daily
    def get_daily(self, ticker='^GSPC', end_ymd='0000-00-00', 
        start_ymd='0000-00-00'):
        """
        get stock historic quota from local SQLite database
        """
        stock_id = YFStock().get_stock_id(ticker)

        if stock_id:
            sql_cmd = """
            SELECT * FROM DailyQuota 
            WHERE StockID=%s """ % (stock_id)

            if end_ymd != '' and end_ymd != '0000-00-00':
                sql_cmd += """
                AND Date<="%s"
                """ % end_ymd

            if start_ymd != '' and start_ymd != '0000-00-00':
                sql_cmd += """
                AND Date>="%s"
                """ % start_ymd
           
            sql_cmd += """
            ORDER by Date
            """

            if self.debug > 5:
                print 'YFQuota.get_daily() - SQL command:', sql_cmd

            return self.fetch_many_rows(sql_code=sql_cmd)
        else:
            return None
    
    # ----------------------------------------------------------------------- #
    # get_daily/classmethod, only for YFDate to load SP
    @classmethod
    def static_get_daily(self, ticker='^GSPC', end_ymd='0000-00-00', 
        start_ymd='0000-00-00', wget_if_none=0):

        """
        get stock historic quota from local SQLite database
        """
        debug_level = 0

        # YG
        stock_id = YFStock().get_stock_id(ticker)

        conn = sql.connect(YF_DB_FILE)
        cursor = conn.cursor()

        sql_cmd = """
        SELECT * FROM DailyQuota 
        WHERE StockID=%s
        """ % (stock_id)

        if end_ymd != '' and end_ymd != '0000-00-00':
            sql_cmd += ' AND Date<="%s"' % end_ymd

        if start_ymd != '' and start_ymd != '0000-00-00': 
            sql_cmd += ' AND Date>="%s"' % start_ymd
           
        sql_cmd += "\nORDER by Date"

        if debug_level > 2: 
            print 'YFQuota.static_get_daily() - SQL command:', sql_cmd

        #return self.fetch_many_rows(sql_code=sql_cmd)
        cursor.execute(sql_cmd)
        rows = cursor.fetchall()

        if len(rows) == 0 and wget_if_none:
            self._wget_daily(ticker=ticker)

            cursor.execute(sql_cmd) 
            rows = cursor.fetchall()

        return rows
    # ----------------------------------------------------------------------- #
    # CREATE TABLE DailyQuota (
    # StockID         integer NOT NULL,
    # Date            char(10),
    # Open            real,
    # High            real,
    # Low             real,
    # Close           real,
    # Volume          integer,
    # AdjClose        real,
    # Amount          real,
    # ClosePertage    real,
    # AverageVolum3M  integer,
    # CorrelationSP3M real,
    # PertSinceCYQtr  real,
    # PertSinceFYQtr  real,
    # PRIMARY KEY(StockID, Date),
    # FOREIGN KEY(StockID)    REFERENCES Stock(StockID));
    # ----------------------------------------------------------------------- #
    def insert(self, ticker, rows):
        stock_id = YFStock().aget_stock_id(ticker)

        rows_ = []
        for row in rows:
            rows_.append([stock_id] + row.split(','))

        r = self.cursor.executemany("""
            INSERT OR REPLACE INTO DailyQuota 
            (StockID, Date, Open, High, Low, Close, Volume, AdjClose)
            VALUES (?,?,?,?,?,?,?,?)
            """, tuple(rows_)
            )

        self.conn.commit()

        return r.rowcount

    # ----------------------------------------------------------------------- #
    def delete(self, ticker, end_ymd = '9999-99-99', start_ymd='0000-00-00'):
        stock_id = YFStock().get_stock_id(ticker)

        if stock_id:
            sql_cmd = """
            DELETE FROM DailyQuota WHERE StockID=%s 
            """ % (stock_id)

            if end_ymd != '':
                sql_cmd += ' AND Date<="%s"' % end_ymd
            if start_ymd != '':
                sql_cmd += ' AND Date>="%s"' % start_ymd
           
            if self.debug: 
                print 'sql command:', sql_cmd

            result = self.cursor.execute(sql_cmd)
            self.conn.commit()

            if self.debug:
                print '%d rows deleted - %s, %s-%s' % (result.rowcount, 
                    ticker, start_ymd, end_ymd)

            return result.rowcount

        else: 
            if self.debug:
                print 'invalid ticker to delete - %s' % ticker

            return 0

    # ----------------------------------------------------------------------- #
    def wget_daily(self, ticker='^GSPC'):
        """
        This is a wrapper of _wget_daily():
        1) get existing rows of this ticker;
        2) only wget rows between last row and last row of sp days
        """

        # if invalid ticker, return None
        if YFStock().aget_stock_id(ticker) == None:
            return None
        
        rows = self.get_daily(ticker)
        
        # len(rows) == 0, means not no records in db
        if rows == None or len(rows) == 0:
            start_date = START_DATE
            if self.debug > 2:
                print "YFQuota.wget_daily(): no records for ticker -", ticker,\
                    "so using -", start_date
        else:
            # if we have records, only get records btwn last day and today
            start_date = rows[-1][1]
            if self.debug > 2:
                print "YFQuota.wget_daily(): start_date for ticker -", ticker,\
                    "is", start_date
       
        end_date = self.yfdate.sp_days[-1]

        n = 0
        if start_date < end_date: 
            n = self._wget_daily(ticker=ticker, start_ymd=start_date, \
                end_ymd= end_date)

        if self.debug > 2: 
            print "YFQuota.wget_daily(): wget", n, "records from YF"
       
        return n
    # ----------------------------------------------------------------------- #
    @classmethod
    def _wget_daily(self, ticker='^GSPC', start_ymd='', end_ymd = ''):
        '''
        The real function to wget yahoofinance historic Quota. 

        vs wget: wget will check SQLite table to have the last date, then
        call this function to wget contents

        url: http://real-chart.finance.yahoo.com/table.csv?
             s=NMBL&d=3&e=1&f=2015&g=d&a=11&b=13&c=2013&ignore=.csv
        NMBL: 2013-12-13 to 2015-3-31

        a = start_month - 1
        b = start_day 
        c = start_year
        d = end_month - 1
        e = end_day
        f = end_year

        if no start/end specified, use: 1900-0-1 to 9999-12-1

        url: http://real-chart.finance.yahoo.com/table.csv?
             s=%s&d=12&e=1&f=9999&g=d&a=0&b=1&c=1900&ignore=.csv
        '''
        stock_id = YFStock().aget_stock_id(ticker)

        if not stock_id:
            print 'ERROR YFQuota._wget_daily: invalid ticker - %s' % ticker
            return 0

        # assign c/a/b based on start_ymd, if not given, use START_DATE
        start_is_date = RE_DATE.match(start_ymd)
        if not start_is_date:
            start_ymd = START_DATE
        c, a, b = map(int, start_ymd.split('-'))

        # assign f/d/e based on end_ymd, if not given, use TODAY_YMD
        end_is_date = RE_DATE.match(end_ymd)
        if not end_is_date:
            end_ymd = TODAY_YMD 
        f, d, e = map(int, end_ymd.split('-'))
      
        # construct url parameters
        params = urlencode({ 
            's': ticker,
            'a': a - 1,
            'b': b,
            'c': c,
            'd': d - 1,
            'e': e,
            'f': f, 
            'g': 'd', 
            'ignore': '.csv', 
        }) 
        
        url = 'http://ichart.yahoo.com/table.csv?%s' % params
        
        req = Request(url)
        try: 
            response = urlopen(req) 
            data = str(response.read().decode('utf-8').strip()) 
        except: 
            print 'ERROR YFQuota._wget_daily: wget historic quota - %s, %s' \
                % (ticker, url)
            return 
        
        rows = []
         
        for line in data.splitlines(): 
            date_ = line.split(',')[0]

            # if first item is date, and between star/end, save it
            if RE_DATE.match(date_) and date_ <= end_ymd and \ 
                date_ >= start_ymd: 
                rows.append(line)
     
        self.update_daily(ticker=ticker, rows=rows)

    self.update_daily(self, ticker='^GSPC', rows):
        conn = sql.connect(YF_DB_FILE)
        cursor = conn.cursor()

        if len(rows): 
            rows_ = []
            for row in rows:
                rows_.append([stock_id] + row.split(','))
    
            r = cursor.executemany("""
                INSERT OR REPLACE INTO DailyQuota 
                (StockID, Date, Open, High, Low, Close, Volume, AdjClose)
                VALUES (?,?,?,?,?,?,?,?)
                """, tuple(rows_)
                )
    
            conn.commit()

        if self.debug:
            print 'YFQuota._wget_daily: get %d records from %s' % (len(rows),
                url)

        return len(rows)

    def wget_daily_all(self):
        """
        get daily historic data from yahoo finance
        """

        # 1st to update SP500 data
        if self.debug: 
            print "YFQuota.wget_daily_all(): wget daily quota - SP500"
        self.wget_daily('^GSPC')

        # load self.yfdate again to have latest sp_days[-1]
        self.yfdate.load_sp_days()

        all_stocks = YFSector().get_all_stock(source='YF', min_active=0)

        #YG
        for stock in all_stocks:
            if self.debug: 
                print "YFQuota.wget_daily_all(): wget daily quota -", stock
            self.wget_daily(stock)

    # ----------------------------------------------------------------------- #
    def calculate_pert_list(self, value=0.0, base_list=[]):
        '''
        '''
        ret_list = []
        for base in base_list:
            ret_list.append(self.calculate_pert(value=value, base=base))
        
        return ret_list

    # ----------------------------------------------------------------------- #
    def calculate_pert(self, value=0.0, base=0.0):
        '''
        calculate the percentage, if base = 0, return 0
        '''
        if base == 0.0:
            return 0
        else:
            return 100.0 * (value - base) / base

    # ----------------------------------------------------------------------- #
    #WORK
    def do_correlation(self, stock_id=0, date_='', num_days=21):
        list_stock_1d = self.fetch_many(
            """
            SELECT Pert1Day
            FROM DialyQuota
            WHERE Date<="%s" AND StockID=%d
            LIMIT %d
            """ % (date_, stock_id, num_days)
        )
    
    # ----------------------------------------------------------------------- #
    def do_calculation(self, ticker=''):
        '''
        based on download daily quota, do a further calculation
        '''

        row = self.fetch_one_row(
            sql_code="""
            SELECT StockID, FYEnds, Start, End
            FROM Stock 
            WHERE Ticker="%s" AND Active>0
            """ % ticker, 
            exit_if_none=False, 
            error_msg='Invalid ticker - %s' % ticker)
        
        if row: 
            (stock_id, fy_ends, start, end) = row
        else:
            return

        data_rows = self.get_daily(ticker=ticker, 
            end_ymd=YFDate().sp_days[-1], start_ymd=end)
        
        if self.debug: 
            print 'YFQuota.do_calculation: # of data_rows - ', len(data_rows)

        if data_rows: 
            if self.debug > 2: print data_rows[0]
            start = data_rows[0][0] 
        else:
            return

        (stock_id, start_date, open, high, low, close, adj_close, volume) = \
                data_rows[0][:8]
        
        # Volume           integer DEFAULT 0,    <<<<< 
        # VolumeAverage3M  integer, 
        # VolumePerAverage integer,
        volume_3m = self.fetch_many(
            """
            SELECT Volume
            FROM DailyQuota
            WHERE Date < "%s" AND StockID=%d
            LIMIT 63
            """ % (start_date, stock_id),
            default_list_value=[volume]*63
            )

        # Amount           real, 
        # AmountAverage3M  integer, 
        # AmountPerAverage integer,
        amount = close * volume
        amount_3m = self.fetch_many(
            """
            SELECT Amount
            FROM DailyQuota
            WHERE Date < "%s" AND StockID=%d
            LIMIT 63
            """ % (start_date, stock_id),
            default_list_value=[amount]*63
            )

        # Pert1Day         real    DEFAULT 0.0,
        # Pert3Day         real    DEFAULT 0.0,
        # Pert5Day         real    DEFAULT 0.0,
        # Pert1Month       real    DEFAULT 0.0,
        # Pert1Quater      real    DEFAULT 0.0,
        # Pert1Year        real    DEFAULT 0.0,
        close_1y = self.fetch_many(
            sql_code="""
            SELECT AdjClose
            FROM DailyQuota
            WHERE Date < %s AND StockID=%d
            LIMIT 252""" % (start_date, stock_id),
            default_list_value=[adj_close]*252
            )

        if self.debug > 10:
             print close_1y 
             print volume_3m
             print amount_3m

        counter = 1

        save_cy_quarter_end, save_fy_quarter_end = '', ''
        cy_quarter_price, fy_quarter_price = adj_close, adj_close

        for row in data_rows:
            (stock_id, date_, open, high, low, close, adj_close, volume) = \
                row[:8]
 
            volume_avg_3m = sum(volume_3m)/len(volume_3m)
            volume_pert = self.calculate_pert(value=volume, 
                base=volume_avg_3m)

            # Daily Amount = adj_close * volume
            amount = int(close * volume)
            amount_avg_3m = sum(amount_3m)/len(amount_3m)
            amount_pert = self.calculate_pert(value=amount, 
                base=amount_avg_3m)

            list_price=[close_1y[0], close_1y[2], close_1y[4], close_1y[20],
                close_1y[62], close_1y[251]]
           
            pert_1d, pert_3d, pert_5d, pert_1m, pert_1q, pert_1y =\
                self.calculate_pert_list(value=adj_close, 
                base_list=list_price)

            (cy_quarter_end, fy_quarter_end) = \
                self.yfdate.get_last_cy_fy_quarter_ends(date_=date_, 
                    fy_ends=fy_ends)

            if save_cy_quarter_end != cy_quarter_end:
                save_cy_quarter_end = cy_quarter_end
                cy_quarter_price = self.fetch_one("""
                    SELECT AdjClose
                    FROM DailyQuota
                    WHERE Date>="%s" AND StockID=%d
                    ORDER BY Date
                    LIMIT 1
                    """ % (save_cy_quarter_end, stock_id),
                    default_value = cy_quarter_price
                    )
            
            if save_fy_quarter_end != fy_quarter_end:
                save_fy_quarter_end = fy_quarter_end
                fy_quarter_price = self.fetch_one("""
                    SELECT AdjClose
                    FROM DailyQuota
                    WHERE Date>="%s" AND StockID=%d
                    ORDER BY Date
                    LIMIT 1
                    """ % (save_fy_quarter_end, stock_id),
                    default_value = fy_quarter_price
                    )

            # PertSinceCYQtr   real,   DEFAULT 0.0, 
            # PertSinceFYQtr   real,   DEFAULT 0.0,
            pert_cy_qtr, pert_fy_qtr = self.calculate_pert_list(
                value=adj_close, 
                base_list=[cy_quarter_price, fy_quarter_price])
            
            # ----------------------------------------------------------------------- #
            # VolumeAverage3M  integer,
            # VolumePerAverage integer,
            # Amount           real,
            # AmountAverage3M  integer,
            # AmountPerAverage integer,
            # Pert1Day         real    DEFAULT 0.0,
            # Pert3Day         real    DEFAULT 0.0,
            # Pert5Day         real    DEFAULT 0.0,
            # Pert1Month       real    DEFAULT 0.0,
            # Pert1Quater      real    DEFAULT 0.0,
            # Pert1Year        real    DEFAULT 0.0,
            # PertSinceCYQtr   real,   DEFAULT 0.0,
            # PertSinceFYQtr   real,   DEFAULT 0.0,

            r = self.cursor.execute("""
            UPDATE DailyQuota 
            SET VolumeAverage3M=%d, VolumePerAverage=%.2f,
                Amount=%d, AmountAverage3M=%d, VolumePerAverage=%.2f,
                Pert1Day=%.2f, Pert3Day=%.2f, Pert5Day=%.2f, Pert1Month=%.2f, 
                Pert1Quater=%.2f, Pert1Year=%.2f,
                PertSinceCYQtr=%.2f, PertSinceFYQtr=%.2f
            WHERE StockID=%d AND Date="%s"
            """ % (
                volume_avg_3m, volume_pert,
                amount, amount_avg_3m, amount_pert,
                pert_1d, pert_3d, pert_5d, pert_1m,
                pert_1q, pert_1y,
                pert_cy_qtr, pert_fy_qtr,
                stock_id, date_,
                )
            )

            if self.debug > 10:
                print "=========== %d ============" % counter
                print stock_id, date_, open, high, low, close, volume, \
                    adj_close
                print 'volume:', volume_3m, volume_avg_3m, volume_pert
                print 'amount:', amount, amount_3m, amount_avg_3m, amount_pert
                print 'perts:', pert_1d, pert_3d, pert_5d, pert_1m, pert_1q, pert_1y
                print 'cy_fy_qtr_ends:', cy_quarter_end, fy_quarter_end
                print 'cy_fy_qtr_price:', cy_quarter_price, fy_quarter_price
                print 'cy_fy_qtr_pert:', pert_cy_qtr, pert_fy_qtr
            
            counter += 1

            amount_3m = [amount] + amount_3m[:-1]
            volume_3m = [volume] + volume_3m[:-1]
            close_1y = [adj_close] + close_1y[:-1]

        self.conn.commit() 
        print 'Total records processed - %d' % counter
    # ----------------------------------------------------------------------- #
    def get_quota_values(self, sql_code, default_value=[None]):
        '''
        get the value based on sql code, default_value is the value to return
        if none record fetched
        '''
        self.cursor.execute(sql_code)
       
        if len(default_value) == 1:
            row = self.cursor.fetchone()
        else:
            row = self.cursor.fetchall()

        if row:
            return row
        else:
            return default_value
        
class YFDate:
    '''
    Yahoo Finance Date Class, colletion of procedures to process all kinds of 
    date related functions
    '''
    # ----------------------------------------------------------------------- #
    def __init__(self, date_ = ''):
        self.weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        self.list_months  = [
            'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
            'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
            ]
        self.list_numbers  = ['1st', '2nd', '3rd', '4th', '5th']
        self.days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

        # set today variables
        self.get_today()

        # load all sp500 trading days
        self.load_sp_days()

        # get all oe days
        self.get_oe_days()
        self.have_oe_day = 0

        self.debug = 1

    # ----------------------------------------------------------------------- #
    def test(self, *args):
        if len(args) < 1 or re.search('usage|help', args[0]):
            print """usage: yahoofinance.py test-yfdate <method> <args> 
            all               
            today              : print out today values
            oe                 : print out oe days
            spdays             : print out sp days 
            date_to_nthweekday <date> <text|number>
            spday_index        <date> 
            spday_of           <date> 
            spday_diff         <date1> <date2>
            spday_offset       <date1> <offset>
            """
        else:
            try: 
                if args[0] == 'all': 
                    list_title = ['========>']
                    list_value = ['Todays']
                    list_title += ['year', 'month', 'day', 'ymd', 'ym']
                    list_value += [self.year, self.month, self.day, 
                        self.today_ymd, self.today_ym]

                    list_title.append('========>')
                    list_value.append('OE related')
                    list_title += ['#', 'start', 'end']
                    list_value += [len(self.oe_days), self.oe_days[0], 
                        self.oe_days[-1]]

                    list_title.append('========>')
                    list_value.append('date_to_nthweekday()')
                    #for d in ['2014-10-01', '2011-01-02', '2011-01-05', '2015-05-21', '2015-05-01', '2015-05-08']: 
                    for d in ['2015-05-01', '2015-05-02', '2015-05-06', '2015-05-07', '2015-05-14', '2015-05-21']: 
                        list_title.append('txt,%s' % d)
                        list_value.append(self.date_to_nthweekday(d))
                    for d in ['2015-06-01', '2015-06-02', '2015-06-06', '2015-06-07', '2015-06-14', '2015-06-21']: 
                        list_title.append('num,%s' % d)
                        list_value.append(self.date_to_nthweekday(d, 'number'))

                    list_title.append('========>')
                    list_value.append('SP Days')
                    list_title += ['#', 'start', 'end']
                    list_value += [len(self.sp_days), self.sp_days[0], 
                        self.sp_days[-1]]

                    list_title.append('========>')
                    list_value.append('spday_index()')
                    for d in ['2015-03-01', '2015-03-02', '2015-03-03', '2015-03-04', '2015-03-05', '2015-03-06']: 
                        list_title.append(d)
                        list_value.append(self.sp_days[self.spday_index(d)])

                    list_title.append('========>')
                    list_value.append('spday_diff()')
                    for d1, d2 in zip(
                        ['2015-03-01', '2015-03-02', '2015-03-03', '2015-03-04', '2015-03-05', '2015-03-06'],
                        ['2015-04-01', '2015-04-01', '2015-04-01', '2015-04-01', '2015-04-01', '2015-04-01']):
                        list_title.append('%s vs %s' % (d1, d2))
                        list_value.append(self.spday_diff(d2, d1))

                    list_title.append('========>')
                    list_value.append('spday_offset()')
                    dates = ['2014-10-01']*2 + ['2011-01-02']*2 + ['2013-03-15']*2 + ['2001-06-08']*2
                    offsets = ['10', '-10', '+1w', '1w', '-1W', '+1M', '-1M', '3M', '-3M', '-2Y', '2Y']
                    for d, o in zip(dates, offsets):
                        list_title.append('%s %s' % (d, o))
                        list_value.append(self.spday_offset(d, o))

                    list_title.append('========>')
                    list_value.append('quarter_ends()')
                    list_title.append('quarter_ends("12-31")')
                    list_value.append(','.join(self.quarter_ends("12-31")))
                    list_title.append('quarter_ends("05-03")')
                    list_value.append(','.join(self.quarter_ends("05-03")))
                    
                    #get_quarter_number(self, date_='', fy_ends='12-31'):
                    list_title.append('========>')
                    list_value.append('get_quarter_number()')
                    list_title.append('(date_="2015-02-01")')
                    list_value.append(str(self.get_quarter_number(date_="2015-02-01")))
                    list_title.append('(date_="2015-02-01", fy_ends="02-06")')
                    list_value.append(str(self.get_quarter_number(date_="2015-02-01", fy_ends="02-06")))

                    #def get_last_quarter_ends(self, date_='', fy_ends='12-31'):
                    list_title.append('========>')
                    list_value.append('get_last_quarter_ends()')
                    list_title.append('(date_="2015-02-01")')
                    list_value.append(str(self.get_last_quarter_ends(date_="2015-02-01")))
                    list_title.append('(date_="2015-12-01")')
                    list_value.append(str(self.get_last_quarter_ends(date_="2015-12-01")))
                    list_title.append('(date_="2015-02-01", fy_ends="02-06")')
                    list_value.append(str(self.get_last_quarter_ends(date_="2015-02-01", fy_ends="02-06")))
                    list_title.append('(date_="2015-12-01", fy_ends="03-06")')
                    list_value.append(str(self.get_last_quarter_ends(date_="2015-12-01", fy_ends="03-06")))
                    list_title.append('(date_="2015-03-07", fy_ends="03-06")')
                    list_value.append(str(self.get_last_quarter_ends(date_="2015-03-07", fy_ends="03-06")))
                    list_title.append('(date_="2015-03-06", fy_ends="03-06")')
                    list_value.append(str(self.get_last_quarter_ends(date_="2015-03-06", fy_ends="03-06")))
                    list_title.append('(date_="2015-06-06", fy_ends="03-06")')
                    list_value.append(str(self.get_last_quarter_ends(date_="2015-06-06", fy_ends="03-06")))
                    list_title.append('(date_="2015-06-07", fy_ends="03-06")')
                    list_value.append(str(self.get_last_quarter_ends(date_="2015-06-07", fy_ends="03-06")))

                    #def get_last_cy_fy_quarter_ends(self, date_='', fy_ends='12-31'):
                    list_title.append('========>')
                    list_value.append('get_last_cy_fy_quarter_ends()')
                    list_title.append('(date_="2015-02-01")')
                    list_value.append(','.join(self.get_last_cy_fy_quarter_ends(date_="2015-02-01")))
                    list_title.append('(date_="2015-02-01", fy_ends="02-01")')
                    list_value.append(','.join(self.get_last_cy_fy_quarter_ends(date_="2015-02-01",fy_ends="02-01")))

                    pprint_name_value(list_title, list_value)

                elif args[0] == 'today': 
                    pprint_name_value(['year', 'month', 'day', 'ymd', 'ym'],
                        [self.year, self.month, self.day, self.today_ymd,
                        self.today_ym])

                elif args[0] == 'oe': 
                    pprint_name_value(['#', 'start', 'end'], \
                    [len(self.oe_days), self.oe_days[0], self.oe_days[-1]])

                elif args[0] == 'spdays': 
                    pprint_name_value(['#', 'start', 'end'], \
                    [len(self.sp_days), self.sp_days[0], self.sp_days[-1]])

                elif args[0] == 'date_to_nthweekday': 
                    print self.date_to_nthweekday(*args[1:])

                elif args[0] == 'spday_index': 
                    print self.sp_days[self.spday_index(*args[1:])]

                elif args[0] == 'spday_of': 
                    print self.spday_of(*args[1:])

                elif args[0] == 'spday_diff':
                    print self.spday_diff(*args[1:])

                elif args[0] == 'spday_offset':
                    print self.spday_offset(*args[1:])

                else: 
                    self.test('help')
            except: 
                raise
                self.test('help')

    # ----------------------------------------------------------------------- #
    def get_today_dates(self):
        '''
        get today dates YYYY-MM-DD and YYYY-MM formats
        '''
        date_time = datetime.datetime.now() 
    
        year = date_time.year
        month = date_time.month
        day = date_time.day
    
        ymd = '%04d-%02d-%02d' % (year, month, day)
    
        ym = '%04d-%02d' % (year, month)
    
        return (ymd, ym)
    
    # ----------------------------------------------------------------------- #
    def get_today(self):
        '''
        get today
        '''
        date_time = datetime.datetime.now() 

        self.year = date_time.year
        self.month = date_time.month
        self.day = date_time.day

        self.today_ymd = '%04d-%02d-%02d' % (self.year, self.month, self.day)

        self.today_ym = '%04d-%02d' % (self.year, self.month)

    # ----------------------------------------------------------------------- #
    def get_oe_days(self):
        '''
        get all OE days, 3rd friday of the mth
        '''
        self.oe_days = []

        number_month = 4 * (self.year - START_YEAR)
        for ym in range_month(self.today_ym, number_month, mode='backward', 
            include_this=1):

            year_, month_ = map(int, ym.split('-')) 
            
            # get the weekday of 1st day of every month 
            first_wkdy = datetime.date(year_, month_, 1).timetuple()[6] + 1

            if first_wkdy > 5 : 
                day_ = (8 - first_wkdy + 5) + 14
            else:
                day_ = 15 + 5 - first_wkdy

            oe_day = "%04d-%02d-%02d" % (year_, month_, day_)

            if len(self.sp_days) and oe_day <= self.sp_days[0] and \
                oe_day >= self.sp_days[-1]: 
                self.oe_days.append(self.spday_of(oe_day, mode='prev'))
            else:
                self.oe_days.append(oe_day)
  
    # ----------------------------------------------------------------------- #
    def load_sp_days(self):
        '''
        load all sp500 trading days
        '''
        self.sp_days = []

        # YFQuota(1) = load_sp_only
        rows = YFQuota.static_get_daily('^GSPC', wget_if_none=1)

        if not rows or not len(rows):
           YFQuota()._wget_daily('^GSPC') 
           rows = YFQuota.get_daily('^GSPC')

        if len(rows):
            for line in rows:
                self.sp_days.append(line[1])
        else:
            self.sp_days.append("0000-00-00")
        
        #self.sp_days.reverse() 
    # ----------------------------------------------------------------------- #
    def date_to_nthweekday(self, date, format='text'):
        '''
        given date, return numbered week/weekday, like 
        ('2001-01-04') -> 2001:1:1:3, 1st wed of 2001/01
        '''
        date = date[:10]
        
        # get year/mth/day of date
        [year, month, day] = map(int, date.split('-'))
    
        weekday_1st = datetime.date(year, month, 1).timetuple()[6] + 1
        weekday = datetime.date(year, month, day).timetuple()[6] + 1

        number_week = int(day + weekday_1st - 1)/7 
    
        if format == 'text':
            return '%s %s of %s' % (
                self.list_numbers[number_week], 
                self.weekdays[weekday - 1], 
                self.list_months[month - 1]
                )
        else: 
            return '%d:%02d:%d:%d' % (year, month, number_week+1, weekday)

    # ----------------------------------------------------------------------- #
    def spday_index(self, date_, mode='next'):
        '''
        spday_index: index of date the closest day in sp_days 
        mode: default is to find the next sp day (the next trading day)
        '''
        # if date_ in sp_days, just return index
        if date_ in self.sp_days:
            return self.sp_days.index(date_)
        
        # if date_ not in sp_days, return the next sp day
        for i, d in enumerate(self.sp_days): 
            if d > date_:
                if mode == 'next':
                    i -= 1
                break

        if i < 0 :
            i = 0

        return i

    # ----------------------------------------------------------------------- #
    def spday_of(self, date_, mode='next'):
        '''
        get_sp_day: given calendar date, return the closest sp trading day
        '''
        return self.sp_days[self.spday_index(date_, mode)]

    # ----------------------------------------------------------------------- #
    def spday_diff(self, date1, date2):
        '''
        spday_diff: given 2 dates, return the sp trading days between
        '''
        return -1 * (self.spday_index(date1) - self.spday_index(date2))
    
    # ----------------------------------------------------------------------- #
    def spday_offset(self, date, offset):
        '''
        given date + offset, return sp trade day, e.g.
        '2013-01-01', '+2w|+10' --> '2013-01-15'
        '2013-02-28', '-10|-2w' --> '2013-02-14'
        '''

        i = self.spday_index(date) - int(self.number_days(offset))

        if i < 0: 
            i = 0

        if i >= len(self.sp_days): 
            i = -1

        return self.sp_days[i]

    # ------------------------------------------------------------------------ #
    def ymd_offset(self, ymd, offset):
        '''
        day_offset: given calendar date and offset like:
        like, 2001-12-31 +90, --> 2002-03-31
        '''
        [month, day] = map(int, mmdd.split('-'))
        new_day = day + int(offset)

        if new_day > 30:
            month += new_day/30
            new_day = new_day % 30 + 1
        
        if month > 12:
            month = month % 13 + 1

        return '%02d-%02d' % (month, new_day)
                
    # ----------------------------------------------------------------------- #
    def number_days(self, str_, mode='trading'):
        '''
        return number of days, like 1m,1w,1d,1Y
        Mode:
        1) trading, Mth=21, Week=5, Year=252
        2) calendar, mth=30, week=7, year=365
        '''

        num_days = 0
        a = re.search('([+|-]{0,1})(\d+)([wmdy]{0,1})', str_.lower())
        
        if a:
            _sign, _num, _unit = a.groups()

            sign = 1
            if _sign == '-':
                sign = -1
    
            unit = 1
            if mode == 'trading':
                if _unit == 'w':
                    unit = 5
                elif _unit == 'm':
                    unit = 21
                elif _unit == 'y':
                    unit = 252
            else:                
                if _unit == 'w':
                    unit = 7
                elif _unit == 'm':
                    unit = 30
                elif _unit == 'y':
                    unit = 365

            num_days = sign * int(_num) * unit

        return str(num_days)

    # ------------------------------------------------------------------------ #
    def get_last_cy_fy_quarter_ends(self, date_='', fy_ends='12-31'):
        cy_quarter_end = self.get_last_quarter_ends(date_, fy_ends='12-31')
        fy_quarter_end = self.get_last_quarter_ends(date_, fy_ends=fy_ends)

        return (cy_quarter_end, fy_quarter_end)

    # ------------------------------------------------------------------------ #
    def get_last_quarter_ends(self, date_='', fy_ends='12-31'):
        """
        Given a date and the date of FY, return last FY/CY quarter ends, like,
        2015-07-01, 12-31 -> 6/30
        """
        year, mth, day = map(int, date_.split('-'))
        mmdd = '%02d-%02d' % (mth, day)

        list_quarter_ends = [fy_ends] + self.quarter_ends(fy_ends)

        if self.debug > 10:
            print date_, mmdd, list_quarter_ends

        for i in range(len(list_quarter_ends) - 1):
            if self.debug > 10: 
                print i, list_quarter_ends[i], list_quarter_ends[i+1]

            # something like 12/1 to 3/1
            if list_quarter_ends[i] > list_quarter_ends[i+1]:
                if mmdd < list_quarter_ends[i+1]: 
                    year -= 1
                    break
            # something like 1/1 to 4/1
            else: 
                if mmdd >= list_quarter_ends[i] and \
                    mmdd < list_quarter_ends[i+1]: 
                    break
        
        return '%04d-%s' % (year, list_quarter_ends[i])

    # ------------------------------------------------------------------------ #
    def get_quarter_number(self, date_='', fy_ends='12-31'):
        """
        Given a date and the date of FY, return qtr number, like, 
        2015-07-01, 12-31 -> 2
        """
        year, mth, day = map(int, date_.split('-'))
        mmdd = '%02d-%02d' % (mth, day)

        list_quarter_ends = [fy_ends] + self.quarter_ends(fy_ends)

        if self.debug > 10:
            print date_, mmdd, list_quarter_ends

        for i in range(len(list_quarter_ends) - 1):
            if self.debug > 10: 
                print i, list_quarter_ends[i], list_quarter_ends[i+1]

            if list_quarter_ends[i] > list_quarter_ends[i+1]:
                if mmdd < list_quarter_ends[i+1]: 
                    return i+1
            else: 
                if mmdd >= list_quarter_ends[i] and \
                    mmdd <= list_quarter_ends[i+1]: 
                    return i+1
        
        return i

    def quarter_ends(self, year_ends='12-31'):
        [month, day] = map(int, year_ends.split('-'))

        ret_list = []
        for i in range(4):
            month = month + 3

            if month > 12:
                month -= 12

            if day > self.days_in_month[month - 1]:
                day -= self.days_in_month[month - 1]
                month += 1

            ret_list.append('%02d-%02d' % (month, day))

        return ret_list

    # ------------------------------------------------------------------------ #
    # get_FY_quarter_ends: get the fiscal-year quarter-ends
    # Input: FY_endsdate_, like 12/31
    # Oput: list of FQ/FY ends [03-31, 05-15, 08-15, 11-15]
    # FY+90day, Q+45days
    # ------------------------------------------------------------------------ #
    def get_FY_quarter_ends(self, FY_end):
        list_fquarter_ends = []

        end_mmdd = FY_end

        for offset in [90, 45, 45, 45]:
            list_fquarter_ends.append(
                self.day_offset(end_mmdd, offset)
                )

            end_mmdd = self.day_offset(end_mmdd, 90)

        return list_fquarter_ends


# --------------------------------------------------------------------------- #
def usage():
    print '''
usage: yahoofinance.py <command> [<args>]

The most commonly used yahoofinance commands are: 
shared       Test shared functions 
yfdate       Test class YFDate 
yfstock      Test class YFStock
yfquota      Test class YFQuota
yfsector     Test class YFSector 
sector       Test class Sector
stocker      Test class StockER
insider      Test class Insider
insidertrans Test class Insider

See 'yahoofinance.py <command> help' for more informationon a specific command.
'''


if __name__ == "__main__":

    TODAY_YMD, TODAY_YM = get_today_dates()
    
    # if no arguments provided, or help|usage
    if len(sys.argv) == 1 or re.search('help|usage', sys.argv[1], 
        flags=re.IGNORECASE):
        usage()
        sys.exit(0)

    elif sys.argv[1] == 'shared': 
        test_shared_func(*sys.argv[2:])

    elif sys.argv[1] == 'yfdate': 
        d = YFDate()
        d.test(*sys.argv[2:])

    elif sys.argv[1] == 'yfstock': 
        s = YFStock()
        s.test(*sys.argv[2:])

    elif sys.argv[1] == 'yfquota': 
        hd = YFQuota()
        hd.run(*sys.argv[2:])

    elif sys.argv[1] == 'yfsector': 
        s = YFSector()
        s.test(*sys.argv[2:])

    elif sys.argv[1] == 'sector': 
        s = Sector()
        s.test(*sys.argv[2:])

    elif sys.argv[1] == 'stocker':
        er = StockER()
        er.test(*sys.argv[2:])

    elif sys.argv[1] == 'insider': 
        i = YFInsider()
        i.test()

    elif sys.argv[1] == 'insidertrans': 
        it = YFInsiderTransaction()
        it.test(*sys.argv[2:])

    elif sys.argv[1] == 'xxxx': 
        #def date_to_nthweekday(self, date, format='text')
        if len(sys.argv) > 2 and sys.argv[2] == 'date_to_nthweekday':
            print d.date_to_nthweekday(*sys.argv[3:])

        elif len(sys.argv) > 2 and sys.argv[2] == 'sp_days':
            try:
                print 'sp500 day:', _yfd.sp_days[int(sys.argv[3])]
            except:
                print 'incorrect range :', sys.argv[3]

    
            #def number_days(self, str):
            for s in ['10d', '-10d', '10', '-10', '10W', '-10w', '10m', '-10M']:
                print s, ': ', _yfd.number_days(s)
    
            for m in ['Jan', 'january', 'Feb', 'March', 'August', 'Ocx']:
                print _yfd.month_atoi(m)
                print month_atoi(m)
    
            for s in ['August 2, 2001', 'Dec 31, 1999', 'Oct 2', 'May 31']: 
                print s, '--> ', _yfd.date_atoymd(s)
        
            #day_offset(self, mmdd, offset):
            for mmdd, offset in zip( 
                ['12-31', '01-15', '05-31'],
                ['90',    '45',    '91']
                ):
                print mmdd, '+', offset, o.day_offset(mmdd, offset)
    
            #get_FY_quarter_ends(self, FY_end):
            for fy_end in ['12-31', '01-15', '04-15', '08-01']:
                print fy_end, ' ==> ', o.get_FY_quarter_ends(fy_end)
