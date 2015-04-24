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
import time
import datetime
import sqlite3 as sql

# all dates starting from 1950, when SP500 starts
START_YEAR = 1950
START_DATE = '1950-01-01'

# LOCAL | yahoofinance
DATA_SOURCE = 'yahoofinance'

BASE_DIR_PATH = './'

SQL_DIR_PATH = '%s/SQL' % BASE_DIR_PATH
YF_DB_FILE = '%s/yahoofinance.db' % SQL_DIR_PATH

MY_LOG_FILE_NAME = '%s/yahoofinance.log' % BASE_DIR_PATH


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
        unit_to_number     <K|M|B>
        convert_int        <number_str> [<unit:K|M|B>]
        convert_int_all    1000K, 1000K in M, and integer 10, etc
        convert_float      <float_str>
        month_atoi         <month: Jan|march>
        date_atoymd        <date: Feb 20, 2012>
        range_month        <start_ym: 2012-01> <number> [forward|backward]
        range_quarter      <start_quarter: 2012Q1> <number> <include_this=1>
        pprint_name_value  <"name1:name2.." "value1:valu2..."
        """
    else:
        try:
            if args[0] == 'unit_to_number':
                print unit_to_number(*args[1:])
            elif args[0] == 'convert_int':
                print convert_int(*args[1:])
            elif args[0] == 'convert_int_all':
                print '1000K:', convert_int('1000K')
                print '1000K in M:', convert_int('1000K', 'm')
                print 'int/10:', convert_int(10)
                print 'int/-10:', convert_int(-10)
                print 'float/-10.9:', convert_int(-10.9)
            elif args[0] == 'convert_float':
                print convert_float(*args[1:])
            elif args[0] == 'month_atoi':
                print month_atoi(*args[1:])
            elif args[0] == 'date_atoymd':
                print date_atoymd(*args[1:])
            elif args[0] == 'range_month':
                print range_month(*args[1:])
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
def convert_int(input_, unit=''):
    """
    1) convert number string to number string in unit like K/M/B, like
       ('100,000K'), ('1.2M'), ('10000', 'k')
    2) input may be an integer itself, just return it. This is some
       functions argument
    """
    if type(input_) is int:
        return input_
       
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
def range_quarter(start_quarter, number, include_this=1):
    '''
    generate list of quarters, like od.range_quarter(2012Q1,12)
    include_this = 1, means staring from the start_quarter
    '''

    number = convert_int(number)
    include_this = convert_int(include_this)
    
    # init var
    list_quarters = []
    year = int(start_quarter[0:4])
    quarter = int(start_quarter[5])

    # if not include this quarter, quarter -1
    if not include_this: 
        quarter -= 1

    for i in range(number):
        if quarter <= 0: 
            quarter = 4
            year -= 1

        list_quarters.append('%04dQ%d' % (year, quarter))
        quarter -= 1

    return list_quarters

# --------------------------------------------------------------------------- #
def pprint_name_value(list_name, list_value):
    max_len = len(max(list_name, key=len))
    
    for name, value in zip(list_name, list_value): 
        try: 
            print '%*s : [%s]' % (max_len, name, value) 
        except: 
            print '%*s : [%s]' % (max_len, name, 'corrupted') 
        
# --------------------------------------------------------------------------- #
# end of Shared functions
# --------------------------------------------------------------------------- #

YF_INSIDER = 1
NONEWLINE = 2

class SimpleHTMLParser:
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

class Log:
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
         
class YFDB:
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

    # ----------------------------------------------------------------------- #
    def pprint(self):
        sql_cmd = "SELECT * FROM %s" % self.table
        self.cursor.execute(sql_cmd)
        for row in self.cursor.fetchall():
            print row

    # ----------------------------------------------------------------------- #
    def fetch_id(self, sql_code):
        self.cursor.execute(sql_code)
        
        row = self.cursor.fetchone()
        if row == None:
            return None
        else:
            return row[0]

class YFStock(YFDB):
    """
    Class YFStock: get stock info from finance.yahoo.com
    """

    # ----------------------------------------------------------------------- #
    def __init__(self):
        YFDB.__init__(self, 'Stock')
        self.debug = 0

        self.list_field_wget = ['Ticker', 'Name', 'FYEnds', 'Beta', 'AvgVol',
            'Shares', 'Floating', 'MarketCap']

        self.list_field_all = ['StockID', 'Ticker', 'Active', 'Name', 'FYEnds', 
            'Beta', 'HasOption', 'Close', 'AvgVol', 'Shares', 'Floating', 
            'MarketCap', 'Start', 'End']

    # ----------------------------------------------------------------------- #
    def test(self, *args):
        if len(args) < 2 or re.search('usage|help', args[0]):
            print """yahoofinance.py test_yfstock <method> <args>
            
            get_stock_id        <ticker> e.g. get_stock_id YHOO
            get_stock_info    <ticker> e.g. get_stock_info TSLA
            wget_stock_info     <ticker> e.g. wget_stock_info OCLR
            upsert_stock_info   <ticker> e.g. upsert_stock_info OCLR
            get_or_add_stock_id <ticker> e.g. get_or_add_stock_id OCLR
            """
        else: 
            try: 
                if args[0] == 'get_stock_id': 
                    print self.get_stock_id(*args[1:])
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
                elif args[0] == 'get_or_add_stock_id':
                    print self.get_or_add_stock_id(*args[1:])
                else:
                    self.test('usage')
            except:
                raise
                self.test('usage')

    # ----------------------------------------------------------------------- #
    def get_stock_id(self, ticker, active=1):
        return self.fetch_id("""
            SELECT StockID FROM Stock
            WHERE Ticker="%s" AND active=%d
            """ % (ticker, active)
            )

    # ----------------------------------------------------------------------- #
    def get_or_add_stock_id(self, ticker):
        """
        return StockID of ticker. If not existing, add it to DB
        """
        id = self.get_stock_id(ticker)

        if id == None:
            self.upsert_stock_info(ticker)

        return self.get_stock_id(ticker)

    # ----------------------------------------------------------------------- #
    def re(self, re_str, line, default=0):
        found = re.match(re_str, line)
        if found:
            return found.group(1)
        else:
            return default

    # ----------------------------------------------------------------------- #
    def get_stock_info(self, ticker, active=1):
        #SELECT StockID, Ticker, Name, FYEnds, Beta, 
        #HasOption, AvgVol, MarketCap, Start, End 
        self.cursor.execute("""
        SELECT * FROM Stock WHERE Ticker=? AND Active=?
        """, (ticker, active,))

        return self.cursor.fetchone() 

    # ----------------------------------------------------------------------- #
    # CREATE TABLE Stock (                      No       Web
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
    # );
    # ----------------------------------------------------------------------- #
    def upsert_stock_info(self, ticker):
        list_value = self.wget_stock_info(ticker)

        if list_value[1] == None or (list_value[1] == 'NA' and 
            list_value[-1] == 0):
            if self.debug:
                print 'failed to get stock info or invalid ticker - %s' % ticker
            return 0

        stock_id = self.get_stock_id(ticker)

        # if stock_id == None, just insert
        if stock_id == None:
            r = self.cursor.execute("""
            INSERT INTO Stock 
            (%s) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """ % ','.join(self.list_field_wget), list_value
            )
        else:
            #ticker, name, fy_ends, beta, avg_vol, shares, floating, mkt_cap
            r = self.cursor.execute("""
            UPDATE Stock SET
            Name=?, FYEnds=?, Beta=?, AvgVol=?, Shares=?, Floating=?, 
            MarketCap=?
            WHERE StockID=?""", ( tuple(list_value[1:]) + tuple([stock_id]) )
            )

        self.conn.commit()

        return r.rowcount

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
       
        name = unicode(name, errors='replace').strip()


        if self.debug:
            print 'wget_stock_info, url - ', url 
            pprint_name_value(self.list_field_wget, [ticker, name, fy_ends, beta, \
                avg_vol, shares, floating, mkt_cap])

        return ticker, name, fy_ends, beta, avg_vol, shares, floating, mkt_cap

class YFInsider(YFDB):
    """
    Class for Insider info in yahoo finance
    """
    # ----------------------------------------------------------------------- #
    def __init__(self):
        YFDB.__init__(self, "Insider")
        self.debug = 0

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
        return self.fetch_id("""
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
        stock_id = YFStock().get_or_add_stock_id(ticker)

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
            if not re.search("\|Direct\|.*[0-9]+", line, re.IGNORECASE): 
                continue

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
                print '\n'.join(row)

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

            record = self.fetch_id("""
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
        YFDB.__init__(self, "Sector")
        self.debug = 1 

        # --------------------------- dictionaries -------------------------- #
        # ticker to sector id, industryid, AAPL->'YF',
        self.ticker_source_sector_industry = {}
        self.tickers_in_source_sector_industry = {}


    # ----------------------------------------------------------------------- #
    def get_source_id(self, name):
        id = self.fetch_id("""
            SELECT SourceID FROM Source WHERE Name='%s' 
            """ % (name,)
            )
        if not id:
            self.add_source(name, name)
            id = self.fetch_id(""" 
                SELECT SourceID FROM Source WHERE Name='%s'
                """ % (name,)
                )
        return id

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
    def get_sector_id(self, name):
        id = self.fetch_id("""
            SELECT SectorID FROM Sector WHERE Name='%s'
            """ % (name,)
            )
        if not id:
            self.add_sector(name, name)
            id = self.fetch_id(""" 
                SELECT SectorID FROM Sector WHERE Name='%s'
                """ % (name,)
                )
        return id

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
    def get_industry_id(self, name):
        id = self.fetch_id("""
            SELECT IndustryID FROM Industry WHERE Name='%s'
            """ % (name,)
            )
        if not id:
            self.add_industry(name, name)
            id = self.fetch_id(""" 
                SELECT IndustryID FROM Industry WHERE Name='%s'
                """ % (name,)
                )
        return id

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
    def load_db(self):
        '''
        load table - YFSector into dictionaries
        '''

        self.dict_stock2sector = {}
        self.dict_stcok2industry = {}
        self.dict_industry2sector = {}

        rows = self.conn.executemany(""" SELECT * FROM StockSector """)


class YFSector(Sector):
    """
    class YFSector: download and api to access yahoo finance sector info
    """

    # ----------------------------------------------------------------------- #
    def __init__(self):
        Sector.__init__(self)
        self.source = 'YF'
        self.source_id = self.get_source_id(self.source)
        self.debug = 1

    def test(self, *args):
        if len(args) < 1 or re.search('usage|help', args[0]):
            print """yahoofinance.py test-yfsector <method> <args>
            
            wget_industry_summary   : get list of yhaoo finance ind codes
            wget_industry  <code>   : get yahoo finance industry info
            """
        else: 
            try: 
                if args[0] == 'wget_industry_summary': 
                    print '\n'.join(self.wget_industry_summary(*args[1:]))
                elif args[0] == 'wget_industry': 
                    self.wget_industry(*args[1:])
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
            return found.group(1)
        else:
            return default

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

        return list_code
    # ----------------------------------------------------------------------- #
    def wget_industry(self, code): 
        url = 'http://biz.yahoo.com/p/%dconameu.html' % convert_int(code)

        sector, industry = '', ''
        stock_list = []
        
        p = SimpleHTMLParser(url) 
       
        if p.html_text == 'Error':
            return 
        
        for line in p.html_text.split('\n'): 
            sector = self.re('^\|.*Sector:\s*([^||^\(]+)', line, sector)
            industry = self.re('^\|.*Industry:\s*([^|(]+)', line, industry)

            stock_found = re.search('^\|.*[^|]+\(([A-Z ]+)\).*\|', line) 
            
            if stock_found: 
                stock_list.append(stock_found.group(1).strip())
                

        if self.debug: 
            print 'url: %s\n' % url
            print p.html_text
            print 'sector:', sector 
            print 'industry:', industry 
            print 'stocks list:\n', '\n'.join(stock_list)

        if sector != '' and industry != '' and len(stock_list):
            rows = []

            sector_id = self.get_sector_id(sector)
            industry_id = self.get_industry_id(industry)

            for stock in stock_list:
                stock_id = YFStock().get_or_add_stock_id(stock)

                # even stock_id = get_or_add, still have the chance
                # stock not in yahoofinance.
                if stock_id: 
                    rows.append((stock_id, self.source_id, sector_id, 
                        industry_id))
            
            if self.debug: 
                print "rows add into StockSector table:"
                print rows

            self.conn.executemany("""
                INSERT INTO StockSector VALUES(?, ?, ?, ?)
                """, tuple(rows)
                )

            self.conn.commit()

class YFHistoryData(YFDB):
    '''
    Yahoofinance Historic Date Class, download, store and read stock
    daliy historic data from yahoofinance.com
    '''
    # ----------------------------------------------------------------------- #
    def __init__(self):
        YFDB.__init__(self, 'DailyQuota')
        self.re_date = re.compile('^(\d\d\d\d)-(\d\d)-(\d\d)')
        self.debug = 1
        return

    # ----------------------------------------------------------------------- #
    def test(self, *args):
        if len(args) < 1 or re.search('usage|help', args[0]):
            print """yahoofinance.py test_yfhisdata <method> <args>
            
            wget   <ticker>
            get    <ticker> [<end_ymd>] [<start_ymd>]
            delete <ticker> [<end_ymd>] [<start_ymd>]
            """

        else:
            try: 
                if args[0] == 'wget': 
                    print self.wget(*args[1:])
                elif args[0] == 'get': 
                    rows = self.get(*args[1:])
                    if rows and len(rows): 
                        pprint_name_value(['#', 'start', 'end'], [len(rows), rows[0], rows[-1]])
                    else:
                        print 'no rows retrieved'
                elif args[0] == 'delete': 
                    self.delete(*args[1:])
                else:
                    self.test('help')
            except:
                raise
                #self.test('help')
    
    # ----------------------------------------------------------------------- #
    def get(self, ticker='^GSPC', end_ymd = '', start_ymd=''):
        '''
        get stock historic quota from local SQLite database
        '''

        stock_id = YFStock().get_stock_id(ticker)

        if stock_id: 
            sql_cmd = """SELECT * FROM DailyQuota WHERE StockID=%s""" % \
                (stock_id)

            if end_ymd != '':
                sql_cmd += ' AND Date<="%s"' % end_ymd
            if start_ymd != '':
                sql_cmd += ' AND Date>="%s"' % start_ymd
           
            if self.debug: 
                print 'YFHistoryData.get() - SQL command:', sql_cmd

            self.cursor.execute(sql_cmd)

            return self.cursor.fetchall()
        else:
            if self.debug: 
                print 'YFHistoryData.get() : no such ticker  - %s' % ticker

            return None

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
        stock_id = YFStock().get_or_add_stock_id(ticker)

        rows_ = []
        for row in rows:
            rows_.append([stock_id] + row.split(','))

        r = self.conn.executemany("""
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

    def wget(self, ticker='^GSPC'):
        rows = self.get(ticker)

        if rows == None:
            # rows == None, means not valid ticker
            return None
        elif len(rows) == 0:
            # len(rows) == 0, means not no records in db
            return self._wget(ticker)
        else:
            last_date = rows[-1][1]
            return self._wget(ticker, YFDate().today_ymd, rows[-1][1])

    # ----------------------------------------------------------------------- #
    def _wget(self, ticker='^GSPC', end_ymd = '', start_ymd=''):
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

        # assign a/b/c/d/e/f to default values in case no dates specfied
        c, a, b = map(int, '1953-01-01'.split('-'))
        f, d, e = map(int, YFDate().today_ymd.split('-'))

        # assign c/a/b based on start_ymd
        re1 = self.re_date.match(start_ymd)
        if re1:
            c, a, b = map(int, re1.groups())

        # assign f/d/e based on end_ymd
        re2 = self.re_date.match(end_ymd)
        if re2:
            f, d, e = map(int, re2.groups())
       
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
        
        if self.debug:
            print url

        req = Request(url)
        try: 
            response = urlopen(req) 
        except: 
            print 'Error to wget yahoo historic quota for %s' % ticker
            print 'url -> %s' % url
            return 
        
        data = str(response.read().decode('utf-8').strip()) 
       
        rows = []
         
        for line in data.splitlines(): 
            # match the DATE in date line 
            re3 = self.re_date.match(line)

            # if not match ^2014-01-01, skip this line
            if re3: 
                date_ = re3.group() 
                
                if (end_ymd == '' or date_ <= end_ymd) and \
                    (start_ymd == '' or date_ >= start_ymd):
                    rows.append(line)
       
        if len(rows): 
            return self.insert(ticker, rows)
        else:
            return 0

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

    # ----------------------------------------------------------------------- #
    def test(self, *args):
        if len(args) < 1 or re.search('usage|help', args[0]):
            print """usage: yahoofinance.py test-yfdate <method> <args> 
            today              : print out today values
            oe                 : print out oe days
            spdays             : print out sp days 
            date_to_nthweekday <date> <text|number>
            spday_index        <date> 
            spday_of           <date> 
            spday_diff         <date1> <date2>
            spday_offset       <date1> <offset>
            all               
            """
        else:
            try: 
                if args[0] == 'today': 
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

                elif args[0] == 'all': 
                    print '------ today -------'
                    pprint_name_value(['year', 'month', 'day', 'ymd', 'ym'],
                        [self.year, self.month, self.day, self.today_ymd,
                        self.today_ym])
                    print '------ spdays -------'
                    pprint_name_value(['#', 'start', 'end'], \
                    [len(self.sp_days), self.sp_days[0], self.sp_days[-1]])
                    print '------ oe -------'
                    pprint_name_value(['#', 'start', 'end'], \
                    [len(self.oe_days), self.oe_days[0], self.oe_days[-1]])
                    print '------ date_to_nthwkday -------'
                    dates = ['2014-10-01', '2011-01-02', '2013-03-15']
                    for d in dates:
                        print d, '-->', self.date_to_nthweekday(d)
                        print d, '-->', self.date_to_nthweekday(d, 'number')
                    print '------ spday_offset -------'
                    dates = ['2014-10-01']*2 + ['2011-01-02']*2 + \
                        ['2013-03-15']*2 + ['2001-06-08']*2
                    offsets = ['10', '-10', '+1w', '1w', '-1W', '+1M', 
                        '-1M', '3M', '-3M', '-2Y', '2Y']
                    for d, o in zip(dates, offsets):
                        print d, '%4s' % o, '-->', self.spday_offset(d, o)
                    print '------ spday_diff -------'
                    dates1 = ['2014-10-01', '2011-01-02', '2013-03-15']
                    dates2 = ['2014-11-01', '2010-11-02', '2015-03-15']
                    for d1, d2 in zip(dates1, dates2): 
                        print d1, '-', d2, '=', self.spday_diff(d1, d2)
                
                else: 
                    self.test('help')
            except: 
                raise
                self.test('help')

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

            if oe_day <= self.sp_days[0] and oe_day >= self.sp_days[-1]: 
                self.oe_days.append(self.spday_of(oe_day, mode='prev'))
            else:
                self.oe_days.append(oe_day)
  
    # ----------------------------------------------------------------------- #
    def load_sp_days(self):
        '''
        load all sp500 trading days
        '''
        self.sp_days = []

        hd = YFHistoryData()

        for line in hd.get('^GSPC'): 
            self.sp_days.append(line[1])
        
        self.sp_days.reverse() 

    # ----------------------------------------------------------------------- #
    def date_to_nthweekday(self, date, format='text'):
        '''
        given date, return numbered week/weekday, like 
        ('2001-01-04') -> 2001:1:1:3, 1st wed of 2001/01
        '''
        date = date[:10]
        
        # get year/mth/day of date
        [year, month, day] = map(int, date.split('-'))
    
        weekday = datetime.date(year, month, day).timetuple()[6] + 1
        number_week = int(day)/7 + 1
    
        if format == 'text':
            return '%s %s of %s' % (
                self.list_numbers[number_week - 1], 
                self.weekdays[weekday - 1], 
                self.list_months[month - 1]
                )
        else: 
            return '%d:%02d:%d:%d' % (year, month, number_week, weekday)

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
            if d < date_:
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


# --------------------------------------------------------------------------- #
def usage():
    print '''
usage: yahoofinance.py <command> [<args>]

The most commonly used yahoofinance commands are: 
test-shared    Test shared functions 
test-yfdate    Test class YFDate 
test-yfstock   Test class YFStock
test-yfhisdata Test class YFHistoryData()
test-yfsector  Test class YFSector 

See 'yahoofinance.py <command> help' for more informationon a specific command.
'''


if __name__ == "__main__":

    # if no arguments provided, or help|usage
    if len(sys.argv) == 1 or re.search('help|usage', sys.argv[1], 
        flags=re.IGNORECASE):
        usage()
        sys.exit(0)

    elif sys.argv[1] == 'test-shared': 
        test_shared_func(*sys.argv[2:])

    elif sys.argv[1] == 'test-yfdate': 
        d = YFDate()
        d.test(*sys.argv[2:])

    elif sys.argv[1] == 'test-yfstock': 
        s = YFStock()
        s.test(*sys.argv[2:])

    elif sys.argv[1] == 'test-yfhisdata': 
        hd = YFHistoryData()
        hd.test(*sys.argv[2:])

    elif sys.argv[1] == 'test-yfsector': 
        s = YFSector()
        s.test(*sys.argv[2:])

    elif sys.argv[1] == 'test-xxxx': 
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

    elif len(sys.argv) > 1 and sys.argv[1] == 'yfsector':
        '''
        yahoofinance.py yfsector : testing class YFSector
        '''

        if len(sys.argv) > 2 and sys.argv[2] == 'wget':
            '''
            wget     --> download all yahoo finance indsutry'
            wget 1   --> download yahoo finance indsutry 1xx
            ''' 

            sector = YFSector() 

            if len(sys.argv) > 3:
                sector.wget_sector_industry_sum(sys.argv[3])
            else:
                sector.wget_sector_industry_sum('all')
    
        if len(sys.argv) > 2 and sys.argv[2] == 'stock':
            '''
            Download yahoofiance stock info
            '''
            stock = YFStock()

            if len(sys.argv) > 3:
                stock.upsert_stock_info(sys.argv[3])



    if len(sys.argv) > 1 and sys.argv[1] == 'db': 
        """
        test class YFDB()
        """
        if len(sys.argv) > 2:
            db = YFDB(sys.argv[2])
            db.pprint()
        else:
            db = YFDB()
            db.pprint()

    if len(sys.argv) > 1 and sys.argv[1] == 'insider': 
        """
        test class Insider
        """
        insider = YFInsider()
        insider.pprint()

    if len(sys.argv) > 1 and sys.argv[1] == 'insidertrans': 
        s = YFInsiderTransaction()

        if len(sys.argv) > 2 and sys.argv[2] == 'wget':
            tick = raw_input("ticker: ") 
            s.wget_insider_transaction(tick)
        else:
            s.pprint()

    if len(sys.argv) > 1 and sys.argv[1] == 'sector': 
        sector = YFSector()
        num = raw_input('first digit of yahoo finance sector: ')
        sector.wget_sector_industry_sum(num)

#-----------------------------------------------------------------------------#
    if len(sys.argv) > 1 and sys.argv[1] == 'yfhisdata': 
        o = YFHistoryData()
        #def load(self, _ticker='^GSPC', _start=START_DATE, _end='9999-99-99'):
        print '# of SP500 days since 1993-01-01: ', len(o.load())

        while True:
            ticker = raw_input('input a ticker: ').strip()
            print ticker
            if ticker == 'end': 
                break
            print '# of %s history data is ' % ticker, len(o.load(ticker))
            print '\n'.join(o.load(ticker))

    elif len(sys.argv) > 1 and sys.argv[1] == 'yfinsider': 
        s = YFInsider()
        s.delete_insider_by_id(1)

        url = 'http://www.yahoo.com'
        for i in ['TEST', 'TEST1', 'TEST2', 'XXX', 'YYY']:
            print i, ':', s.get_insider_id(i, url)
      
        i = 'XXX'
        print 'add_insider_id :', i
        s.add_insider_id(i, url)
        print i, ':', s.get_insider_id(i, url)

        i = 'YYY'
        print i, 'add_or_get :', s.get_or_add_insider_id(i, url)
    
        i = 'TEST'
        print i, 'add_or_get :', s.get_or_add_insider_id(i, url)

        s.pprint('Insider')

    elif len(sys.argv) > 1 and sys.argv[1] == 'test_basic': 
        for u in ['b', 'M', 'k', '']:
            print u, '-->', unit_to_number(u)
        for n in ['100b', '1.68B', '2.5M', '2.4M', '1568k', '6,056,840']:
            print n, '==>', convert_int(n)

    # test - SimpleHTMLParser
    elif len(sys.argv) > 1 and sys.argv[1] == 'html': 
        url = raw_input('url: ')
        p = SimpleHTMLParser(url)
        if p.html_text == 'Error':
            print 'Wrong'
        else: 
            print p.html_text
