import pandas as pd
import datetime as dt, time
import yfinance as yf
import mysql.connector
import sqlalchemy
from sqlalchemy import text
from futu import *
import schedule


with open('./data/code_pool.txt','r') as fp:
    CODE_LIST = [line.rstrip() for line in fp]

def download_today_capital_flow(tickers):
    data = []
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    for ticker in tickers:
        ret, tmp = quote_ctx.get_capital_flow('HK.0'+ticker, period_type = PeriodType.INTRADAY) # DAY, WEEK, MONTH
        if ret == RET_OK:
            tmp = tmp[['capital_flow_item_time','in_flow','super_in_flow','big_in_flow','mid_in_flow','sml_in_flow']]
            tmp.rename(columns={'capital_flow_item_time':'time'}, inplace=True)
            tmp['time'] = pd.to_datetime(tmp.time)
            data.append(tmp)   
            time.sleep(1)  # Interface limitations: A maximum of 30 requests per 30 seconds
        else:
            print("error: ",tmp)
    quote_ctx.close() # After using the connection, remember to close it to prevent the number of connections from running out
    return data


def download_price(tickers, start, end, interval='1d'):
    data = []
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    if start == None:
        start = dt.date(2021,1,1)
    if end == None:
        end = dt.date.today()
    if interval=='1d':
        for ticker in tickers:
            ret, df, _ = quote_ctx.request_history_kline('HK.0'+ticker, str(start), str(end), ktype='K_DAY') 
            if ret !=RET_OK:
                print('error: ', df)
            else: 
                df = df[['time_key','open','high','low','close','last_close','volume']]
                df.rename(columns={'time_key':'date'},inplace=True)
                df['date'] = pd.to_datetime(df.date).dt.date
                df.columns=['Date','Open','High','Low','Close','Adj Close','Volume']
                df['Adj Close'] = df.Close
                data.append(df)
                time.sleep(0.5)
        quote_ctx.close()
    return data
            
            
# def download_price(tickers, start, end, interval):
#     data = []
#     if interval == '1d':
#         for ticker in tickers:
#             try:
#                 data.append(yf.download(ticker+'.HK', start=start, end=end, interval='1d').reset_index())
#             except Exception:
#                 print(Exception)
#     if interval == '1h':
#         for ticker in tickers:
#             try:
#                 df = yf.download(ticker+'.HK', start=start, end=end, interval='1h').reset_index()
#                 df['Datetime'] = pd.to_datetime(df.Datetime).dt.tz_localize(None)
#             except Exception:
#                 print(Exception)
#                 continue
#             data.append(df)
#     return data



class DB_ops():
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.tickers = CODE_LIST

    def build_mysqlconn(self, database=None):
        if not database:
            self.conn = mysql.connector.connect(host = self.host, user = self.user, password=self.password)
        else:
            self.conn = mysql.connector.connect(host = self.host, user = self.user, password=self.password, database= database)
       
    def close_mysqlconn(self):
        self.conn.close()

    def createdb(self, name):
        self.build_mysqlconn()
        cursor = self.conn.cursor()
        cursor.execute("CREATE database "+ name)
        self.close_mysqlconn()

    def build_engine(self, database):
        self.engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{database}")

    def import_historical_price(self, interval='1d'):
        if interval == '1d':
            database = 'HK_stocks_daily'
            key = 'Date'
            start = None
            end = None
        if interval == '1h':
            database = 'HK_stocks_hourly'
            key = 'Datetime'
            end = dt.datetime.now()
            start = end- dt.timedelta(days=729)  # maximum 730 past records are available
        df_list = download_price(self.tickers, start=start, end=end, interval=interval)
        if df_list != []:
            self.build_engine(database)
            conn = self.engine.connect()
            for frame, symbol in zip(df_list, self.tickers):
                frame.to_sql(con= self.engine, name = symbol, index=False, if_exists='replace')
                if symbol.isnumeric():
                    symbol_fm = "`"+symbol+"`"
                    conn.execute(text("ALTER TABLE "+ symbol_fm +" ADD PRIMARY KEY ("+ key +")"))               
            print('Successfully imported price')


    def import_capital_flow(self, if_exists='replace'):
        database = 'HK_stocks_capital_flow'
        key = 'time'
        df_list = download_today_capital_flow(self.tickers)
        if df_list != []:
            self.build_engine(database)
            conn = self.engine.connect()
            for frame, symbol in zip(df_list, self.tickers):
                try:
                    frame.to_sql(con= self.engine, name = symbol, if_exists=if_exists, index=False)
                except Exception as e:
                    print(e)
                    continue
                if if_exists=='replace':
                    if symbol.isnumeric():
                        symbol_fm = "`"+symbol+"`"
                        conn.execute(text("ALTER TABLE "+ symbol_fm +" ADD PRIMARY KEY ("+ key +")"))     
            print('Successfully insert todays captital_flow')
            conn.close()
      
    
    def update_today_price(self, interval):
        if interval == '1d':
            database = 'HK_stocks_daily'
            start = dt.date.today()
            df_list = download_price(self.tickers, start= start, end= start+dt.timedelta(days=1), interval='1d')
            if df_list != []:
                self.build_mysqlconn(database)
                cursor = self.conn.cursor()
                for frame, symbol in zip(df_list, self.tickers):
                    if symbol.isnumeric():
                        symbol = "`"+symbol+"`"
                    if len(frame)==1:
                        sql_stmt = "INSERT INTO " + symbol+ "(Date, Open, High, Low, Close, `Adj Close`, Volume) VALUES (%s,%s,%s,%s,%s,%s,%s) \
                            ON DUPLICATE KEY UPDATE\
                                Open = VALUES (Open),\
                                High = VALUES (High),\
                                Low = VALUES(Low),\
                                Close = VALUES(Close),\
                                `Adj Close` = VALUES(`Adj Close`),\
                                Volume = VALUES(Volume)"
                        data = (frame.iloc[0]['Date'],frame.iloc[0]['Open'],frame.iloc[0]['High'],frame.iloc[0]['Low'],frame.iloc[0]['Close'],frame.iloc[0]['Adj Close'],int(frame.iloc[0]['Volume'])) 
                        cursor.execute(sql_stmt, data)  
                        self.conn.commit()    #commit the changes to database      
                print('Successfully insert todays daily price')
                self.close_mysqlconn()
            else:
                print('None of todays data is found')
        if interval == '1h':
            database = 'HK_stocks_hourly'
            start = dt.date.today()
            df_list = download_price(self.tickers, start= start, end= start+dt.timedelta(days=1), interval='1h')
            if df_list != []:
                self.build_engine(database)
                for frame, symbol in zip(df_list, self.tickers):   
                    frame.to_sql(con= self.engine, name = symbol, if_exists='append', index=False)
                print('Successfully insert todays hourly price')
    


    def update_today_capital_flow(self):
        self.import_capital_flow(if_exists='append')


    # fetch price from database in batch
    def fetch_batch_price_from_db(self, ticker, interval, start=None, end=None, limit=None):
        if interval == '1d':
            database = 'HK_stocks_daily'
            key = 'Date'
        if interval == '1h':
            database = 'HK_stocks_hourly'
            key = 'Datetime'
        self.build_mysqlconn(database)
        cursor = self.conn.cursor()
        if ticker.isnumeric():
            ticker = "`"+ticker+"`"
        if end == None:
            end = str(dt.datetime.now())
        end_fm = "'"+end +"'"
        if start != None:
            start_fm = "'"+start +"'"
            cursor.execute(f"SELECT * FROM {ticker} WHERE {key} BETWEEN {start_fm} AND {end_fm}")
        elif limit != None:
            end_fm = "'"+end +"'"
            cursor.execute(f"SELECT * FROM {ticker} WHERE {key} <= {end_fm} ORDER BY {key} DESC LIMIT {limit}")
        else:
            cursor.execute(f"SELECT * FROM {ticker} WHERE {key} <= {end_fm}")
        rows = cursor.fetchall()
        if (start == None) & (limit != None):
            rows = rows[::-1]
        self.close_mysqlconn()

        if interval=='1d':
            df = pd.DataFrame(rows, columns =['date','open','high','low','close','adj_close','volume'])
        if interval == '1h':
            df = pd.DataFrame(rows, columns =['datetime','open','high','low','close','adj_close','volume'])
        df.drop(columns='adj_close',inplace=True)
        return df


    def auto_update(self):
        self.update_today_price(interval='1d')
        self.update_today_capital_flow()
        


if __name__ == '__main__':  
    ## schedule daily price update
    mydb = DB_ops('localhost','root','mlu123456')
    # schedule.every().day.at("16:20").do(mydb.auto_update)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
    mydb.import_historical_price()
    
   

  