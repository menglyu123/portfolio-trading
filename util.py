<<<<<<< HEAD
=======
print('remot')
>>>>>>> refs/remotes/origin/main
import tensorflow as tf 
print('local')
from keras import backend as K
import numpy as np
from sklearn.cluster import KMeans
import pandas as pd
import matplotlib.pyplot as plt
from futu import *
from keys import AlpacaConfig
from datetime import datetime, timedelta

import time
import requests


def get_price_from_alpaca(symbol, start:datetime, end:datetime, interval) -> list[dict]:
    config = AlpacaConfig()
    url = config.BARS_URL + '/bars'
    params = {"end": end} 
    params['symbols'] = symbol
    params["timeframe"] = interval
    params["start"] = start
    params["limit"] = 1000
    response: list[list] = requests.get(url, params=params, headers=config.HEADERS).json()
    print(response['bars'])
    




__BINANCE_WEB = "https://api.binance.com"
__NUM_TRIALS = 1
BINANCE_LIMIT = 1000


def __get_limit(start_date: datetime, today: datetime) -> int:
    return min(BINANCE_LIMIT, int((today - start_date).total_seconds()) // 3600)


def __parse_record(web_data: list, symbol: str) -> dict:
    """
    Convert the return data from Binance to make it a dict data format, and later convert it to DataFrame
    """
    return {
        "date": datetime.fromtimestamp(web_data[0] / 1000.0),
        # "symbol": symbol,
        "open": float(web_data[1]),
        "high": float(web_data[2]),
        "low": float(web_data[3]),
        "close": float(web_data[4]),
        "volume": float(web_data[5]),
        "quoteAssetVolume": float(web_data[7]),
        "numOfTrades": int(web_data[8]),
        "takerBaseVolume": float(web_data[9]),
        "takerQuoteVolume": float(web_data[10]),
    }


def get_binance_data_hourly(symbol, since: datetime, until: datetime) -> list[dict]:
    fail = 0
    data: list[dict] = []
    while fail < __NUM_TRIALS:
        try:
            limit = __get_limit(since, until)
            params = {"symbol": symbol, "endTime": int(until.timestamp() * 1000)}
            params["interval"] = "1h"
            params["startTime"] = int(since.timestamp() * 1000)
            params["limit"] = limit

            response: list[list] = requests.get(
                __BINANCE_WEB + "/api/v3/klines", params=params
            ).json()
            new_data = 0
            for x in response:
                if type(x) is list:
                    md = __parse_record(x, symbol)
                    if md["date"] < until:
                        new_data += 1
                        data.append(md)
            if new_data == 0:
                raise Exception
            last_hour = until - timedelta(hours=1)
            since = data[-1]["date"]
            since = since + timedelta(hours=1)
            if since > last_hour and since <= until:
                return data
        except Exception as e:
            print(e)
            fail += 1
            time.sleep(30)
    raise Exception(f"Nothing is fetched for {symbol} on Binance")


def get_binance_daily_data(symbol, since: datetime, until: datetime) -> list[dict]:
    fail = 0
    data: list[dict] = []
    while fail < __NUM_TRIALS:
        try:
            limit = __get_limit(since, until)
            params = {"symbol": symbol, "endTime": int(until.timestamp() * 1000)}

            params["interval"] = "1d"
            params["startTime"] = int(since.timestamp() * 1000)
            params["limit"] = limit

            response: list[list] = requests.get(
                __BINANCE_WEB + "/api/v3/klines", params=params
            ).json()
            new_data = 0
            for x in response:
                if type(x) is list:
                    md = __parse_record(x, symbol)
                    if md["date"] < until:
                        new_data += 1
                        data.append(md)
            if new_data == 0:
                raise Exception
            last_day = until - timedelta(days=1)
            since = data[-1]["date"]
            since = since + timedelta(days=1)
            if since > last_day and since <= until:
                return data
        except Exception as e:
            print(e)
            fail += 1
            time.sleep(30)
    raise Exception(f"Nothing is fetched for {symbol} on Binance")




def place_order(code, price, qty, trd_side):
    trd_ctx = OpenHKTradeContext(host='127.0.0.1', port=11111, security_firm=SecurityFirm.FUTUSECURITIES)
    if trd_side == 'buy':
        ret, data = trd_ctx.place_order(price=price, qty=qty, code="HK.0"+ code, trd_side=TrdSide.BUY, trd_env=TrdEnv.SIMULATE)
    if trd_side == 'sell':
        ret, data = trd_ctx.place_order(price=price, qty=qty, code="HK.0"+ code, trd_side=TrdSide.SELL, trd_env=TrdEnv.SIMULATE)
    if ret == RET_OK:
        order_id = data['order_id'][0]
    else:
        print('place_order error: ', data)
    trd_ctx.close()
    return order_id


def get_order_status(order_id):
    trd_ctx = OpenHKTradeContext(host='127.0.0.1', port=11111, security_firm=SecurityFirm.FUTUSECURITIES)
    ret, data = trd_ctx.order_list_query(order_id= order_id, trd_env=TrdEnv.SIMULATE, refresh_cache=True)
    if ret == RET_OK:
        print('order status: ', data['order_status'][0])
    else:
        print('order_status_query error: ', data)
    trd_ctx.close()


def get_balance_list():
    trd_ctx = OpenHKTradeContext(host='127.0.0.1', port=11111, security_firm=SecurityFirm.FUTUSECURITIES)
    ret, data = trd_ctx.position_list_query(trd_env=TrdEnv.SIMULATE, refresh_cache=True)
    if ret == RET_OK:
        if data.shape[0] > 0:  
            print(data[['code', 'stock_name', 'qty', 'cost_price', 'pl_ratio']])  
    else:
        print('position_list_query error: ', data)
    trd_ctx.close() 



#------ custom metric adjusted F1 ------
class custom_metric(tf.keras.metrics.Metric):
    def __init__(self):
        super().__init__(name="custom_score")
        self.tp = tf.Variable(0.0)
        self.fp = tf.Variable(0.0)
        self.fn = tf.Variable(0.0)
        self.tn = tf.Variable(0.0)

    def update_state(self, y_true, y_pred, sample_weight=None):
        true = K.cast(y_true, "float32")
        pred = K.round(y_pred)
        self.tp.assign_add(K.sum(true * pred))
        self.fn.assign_add(K.sum(true * (K.ones_like(pred) - pred)))
        self.fp.assign_add(K.sum((K.ones_like(pred) - true) * pred))
        self.tn.assign_add(K.sum((K.ones_like(pred) - true) * (K.ones_like(pred) - pred)))
        return self.tp, self.fp, self.fn, self.tn

    def result(self):
        tp = self.tp
        fp = self.fp
        fn = self.fn
        tn = self.tn
        precision = tp / (tp + fp + K.epsilon())
        sensitivity = tp / (tp + fn + K.epsilon())  # recall        
        # Fbeta-measure; F0.5-score when false positive more costly; F2-score when false negative more costly
        if (precision/sensitivity > 0.8)&(precision/sensitivity < 1.2): 
            rs = (1+0.5**2)*precision*sensitivity/(0.5**2*precision+sensitivity+1e-10) 
        else:
            rs = 0.0
        return rs
#------------------------------------------




def cal_trade_performance(profit_seq, buy, sell):
    trade_count = 0
    win_count, loss_count = 0, 0
    gross_profit, gross_loss = 0, 0
    buy_pos = len(profit_seq)-1
    for i in range(len(profit_seq)):
        if buy[i]==1:
            buy_pos = i
        if (sell[i]==1)|(i==len(profit_seq)-1):
            trade_count += 1 
            profit_per_trade = profit_seq[i] - profit_seq[buy_pos]
            if profit_per_trade >0:
                win_count += 1
                gross_profit += profit_per_trade
            else:
                loss_count += 1
                gross_loss -= profit_per_trade
                
    if gross_loss == 0:
        profit_factor = 10
    else:
        profit_factor = gross_profit/ gross_loss
    if trade_count == 0:
        percent_profitable, profit_factor, avg_trade_profit = 0, 0, 0
    else:
        percent_profitable = win_count/trade_count
        avg_trade_profit = (gross_profit-gross_loss)/trade_count
    return {'percent_profitable': percent_profitable, 'profit_factor':profit_factor, 'avg_trade_profit':avg_trade_profit, 'trade_count': trade_count}


def cal_drawdown(balance_list):
    draw_down = [0]*len(balance_list)
    peak = balance_list[0]
    local_trough = peak

    for i in range(2, len(balance_list)):
        if (balance_list[i-2] < balance_list[i-1]) &(balance_list[i-1]> balance_list[i]):
            candidate_peak = balance_list[i-1]
            if candidate_peak > peak:
                peak = candidate_peak
                local_trough = balance_list[i]               
            else:
                if balance_list[i] < local_trough:
                    local_trough = balance_list[i]
        else:
            if balance_list[i] < local_trough:
                local_trough = balance_list[i]
        draw_down[i] = 1- local_trough/peak    
    return draw_down



def spt_rst_levels(df):
    ## detect fractals
    spt_fractals_pos = (df.low.shift(2) > df.low.shift(1))& (df.low.shift(1) > df.low)& (df.low < df.low.shift(-1))& (df.low.shift(-1) < df.low.shift(-2))
    rst_fractals_pos = (df.high.shift(2) < df.high.shift(1))& (df.high.shift(1) < df.high)& (df.high > df.high.shift(-1))& (df.high.shift(-1) > df.high.shift(-2))
    
    fractals = np.append(df[spt_fractals_pos]['low'].values, df[rst_fractals_pos]['high'].values)
    fractals = fractals.reshape(-1,1)
    kmeans = KMeans(4, random_state=0).fit(fractals)
    labels = kmeans.labels_
    tmp_df = pd.DataFrame({'fractals': fractals[:,0], 'labels': labels})
    levels = tmp_df.groupby('labels').median().values[:,0]
    return np.sort(levels)



def calculate_curvature(x, y):
    dx = np.gradient(x)
    dy = np.gradient(y)
    d2x = np.gradient(dx)
    d2y = np.gradient(dy)
    curvature = np.abs(d2x*dy - dx*d2y) / np.power(dx*dx + dy*dy, 1.5)
    return curvature



def plot_bdf(code, bdf, fname):
    fig, axes = plt.subplots(2,1)
    fig.set_size_inches((10,8))
    plt.title(f'{code}')
    axes[0].plot(bdf.date, bdf.close)
    axes[0].plot(bdf.date, bdf.EMA_10)
    axes[0].plot(bdf.date, bdf.EMA_20)
    axes[0].plot(bdf.date, bdf.EMA_60)
    
    mask = [True if p>0 else False for p in bdf.predict]
    axes[0].scatter(bdf[mask]['date'], bdf[mask]['close'], s=30*bdf[mask]['predict'] ,c='blue',marker='o')  
    
    profit_list = np.array(bdf.profit)
    axes[1].plot(bdf.date, bdf.profit)
    
    mask_buy = [True if b==1 else False for b in bdf.buy]
    mask_sell = [True if s==1 else False for s in bdf.sell]
    axes[1].scatter(bdf[mask_buy]['date'], profit_list[mask_buy], marker = 'o', c='blue')
    axes[1].scatter(bdf[mask_sell]['date'], profit_list[mask_sell],  marker = 'o', c='red')
    plt.savefig(f'{fname}.png')
    




#-------- send email --------
def send_email(body, img_path):
    import smtplib
    import ssl
    from email.mime.multipart import MIMEMultipart
    from email.mime.image import MIMEImage
    from email.mime.text import MIMEText
   
    email_sender = 'lvmeng0502@gmail.com'
    email_password = 'yfpihqmzhcjnilao'
    email_receiver = 'lvmeng0502@gmail.com'
    subject  = 'check out my signal'

    msg = MIMEMultipart()
    msg['From'] = email_sender
    msg['To'] = email_receiver
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    msg.attach(MIMEImage(open(img_path, 'rb').read()))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(email_sender, email_password)
        smtp.sendmail(email_sender, email_receiver, msg.as_string())
