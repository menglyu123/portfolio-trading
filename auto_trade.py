from util import cal_trade_performance, cal_drawdown, cal_trade_performance, plot_bdf, calculate_curvature
from data.prepare_data import prepare_data
from model.mylenet import lenet_regression
from model.myencoder import encoder_regression
import numpy as np, pandas as pd
import pandas_ta as pta
import argparse
import datetime as dt
import statsmodels.api as sm
from data.import_data import DB_ops
from futu import *
import joblib

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType, BooleanType




class auto_trade():
    def __init__(self, model):
        with open('./data/code_pool.txt','r') as fp:
            self.code_list = [line.rstrip() for line in fp]
        self.winlen = 120
        self.future = 1
        self.fea_num = 16#15
        self.host = 'localhost'
        self.user = 'root'
        self.password = 'mlu123456'
        self.database = 'HK_stocks_daily'
         
        if model == 'lenet':
            self.model_folder = f'./model/regression/lenet'  
            # build model
            self.model = lenet_regression(shape= (self.winlen, self.fea_num))
        if model == 'encoder':
            self.model_folder = f'./model/regression/encoder'  
            # build model
            self.model = encoder_regression(shape= (self.winlen, self.fea_num))


    def load_model(self):
        self.model_path = self.model_folder+ '/epoch_sel.h5'
        self.model.load(self.model_path)
        return self.model


    def fetch_train_data(self):
        db_ops = DB_ops(host= self.host, user= self.user, password = self.password)
        for code in self.code_list:
            train_df = db_ops.fetch_data_batch_from_db(code, end='2021-12-31', database=self.database)
          #  test_df = db_ops.fetch_data_batch_from_db(code, end='2023-01-01', database=self.database, start='2022-01-01')
            train_df.to_csv( f'./data/HK_stocks_data/daily/train/{code}.csv', index=False)
          #  test_df.to_csv( f'./data/HK_stocks_data/daily/test/{code}.csv', index=False)

        
    def train(self, batch_size=32, lr=2e-4, epoch=20):
        big_train_X, big_val_X = [],[]
        big_train_y, big_val_y = [],[]

        for code in self.code_list:
            print(f'code: {code}')
            df = pd.read_csv(f'./data/HK_stocks_data/daily/train/{code}.csv')
            df.dropna(inplace=True)
            try:
                train_X, train_y, val_X, val_y = prepare_data(df, self.winlen, self.future, training=True)
                big_train_X.append(train_X)
                big_val_X.append(val_X)
                big_train_y.append(train_y)
                big_val_y.append(val_y)
            except:
                print('data issue')
                continue
        big_train_X = np.concatenate(big_train_X, axis=0)
        big_train_y = np.concatenate(big_train_y, axis=0)
        big_val_X = np.concatenate(big_val_X, axis=0)
        big_val_y = np.concatenate(big_val_y, axis=0)
        print("train shape: ", big_train_X.shape)
        print('val shape: ', big_val_X.shape)

        self.model.model.summary()
        # train model
        self.model.train(big_train_X, big_train_y, big_val_X, big_val_y, batch_size, lr, epoch, self.model_folder)
        return



    def backtest_single(self, df, init_balance=10000, fee=0.001, pct = 1):
        bdf, test_X = prepare_data(df, self.winlen, self.future, training=False)
        if test_X.shape[0] == 0:
            return
        
        # model prediction
        _ = self.load_model()
        pred_val = self.model.predict(test_X)
        bdf['predict'] = pred_val
        
        close = bdf.close.to_numpy()
        pred = bdf.predict.to_numpy()
    
        price_change = [p/close[0] -1 for p in close]
        period_len = bdf.shape[0]
        buy, sell = [0]*period_len, [0]*period_len
        status = 'empty'
        new_status = status
        status_list = ['empty']
        balance_list = []
        balance = init_balance
        in_amt_list = []
        in_amt = 0
        
        rs = bdf.close.rolling(15).apply(lambda x: -np.mean(x[x>0])/(np.mean(x[x<0])+1e-5))
        bdf['close_rsi'] =  100- 100/(1+rs)  #bdf.predict.rolling(5).mean().fillna(0)
        bdf.close_rsi.fillna(method='bfill', inplace=True)


        for i in range(period_len):  
            signal = bdf.iloc[i]['predict']>0.5 #(sum(bdf.iloc[i-3:i+1]['predict']>0)>=2)
            down_deep = (bdf.iloc[i]['EMA_60']/bdf.iloc[i]['close']>1.1)  #the down deeper, the risk lower    
            
            if  (status == 'empty') &signal&down_deep: 
                balance *= (1-fee)
                buy[i] = 1 
                buy_price = bdf.iloc[i]['close']
                new_status = 'hold'
                    
            if status == 'hold':
                balance *= close[i]/close[i-1]
                if (not signal)|(bdf.iloc[i]['close']<0.98*buy_price): 
                    sell[i] = 1
                    new_status = 'empty'

            status = new_status
            status_list.append(status)
            balance_list.append(balance)
            in_amt_list.append(in_amt)
        
        
        profit = [b- init_balance for b in balance_list]
        advantage = [b/init_balance-p for b,p in zip(profit, price_change)]
        drawdown = cal_drawdown(balance_list)

        metric = cal_trade_performance(profit, buy, sell)  
        eval_df = pd.DataFrame({"profit": profit, "advantage":advantage, "price_change": price_change, "buy": buy, "sell":sell, 
                     "drawdown": drawdown})
        bdf.reset_index(inplace=True, drop=True)
        result_df = pd.concat([bdf, eval_df], axis=1)
        return result_df, metric




    def backtest(self, days, code_list= None, folder= './backtest', plot= True, record = False):
        if code_list == None:
            code_list = self.code_list
        profit_list = []
        price_change_list = []
        drawdown_list = []
        adv_list = []
        percent_profitable = []
        profit_factor = []
        avg_trade_profit = []
        trade_count = []
        available_code = []
        today= dt.date.today()

        for code in code_list:
            quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)   
            ret, df, _ = quote_ctx.request_history_kline('HK.0'+code, str(today-dt.timedelta(days=self.winlen+60+days)), str(today),ktype='K_DAY')
            if ret !=RET_OK:
                print('error: ', df)
            else: 
                df = df[['time_key','open','high','low','close','volume']]
                df.rename(columns={'time_key':'date'},inplace=True)
                df['date'] = pd.to_datetime(df.date).dt.date       
            quote_ctx.close()
            time.sleep(0.5)

            bdf, metric = self.backtest_single(df)
            if record:    
                bdf.to_csv(f'{folder}/record_{code}_{days}.csv', index= False)
            if plot:
                plot_bdf(code, bdf, f'{folder}/backtest_{code}_{days}')
            try:
                profit_list.append(bdf.iloc[-1]['profit'])
                price_change_list.append(bdf.iloc[-1]['price_change'])
                adv_list.append(bdf.iloc[-1]['advantage'])
                drawdown_list.append(max(bdf.drawdown))
                percent_profitable.append(metric['percent_profitable'])
                profit_factor.append(metric['profit_factor'])
                avg_trade_profit.append(metric['avg_trade_profit'])
                trade_count.append(metric['trade_count'])
                available_code.append(code)
            except:
                continue

        summary_df = pd.DataFrame({'code':available_code, 'profit':profit_list, 'price_change':price_change_list, 'advantage':adv_list, 'drawdown':drawdown_list,
                      'percent_profitable': percent_profitable,
                      'profit_factor': profit_factor,
                      'avg_trade_profit': avg_trade_profit,
                      'trade_count': trade_count
                      })
        
        summary_df.to_csv(f"backtest_summary_{days}.csv", index=False)
        return summary_df
    

    def single_day_predict(self, date, code_list= None, save=False):
        if code_list == None:
            code_list = self.code_list
        
        fetched_code, close_list, pred_list, down_deep_list, start_up_break_list, recent_uptrend_start_date_list =[], [], [], [], [],[]
        today = dt.date.today()
        _ = self.load_model()
        quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)

        for code in code_list:
            if dt.datetime.strptime(date,'%Y-%m-%d').date() == today:
                ret, df, _ = quote_ctx.request_history_kline('HK.0'+code, str(today-dt.timedelta(days=self.winlen+60+90)), str(today), ktype='K_DAY')
                if ret !=RET_OK:
                    print('error: ', df)
                else: 
                    df = df[['time_key','open','high','low','close','volume']]
                    df.rename(columns={'time_key':'date'},inplace=True)
                    df['date'] = pd.to_datetime(df.date).dt.date
                
                time.sleep(0.5)
            elif dt.datetime.strptime(date,'%Y-%m-%d').date() < today:
                db_ops = DB_ops(host= self.host, user= self.user, password= self.password)
                df = db_ops.fetch_batch_price_from_db(code, '1d', end=date, limit=self.winlen+100)#60
         
            if len(df)!=0:
                start_up_break = False
                try:
                    _, trend = sm.tsa.filters.hpfilter(df.close, 60)  
                    bdf, test_X = prepare_data(df, self.winlen, self.future, training=False)
                    preds = self.model.predict(test_X)
                    up_trend = (bdf.iloc[-1]['EMA_60']/(bdf.iloc[-10]['EMA_60'])>1) &(bdf.iloc[-1]['EMA_60']/(bdf.iloc[-30]['EMA_60'])>1)
                    down_deep = bdf.iloc[-1]['EMA_60']/bdf.iloc[-1]['close'] >1.1   #the down deeper, the risk lower. the last EMA60 is different if calculate using different-len tables
                    emas = [pta.ema(df.close, day) for day in [5,10,20,30,60]]
                    mvgs = [ema.iloc[-1] for ema in emas]
                    # find the rough recent uptrend turning id
                    turning_id = trend[(trend<trend.shift(1))&(trend<trend.shift(-1))].index.tolist()[-1]
                    # adjust the rough recent uptrend turning id to find the specific accurate one for each mvg line
                    chopped_emas = [ema.iloc[turning_id:] for ema in emas]
                    up_trend_ids = [chopped_ema[chopped_ema> chopped_ema.shift(1)].index.tolist()[0] if sum(chopped_ema> chopped_ema.shift(1))>0 else -1 for chopped_ema in chopped_emas]
                    up_trend_emas = [ema.iloc[up_trend_id:] for ema, up_trend_id in zip(emas,up_trend_ids)]
                    
                    # check if each up_trend mvg line has ever turn downside since the turning id
                    mvg_stables = [sum(up_trend_ema/up_trend_ema.shift(1)<=1)==0 for up_trend_ema in up_trend_emas]
                    # check if today is a upbreak of the stable mvg line
                    for mvg, mvg_stable in zip(mvgs, mvg_stables):
                        if up_trend& mvg_stable& (bdf.iloc[-1]['close']/mvg <1.02)&(bdf.iloc[-1]['close']/mvg >=0.98)&(preds[-1][0]>0)*(preds[-2][0]<0):
                            start_up_break = True
                            break
                    fetched_code.append(code)
                    close_list.append(bdf.iloc[-1]['close'])
                    pred_list.append(preds[-1][0])
                    down_deep_list.append(down_deep)
                    start_up_break_list.append(start_up_break)
                    recent_uptrend_start_date_list.append(df.iloc[turning_id]['date'])
                except Exception as e:
                    print(code, e)
                    continue
        
        df = pd.DataFrame({'code':fetched_code, 'close': close_list, 'pred':pred_list, 'down_deep':down_deep_list,  'start_up_break': start_up_break_list, 'recent_uptrend_start_date': recent_uptrend_start_date_list})
        df.set_index('code', inplace=True)
        if save & (len(df)!=0):
            df.to_csv(f'./results/{date}_pred_all.csv')
        quote_ctx.close()  
        return df
       


    def spark_single_day_predict(self, date, code_list=None, save=False):
        if code_list == None:
            code_list = self.code_list
        date = dt.datetime.strptime(date,'%Y-%m-%d').date()

        # update today's price to db
        db_ops = DB_ops(host= self.host, user= self.user, password = self.password)
        if date == dt.date.today():
            db_ops.update_today_price(interval='1d')
        
        spark = SparkSession.builder.appName("predict").getOrCreate()
        sc = spark.sparkContext
        df = spark.createDataFrame(pd.DataFrame({'code':code_list}))

        price_model = joblib.load('./model/price_model.pkl')
        broadcast_price_model = sc.broadcast(price_model)
        
        schema = StructType([StructField('close', DoubleType(), False), StructField('pred', DoubleType(), False), StructField('down_deep', BooleanType(), False)])
        @udf(schema)
        def add_past_arr(code):
            db_ops = DB_ops(host= self.host, user= self.user, password = self.password)
            price_df = db_ops.fetch_batch_price_from_db(code, '1d', end= str(date), limit=self.winlen+60)
            # data transform and preprocessing
            try:
                bdf, test_X = prepare_data(price_df, 120, 1, training=False)
                down_deep = bool(bdf.iloc[-1]['EMA_60']/bdf.iloc[-1]['close'] >1.1)
                tmp = broadcast_price_model.value.predict(test_X).tolist()
                close = float(bdf.iloc[-1]['close'])
            except:
                down_deep = False
                close = -0.0
                tmp = [[-999.0]]
            return [close, tmp[0][0], down_deep]
        
        df = df.withColumn("results", add_past_arr(df['code'])).select('code','results.*').toPandas()
        df.set_index('code', inplace=True)
        if save & (len(df)!=0):
            df.to_csv(f'./results/{date}_pred_all.csv')
        return df



if __name__ =='__main__':
    auto_trade_today = auto_trade('lenet')
    parser = argparse.ArgumentParser()
    parser.add_argument('--download_data', action='store_true')
    parser.add_argument('--train', action='store_true')
    parser.add_argument('--backtest', action='store_true')
    parser.add_argument('--realtimepredict', action='store_true')
    args = parser.parse_args()

    if args.download_data:
        auto_trade_today.fetch_train_data()
    if args.train:
        auto_trade_today = auto_trade('lenet')
        auto_trade_today.train(lr=1e-4)   
    if args.backtest:
        auto_trade_today.backtest(days=500, record=True) #code_list=['0285']

    if args.realtimepredict:
        pred= auto_trade_today.spark_single_day_predict(str(dt.date.today()), save=True) 
    
        
      