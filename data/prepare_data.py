import pandas_ta as pta, pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import statsmodels.api as sm
import warnings
warnings.filterwarnings("ignore")
import dill
dill.settings['recurse'] = True

def filter(s, len):
    _, trend = sm.tsa.filters.hpfilter(s, len)
    return trend.iloc[-1]


#------ Function for process data -------
def prepare_data(
    dataframe,
    winlen= 120,
    future= 1,
    split = 0.3,
    training = False
):
    dataframe = dataframe.dropna(subset=["close"])   
 
    diff5 =  (dataframe.volume*dataframe.close)/((dataframe.volume*dataframe.close).rolling(5).mean())
    diff10 =  (dataframe.volume*dataframe.close)/((dataframe.volume*dataframe.close).rolling(10).mean())
    diff20 =  (dataframe.volume*dataframe.close)/((dataframe.volume*dataframe.close).rolling(20).mean())
    diff30 =  (dataframe.volume*dataframe.close)/((dataframe.volume*dataframe.close).rolling(30).mean())
    diff60 =  (dataframe.volume*dataframe.close)/((dataframe.volume*dataframe.close).rolling(60).mean())
    diff_vmvg5 = pd.Series(diff5, name= 'diff_vmvg5')
    diff_vmvg10 = pd.Series(diff10, name= 'diff_vmvg10')
    diff_vmvg20 = pd.Series(diff20, name= 'diff_vmvg20')
    diff_vmvg30 = pd.Series(diff30, name= 'diff_vmvg30')
    diff_vmvg60 = pd.Series(diff60, name= 'diff_vmvg60')

    mvg5 = pta.ema(dataframe.close, 5)
    mvg10 = pta.ema(dataframe.close, 10)
    mvg20 = pta.ema(dataframe.close, 20)
    mvg30 = pta.ema(dataframe.close, 30)
    mvg60 = pta.ema(dataframe.close, 60)

    # diff of filterd close price from bollinger upper bound and lower bound
    upper_20 = dataframe.close.rolling(20).mean()+ 2*(dataframe.close.rolling(20).std())
    lower_20 = dataframe.close.rolling(20).mean()- 2*(dataframe.close.rolling(20).std())
    upper_60 = dataframe.close.rolling(60).mean()+ 2*(dataframe.close.rolling(60).std())
    lower_60 = dataframe.close.rolling(60).mean()- 2*(dataframe.close.rolling(60).std())
    diff_upper_20 = pd.Series(dataframe.close/upper_20, name= 'diffupper20')
    diff_lower_20 = pd.Series(dataframe.close/lower_20, name= 'difflower20')
    diff_upper_60 = pd.Series(dataframe.close/upper_60, name= 'diffupper60')
    diff_lower_60 = pd.Series(dataframe.close/lower_60, name= 'difflower60')

    # rsi
    rsi_5 = pta.rsi(dataframe.close, 5)/100
    rsi_10 = pta.rsi(dataframe.close, 10)/100
    rsi_20 = pta.rsi(dataframe.close, 20)/100

    
    # Volume, OHLC change ratio
    open_chg = pd.Series(dataframe.open/dataframe.close.shift(1)-1, name='open_chg')
    high_chg = pd.Series(dataframe.high/dataframe.close.shift(1)-1, name='high_chg')
    low_chg = pd.Series(dataframe.low/dataframe.close.shift(1)-1, name='low_chg')
    close_chg = pd.Series(dataframe.close.pct_change(), name = 'close_chg')
    #vol_chg = pd.Series(np.log(dataframe.volume).pct_change(),name='vol_chg')

    # # slope
    # slope_5 = pd.Series(dataframe.close/dataframe.close.shift(5), name='slope_5')
    # slope_10 = pd.Series(dataframe.close/dataframe.close.shift(10), name='slope_10')
    # slope_20 = pd.Series(dataframe.close/dataframe.close.shift(20), name='slope_20')
    # slope_60 = pd.Series(dataframe.close/dataframe.close.shift(60), name='slope_60')
    ## instead of the above slope, consider EMA,close/close[0] at the starting point of the window or consider close,EMA_5/10/20/60.pct_change.

    
    if training:
        _, trend = sm.tsa.filters.hpfilter(dataframe.close, 50)
        trend = pd.Series(trend, name='trend')
        dataframe = pd.concat([dataframe, mvg5, mvg10, mvg20, mvg30, mvg60, diff_vmvg5, diff_vmvg10, diff_vmvg20, diff_vmvg30, diff_vmvg60, diff_upper_20, diff_lower_20, diff_upper_60, diff_lower_60, rsi_5, rsi_10, rsi_20, open_chg, high_chg,low_chg, close_chg, trend], axis=1)   
    else:
        dataframe = pd.concat([dataframe, mvg5, mvg10, mvg20, mvg30, mvg60, diff_vmvg5, diff_vmvg10, diff_vmvg20, diff_vmvg30, diff_vmvg60, diff_upper_20, diff_lower_20, diff_upper_60, diff_lower_60,  rsi_5, rsi_10, rsi_20, open_chg, high_chg, low_chg, close_chg], axis=1)   
    
    allcolumns = dataframe.columns
    keepcols = ["date", "open", "high", "low", "close", "volume", 'EMA_5', 'EMA_10', 'EMA_20','EMA_30', 'EMA_60', 'trend']
    tempcols = []
    
    #********** transform features *********
    for col in allcolumns:
        if col not in keepcols:
            dataframe[col + "_trans"] = dataframe[col]
            tempcols.append(col + "_trans")
    dataframe = dataframe.dropna()  

    scaler = StandardScaler()
    #******** generate df including original price data for future backtesting use ********
    if training:
        ## derive the label 
        #ref_col = dataframe.trend
        m = dataframe.trend.rolling(5).mean()
        std = dataframe.trend.rolling(5).std()
        ref_col1 = m
        tmp1 = ref_col1.shift(-future)
        ref_col2 = std
        tmp2 = ref_col2.shift(-future)
        dataframe['label'] = ((tmp1/ref_col1)**(ref_col2/tmp2)- 1)*100  #(tmp/ref_col-1)*100
        df = dataframe.dropna(subset=["label"])
        #******** transform input to time-series type ********
        tmp_df = df[tempcols+['label']]
        whole_arr = [win.to_numpy() for win in tmp_df.rolling(winlen) if len(win)==winlen]
        whole_arr = np.array(whole_arr)
        X_arr = whole_arr[:,:,:-1]
        label = whole_arr[:,-1,-1]
        for i in range(len(X_arr)):
            X_arr[i] = scaler.fit_transform(X_arr[i])
        label = scaler.fit_transform(label.reshape(-1,1))
        label = label[:,0]
       
        # split to training set and validation set
        pos = round(split*X_arr.shape[0])
        train_X = X_arr[:-pos]
        val_X = X_arr[-pos:] 
        train_y = label[:-pos]
        val_y = label[-pos:]
        return train_X, train_y, val_X, val_y
    else:
        df = dataframe.drop(columns=tempcols)
        #******** transform input to time-series type ********
        tmp_df = dataframe[tempcols]
        whole_arr = [win.to_numpy() for win in tmp_df.rolling(winlen) if len(win)==winlen]
        X_arr = np.array(whole_arr)
        for i in range(len(X_arr)):
            X_arr[i] = scaler.fit_transform(X_arr[i])
        return df.iloc[winlen-1:], X_arr
