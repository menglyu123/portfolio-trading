from auto_trade import *
from futu import *
import schedule, time, datetime
import json, copy
from data.import_data import DB_ops

## portfolio simulation
# run simulation individually for a while
class portfolio_trade(auto_trade):
    def __init__(self, portfolio_size=3, capital= 100000, fee= 0.002, auto_trade=False):
        super().__init__('lenet')
        self.portfolio_size = portfolio_size
        self.capital = capital
        self.fee = fee
        self.auto_trade = auto_trade
        if self.auto_trade:
            self.create_record_file('auto_trade_record.json')


    def create_record_file(self, fname):
        record = {'date':[None], 'daily_portfolio':[{}], 'portfolio_total_capital':[0], 'portfolio_cash': [self.capital],'take_out_profit':[0]}
        with open(fname, 'w') as f:
            json.dump(record, f)
        return


    def trade(self, since=None):
        today = datetime.date.today()
        code_info = pd.read_csv('./data/code_info.csv',converters={'code': str})
        code_info.set_index('code',inplace=True)
       
        if self.auto_trade:
            date = today
            fname = 'auto_trade_record.json'
            mydb_ops = DB_ops(host='localhost', user='root', password='mlu123456')
            mydb_ops.update_today_price('HK_stocks_daily')
            trd_ctx = OpenHKTradeContext(host='127.0.0.1', port=11111, security_firm=SecurityFirm.FUTUSECURITIES)
        else:
            fname = f'simulate_trade_record_{since}.json'
            self.create_record_file(fname)
            date = datetime.datetime.strptime(since, '%Y-%m-%d').date()
        
        total_ts_count = 0
        cut_count = 0

        while date <= today:
            ## load the latest record
            with open(fname, 'r') as f:
                trade_record = json.load(f)
            last_record = trade_record['daily_portfolio'][-1]
            new_daily_portfolio = copy.deepcopy(last_record)
            portfolio_assets = list(last_record.keys())
            cash = trade_record['portfolio_cash'][-1]
            take_out_profit = trade_record['take_out_profit'][-1]
            
            # Rank candidate stocks based on signal values 
            signal_df = self.single_day_predict(str(date), self.code_list)
            cur_close = signal_df.close
            if len(signal_df) == 0:
                date += datetime.timedelta(days=1)
                continue
            candidate_assets= signal_df[~signal_df.index.isin(portfolio_assets)]
            #''' Candidates Showing Buy Signal'''
            tmp = candidate_assets[(candidate_assets.pred>0.5)&(candidate_assets.down_deep)].sort_values(by=['pred'], ascending=False)
            candidate_assets = tmp.index.to_list()   
            num_candidates = len(candidate_assets)
           
            ## SELL 
            if portfolio_assets == []:
                available_quota = self.portfolio_size
            else:
                available_quota = self.portfolio_size - len(portfolio_assets)   
                sell_assets = [asset for asset in portfolio_assets if (signal_df.loc[asset]['close']< 0.98*last_record[asset]['buy_price'])|(signal_df.loc[asset]['pred']<0.5)]
                cut_assets = [asset for asset in portfolio_assets if (signal_df.loc[asset]['close']< 0.98*last_record[asset]['buy_price'])]
                total_ts_count += len(sell_assets)
                cut_count += len(cut_assets)
                print('assets to sell:', sell_assets)
                if len(sell_assets) > 0:  # SELL
                    for asset in sell_assets:
                        success_sell = True
                        if self.auto_trade:
                            ret, data = trd_ctx.place_order(price=cur_close[asset], qty=last_record[asset]['position'], code="HK.0"+ asset, trd_side=TrdSide.SELL, trd_env=TrdEnv.SIMULATE) #order_type=OrderType.MARKET for real market
                            if ret != RET_OK:
                                success_sell = False
                                print('Place SELL order error:', data)                               
                            else:
                                while True:
                                    time.sleep(10)
                                    order_id = data['order_id'][0]
                                    trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.HK, host='127.0.0.1', port=11111, security_firm=SecurityFirm.FUTUSECURITIES)
                                    ret, data = trd_ctx.history_order_list_query(trd_env=TrdEnv.SIMULATE)
                                    tsc_rcd = data[data['order_id']==order_id]
                                    if tsc_rcd['order_status'][0]==OrderStatus.FILLED_ALL:
                                        break
                                    # modify the current order
                                    print(f'fail to sell at {tsc_rcd['price'][0]} for asset {asset}')
                                    modify_price = tsc_rcd['price'][0]*0.999
                                    print(f'modify to sell at {modify_price} for asset {asset}')
                                    ret, data = trd_ctx.modify_order(ModifyOrderOp.NORMAL, order_id= order_id, qty=tsc_rcd['qty'][0], price= modify_price, trd_env=TrdEnv.SIMULATE) 
                                    if ret != RET_OK:
                                        print('error:', data)
                                        success_sell = False
                                        break
                        if success_sell:
                            portfolio_assets.remove(asset)
                            new_daily_portfolio.pop(asset)
                            available_quota += 1
                            cash += last_record[asset]['position']*cur_close[asset]
                            half_profit = 0.5*(cur_close[asset]-last_record[asset]['buy_price'])*last_record[asset]['position']
                            if half_profit > 0:
                                take_out_profit += half_profit
                                cash -= half_profit     
                            print('sell asset:', asset, 'at price:', tsc_rcd['price'][0], 'position:', last_record[asset]['position'])    
               
            ## BUY
            if (num_candidates > 0) &(available_quota > 0) &(cash > 0):   ## BUY 'buy_quota' num of candidate assets using all the available amount
                buy_quota = min(available_quota, num_candidates)
                buy_assets = candidate_assets[:buy_quota]
                for asset in buy_assets:  # add in new assets
                    lot_size = code_info.loc[asset]['lot_size']
                    cash_buy_asset = cash/available_quota
                    position = int(cash_buy_asset*(1-self.fee)/cur_close[asset]/lot_size)*lot_size
                    if position == 0:
                        position = int(cash*(1-self.fee)/cur_close[asset]/lot_size)*lot_size
                        cash_buy_asset = cash
                        if position == 0:
                            print('cash is not enough to buy 1 lot')
                            break
                    if position > 0:    
                        success_buy = True
                        if self.auto_trade:
                        
                            ret, data = trd_ctx.place_order(price= cur_close[asset], qty= position, code="HK.0"+ asset, trd_side=TrdSide.BUY, trd_env=TrdEnv.SIMULATE) #order_type=OrderType.MARKET for real market
                            if ret != RET_OK:
                                success_buy = False
                                print('Place BUY oder error:', data)
                            else:
                                while True:
                                    time.sleep(10) 
                                    order_id = data['order_id'][0]
                                    trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.HK, host='127.0.0.1', port=11111, security_firm=SecurityFirm.FUTUSECURITIES)
                                    ret, data = trd_ctx.history_order_list_query(trd_env=TrdEnv.SIMULATE)
                                    tsc_rcd = data[data['order_id']==order_id]
                                    if tsc_rcd['order_status'][0]==OrderStatus.FILLED_ALL:
                                        break
                                    # modify the current order
                                    print(f'fail to buy at {tsc_rcd['price'][0]} for asset {asset}')
                                    modify_price = tsc_rcd['price'][0]*1.001
                                    print(f'modify to buy at {modify_price} for asset {asset}')
                                    ret, data = trd_ctx.modify_order(ModifyOrderOp.NORMAL, order_id= order_id, qty=tsc_rcd['qty'][0], price= modify_price, trd_env=TrdEnv.SIMULATE) 
                                    if ret != RET_OK:
                                        print('error:', data)
                                        success_buy = False
                                        break
                        if success_buy:
                            new_daily_portfolio[asset] = {}
                            new_daily_portfolio[asset]['buy_price'] = cur_close[asset]
                            new_daily_portfolio[asset]['position'] = int(position)
                            new_daily_portfolio[asset]['capital'] = cur_close[asset]*position
                            new_daily_portfolio[asset]['status'] = 'buy'
                            portfolio_assets.append(asset)
                            cash -= cur_close[asset]*position + self.fee*cash_buy_asset   
                            print("buy asset:", asset, 'at price:', tsc_rcd['price'][0], "position:", position)                
                        available_quota -= 1
                    
            # current portfolio
            print('date:', str(date), 'candidate_assets:', candidate_assets, 'current portfolio:', portfolio_assets)

            ## Update record of hold assets
            portfolio_asset_capital = 0
            for asset in portfolio_assets:
                if asset in last_record.keys():  
                    new_daily_portfolio[asset]['capital'] = last_record[asset]['position']* cur_close[asset]
                    new_daily_portfolio[asset]['status'] = 'hold'
                portfolio_asset_capital += new_daily_portfolio[asset]['capital']
            
            ## Append to record   
            trade_record['date'].append(date.strftime('%Y-%m-%d'))
            trade_record['daily_portfolio'].append(new_daily_portfolio)
            trade_record['portfolio_total_capital'].append(portfolio_asset_capital+ cash+ take_out_profit)  
            trade_record['portfolio_cash'].append(cash)
            trade_record['take_out_profit'].append(take_out_profit)
            with open(fname, 'w') as f:
                json.dump(trade_record, f)
            date += datetime.timedelta(days=1)
        print('trade(sell) count:', total_ts_count, 'cut(sell) count:', cut_count)
        return


    def auto(self):
        if not self.auto_trade:
            return
        else:
            schedule.every().day.at("10:41").do(self.trade)
            while True:
                schedule.run_pending()
                time.sleep(1)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--simulate', action='store_true')
    parser.add_argument('--auto_trade', action='store_true')
    args = parser.parse_args()

    if args.simulate:
        myportfolio = portfolio_trade()
        myportfolio.trade(since='2023-10-1')

    if args.auto_trade:
        myportfolio = portfolio_trade(auto_trade=True)
        myportfolio.auto()


