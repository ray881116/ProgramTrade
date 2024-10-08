import time
import sqlite3
import requests
import datetime
import pandas as pd
import shioaji as sj
from shioaji import TickFOPv1, Exchange
from exchange_calendars import get_calendar
import mplfinance as mpf

import finlab
from finlab.online.sinopac_account import SinopacAccount
from finlab import data
from finlab.backtest import sim
finlab.login('qJ5mydSbDgtQLXFOf7QNTs7TOTexbhAbhgFIQ9CmXyifNbb45nxtM/RV7Kwi4T49#free')

#%%
#獲取指定日期及前三日ticks

def get_ticks(connection, api, date, codes = str, is_Futures = False):
    print(date)

    dates = pd.to_datetime(date)
    tw_calendar = get_calendar('XTAI')
    if date not in tw_calendar.opens:
        print('{} is not trading date'.format(date))

    if is_Futures:
        
        #若為期貨只取英文代號作為code name
        code = ''.join(char for char in codes if char.isalpha())

        if dates.hour > 15:
            date = dates.date()+datetime.timedelta(days=1)
        
        else:
            date = dates.date()

        prev_trading_date = tw_calendar.sessions_window(date,-3)
        for trading_date in prev_trading_date:
            if trading_date.weekday() == 5:
                trading_dates = trading_date+datetime.timedelta(days=1)
                prev_trading_date = prev_trading_date.append(trading_dates)

    else:
        code = codes
        date = dates.date()
        
        prev_trading_date = tw_calendar.sessions_window(date,-3)


    #儲存 Total Data
    main_df = pd.DataFrame()

    for trading_date in prev_trading_date:
    #檢查是否為固有資料        
        try:        
            sql = "SELECT * FROM ticks WHERE code = '{}' and ts BETWEEN '{}' AND '{}' ".format(code,
                                                                                        trading_date,
                                                                                        trading_date+datetime.timedelta(days=1))
            df = pd.read_sql(sql, connection, parse_dates = ['ts'], index_col=['ts'])
        except:
            df = pd.DataFrame()

        #若已有資料，直接回傳dataframe    
        if (not df.empty):
            main_df = pd.concat([main_df, df], sort=False)
        
        else:
            print(f'Fetching missing data for {trading_date.strftime("%Y-%m-%d")}')

            #若資料不存在，利用永豐API獲取 
            if is_Futures: #若為期貨
                ticks = api.ticks(
                    contract = api.Contracts.Futures.get(code)[codes],  # For futures, use the Futures contract
                    date=date.strftime('%Y-%m-%d') 
                    )

            else: #若為證券
                ticks = api.ticks(
                    contract=api.Contracts.Stocks[codes],  # For stocks, use the Stocks contract
                    date=date.strftime('%Y-%m-%d')
                )

            temp_df = pd.DataFrame({**ticks})
            temp_df.ts = pd.to_datetime(temp_df.ts)
            temp_df['code'] = code
            temp_df = temp_df.set_index('ts')
        
            main_df = pd.concat([main_df, temp_df], sort= False)

    if not main_df.empty:
        main_df = main_df.reset_index().drop_duplicates(subset='ts').set_index('ts').sort_index()
        main_df = main_df.sort_index()
        main_df.to_sql('ticks', connection, if_exists='append')
        
    return main_df, False

#%%
#獲取ticks資料並更新資料庫(DB)

def update_ticks(connection, api, date, codes = str, is_Futures = False):

    main_df = pd.DataFrame()

    dates = pd.to_datetime(date)
    tw_calendar = get_calendar('XTAI')

    if dates.hour > 15:
        date = dates.date()+datetime.timedelta(days=1)
        print(date)

    else:
        date = dates.date()


    df, in_db = get_ticks(connection, api, date, codes , is_Futures)
  
    if df is not None and not in_db:
        main_df = pd.concat([main_df, df], sort = False, axis = 0)
        time.sleep(1)
    

    prev_trading_date = tw_calendar.sessions_window(date,-3)
    for prev_date in prev_trading_date:
        print('正在更新{}:{}的逐筆成交資料'.format(prev_date.strftime('%Y-%m-%d'), codes))
        prev_df, prev_in_db = get_ticks(connection, api, prev_date, codes, is_Futures)

        if prev_df is not None and not prev_in_db:
            main_df = pd.concat([main_df, prev_df], sort = False)
            time.sleep(1)
        
    if not main_df.empty:
        main_df = main_df.reset_index().drop_duplicates().set_index('ts')
        main_df = main_df.sort_index()
        main_df.to_sql('ticks', connection, if_exists='append')
        
    return main_df if not main_df.empty else None

    
#%%
#將ticks轉換成kbar
def ticks_to_kbars(ticks, interval = '30Min'):

    kbars = pd.DataFrame()

    kbars['open'] = ticks['close'].resample(interval, closed='right', label = 'left').first()
    kbars['close'] = ticks['close'].resample(interval, closed='right', label = 'left').last()
    kbars['high'] = ticks['close'].resample(interval, closed='right', label = 'left').max()
    kbars['low'] = ticks['close'].resample(interval, closed='right', label = 'left').min()
    kbars['volume'] = ticks['volume'].resample(interval, closed='right', label = 'left').sum()

    kbars.dropna(inplace=True)

    return kbars

#%%
def historical_kbars(connection, api, start_date, end_date,
                      interval = '30Min', codes = str, is_Futures = False):
    
    if is_Futures:

        code = ''.join(char for char in codes if char.isalpha())

        kbars = api.kbars(
            contract = api.Contracts.Futures[codes],
            start = start_date,
            end = end_date
        )
        kbars = pd.DataFrame({**kbars})
        kbars.ts = pd.to_datetime(kbars.ts)
        k_columns = {'Close': 'close', 'High': 'high', 'Low': 'low', 'Open': 'open', 'Volume': 'volume'}
        kbars = kbars.rename(columns=k_columns)
        kbars['code'] = code
        kbars = kbars.set_index('ts')
        kbars = kbars.resample(interval, closed= 'right', label= 'left').agg({'close':'last',
                                                                              'high':'max',
                                                                              'low':'min',
                                                                              'open': 'first',
                                                                              'volume':'sum'})

        kbars.dropna(inplace=True)

    return kbars


#%%
def get_MA(kbars):
    import talib  

    kbars['ma5'] = talib.MA(kbars.close, 5) 
    kbars['ma10'] = talib.MA(kbars.close, 10)
    kbars['ma20'] = talib.MA(kbars.close, 20)  

    return kbars

#%%
