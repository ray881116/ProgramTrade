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
'''
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
        
        #else:
            #print(f'Fetching missing data for {trading_date.strftime("%Y-%m-%d")}')

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
'''
#%%

def get_ticks(connection, api, date, codes = str, is_Futures = False):

    dates = pd.to_datetime(date)
    tw_calendar = get_calendar('XTAI')

    #name tickers and correct time stamp

    if is_Futures:
        
        #若為期貨只取英文代號作為code name
        code = ''.join(char for char in codes if char.isalpha())

        if dates.hour > 15:
            date = dates.date()+datetime.timedelta(days=1)
        
        else:
            date = dates.date()

    else:
        code = codes
        date = dates.date()
        

    try:    
        sql = "SELECT * FROM ticks WHERE code = '{}' and ts BETWEEN '{}' AND '{}' ".format(code,
                                                                                 date,
                                                                                 date+datetime.timedelta(days=1))
        df = pd.read_sql(sql, connection, parse_dates = ['ts'], index_col=['ts'])
    except:
        df = pd.DataFrame()
    
    if not df.empty:
        return df, True
    
    if is_Futures: #若為期貨
            ticks = api.ticks(
                contract = api.Contracts.Futures.get(code)[codes],  # For futures, use the Futures contract
                date=date.strftime('%Y-%m-%d') 
                )

    else: #若為證券
            ticks = api.ticks(
            contract=api.Contracts.Stocks[code],  # For stocks, use the Stocks contract
                date=date.strftime('%Y-%m-%d')
            )    
    df = pd.DataFrame({**ticks})

    df.ts = pd.to_datetime(df.ts)
    df['code'] = code
    df = df.set_index('ts')

    return df, False

#%%
#獲取ticks資料並更新資料庫(DB)
def update_ticks(connection, api, daily_target, is_Futures = False):
    main_df = pd.DataFrame()

    tw_calendar = get_calendar('XTAI')

    for date, codes in daily_target.items():

        day_trading_codes = [code for code in codes ]

        print('正在更新{}逐筆成交資料，總共{}檔標的，為{}'.format(date.strftime('%Y/%m/%d'), len(day_trading_codes),day_trading_codes))

        for code in day_trading_codes:
            df, in_db = get_ticks(connection, api, date, code, is_Futures)

            if df is not None and in_db:
                main_df = pd.concat([main_df, df], sort = False)
                time.sleep(1)

            prev_trading_date = tw_calendar.previous_close(date).date()
            prev_df, prev_in_db = get_ticks(connection, api, date, code, is_Futures)

            if prev_df is not None and not prev_in_db:
                main_df = pd.concat([main_df, prev_df], sort = False)
                time.sleep(1)
    
    if not main_df.empty:
        try:
            main_df.to_sql('ticks', connection, if_exists='append', index=False)
            print("Data stored successfully.")
        except Exception as e:
            print("Failed to store data:", e)
    return main_df


    
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
def retry_request(url, payloads, headers):
    
    for i in range(3):
    
        try:
            return requests.get(url, params= payloads, headers=headers)
        
        except:
            print('發生錯誤，請等待一分種後再嘗試')
            time.sleep(60)
    
    return None

#%%

def get_daily_prices(date, connection):

    try:
        df = pd.read_sql("select * from daily_prices where 日期 = '{}'".format(date),
                         connection,
                         parse_dates= ['日期'],
                         index_col= ['證券代號','日期'])
    except:
        df = pd.DataFrame()
    
    if not df.empty:
        return df, True
    #INDB TRUE
    #先在資料庫裡抓取每日收盤價，沒有資料則利用爬蟲重新抓取
    
    url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX'

    payloads = {
        'response': 'html',
        'date': date.strftime('%Y%m%d'),
        'type': 'ALLBUT0999'
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.80 Safari/537.36'
    }

    response = retry_request(url, payloads, headers)

    try:
        df = pd.read_html(response.text)[-1]
    
    except:
        print('{} 找不到資料'.format(
            date.strftime('%Y%m%d')))
        
        return None, False
    #INDB False
    df.columns = df.columns.get_level_values(2)
    df['漲跌價差'] = df['漲跌價差'].where(df['漲跌(+/-)'] != '-', -df['漲跌價差'])
    df.drop(['證券名稱','漲跌(+/-)'],inplace=True, axis=1)
    df['日期'] = pd.to_datetime(date)
    df = df.set_index(['證券代號','日期'])
    df = df.apply(pd.to_numeric, errors = 'coerce')
    df.drop(df[df['收盤價'].isnull()].index, inplace=True)
    df['昨日收盤價'] = df['收盤價'] - df['漲跌價差']
    df['股價震幅'] = (df['最高價'] - df['最低價']) / df['昨日收盤價']*100

    return df, False

#%%
def update_daily_prices(start_date, end_date, connection):

    tw_calendars = get_calendar("XTAI")

    main_df = pd.DataFrame()

    for date in pd.date_range(start_date, end_date):

        if date not in tw_calendars.opens:
            continue

        df, in_db = get_daily_prices(date, connection)

        if df is not None and not in_db:
            main_df = pd.concat([main_df,df],axis = 0)
            print('{} 每日收盤行情抓取完成，等待15秒'.format(date.strftime('%Y%m%d')))
            time.sleep(15)


    if not main_df.empty:
        main_df.to_sql('daily_price', connection, if_exists='append')
        
        return main_df

#%%

def update_historical_data(start_date, end_date, daily_target, connection, api):

    tw_calendar = get_calendar('XTAI')

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)


    for date in pd.date_range(start, end):

        if date not in tw_calendar.opens:
            continue
        
        elif (
            date == datetime.datetime.now().date() and
            datetime.datetime.now().hour < 15
        ):
            break

    print('正在更新每日收盤行情...')

    update_daily_prices(tw_calendar.previous_close(start).date(), end, connection)

    print('正在更新逐筆交易...')
    
    update_ticks(connection, api, daily_targets)

    print('股市資料更新完成')

#%%
def get_MA(kbars):
    import talib  

    kbars['ma5'] = talib.MA(kbars.close, 5) 
    kbars['ma10'] = talib.MA(kbars.close, 10)
    kbars['ma20'] = talib.MA(kbars.close, 20)  

    return kbars

#%%
