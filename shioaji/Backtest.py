
import pandas as pd
#%%
def ticks_to_kbars(ticks, interval = '1Min'):
    import pandas as pd

    kbars = pd.DataFrame()

    kbars['open'] = ticks['close'].resample(interval, closed='right', label = 'left').first()
    kbars['close'] = ticks['close'].resample(interval, closed='right', label = 'left').last()
    kbars['high'] = ticks['close'].resample(interval, closed='right', label = 'left').max()
    kbars['low'] = ticks['close'].resample(interval, closed='right', label = 'left').min()
    kbars['volume'] = ticks['volume'].resample(interval, closed='right', label = 'left').sum()

    kbars.dropna(inplace=True)

    return kbars

#%%
def day_trading_backtest(code, date, connection, api):
    import pandas as pd

    ticks = get_ticks(code, date, connection, api)[0]
    kbars = ticks_to_kbars(ticks)

    entry_time = None
    entry_price = 0

    exit_time = None
    exit_price = 0

    position = 0

    for ts in range(len(kbars)):

        current_time = kbars.iloc[ts].name + pd.Timedelta(minutes=1)
        current_price = kbars['close'][ts]

        if (
            current_time == date.replace(hour=9, minute=30, second=0) and
            position == 0                                    
        ):
            
            position = 1
            entry_time = current_time.time()
            entry_price = current_price

            print( '[{}] buy {} at {}'.format(current_time, code, current_price))

        elif (
            current_time == date.replace(hour=13, minute=0, second=0) and 
            position != 0
        ):
            exit_time = current_time.time()
            exit_price = current_price

            print( '[{}] sell {} at {}'.format(current_time, code, current_price))
            
            break

    if entry_time and exit_time:
        transaction = pd.DataFrame([[date,
                                     code,
                                     entry_time,
                                     entry_price,
                                     position * 1000,
                                     entry_price * position * 1000,
                                     exit_time,
                                     exit_price,
                                     position * 1000,
                                     exit_price * position * 1000]                                 
                                    ],
                                    columns = [
                                        '成交日期',
                                        '股票代號',
                                        '買進時間',
                                        '買進價格',
                                        '買進股數',
                                        '買進金額',
                                        '賣出時間',
                                        '賣出價格',
                                        '賣出股數',
                                        '賣出金額'])
        return transaction
    
    else:
        return pd.DataFrame()
    
#Technical Indicators
    
#%%
def get_technical_indicator(kbars):
    import pandas as pd
    import talib
    
    kbars['rsi'] = talib.RSI(kbars.close, timeperiod=14)

    macd, macdsignal, macdchhist = talib.MACD(kbars.close, fastperiod= 12, slowperiod=26, signalperiod=9)

    kbars['macd'] = macd
    kbars['macdsignal'] = macdsignal
    kbars['macdhist'] = macdchhist

    return kbars

#%%
def day_trading_backtest_RSI(code, date, connection, api):
    import pandas as pd

    tw_calendar = get_calendar('XTAI')
    prev_trading_date = tw_calendar.previous_close(date).date()

    ticks = pd.concat([get_ticks(code, prev_trading_date, connection, api)[0],get_ticks(code, date, connection, api)[0]])
    kbars = ticks_to_kbars(ticks)
    kbars = get_technical_indicator(kbars)
    kbars = kbars[date:]

    entry_time = None
    entry_price = 0

    exit_time = None
    exit_price = 0

    position = 0

    for ts in range(len(kbars)):

        current_time = kbars.iloc[ts].name + pd.Timedelta(minutes=1)
        current_price = kbars['close'][ts]

        if (
            kbars.iloc[ts]['rsi'] < 20 and
            position == 0                                    
        ):
            
            position = 1
            entry_time = current_time.time()
            entry_price = current_price

            print( '[{}] buy {} at {}'.format(current_time, code, current_price))

        elif (
            kbars.iloc[ts]['rsi'] > 80 and 
            position != 0
        ):
            exit_time = current_time.time()
            exit_price = current_price

            print( '[{}] sell {} at {}'.format(current_time, code, current_price))

            break
            
        elif (
            current_time == date.replace(hour=13, minute=0, second=0) and 
            position != 0
        ):
            exit_time = current_time.time()
            exit_price = current_price

            print( '[{}] sell {} at {}'.format(current_time, code, current_price))
            
            break

    if entry_time and exit_time:
        transaction = pd.DataFrame([[date,
                                     code,
                                     entry_time,
                                     entry_price,
                                     position * 1000,
                                     entry_price * position * 1000,
                                     exit_time,
                                     exit_price,
                                     position * 1000,
                                     exit_price * position * 1000]                                 
                                    ],
                                    columns = [
                                        '成交日期',
                                        '股票代號',
                                        '買進時間',
                                        '買進價格',
                                        '買進股數',
                                        '買進金額',
                                        '賣出時間',
                                        '賣出價格',
                                        '賣出股數',
                                        '賣出金額'])
        return transaction
    
    else:
        return pd.DataFrame()

#%%

def day_trading_backtest_Multi(code, date, connection, api):
    import pandas as pd

    tw_calendar = get_calendar('XTAI')
    #若求單日則前14mins會無法計算rsi
    prev_trading_date = tw_calendar.previous_close(date).date()

    ticks = pd.concat([get_ticks(code, prev_trading_date, connection, api)[0],get_ticks(code, date, connection, api)[0]])
    kbars = ticks_to_kbars(ticks)
    kbars = get_technical_indicator(kbars)
    kbars = kbars[date:]

    entry_time = None
    entry_price = 0

    exit_time = None
    exit_price = 0

    position = 0

    for ts in range(len(kbars)):

        current_time = kbars.iloc[ts].name + pd.Timedelta(minutes=1)
        current_price = kbars['close'][ts]

        if (
            kbars.iloc[ts-3 : ts]['rsi'].min() < 30 and
            kbars.iloc[ts-1]['macdhist'] < 0 and 
            kbars.iloc[ts]['macdhist'] > 0 and 
            position == 0                                    
        ):
            
            position = 1
            entry_time = current_time.time()
            entry_price = current_price

            print( '[{}] buy {} at {}'.format(current_time, code, current_price))

        elif (
            kbars.iloc[ ts-3 :ts]['rsi'].max() > 70 and
            kbars.iloc[ts-1]['macdhist'] > 0 and 
            kbars.iloc[ts]['macdhist'] < 0 and 
            position != 0
        ):
            exit_time = current_time.time()
            exit_price = current_price

            print( '[{}] sell {} at {}'.format(current_time, code, current_price))

            break
            
        elif (
            current_time == date.replace(hour=13, minute=0, second=0) and 
            position != 0
        ):
            exit_time = current_time.time()
            exit_price = current_price

            print( '[{}] sell {} at {}'.format(current_time, code, current_price))
            
            break

    if entry_time and exit_time:
        transaction = pd.DataFrame([[date,
                                     code,
                                     entry_time,
                                     entry_price,
                                     position * 1000,
                                     entry_price * position * 1000,
                                     exit_time,
                                     exit_price,
                                     position * 1000,
                                     exit_price * position * 1000]                                 
                                    ],
                                    columns = [
                                        '成交日期',
                                        '股票代號',
                                        '買進時間',
                                        '買進價格',
                                        '買進股數',
                                        '買進金額',
                                        '賣出時間',
                                        '賣出價格',
                                        '賣出股數',
                                        '賣出金額'])
        return transaction
    
    else:
        return pd.DataFrame()

#Resistance Line  
#%%
def day_trading_backtest_RL(code, date, connection, api):
    import pandas as pd

    tw_calendar = get_calendar('XTAI')
    prev_trading_date = tw_calendar.previous_close(date).date()

    ticks = pd.concat([get_ticks(code, prev_trading_date, connection, api)[0],get_ticks(code, date, connection, api)[0]])
    kbars = ticks_to_kbars(ticks)
    kbars = kbars[date:]

    entry_time = None
    entry_price = 0

    exit_time = None
    exit_price = 0

    position = 0
    volume_today = 0

    for ts in range(len(kbars)):

        current_time = kbars.iloc[ts].name + pd.Timedelta(minutes=1)
        current_price = kbars['close'][ts]
        volume_today += kbars['volume'][ts]

        if (
            current_time >= date.replace(hour=9, minute=15, second=0) and 
            current_time <= date.replace(hour=9, minute=30, second=0) and
            position == 0                                  
        ):
            high_15m = kbars[:date.replace(hour=9, minute=14, second=0)]['high'].max()
            low_15m = kbars[:date.replace(hour=9, minute=14, second=0)]['low'].min()

            if (
                current_price > high_15m and
                volume_today > 2000
            ):
                
                position = 1
                entry_time = current_time.time()
                entry_price = current_price

                target_price = current_price * 1.03
                stop_price = low_15m

                print( '[{}] buy {} at {}'.format(current_time, code, current_price))

        elif (
            current_time >= date.replace(hour=9, minute=15, second=0) and 
            current_time < date.replace(hour=13, minute=0, second=0) and 
            position != 0
        ):
            
            if (
                current_price > target_price or
                current_price < stop_price
            ):
                
                exit_time = current_time.time()
                exit_price = current_price

                print( '[{}] sell {} at {}'.format(current_time, code, current_price))

                break
            
        elif (
            current_time == date.replace(hour=13, minute=0, second=0) and 
            position != 0
        ):
            exit_time = current_time.time()
            exit_price = current_price

            print( '[{}] sell {} at {}'.format(current_time, code, current_price))
            
            break

    if entry_time and exit_time:
        transaction = pd.DataFrame([[date,
                                     code,
                                     entry_time,
                                     entry_price,
                                     position * 1000,
                                     entry_price * position * 1000,
                                     exit_time,
                                     exit_price,
                                     position * 1000,
                                     exit_price * position * 1000]                                 
                                    ],
                                    columns = [
                                        '成交日期',
                                        '股票代號',
                                        '買進時間',
                                        '買進價格',
                                        '買進股數',
                                        '買進金額',
                                        '賣出時間',
                                        '賣出價格',
                                        '賣出股數',
                                        '賣出金額'])
        return transaction
    
    else:
        return pd.DataFrame()
    
#%%
def backtest(start_date, end_date, connection, api, discount = 0.38):
    import pandas as pd

    tw_calendar = get_calendar('XTAI')

    transactions = pd.DataFrame()

    for date in pd.date_range(start_date, end_date):

        if date not in tw_calendar.opens:
            continue
            
        codes = get_stocks(date, connection)
        day_trading_codes = [code for code in codes if get_stock(code, connection, api)[0].iloc[0]['day_trade'] == 'Yes']
        day_trading_codes = list(set(day_trading_codes))

        for code in day_trading_codes:

            transaction = day_trading_backtest_RSI(code, pd.to_datetime(date), connection, api)

            if not transaction.empty:
                transactions = pd.concat([transactions,transaction], sort = False)
    
    if not transactions.empty:
        transactions['買進手續費'] = transactions['買進金額'] * 0.001425 * discount
        transactions['買進手續費'] = transactions['買進手續費'].apply( lambda fee: fee if fee > 20 else 20)
        transactions['買進手續費'] = transactions['買進手續費'].astype(int)

        transactions['賣出手續費'] = transactions['賣出金額'] * 0.001425 * discount
        transactions['賣出手續費'] = transactions['賣出手續費'].apply( lambda fee: fee if fee > 20 else 20)
        transactions['賣出手續費'] = transactions['賣出手續費'].astype(int)

        transactions['交易稅'] = transactions['賣出金額'] * 0.0015
        transactions['交易稅'] = transactions['交易稅'].astype(int)

        transactions['損益'] = (transactions['賣出金額']-transactions['買進金額']) - (transactions['買進手續費'] + transactions['賣出手續費'] + transactions['交易稅'])

        transactions['成交日期'] = pd.to_datetime(transactions['成交日期']).dt.date
        transactions = transactions.set_index(['成交日期'])

    return transactions



