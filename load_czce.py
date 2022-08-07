from typing import Type
import pandas as pd
import numpy as np

def czce_data():
  
    dates = ['2015.txt', '2016.txt', '2017.txt', '2018.txt', '2019.txt', '2020.txt', '2021.txt']
    df_list = []
    for date in dates:
        df=pd.read_csv(date, sep='|', low_memory=False)
        df.reset_index(inplace=True)
        df.columns = df.iloc[0]
        df.drop(0, inplace=True)
        df=df.iloc[:, :-1]
        try:
            df.drop(columns=['DeliverySettlementPrice'], inplace=True)
        except KeyError:
            pass
        df.columns = ['Trading Day',
                        'Contract Code',
                        'Prev.Settle',
                        'Open     ',
                        'High     ',
                        'Low      ',
                        'Close    ',
                        'Settlement',
                        'Change1   ',
                        'Change2   ',
                        'Volume    ',
                        'OpenInterest',
                        'OI Change',
                        'Turnover     ']
        df.columns = df.columns.str.rstrip()
        df_list.append(df)
    df_czce = pd.concat(df_list)
    df_czce.set_index('Trading Day', inplace=True)
    df_czce.index = pd.to_datetime(df_czce.index).to_period('D')
    df_czce = df_czce.applymap(lambda x: x.rstrip() if isinstance(x,str) else x)
    df_czce.iloc[:,1:] = df_czce.iloc[:,1:].applymap(lambda x: float(x.replace(',','')))
    df_czce[['Sym', 'Contract']] = df_czce['Contract Code'].str.extract('(?P<Sym>^[\w]{2})(?P<Contract>[\d]*)')
    df_czce.Contract = df_czce.Contract.astype(int)
    df_czce.drop(columns=['Contract Code'], inplace=True)
    df_czce.reset_index(inplace=True)
    df_czce.set_index(['Sym', 'Trading Day', 'Contract'], inplace=True)
    df_czce.sort_index(inplace=True)
    
    return df_czce


def get_contact(czce,symbol, period_i, period_e, contract_num):
  
    try:
        dfsym = czce.loc[symbol].loc[period_i:period_e]
    except TypeError:
        dfsym = czce.loc[symbol].loc[period_i:]
    dates = set(np.array(list([x[0],x[1]] for x in dfsym.index.values))[:,0])
    close_val={}
    for date in dates:
        close_val[date] = dfsym.loc[date].iloc[contract_num-1].Close
    df = pd.DataFrame(close_val, index=[0]).T
    df.columns = [symbol]
    df.sort_index(inplace=True, ascending=True)
    df = df[df[symbol] != 0]
    
    return df
