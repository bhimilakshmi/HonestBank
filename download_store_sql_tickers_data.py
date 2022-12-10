import numpy as np
import datetime
import pandas as pd
from pandas_datareader import data as pdr
import os
import yfinance as yf
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket
import pymysql
import pathlib
import glob


def download_stock_data(ticker,start,end):
    df = yf.download(tickers=ticker, start=start, end=end, period='1d', interval='1d')
    return df


def geteSqlConnection():
    # Connect to the database
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='12345678',
                                 db='honest_bank')
    return connection
def store_data_sql(connection,data,threadsleep):
    # create cursor
    cursor = connection.cursor()
    # creating column list for insertion
    cols = "`,`".join([str(i) for i in data.columns.tolist()])
    print(cols)
    # Insert DataFrame recrds one by one.
    for i, row in data.iterrows():
        sql = "INSERT INTO `tickers` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        time.sleep(threadsleep)
        print(tuple(row))
        cursor.execute(sql, tuple(row))
        # the connection is not autocommitted by default, so we must commit to save our changes
        connection.commit()
def load_ticker_list(path):
    return pd.read_csv(path)
def load_data_sql(tickers_df,connection, start_date,end_date,threadsleep):
    for i, row in tickers_df.iterrows():
        print(row['Ticker'],row['Name'])
        ticker_id = row['Ticker']
        data = download_stock_data(ticker_id, start_date, end_date)
        data['ticker'] = ticker_id
        data['ticker_name'] = row['Name']
        store_data_sql(connection,data,threadsleep)
def load_csv_data_sql(tickers_df,connection,threadsleep):
    for i, row in tickers_df.iterrows():
        store_data_sql(connection,tickers_df,threadsleep)

def load_data_csv(tickers_df, start_date,end_date,path):
    for i, row in tickers_df.iterrows():
        print(row['Ticker'],row['Name'])
        ticker_id = row['Ticker']
        data = download_stock_data(ticker_id, start_date, end_date)
        data['ticker'] = ticker_id
        data['ticker_name'] = row['Name']
        if not os.path.isdir(path):
            os.makedirs(path)
        data.to_csv(path+"\\"+ticker_id+".csv")
def read_all_csv_files(path):
    df = pd.concat(map(pd.read_csv, glob.glob(path+'//'+'*.csv')))
    return df
def cleanup_existing_data(dir):
    files = glob.glob(dir+'//*')
    for f in files:
        os.remove(f)
def add_week_days(date):
    d = datetime.timedelta(weeks=1)
    t = date + d
    return t

if __name__ == '__main__':
    tmp = 'test'
    tickers_df = load_ticker_list('tickers_list.csv')
    connection = geteSqlConnection()
    current_date = datetime.datetime.today().strftime('%Y-%m-%d')
    current_date = datetime.datetime.strptime(current_date,'%Y-%m-%d')
    start_date = datetime.datetime.strptime('2017-01-07','%Y-%m-%d')
    end_date = add_week_days(start_date)
    print(type(start_date))
    print(type(end_date))
    while end_date < current_date:
        cleanup_existing_data(tmp)
        load_data_csv(tickers_df, start_date, end_date, 'test')
        csv_df = read_all_csv_files(tmp)
        tickers_df = csv_df.drop_duplicates()
        load_csv_data_sql(tickers_df,connection,0)
        start_date = end_date
        end_date = add_week_days(start_date)






