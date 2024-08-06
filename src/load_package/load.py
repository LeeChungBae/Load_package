# load.py
import requests
import os
import pandas as pd

# 데이터 추가 전처리
def col_type(load_dt,path):
    #read data from transform / 경로 monthly 추가 변경 
    df=pd.read_parquet(f"{path}/load_dt={load_dt}")
    print("flag0: start data")
    df.info()
    #change data type and format
    col=['rankInten','salesInten','salesChange']
    for i in col:
        df[i]=pd.to_numeric(df[i])
    df['openDt'] = pd.to_datetime(df['openDt'])
    df['openDt'] = df['openDt'].dt.strftime('%Y%m%d')
    df['load_dt'] = df['load_dt'].astype(str)
    
    print("flag1 : 데이터 타입, 형식  변했는지 확인:")
    print(df.info())
    print(df['load_dt'].head(3)) 
    # 달 박스오피스 1위 출력 
    grouped = df.groupby('movieNm').agg({
    'audiCnt': 'sum',
    'openDt': 'max',
    'load_dt' :'max'
    }).reset_index()
    grouped.sort_values('audiCnt',ascending=False) 
