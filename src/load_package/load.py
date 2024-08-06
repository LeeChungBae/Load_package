# load.py
import requests
import os
import pandas as pd

# 데이터 추가 전처리
def best():
    df=pd.read_parquet("~/code/team_repo/transform_path")
    col=['rankInten','salesInten','salesChange']
    for i in col:
        df[i]=pd.to_numeric(df[i])

    df['openDt'] = pd.to_datetime(df['openDt'],errors='coerce')
    df['openDt'] = df['openDt'].dt.strftime('%Y%m%d')
    df['load_dt'] = df['load_dt'].astype(str)
    
    grouped = df.groupby('movieNm').agg({
        'audiCnt': 'sum',
        'openDt': 'max',
        'load_dt' :'max'
    }).reset_index()

    grsort=grouped.sort_values('audiCnt',ascending=False)
    grsort

def special_dat():
    day=['20230101','20230121','20230122','20230123','20230124','20230301','20230505','20230527','20230529','20230606','20230815','20230928','20230929','20230930','20231003','20231009','20231225']
    special_day = df[df['load_dt'].isin(day)]
    special_day.groupby('movieNm').agg({
        'rank' : 'max',
        'rankInten' : 'max',
        'audiCnt': 'sum',
        'openDt': 'max',
        'load_dt' :'max'
    }).sort_values(['load_dt','audiCnt'], ascending = False)
    
