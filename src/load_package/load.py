# load.py
import pandas as pd

# 데이터 추가 전처리
import pandas as pd
df=pd.read_parquet("~/code/team_repo/transform_path")
col=['rankInten','salesInten','salesChange']
for i in col:
    df[i]=pd.to_numeric(df[i])

    df['openDt'] = pd.to_datetime(df['openDt'])
    df['openDt'] = df['openDt'].dt.strftime('%Y%m%d')
    df['load_dt'] = df['load_dt'].astype(str)
    
grouped = df.groupby('movieNm').agg({
    'audiCnt': 'sum',
    'openDt': 'max',
    'load_dt' :'max'
    }).reset_index()
grouped.sort_values('audiCnt',ascending=False)
