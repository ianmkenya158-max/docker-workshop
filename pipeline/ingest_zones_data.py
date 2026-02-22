#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


def run():
    engine=create_engine('postgresql://admin:root@localhost:5432/ny_taxi')

    url='https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    df_zones_iter = pd.read_csv(url, iterator=True, chunksize=100)
    
    first=True
    for df in tqdm(df_zones_iter):
        if first:
            df.head(0).to_sql(name='zones', con=engine, if_exists='replace')
        first=False
        
        df.to_sql(name='zones', con=engine, if_exists='append')



if __name__ == '__main__':
    run()