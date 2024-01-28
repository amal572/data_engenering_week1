#!/usr/bin/env python
# coding: utf-8



import pandas as pd

import os

from time import time

import argparse

from sqlalchemy import create_engine, text


def main(params):
        
        user = params.user
        Password = params.Password
        host = params.host
        port = params.port
        db = params.db
        table_name = params.table_name
        url =params.url

        csv_name = 'output.csv'

        print(pd.__version__)

        print(url)

        # download the csv 
        os.system(f"wget {url} -O {csv_name}")
        print(f'postgresql://{user}:{Password}@{host}:{port}/{db}')

        engine = create_engine(f'postgresql://{user}:{Password}@{host}:{port}/{db}')
        #postgresql://root:root@localhost:5432/ny_taxi
        #engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

        df_iter = pd.read_csv(csv_name,compression='gzip', iterator= True ,chunksize = 100000)

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.head(n=0).to_sql(name=table_name,con = engine,if_exists = 'replace')
        print(table_name)
        df.to_sql(name=table_name,con = engine ,if_exists = 'append')

        while True:
            t_start = time()
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name,con = engine,if_exists = 'append')

            t_end = time()
            print('insert another chunk , took %.3f second' % (t_end - t_start))
            break

if __name__ == '__main__':

        parser = argparse.ArgumentParser(description='Integers CSV data to Postgres')

        #  user, 
        #  Password,
        #  host,
        #  port,
        #  database name, 
        #  table name,
        #  url of the csv

        parser.add_argument('--user',help='user name for Postgres')
        parser.add_argument('--Password',help='Password for Postgres')
        parser.add_argument('--host',help='host for Postgres')
        parser.add_argument('--port',help='port for Postgres')
        parser.add_argument('--db',help='database name for Postgres')
        parser.add_argument('--table_name',help='name of the table where we will write the result to')
        parser.add_argument('--url',help='url of the csv file')

        args = parser.parse_args()

        main(args)





