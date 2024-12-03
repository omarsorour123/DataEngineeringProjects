from sqlalchemy import create_engine
import pandas as pd
import configparser
config = configparser.ConfigParser()
config.read('config.ini')
DATABASE_URL = config['database']['uri']
engine = create_engine(DATABASE_URL)


def load_sales_data(clean_data):

    clean_data.to_sql('sales_data',engine,if_exists='append',index=False)

def load_agg_data(agg_data):

    agg_data.to_sql('yearly_monthly_sales',engine,if_exists='append',index=False)  









