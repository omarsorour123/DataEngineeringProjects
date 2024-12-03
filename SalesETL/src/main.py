from extract import extract
from transfrom import transform_clean_data,transform_agg_monthly_sales
from load import load_agg_data,load_sales_data

file_path = '../data/Walmart_Sales.csv'

raw_data = extract(file_path)

clean_data = transform_clean_data(raw_data)

agg_data = transform_agg_monthly_sales(clean_data)

print(clean_data.dtypes)
load_sales_data(clean_data)
load_agg_data(agg_data)