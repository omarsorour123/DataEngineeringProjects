import pandas as pd


def transform_clean_data(raw_data):
    clean_data = raw_data.dropna()
    
    clean_data.columns = clean_data.columns.str.lower()
    
    clean_data['store'] = clean_data['store'].astype(int)
    clean_data['holiday_flag'] = clean_data['holiday_flag'].astype(int)
    
    clean_data['date'] = pd.to_datetime(clean_data['date'],format='mixed')
    clean_data['month'] = clean_data['date'].dt.month
    clean_data['year'] = clean_data['date'].dt.year
   
    
    


    return clean_data

def transform_agg_monthly_sales(clean_data):
    selected_columns = ['month','year','weekly_sales']
    agg_data_initial = clean_data.loc[:,selected_columns]

    agg_data = (
        agg_data_initial
        .groupby(by=['year','month'],as_index=False)
        .agg({'weekly_sales':'sum'})
        )
    
    agg_data.rename(columns={"weekly_Sales":"month_Sales"},inplace=True)

    return agg_data

    
    
