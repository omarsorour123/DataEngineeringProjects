import os
import pandas as pd


def extract(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError("No File Founded")   
    data = pd.read_csv(file_path)
    return data




raw_data = extract('../data/Walmart_Sales.csv')
