from utils.Connection import create_connection
import pandas as pd

def ingest_data(chunk_size=1000):
    engine = create_connection()
    

    df = pd.read_excel('data/Online Retail.xlsx')
    
    # Convert all column names to lowercase prevent the "table_name" issue
    df.columns = df.columns.str.lower()
    
 
    num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)
    
 
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(df))
        chunk = df[start_idx:end_idx]
        
        # For first chunk, Create the table
        if i == 0:
            chunk.to_sql('retails', engine, index=False, if_exists='replace', method='multi')
        # For subsequent chunks, append to the table
        else:
            chunk.to_sql('retails', engine, index=False, if_exists='append', method='multi')
        
        print(f"Chunk {i+1}/{num_chunks} processed")

ingest_data()