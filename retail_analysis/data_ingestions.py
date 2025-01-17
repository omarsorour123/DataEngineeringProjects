from utils.Connection import create_connection
import pandas as pd


def ingest_data(chunk_size=1000):
    engine = create_connection()
    
    # Read the entire Excel file
    df = pd.read_excel('data/Online Retail.xlsx')
    
    # Calculate number of chunks
    num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)
    
    # Process each chunk
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(df))
        chunk = df[start_idx:end_idx]
        
        # For first chunk, replace the table
        if i == 0:
            chunk.to_sql('retails', engine, index=False, if_exists='replace', method='multi')
        # For subsequent chunks, append to the table
        else:
            chunk.to_sql('retails', engine, index=False, if_exists='append', method='multi')
        
        print(f"Chunk {i+1}/{num_chunks} processed")


ingest_data()