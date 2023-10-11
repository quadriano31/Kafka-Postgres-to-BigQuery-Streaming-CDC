import pandas as pd
from sqlalchemy import create_engine
import time
import random

def load_data_to_postgres(batch_size=50):
    try:
        engine = create_engine('postgresql://postgres:postgres@localhost:5432/spotify_db')
        # Connect to the DB engine
        engine.connect()
        
        while True:
            # Generate a random number between 10 and 100 for the number of rows to load in each batch
            num_rows = random.randint(10, 100)
            
            # Read a random number of rows from the CSV file
            df = pd.read_csv('data\spotify.csv', nrows=num_rows)
            
            # Drop some NAN columns
            df.drop(['Unnamed: 0', 'title'], axis=1, inplace=True)
            
            # Remove duplicates based on a unique identifier column (e.g., 'id')
            df.drop_duplicates(subset='id', keep='first', inplace=True)
            
            if not df.empty:
                # Load the data to Postgres
                df.to_sql(name='spotify_streaming', con=engine, index=False, if_exists='append')
                print(f"Loaded {len(df)} rows into the database.")
            
            # Generate a random waiting time between 5 seconds and 5 minutes
            wait_time = random.uniform(5, 60)
            print(f"Waiting for {wait_time} seconds...")
            time.sleep(wait_time)
        
    except Exception as e:
        print(e)

if __name__ == "__main__":
    load_data_to_postgres()
