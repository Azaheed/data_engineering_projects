import pandas as pd
from pymongo import MongoClient
from collections import defaultdict
import random
from mongo_connection import MakeConnection
from tqdm import tqdm

input_file = "archive/movie_data.csv"
output_file = (f"{input_file}_large_file_half.csv")

chunk_size = 100_000  # adjust depending on RAM
batch_size = 10000

first_chunk = True

def chunking(input_file,output_file): 

    for chunk in pd.read_csv(input_file, chunksize=chunk_size):
        # Randomly keep ~50% of rows in this chunk
        chunk = chunk.sample(frac=0.5)

        chunk.to_csv(
            output_file,
            mode="w" if first_chunk else "a",
            header=first_chunk,
            index=False
        )

        first_chunk = False

def make_dict(df):
    # Group reviews by movie
    movies = defaultdict(lambda: {
        "movie_id": None,
        "reviews": []
    })
    c = 0
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing rows"):
        movie = row["movie_id"]
        
        if movies[movie]["movie_id"] is None:
            movies[movie]["movie_id"] = movie
        #print(movies)
        movies[movie]["attributes"].append({
            "user_id": row["user_id"],
            "rating": row["rating_val"]
        }) 
        # c +=1 
        # if c == 1000000:
        #     #print(f"Processed {c} rows")
        #     return list(movies.values())    
    return list(movies.values())    

# def process_batch(batch_df):
#     # Replace this with your actual processing
#     print(f"Processing batch with {len(batch_df)} rows")
#     batch_df['new_col'] = batch_df['some_col'] * 2  # example operation
#     return batch_df   

def main():
    import os

    if os.path.exists(output_file):
        print(f"{output_file} exists!")
    else:
        print(f"{output_file} does not exist.")
        if len(pd.read_csv(input_file)) > 500_000:
            chunking(input_file,output_file)
            df = pd.read_csv('large_file_half.csv')
        else:
            df = pd.read_csv(input_file)
            df.head()

    new_list=make_dict(df)
    mong = MakeConnection()

    total_docs = len(new_list)

    with tqdm(total=total_docs, desc="Uploading to MongoDB", unit="docs") as pbar:
        for start in range(0, total_docs, batch_size):
            end = start + batch_size
            batch = new_list[start:end]

            mong.collection.insert_many(batch)
            print("Upload complete for lines",start,' to ', end)
            pbar.update(len(batch)) 

    # for start in range(0, len(new_list), batch_size):
    #     end = start + batch_size
    #     batch = new_list[start:end]
    #     #print("Processing batch:", batch)
    #     mong.collection.insert_many(batch)
    #     print("Upload complete for lines",start,' to ', end)

    
if __name__ == "__main__":
    main()

