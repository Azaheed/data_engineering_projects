import pandas as pd
from pymongo import MongoClient
from collections import defaultdict
import random
from mongo_connection import MakeConnection
from tqdm import tqdm
import os
import shutil


input_file_path = ["archive/movie_data.csv","archive/ratings_export.csv","archive/users_export.csv"]
input_file_name = ["movie_data","ratings_export","users_export"]
path_for_df = []

chunk_size = 100_000  # adjust depending on RAM
batch_size = 10000
destination = []


def chunking(input_file,output_file): 
    first_chunk = True
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

def make_docs_useable():
    c = 0 
    for x in input_file_path:
        output_file = (f"{input_file_name[c]}_large_file_half.csv")
        #destination.append() = (f"archive/transformed/{output_file}")
        destination.append(f"archive/transformed/{output_file}") 
        if os.path.exists(destination[c]):
            print('files already exists')
            c += 1
            pass
        else: 
            if len(pd.read_csv(x)) > 500_000:
                chunking(x,output_file)
                # Make sure destination folder exists
                os.makedirs(os.path.dirname(destination[c]), exist_ok=True)
                # If file already exists at destination, remove it first
                if os.path.exists(destination[c]):
                    os.remove(destination[c])
                # Move the file (cut & paste)
                shutil.move(output_file, destination[c])
                print("File pasted successfully!")
            else:
                # Make sure destination folder exists
                os.makedirs(os.path.dirname(destination[c]), exist_ok=True)
                # If file already exists at destination, remove it first
                if os.path.exists(destination[c]):
                    os.remove(destination[c])
                # Move the file (cut & paste)
                shutil.copy(x, destination[c])
                print("File pasted successfully!")
            c += 1

def unify(dfs):
    grouped_reviews = (
        dfs['ratings_export_large_file_half']
        .groupby("movie_id")
        .apply(lambda x: x[["user_id", "rating_val"]].to_dict("records"))
        .reset_index(name="reviews")
    )
    merged_df = dfs['movie_data_large_file_half'].merge(grouped_reviews, on="movie_id", how="left")
    print(merged_df.head())
    # Save to CSV
    merged_df.to_csv("archive/transformed/merged_data.csv", index=False)

def make_dict(df):
    # Group reviews by movie
    movies = defaultdict(lambda: {
        "movie_id": None,
        "attributes": []
    })
    c = 0
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing rows"):
        movie = row["movie_id"]
        if 1==1:#row["movie_title"] in ["The Shawshank Redemption", "Pulp Fiction"]:
            if movies[movie]["movie_id"] is None:
                movies[movie]["movie_id"] = movie
            #print(movies)
            movies[movie]["attributes"].append({
                "movie_title": row["movie_title"],
                "genre": row["≈"],
                "imdb_id": row["imdb_id"],
                "overview": row["overview"],
                "popularity": row["popularity"],
                "production_countries": row["production_countries"],
                "release_date": row["release_date"],
                "runtime": row["runtime"],
                "spoken_languages": row["spoken_languages"],
                "vote_average": row["vote_average"],
                "vote_count": row["vote_count"],
                "year_released": row["year_released"],
                "reviews": row["reviews"]
            }) 
        else:
            continue 
    return list(movies.values())  

def main():
    make_docs_useable()
    dfs={}
    for x,dest in enumerate(destination):
        name = os.path.splitext(os.path.basename(dest))[0]
        dfs[name] = pd.read_csv(dest)
    if os.path.exists("archive/transformed/merged_data.csv"):
        pass
    else:
        unify(dfs)
    df = pd.read_csv("archive/transformed/merged_data.csv")
    new_list = make_dict(df)
    mong = MakeConnection()

    total_docs = len(new_list)

    with tqdm(total=total_docs, desc="Uploading to MongoDB", unit="docs") as pbar:
        for start in range(0, total_docs, batch_size):
            end = start + batch_size
            batch = new_list[start:end]

            mong.collection.insert_many(batch)
            tqdm.write(f"Upload complete for lines {start} to {end}")
            pbar.update(len(batch)) 

if __name__ == "__main__":
    main()

