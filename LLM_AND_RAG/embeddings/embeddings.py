from mongo_connection import MakeConnection
from sentence_transformers import SentenceTransformer
import numpy as np
from numpy.linalg import norm
from tqdm import tqdm  
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams
from qdrant_client.models import PointStruct, Distance

points = []
john = MakeConnection()
model = SentenceTransformer("BAAI/bge-base-en-v1.5")
documents = john.collection.find({}) 

stored_vectors = []
batch_size = 10  # or 1000 depending on your RAM
batch_docs = []
batch_texts = []

def document_to_text(doc):
    attr = doc.get("attributes", [{}])[0]  # Get first dictionary, default to empty
    return f"""
    Title: {attr.get("movie_title", "")}
    Description: {attr.get("overview", "")}
    Vote Average: {attr.get("vote_average", "")}
    Genre: {attr.get("genre", "")}
    Vote Count: {attr.get("vote_count", "")}
    Release Date: {attr.get("release_date", "")}
    Runtime: {attr.get("runtime", "")}
    Popularity: {attr.get("popularity", "")}
    Priority: {attr.get("priority", "")}
    """

def cosine_similarity(a, b):
    return np.dot(a, b) / (norm(a) * norm(b))

c = 0 

for i, doc in enumerate(tqdm(documents, desc="Processing documents")):
    text = document_to_text(doc)
    batch_docs.append(doc)
    batch_texts.append(text)

    if len(batch_docs) >= batch_size:
        
        vectors = model.encode(batch_texts, show_progress_bar=False)
        for d, vec, txt in zip(batch_docs, vectors, batch_texts):
            attr = d.get("attributes", [{}])[0] 
            #print(attr)
            stored_vectors.append({
                "mongo_id": str(d["_id"]),
                "movie_id": d["movie_id"],
                "vector": vec,
                "text": txt,
                "Rating": attr["vote_average"]
            })
        batch_docs = []
        batch_texts = []
    c += 1 
    if c > 10:
        break

# # Process remaining documents
# if batch_docs:
#     vectors = model.encode(batch_texts, show_progress_bar=True)
#     for d, vec, txt in zip(batch_docs, vectors, batch_texts):
#         stored_vectors.append({
#             "mongo_id": str(d["_id"]),
#             "movie_id": d["movie_id"],
#             "vector": vec,
#             "text": txt
#         })

client = QdrantClient(url="http://localhost:6333")

# Assuming your embeddings are 384-dimensional (all-MiniLM-L6-v2)
vector_dim = stored_vectors[0]['vector'].shape[0]

if client.collection_exists("movies_2"):
    client.delete_collection("movies_2")

client.create_collection(
    collection_name="movies_2",
    vectors_config=VectorParams(size=vector_dim, distance=Distance.COSINE)
)

for i, item in enumerate(tqdm(stored_vectors, desc="Processing vectors")):
    print(item["text"])
    points.append(
        PointStruct(
            id=int(i),  # unique integer ID
            vector=item['vector'].tolist(),
            payload={
                "mongo_id": item["mongo_id"],
                "movie_id": item["movie_id"],
                "text": item["text"],
                "rating average": item["Rating"]
            }
        )
    )

    # Upload in batches
    if len(points) >= batch_size or i == len(stored_vectors) - 1:
        client.upsert(
            collection_name="movies_2",
            points=points
        )
        points = []

def get_embeddings(query):
    query_vector = model.encode(query).tolist()

    results = client.query_points(
        collection_name="movies_2",
        query=query_vector,
        limit=5
    )
    return results

#query_vector = model.encode("Movies in the Thriller Genre With a Voting Average > 8.7 and runtime 154").tolist()

# results = client.query_points(
#     collection_name="movies",
#     query=query_vector,
#     limit=5
# )


# for res in results.points:
#     print(res.payload["movie_id"], res.score)