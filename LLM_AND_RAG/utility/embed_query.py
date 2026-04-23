from sentence_transformers import SentenceTransformer
model = SentenceTransformer("BAAI/bge-base-en-v1.5")
#from qdrant_client import QdrantClient

#qdrant_client = QdrantClient(url="http://localhost:6333") 

def get_embedding(query):
    query_vector = model.encode(query).tolist()
    return query_vector


# embedding = get_embedding('The Godfather directed by francis ford coppolla which stars Al Pacino and Marlon Brando released date 1972-03-24 ')
# results = qdrant_client.query_points(
#     collection_name="movies",
#     query=embedding,
#     limit=5
# )


# for res in results.points:
#     print(res.payload["movie_id"], res.score)