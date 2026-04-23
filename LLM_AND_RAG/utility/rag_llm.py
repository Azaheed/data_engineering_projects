

# rag_groq_vector_only.py

from embed_query import get_embedding  # Your existing embeddings code
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue
from groq import Groq

# 1️⃣ Initialize Qdrant client (your vector DB)
qdrant_client = QdrantClient(url="http://localhost:6333")  # Or your Qdrant URL

# 2️⃣ Initialize Groq client
#groq_api_key = "YOUR_GROQ_API_KEY"
client = Groq(api_key=groq_ai_key)

# 3️⃣ Default answer if nothing relevant is found
DEFAULT_ANSWER = "I'm sorry, I couldn't find information related to that."

def query_vector_db(user_query: str, top_k: int = 3, similarity_threshold: float = 0.5):
    """
    Queries your vector DB for top-k most similar items.
    Returns a list of text/context or empty list if no match passes threshold.
    """
    embedding = get_embedding(user_query)
    results = qdrant_client.query_points(
        collection_name="movies_2",
        query=embedding,
        limit=top_k,
        with_payload=True
    )

    for res in results.points:
        print(res.payload["movie_id"], res.score)
    
    # Filter results by score threshold
    matched_texts = [
        hit.payload["text"]  # assuming your embeddings have 'text' field
        for hit in results.points
        if hit.score >= similarity_threshold
    ]
 #   print (matched_texts)
    return matched_texts

def ask_llm_with_context(query: str):
    # Retrieve relevant context from vector DB
    context_texts = query_vector_db(query)
    
    if not context_texts:
        # No good match found
        return DEFAULT_ANSWER
    
    # Combine retrieved context for LLM prompt
    context_for_prompt = "\n".join(context_texts)
    
    prompt = f"""
    You are an AI that only answers based on the provided context below.
    Do NOT hallucinate information. If the answer is not present, respond with:
    "{DEFAULT_ANSWER}"
    
    Context:
    {context_for_prompt}
    
    Question:
    {query}
    """
    
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}]
    )
    
    return response.choices[0].message.content.strip()


# ✅ Example usage
user_query = "Which movie has the highest rating?"
answer = ask_llm_with_context(user_query)
print(answer)






# from qdrant_client import QdrantClient

# client = QdrantClient(url="http://localhost:6333")

# print(client.get_collection("movies"))