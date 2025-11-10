import os
from typing import List, Tuple, Dict, Any
#from langchain.vectorstores import Chroma
from langchain_chroma import Chroma
#from langchain.docstore.document import Document
from langchain_core.documents import Document
from news_search.vector_database import VectorDatabase
from datetime import datetime

class ChromaVectorEngine(VectorDatabase):
    def __init__(self, embedding, persist_directory: str = "./chroma_db", sqlite_path: str = "mydb.sqlite"):
        self.embedding = embedding
        self.persist_directory = persist_directory
        self.index = Chroma(
            collection_name='news_collection',
            embedding_function=self.embedding,
            persist_directory=persist_directory
        )

    def insert_embeddings_bulk(self, chunks_with_vectors: List[Tuple[List[float], Dict]]):
        documents = [
            Document(page_content=chunk["chunk_text"], metadata=chunk)
            for chunk in chunks_with_vectors
        ]
        self.index.add_documents(documents)
        #self.index.persist()


    def retrieve_related_documents(self, query_vector, k=10, date_from=None, date_to=None, score_threshold=0.25):
        candidate_chunks = self.index.similarity_search_with_relevance_scores(query_vector, k*5)

        id_seen = set()
        articles = []

        for doc, score in candidate_chunks:
            metadata = doc.metadata
            article_id = metadata.get("url")
            
            if not article_id or article_id in id_seen or score < score_threshold:
                continue

            if date_from and date_to:
                pub_date_str = metadata.get("fecha_publicacion")
                pub_date = datetime.strptime(pub_date_str, "%Y-%m-%d").date()
                if pub_date < date_from or pub_date > date_to:
                    continue

            id_seen.add(article_id)
            articles.append(metadata)

            if len(articles) >= k:
                break

        return articles