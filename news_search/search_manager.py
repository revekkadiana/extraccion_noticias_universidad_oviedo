#from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_text_splitters import RecursiveCharacterTextSplitter
import re

class SearchManager:
    def __init__(self, embedding_model, vector_engine, chunk_size=1000, chunk_overlap=200):
    #def __init__(self, embedding_model, vector_engine, chunk_size=500, chunk_overlap=100):
        self.embedding_model = embedding_model
        self.vector_engine = vector_engine
        self.text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size,
                                                            chunk_overlap=chunk_overlap,
                                                            separators=["\n\n", "\n", " "])

    def __clean_text(self, text):
        # Remover URLs
        text = re.sub(r'http\S+', '', text)
        # Remover emails
        text = re.sub(r'\S+@\S+', '', text)
        # Remover tags HTML
        text = re.sub(r'<[^>]+>', ' ', text)
        # Colapsar varios espacios en uno sin considerar saltos de línea
        text = re.sub(r'[^\S\n]+', ' ', text)
        # Quitar los espacios en blanco iniciales y finales
        text = text.strip()
        # Convertir a minúsculas
        text = text.lower()
        return text

    def ingest_article(self, content, metadata):
        content = self.__clean_text(content)
        chunks = self.text_splitter.split_text(content)
        chunks_with_vectors = [
            {**metadata, "chunk_text": chunk}
            for chunk in chunks
        ]
        self.vector_engine.insert_embeddings_bulk(chunks_with_vectors)

    def retrieve_related_news(self, query, k=10, date_from=None, date_to=None, score_threshold=0.25):
        query = self.__clean_text(query)
        return self.vector_engine.retrieve_related_documents(query, k=k, date_from=date_from, date_to=date_to, score_threshold=score_threshold)
