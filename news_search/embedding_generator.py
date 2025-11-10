from sentence_transformers import SentenceTransformer

class EmbeddingGenerator:
    def __init__(self, model_name = 'sentence-transformers/distiluse-base-multilingual-cased-v2'):
        self.model = SentenceTransformer(model_name)
        self.embedding_size = self.model.get_sentence_embedding_dimension()

    def generate_embedding(self, text):
        return self.model.encode(text).tolist()

    def get_embedding_size(self):
        return self.embedding_size

    def embed_query(self, text):
        return self.generate_embedding(text)

    def embed_documents(self, texts):
        # texts es una lista de strings
        embeddings = self.model.encode(texts, show_progress_bar=False)
        return [embedding.tolist() for embedding in embeddings]
