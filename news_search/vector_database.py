from abc import ABC, abstractmethod

class VectorDatabase(ABC):
    @abstractmethod
    def insert_embeddings_bulk(self, list_of_vectors_and_metadata):
        pass

    @abstractmethod
    def retrieve_related_documents(self, query_vector, k=10, date_from=None, date_to=None):
        pass
