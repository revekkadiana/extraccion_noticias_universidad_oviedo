import streamlit as st
from news_interface.authentication.auth import Authenticator
from news_database.interface_db import NewsInterfaceDatabase
from news_interface.interfaces import user_interface, admin_interface
from news_interface.sidebar import display_sidebar
import os
from news_search.embedding_generator import EmbeddingGenerator
from news_search.chroma_engine import ChromaVectorEngine
from news_search.search_manager import SearchManager


def initialize_paths():
    auth_config_path = os.path.join("news_interface", "authentication", "config.yaml")
    database_path = os.path.join("news_database", "news.db")
    vector_db_path = os.path.join("news_search", "chroma_db")
    return auth_config_path, database_path, vector_db_path

def initialize_search_manager(vector_db_path, database_path):
    model_name = 'intfloat/multilingual-e5-small'
    embedding_model = EmbeddingGenerator(model_name = model_name)
    vector_db = ChromaVectorEngine(embedding=embedding_model, persist_directory=vector_db_path, sqlite_path=database_path)
    search_manager = SearchManager(embedding_model=embedding_model, vector_engine=vector_db, chunk_size=500, chunk_overlap=100)
    return search_manager

def handle_interfaces(news_db, search_manager):
    if st.session_state['admin_logged_in']:
        admin_interface(news_db, search_manager)
    else:
        user_interface(news_db, search_manager)

def main():
    # Inicializa rutas
    AUTH_CONFIG_PATH, DATABASE_PATH, VECTOR_DB_PATH = initialize_paths()
    authenticator = Authenticator(AUTH_CONFIG_PATH)

    st.set_page_config(page_title="Noticias de España", layout="wide")

    # Mostrar el sidebar y manejo de autenticación
    display_sidebar(authenticator)

    # Inicializar el administrador de búsqueda
    search_manager = initialize_search_manager(VECTOR_DB_PATH, DATABASE_PATH)

    # Acceder a la base de datos y mostrar la interfaz
    with NewsInterfaceDatabase(DATABASE_PATH) as news_db:
        handle_interfaces(news_db, search_manager)

if __name__ == "__main__":
    main()
