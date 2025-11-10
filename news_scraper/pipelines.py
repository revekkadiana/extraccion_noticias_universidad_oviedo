# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

class NewsScraperPipeline:
    def process_item(self, item, spider):
        return item

# Pipeline para que la base de datos almacene información de los articulos encontrados
class SQLitePipeline:
    def __init__(self):
        self.buffer = []
        self.batch_size = 100

    def open_spider(self, spider):
        self.db = spider.news_db

    def process_item(self, item, spider):
        self.buffer.append(item)
        if len(self.buffer) >= self.batch_size:
            self._flush_buffer()
        return item

    def close_spider(self, spider):
        self._flush_buffer()

    def _flush_buffer(self):
        if self.buffer:
            self.db.bulk_insert_articles(self.buffer)
            self.db.bulk_insert_palabra_clave_articulos(self.buffer)
            self.buffer.clear()


###############################################################################
import re
import unicodedata
import nltk
from nltk.corpus import stopwords
from news_database.news_db import NewsDatabase
import scrapy
from scrapy.utils.project import get_project_settings

nltk.download('stopwords', quiet=True)

class FiltradoNoticiasPipeline:
    PUNCTUATION_RE = re.compile(r'[^\w\s]')

    def __init__(self):
        settings = get_project_settings()
        db_path = settings.get('DATABASE_PATH')
        news_db = NewsDatabase(db_path)
        #news_db = NewsDatabase()
        rules_list = news_db.get_all_keywords(id=True)
        news_db.close_db()
        self.stopwords = set(stopwords.words('spanish'))
        self.parsed_rules = [self._parse_regla(rule, rule_id) for rule_id, rule in rules_list]

    def _preprocesar_palabra_clave(self, word, eliminar_puntuacion = True):
        if not isinstance(word, str):
            return ''
        word = word.lower()
        word = unicodedata.normalize('NFKD', word).encode('ASCII', 'ignore').decode('utf-8')
        if eliminar_puntuacion:
            word = self.PUNCTUATION_RE.sub(' ', word)
        word = re.sub(r'\s+', ' ', word).strip()  # elimina espacios múltiples
        palabras = word.split()
        palabras = [p for p in palabras if p not in self.stopwords]
        return ' '.join(palabras)

    def _preprocesar_texto(self, texto, eliminar_puntuacion = True):
        if not isinstance(texto, str):
            return ''
        texto = texto.lower()
        texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
        if eliminar_puntuacion:
            texto = self.PUNCTUATION_RE.sub(' ', texto)
        texto = re.sub(r'\s+', ' ', texto).strip()  # elimina espacios múltiples
        palabras = texto.split()
        palabras = [p for p in palabras if p not in self.stopwords]
        return ' '.join(palabras)

    def _compilar_patron(self, word):
        escaped = re.escape(word)
        return re.compile(rf'\b{escaped}\b', re.IGNORECASE)

    def _parse_regla(self, rule, rule_id):
        """
        Analiza una cadena de reglas y retorna un diccionario con el tipo y una lista de patrones de expresiones regulares compilados.
        Admite:
        - Reglas AND con '+'
        - Reglas OR con 'o'
        - Reglas de una sola palabra clave
        """
        #rule = self._preprocesar_palabra_clave(rule.strip())
        if '+' in rule:
            parts = [self._preprocesar_palabra_clave(part.strip()) for part in rule.split('+')]
            patterns = [self._compilar_patron(part) for part in parts]
            return {'type': 'AND', 'patterns': patterns, 'original': rule, 'id':rule_id}
        elif ' o ' in rule:
            parts = [self._preprocesar_palabra_clave(part.strip()) for part in rule.split(' o ')]
            patterns = [self._compilar_patron(part) for part in parts]
            return {'type': 'OR', 'patterns': patterns, 'original': rule, 'id':rule_id}
        else:
            pattern = self._compilar_patron(self._preprocesar_palabra_clave(rule.strip()))
            return {'type': 'SINGLE', 'patterns': [pattern], 'original': rule, 'id':rule_id}

    def cumple_regla(self, texto, parsed_rule):
        """
        Verificar el tipode regla y si el texto cumple con la regla
        """
        if parsed_rule['type'] == 'AND':
            return all(p.search(texto) for p in parsed_rule['patterns'])
        elif parsed_rule['type'] == 'OR':
            return any(p.search(texto) for p in parsed_rule['patterns'])
        else:  # SINGLE
            return parsed_rule['patterns'][0].search(texto) is not None

    def filtrar_texto(self, texto):
        reglas_encontradas = []
        for parsed_rule in self.parsed_rules:
            if self.cumple_regla(texto, parsed_rule):
                reglas_encontradas.append(parsed_rule['id'])
        cumple = bool(reglas_encontradas)
        return cumple, reglas_encontradas

    def process_item(self, item, spider):
        texto_original = item.get('texto', '')
        texto = self._preprocesar_texto(texto_original)
        cumple, reglas = self.filtrar_texto(texto)
        if cumple:
            item['reglas_id'] = reglas
            print(f"Artículo aceptado: {item.get('url', '')}")
            return item
        else:
            raise scrapy.exceptions.DropItem(f"Artículo descartado: {item.get('url', '')}")


###############################################################################
from news_search.embedding_generator import EmbeddingGenerator
from news_search.chroma_engine import ChromaVectorEngine
from news_search.search_manager import SearchManager

class EmbeddingPipeline:

    def __init__(self):
        model_name = 'intfloat/multilingual-e5-small'
        embedding_model = EmbeddingGenerator(model_name = model_name)
        vector_db = ChromaVectorEngine(embedding=embedding_model, persist_directory="./news_search/chroma_db", sqlite_path="./news_database/news.db")
        self.search_manager = SearchManager(embedding_model=embedding_model, vector_engine=vector_db, chunk_size=500, chunk_overlap=100)

    def process_item(self, item, spider):
        page_content=f"{item['titulo']}\n\n{item['texto']}"
        metadata={
            "titulo": item["titulo"],
            "url": item["url"],
            "fuente": item["fuente"],
            "fecha_publicacion": item["fecha_publicacion"].strftime("%Y-%m-%d")
        }
        print(f'Metadata vector db: {metadata}')
        self.search_manager.ingest_article(page_content, metadata)
        return item

