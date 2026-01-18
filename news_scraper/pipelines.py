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
import sqlite3
import traceback
import json
import time
import os

class SQLitePipeline:
    def __init__(self):
        self.buffer = []
        self.batch_size = 10

    def open_spider(self, spider):
        self.db = spider.news_db

    def process_item(self, item, spider):
        self.buffer.append(item)
        if len(self.buffer) >= self.batch_size:
            self._flush_buffer()
        return item

    def close_spider(self, spider):
        self._flush_buffer()

    def _get_list_url_keyword_id(self):
        lista_palabra_url = []
        for articulo in self.buffer:
            lista_palabra_url.extend( [(articulo['url'], keyword_id) for keyword_id in articulo['keyword-ids']])
        return lista_palabra_url


    def _flush_buffer(self):

        if self.buffer:
            max_retries = 2
            retry_count = 0
            insertados = False
            while retry_count < max_retries:
                try:
                    print(f"Intento {retry_count + 1}/{max_retries}")

                    print("Intentando insertar artículos y relaciones...")

                    # Mostrar los datos del buffer antes de insertar
                    print(f"Buffer a insertar ({len(self.buffer)} elementos):")
                    
                    if retry_count > 0:
                        # Insertar URLs primero en caso aún no se haya realizado
                        urls = [item['url'] for item in self.buffer]
                        self.db.bulk_insert_crawled_urls(urls)

                        # CRÍTICO: commit explícito después URLs
                        #self.db.conn.commit()

                        # Verificar URLs existen (debug temporal)
                        print("Se insertaron las URLs faltantes correctamente.")

                    # Intentar insertar los artículos
                    self.db.bulk_insert_articles(self.buffer)

                    # Intentar insertar las relaciones (palabra clave - artículo)
                    lista_palabra_url = self._get_list_url_keyword_id()
                    self.db.bulk_insert_palabra_clave_articulos(lista_palabra_url)

                    print(f"Se insertaron {len(self.buffer)} elementos con éxito.")
                    insertados = True
                    break  # Éxito

                except sqlite3.IntegrityError as e:
                    retry_count += 1
                    print(f"Error en intento {retry_count}: {e}")
                    self.db.conn.rollback()

                    # Delay para Scrapy threads
                    #import time
                    #time.sleep(0.2 * retry_count)  # Delay progresivo

                    if retry_count >= max_retries:
                        #print("MÁXIMO Intentos - Guardando debug")
                        print("MÁXIMO Intentos - No se pudieron guardar todos los articulos")
                        print(self.buffer)
                        #import json, time
                        #with open(f"failed_batch_{int(time.time())}.json", "w") as f:
                        #    json.dump(self.buffer, f, default=str, indent=2)

                except Exception as e:
                    print(f"Error inesperado: {e}")
                    self.db.conn.rollback()
                    break

                #finally:
                #    # Limpiar el buffer después de la inserción
                #    #self.buffer.clear()
                #    #print("Buffer limpiado.")
            
            if insertados:
                # Limpiar el buffer después de la inserción
                self.buffer.clear()
                print("Buffer limpiado.")
            else:
                print("Fallback: Insertando artículos individuales...")
                self._insert_individual()



    def _get_list_url_keyword_id_subset(self, valid_items):
        """Keywords solo para items válidos"""
        lista_palabra_url = []
        for item in valid_items:
            lista_palabra_url.extend([(item['url'], kid) for kid in item['keyword-ids']])
        return lista_palabra_url
        
    def _insert_individual(self):
        """ Insertar de forma individual cada articulo para detectar errores"""
        successful_articles = []
        failed_articles = []
    
        for item in self.buffer:
            if self.db.insert_article_individual(item):
                successful_articles.append(item)
            else:
                failed_articles.append(item)
        
        # 3. Keywords solo para artículos exitosos
        if len(successful_articles) > 0:
            lista_palabra_url = self._get_list_url_keyword_id_subset(successful_articles)
            self.db.bulk_insert_palabra_clave_articulos(lista_palabra_url)
            
        print(f"{len(successful_articles)} artículos OK | {len(failed_articles)} fallidos")
        
        if failed_articles:
            self._save_failed_batch(failed_articles)
        
        self.buffer.clear()

    def _save_failed_batch(self, failed_items):
        """Guarda batch fallido con timestamp"""
        # Crear carpeta failed_batches
        failed_dir = "failed_article_insertion"
        if not os.path.exists(failed_dir):
           os.makedirs(failed_dir)  # Crea recursivamente
        
        
        timestamp = int(time.time())
        filename = os.path.join(failed_dir, f"failed_batch_{timestamp}.json")
        #filename = f"failed_batch_{timestamp}.json"
        
        # Info útil para debug
        debug_data = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'total_failed': len(failed_items),
            'errors': []
        }
        
        # Solo datos básicos (sin datetime)
        for item in failed_items:
            error_type = self._diagnose_item(item)
            debug_data['errors'].append({
                'url': item['url'],
                'fuente': item.get('fuente', 'N/A'),
                'titulo': str(item.get('titulo', '')),
                'fecha_publicacion': str(item.get('fecha_publicacion', 'N/A')),
                'error_type': error_type
            })
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(debug_data, f, indent=2, ensure_ascii=False)
        
        print(f"Fallidos guardados: {filename} ({len(failed_items)} errores)")


    def _diagnose_item(self, item):
        """Diagnostica por qué falló el item"""

        # 1. ¿Existe fuente?
        fuente = item.get('fuente')
        if fuente:
            if not self.db.exists_fuente(fuente):
                return f"FUENTE_NO_EXISTE: {fuente}"
        
        # 2. ¿Keywords inválidos?
        invalid_keywords = []
        for kid in item.get('keyword-ids', []):
            if not self.db.exists_keyword_by_id(kid):
                invalid_keywords.append(kid)
        
        if invalid_keywords:
            return f"KEYWORDS_INVALIDOS: {invalid_keywords[:3]}"
            
            
        # 3. Urls no insertadas como exploradas
        url = item['url']
        if not self.db.exists_url_explorada(url):
            return f"URL_NO_EN_EXPLORADAS: '{url[:50]}...'"

        return "DESCONOCIDO"


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
        self.news_db = NewsDatabase(db_path)

        # Usar NLTK stemmer de la base de datos para consistencia
        self.stemmer = self.news_db.stemmer

        self.stopwords = set(stopwords.words('spanish'))

        # Cargar todas las reglas (AND, OR, simple) desde la tabla de uniones
        self.parsed_rules = self._cargar_todas_las_reglas()


    def _cargar_todas_las_reglas(self):
        # Cargar reglas y metadata de la union de reglas con palabras_clave_reglas
        reglas_metadata = self.news_db.obtener_reglas_con_palabras_clave()

        # Agrupar reglas por id
        reglas = {}
        for rule_id, rule_desc, posicion, keyword, operador in reglas_metadata:
            if rule_id not in reglas:
                reglas[rule_id] = {
                    'desc': rule_desc,
                    'keywords': [],
                    'preprocessed': [],
                    'operator': operador
                }
            reglas[rule_id]['keywords'].append(keyword)
            reglas[rule_id]['preprocessed'].append(self._preprocesar_palabra_clave(keyword))

        # Convertir al formato de diccionario
        """
        Analiza una cadena de reglas y retorna un diccionario con el tipo y una lista de patrones d
        e expresiones regulares compilados.
        Admite:
        - Reglas AND con '+'
        - Reglas OR con 'o'
        - Reglas de una sola palabra clave
        """
        parsed_rules = []
        for rule_id, rule_data in reglas.items():
            patterns = [self._compilar_patron(preprocessed) for preprocessed in rule_data['preprocessed']]

            # Determine rule type based on count and operator
            if rule_data['operator'] == '+':
                rule_type = 'AND'
            elif rule_data['operator'] == 'o':  # ',' operator
                rule_type = 'OR'
            else:
                rule_type = 'SINGLE'

            parsed_rules.append({
                'type': rule_type,
                'patterns': patterns,
                'keywords': rule_data['keywords'], #palabras_clave_originales
                #'rule': rule_data['desc'],
                #'id': rule_id
            })
        return parsed_rules


    def _preprocesar_palabra_clave(self, word, eliminar_puntuacion = True):
        if not isinstance(word, str):
            return ''
        word = word.lower()
        word = unicodedata.normalize('NFKD', word).encode('ASCII', 'ignore').decode('utf-8')
        if eliminar_puntuacion:
            word = self.PUNCTUATION_RE.sub(' ', word)
        word = re.sub(r'\s+', ' ', word).strip()  # elimina espacios múltiples
        palabras = word.split()
        palabras_limpias = [p for p in palabras if p not in self.stopwords]
        stems = [self.stemmer.stem(palabra) for palabra in palabras_limpias] # stemming
        #return ' '.join(palabras)
        return ' '.join(stems)

    def _preprocesar_texto(self, texto, eliminar_puntuacion = True):
        if not isinstance(texto, str):
            return ''
        texto = texto.lower()
        texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
        if eliminar_puntuacion:
            texto = self.PUNCTUATION_RE.sub(' ', texto)
        texto = re.sub(r'\s+', ' ', texto).strip()  # elimina espacios múltiples
        palabras = texto.split()
        palabras_limpias = [p for p in palabras if p not in self.stopwords]
        stems = [self.stemmer.stem(palabra) for palabra in palabras_limpias] # stemming
        #return ' '.join(palabras)
        return ' '.join(stems)

    def _compilar_patron(self, word):
        escaped = re.escape(word)
        return re.compile(rf'\b{escaped}\b', re.IGNORECASE)


    def cumple_regla(self, texto, parsed_rule):
        """
        Verificar el tipo de regla y si el texto cumple con la regla
        """
        if parsed_rule['type'] == 'AND':
            return all(p.search(texto) for p in parsed_rule['patterns']), parsed_rule['patterns']
        elif parsed_rule['type'] == 'OR':
            presentes = []
            for p, kw in zip(parsed_rule['patterns'], parsed_rule['keywords']):
                if p.search(texto):
                    presentes.append(kw)
            cumple = len(presentes) > 0
            return cumple, presentes
        else:
            return parsed_rule['patterns'][0].search(texto) is not None, parsed_rule['patterns']

    def filtrar_texto(self, texto):
        palabras_clave_single = set()
        palabras_clave_compuestas_and = set()
        palabras_clave_compuestas_or = set()
        palabras_clave_compuestas_secundarias_and = set()

        for parsed_rule in self.parsed_rules:
            #if self.cumple_regla(texto, parsed_rule):
            cumple, palabras_clave_encontradas = self.cumple_regla(texto, parsed_rule)
            if cumple:
                print(f"Parsed rule: {parsed_rule}")
                if parsed_rule['type'] == 'AND':
                    # solo la primera palabra es la palabra clave asociada
                    palabras_clave_compuestas_and.add(parsed_rule['keywords'][0])
                    palabras_clave_compuestas_secundarias_and.update(parsed_rule['keywords'][1:])
                elif parsed_rule['type'] == 'OR':
                    # todas son posibles palabras clave encontradas en el texto
                    palabras_clave_compuestas_or.update(palabras_clave_encontradas)
                else:
                    # single keyword
                    palabras_clave_single.update(parsed_rule['keywords'])

        # palabras clave unicas que no forman parte de palabras compuestas por AND
        palabras_validas_single = palabras_clave_single - palabras_clave_compuestas_secundarias_and

        # unimos palabras clave de and, or y unicas
        palabras_clave = palabras_clave_compuestas_and | palabras_clave_compuestas_or | palabras_validas_single

        cumple = len(palabras_clave) > 0

        #if cumple:
        #    print(f"Palabras clave and: {palabras_clave_compuestas_and}")
        #    print(f"Palabras clave and secundaria: {palabras_clave_compuestas_secundarias_and}")
        #    print(f"Palabras clave or: {palabras_clave_compuestas_or}")
        #    print(f"Palabras clave unicas: {palabras_clave_single}")
        #    print(f"Palabras clave unicas validas: {palabras_validas_single}")

        return cumple, list(palabras_clave)

    def process_item(self, item, spider):
        texto_original = item.get('texto', '')
        texto = self._preprocesar_texto(texto_original)
        #print("Preprocessed")
        cumple, palabras_clave = self.filtrar_texto(texto)
        if cumple:
            print(f"Palabras clave encontradas: {palabras_clave}")
            item['keyword-ids'] = self.news_db.get_keyword_ids(palabras_clave)
            print(f"Artículo aceptado: {item.get('url', '')}")
            #print(f"Metadata: {item}") # for testing
            return item
        else:
            if 'texto' in item:
                del item['texto']
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

