from news_database.core_db import NewsCoreDatabase
from news_database.utils import get_domain
from datetime import datetime
import sqlite3

class NewsDatabase(NewsCoreDatabase):

    def bulk_insert_sitemap(self, sitemap_data):
        cursor = self.conn.cursor()
        try:
            cursor.executemany("""
                INSERT OR IGNORE INTO sitemap (url_relativa)
                VALUES (?)
            """, sitemap_data)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error al insertar url del mapa de sitio: {e}")

    def bulk_insert_fuentes_sitemap(self, fuentes_sitemap_data):
        cursor = self.conn.cursor()
        try:
            cursor.executemany("""
                INSERT OR IGNORE INTO fuentes_sitemap (fuente_id, url_relativa)
                VALUES (?, ?)
            """, fuentes_sitemap_data)
            self.conn.commit()
            #print(f"{len(fuentes_sitemap_data)} entries inserted into fuentes_sitemap.")
        except sqlite3.Error as e:
            print(f"Error al insertar en fuentes_sitemap {e}")

    def bulk_insert_fuentes(self, fuentes_data):
        cursor = self.conn.cursor()
        try:
            cursor.executemany("""
                INSERT OR IGNORE INTO fuentes (fuente_id, nombre, url_home)
                VALUES (?, ?, ?)
            """, fuentes_data)
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error al insertar en fuentes: {e}")

    def bulk_insert_articles(self, articles):
        cursor = self.conn.cursor()
        cursor.executemany('''
        INSERT OR IGNORE INTO articulos (url, fuente, titulo, fecha_publicacion)
        VALUES (?, ?, ?, ?)
        ''', [(a['url'], a['fuente'], a['titulo'], a['fecha_publicacion']) for a in articles])
        self.conn.commit()

    def is_crawled_url(self, url):
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM urls_exploradas WHERE url = ?', (url,))
        return cursor.fetchone() is not None

    def bulk_insert_crawled_urls(self, urls):
        cursor = self.conn.cursor()

        values = []
        for url in urls:
            crawled_time = datetime.now().isoformat(sep=' ', timespec='seconds')  # e.g., '2025-05-04 14:30:00'
            values.append((url, crawled_time))

        cursor.executemany('''
        INSERT OR IGNORE INTO urls_exploradas (url, crawled_at) VALUES (?, ?)
        ''', values)
        self.conn.commit()

    def remove_old_articles(self, limit_date):
        cursor = self.conn.cursor()

        cursor.execute('''
        DELETE FROM articulos
        WHERE fecha_publicacion < ?
        ''', (limit_date,))
        self.conn.commit()

    def vacuum_database(self):
        cursor = self.conn.cursor()
        cursor.execute('VACUUM;')
        self.conn.commit()

    def get_news_urls(self):
        """ Obtener las urls de todos los sitios web de noticias """
        cursor = self.conn.cursor()
        cursor.execute('SELECT url_home FROM fuentes')
        return [row[0] for row in cursor.fetchall()]

    def news_url_exist(self, news_url):
        """
          Verificar si existe fuente basado en url
        """
        domain_url = get_domain(news_url)
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM fuentes WHERE fuente_id = ?', (domain_url,))
        return cursor.fetchone() is not None

    def source_has_sitemap(self, news_url):
        """
          Comprueba si fuente_id indicado tiene alguna entrada de mapa del sitio asociada.
          Devuelve "True" si hay al menos un mapa del sitio asociado; "False" en caso contrario.
        """
        domain_url = get_domain(news_url)
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM fuentes_sitemap WHERE fuente_id = ?  LIMIT 1', (domain_url,))
        return cursor.fetchone() is not None

    def get_sitemaps_for_source_simple(self, news_url):
        """
        Obtener mapas de sitio asociados a una fuente (url)
        """
        domain_url = get_domain(news_url)
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT url_relativa FROM fuentes_sitemap WHERE fuente_id = ?
        """, (domain_url,))
        rows = cursor.fetchall()
        return [row[0] for row in rows]

    def get_source_name(self, news_url):
        """
          Obtener el nombre de una fuente dada su url
        """
        domain_url = get_domain(news_url)
        cursor = self.conn.cursor()
        cursor.execute('SELECT nombre FROM fuentes WHERE fuente_id = ?', (domain_url,))
        result = cursor.fetchone()
        if result is None:
            return None
        return result[0]


    def bulk_insert_keywords(self, palabras):
        """
        Inserta palabras clave que es una tupla (categorica, palabra)
        """
        cursor = self.conn.cursor()
        cursor.executemany("""
            INSERT INTO palabras_clave (categoria, palabra)
            VALUES (?, ?)
        """, palabras)
        self.conn.commit()

    def get_all_keywords(self, id=False):
        """
        Obtener todas las palabras clave sin categoria
        """
        cursor = self.conn.cursor()
        if id:
            cursor.execute("SELECT palabra_id, palabra FROM palabras_clave")
            rows = cursor.fetchall()
            # Extraemos el primer elemento de cada tupla (la palabra)
            palabras = [ (row[0], row[1]) for row in rows]
        else:
            cursor.execute("SELECT palabra FROM palabras_clave")
            rows = cursor.fetchall()
            # Extraemos el primer elemento de cada tupla (la palabra)
            palabras = [row[0] for row in rows]
        return palabras


    def bulk_insert_categorias(self, nombres):
        """
        Insertar categorias a partir de una lista [(nombre1,), ...]
        """
        cursor = self.conn.cursor()
        cursor.executemany('INSERT OR IGNORE INTO categoria (nombre) VALUES (?)', [(nombre,) for nombre in nombres])
        self.conn.commit()


    def bulk_insert_palabras_clave(self, palabras):
        """
        Insertar múltiples palabras clave de una lista [(palabra1,), ...]
        """
        cursor = self.conn.cursor()
        cursor.executemany('INSERT OR IGNORE INTO palabras_clave (palabra) VALUES (?)', [(palabra,) for palabra in palabras])
        self.conn.commit()


    def bulk_insert_palabra_clave_categorias(self, palabra_categoria_pairs):
        """
        Insertar múltiple pares (palabra_id, categoria_id)
        """
        cursor = self.conn.cursor()
        cursor.executemany(
            'INSERT OR IGNORE INTO palabras_clave_categoria (palabra_id, categoria_id) VALUES (?, ?)',
            palabra_categoria_pairs
        )
        self.conn.commit()


    def bulk_insert_palabra_clave_articulos(self, lista_palabra_url):
        cursor = self.conn.cursor()
        cursor.executemany(
            'INSERT OR IGNORE INTO palabra_clave_articulos (url, palabra_id) VALUES (?, ?)',
            lista_palabra_url
        )
        self.conn.commit()


    def get_category(self, categoria):
        """
          Obtener el id de la categoria
        """
        cursor = self.conn.cursor()
        cursor.execute('SELECT categoria_id FROM categoria WHERE nombre = ?', (categoria,))
        result = cursor.fetchone()
        if result is None:
            return None
        return result[0]


    def get_keywords_by_category(self, categoria):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT pc.palabra
            FROM palabras_clave pc
            JOIN palabras_clave_categoria pcc ON pc.palabra_id = pcc.palabra_id
            JOIN categoria c ON pcc.categoria_id = c.categoria_id
            WHERE c.nombre = ?
            ORDER BY pc.palabra
        ''', (categoria,))
        return [row[0] for row in cursor.fetchall()]


    def get_all_keyword_category_pairs(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT pc.palabra, c.nombre
            FROM palabras_clave pc
            JOIN palabras_clave_categoria pcc ON pc.palabra_id = pcc.palabra_id
            JOIN categoria c ON pcc.categoria_id = c.categoria_id
            ORDER BY c.nombre, pc.palabra
        ''')
        return cursor.fetchall()


    def bulk_insert_regla(self, reglas):
        """
        Insertar múltiples reglas de una lista [(regla,), ...]
        """
        cursor = self.conn.cursor()
        cursor.executemany(
            'INSERT OR IGNORE INTO reglas (descripcion) VALUES (?)',
            [(regla,) for regla in reglas])
        self.conn.commit()


    def get_all_rules(self, id=False):
        """
        Obtener todas las reglas de la base de datos
        """
        cursor = self.conn.cursor()
        if id:
            cursor.execute("SELECT regla_id, descripcion FROM reglas")
            rows = cursor.fetchall()
            # Extraemos el primer elemento de cada tupla
            palabras = [ (row[0], row[1]) for row in rows]
        else:
            cursor.execute("SELECT descripcion FROM reglas")
            rows = cursor.fetchall()
            # Extraemos el primer elemento de cada tupla
            palabras = [row[0] for row in rows]
        return palabras


    def get_keyword_ids(self, palabras_clave):
        """
        Obtener los IDs de una lista de palabras clave.
        Retorna lista de enteros con los palabra_id encontrados.
        """
        if not palabras_clave:
            return []

        cursor = self.conn.cursor()
        placeholders = ','.join('?' for _ in palabras_clave)
        query = f'SELECT palabra_id FROM palabras_clave WHERE palabra IN ({placeholders})'
        cursor.execute(query, palabras_clave)
        results = cursor.fetchall()

        # Extraer solo los IDs (enteros) de las tuplas
        keyword_ids = [row[0] for row in results]
        return keyword_ids


    def exist_keyword(self, keyword):
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM palabras_clave WHERE palabra = ?', (keyword,))
        return cursor.fetchone() is not None


    def delete_regla_by_id(self, regla_id):
        cursor = self.conn.cursor()
        query = "DELETE FROM reglas WHERE regla_id = ?"
        cursor.execute(query, (regla_id,))
        self.conn.commit()


    def bulk_insert_palabras_clave_stem(self, palabras_originales):
        """palabra_original + NLTK stem"""
        cursor = self.conn.cursor()

        data_to_insert = []
        stems_existentes = set()

        # Obtener stems que ya existen en la base de datos
        cursor.execute("SELECT stem FROM palabras_clave WHERE stem IS NOT NULL")
        stems_existentes = {row[0] for row in cursor.fetchall()}

        # Filtrar solo palabras cuyo stem no existe
        for palabra in set(palabras_originales):
            stem = self.get_stem(palabra.strip().lower())
            if stem and stem not in stems_existentes:
                data_to_insert.append((palabra, stem))

        if data_to_insert:
            cursor.executemany("""
                INSERT OR IGNORE INTO palabras_clave (palabra, stem)
                VALUES (?, ?)
            """, data_to_insert)
            self.conn.commit()


    def get_keyword_ids_stemmed(self, palabras_clave):
        """Busca por NLTK stems"""

        if not palabras_clave:
            return []

        stems = [self.get_stem(p.strip().lower()) for p in palabras_clave]
        stems_unicos = list(set(stems))

        cursor = self.conn.cursor()
        placeholders = ','.join('?' * len(stems_unicos))
        cursor.execute(f"""
            SELECT stem, palabra_id FROM palabras_clave
            WHERE stem IN ({placeholders})
        """, stems_unicos)

        stem_to_id = {row[0]: row[1] for row in cursor.fetchall()}
        return [stem_to_id.get(stem) for stem in stems]


    def obtener_reglas_con_palabras_clave(self):
        """
        Obtener todas las reglas de la base de datos
        """
        cursor = self.conn.cursor()
        query = '''
            SELECT r.regla_id, r.descripcion, rpc.posicion, pc.palabra, rpc.operador
            FROM reglas r
            JOIN regla_palabras_clave rpc ON r.regla_id = rpc.regla_id
            JOIN palabras_clave pc ON rpc.palabra_id = pc.palabra_id
            ORDER BY r.regla_id, rpc.posicion
        '''
        cursor.execute(query)
        resultados = cursor.fetchall()
        return resultados


    def insert_article_individual(self, article):
        """Inserta artículo individual"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO articulos (url, fuente, titulo, fecha_publicacion)
                VALUES (?, ?, ?, ?)
            ''', (article['url'], article['fuente'], article['titulo'], article['fecha_publicacion']))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError as e:
            print(f"Inserción de artículo falló: {article['url']} → {e}")
            self.conn.rollback()
            return False
            
    def exists_fuente(self, fuente_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM fuentes WHERE fuente_id = ?', (fuente_id,))
        return cursor.fetchone() is not None
        
        
    def exists_keyword_by_id(self, keyword_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM palabras_clave WHERE palabra_id = ?', (keyword_id,))
        return cursor.fetchone() is not None
        
     
    def exists_url_explorada(self, url):
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM urls_exploradas WHERE url = ?', (url,))
        return cursor.fetchone() is not None