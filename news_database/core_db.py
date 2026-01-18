import sqlite3
from datetime import datetime
from news_database.utils import get_domain
import nltk
from nltk.stem.snowball import SnowballStemmer
nltk.download('punkt', quiet=True)

class NewsCoreDatabase:
    def __init__(self, db_path='news.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.stemmer = SnowballStemmer('spanish')
        #self.create_tables()

    def create_tables(self):
        cursor = self.conn.cursor()

        # Habilitar claves foraneas
        cursor.execute("PRAGMA foreign_keys = ON;")

        # Crear tabla de categorias
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS categoria (
                categoria_id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT UNIQUE
            );
        """)

        # Crear tabla de palabras clave
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS palabras_clave (
                palabra_id INTEGER PRIMARY KEY AUTOINCREMENT,
                palabra TEXT UNIQUE,
                stem TEXT UNIQUE
            );
        """)

        # Crear tabla de palabras clave y categoria
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS palabras_clave_categoria (
                palabra_id INTEGER,
                categoria_id INTEGER,
                PRIMARY KEY (palabra_id, categoria_id),
                FOREIGN KEY (palabra_id) REFERENCES palabras_clave(palabra_id) ON DELETE CASCADE,
                FOREIGN KEY (categoria_id) REFERENCES categoria(categoria_id) ON DELETE CASCADE
            );
        """)

        # Crear tabla de palabras clave y articulos
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS palabra_clave_articulos (
                url TEXT,
                palabra_id INTEGER,
                PRIMARY KEY (url, palabra_id),
                FOREIGN KEY (url) REFERENCES articulos(url) ON DELETE CASCADE,
                FOREIGN KEY (palabra_id) REFERENCES palabras_clave(palabra_id) ON DELETE CASCADE
            );
        """)


        # Crear tabla de url exploradas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS urls_exploradas (
                url TEXT PRIMARY KEY,
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Crear tabla de fuentes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fuentes (
                fuente_id TEXT PRIMARY KEY,
                nombre TEXT,
                url_home TEXT
            )
        """)


        cursor.execute('''
            CREATE TABLE IF NOT EXISTS articulos (
                url TEXT PRIMARY KEY,
                fuente TEXT,
                titulo TEXT,
                fecha_publicacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fuente) REFERENCES fuentes(fuente_id) ON DELETE CASCADE,
                FOREIGN KEY (url) REFERENCES urls_exploradas(url) ON DELETE CASCADE
            )
        ''')

        # Crear una tabla de mapa del sitio
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sitemap (
                url_relativa TEXT PRIMARY KEY
            )
        """)


        # Crear la tabla puente fuentes_sitemap
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fuentes_sitemap (
                fuente_id TEXT NOT NULL,
                url_relativa TEXT NOT NULL,
                PRIMARY KEY (fuente_id, url_relativa),
                FOREIGN KEY (fuente_id) REFERENCES fuentes(fuente_id) ON DELETE CASCADE,
                FOREIGN KEY (url_relativa) REFERENCES sitemap(url_relativa) ON DELETE CASCADE
            )
        """)


        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reglas (
                regla_id INTEGER PRIMARY KEY AUTOINCREMENT,
                descripcion TEXT UNIQUE COLLATE NOCASE
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS regla_palabras_clave (
                regla_id INTEGER,
                palabra_id INTEGER,
                posicion INTEGER,
                operador TEXT CHECK(operador IN ('+', 'o', '-')),
                PRIMARY KEY (regla_id, posicion),
                FOREIGN KEY (regla_id) REFERENCES reglas(regla_id) ON DELETE CASCADE,
                FOREIGN KEY (palabra_id) REFERENCES palabras_clave(palabra_id) ON DELETE CASCADE
            );
        """)


        # TRIGGER 1: Detectar cuando regla AND queda incompleta
        # TRIGGER 2: Cleanup general después de delete palabra_clave
        cursor.executescript("""
            CREATE TRIGGER IF NOT EXISTS cleanup_and_rules_after_junction_delete
            AFTER DELETE ON regla_palabras_clave
            WHEN OLD.operador = '+'
            BEGIN
                DELETE FROM reglas WHERE regla_id = OLD.regla_id;
            END;

            CREATE TRIGGER IF NOT EXISTS cleanup_orphans_after_keyword_delete
            AFTER DELETE ON palabras_clave
            FOR EACH ROW
            BEGIN
                -- Eliminar reglas OR vacías
                DELETE FROM reglas WHERE regla_id NOT IN (
                    SELECT DISTINCT rpc.regla_id FROM regla_palabras_clave rpc
                );

                -- Eliminar keywords huérfanos
                DELETE FROM palabras_clave WHERE palabra_id NOT IN (
                    SELECT DISTINCT palabra_id FROM regla_palabras_clave
                );
            END;
        """)

        self.conn.commit()


    # Métodos utilitarios compartidos
    def get_stem(self, palabra):
        """NLTK SnowballStemmer español"""
        if not isinstance(palabra, str):
            return ''
        return self.stemmer.stem(palabra.strip().lower())

    def get_keyword_id(self, palabra):
        """Obtiene ID de palabra clave"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT palabra_id FROM palabras_clave WHERE palabra = ?', (palabra,))
        result = cursor.fetchone()
        return result[0] if result else None

    def get_category_id(self, categoria):
        """Obtiene ID de categoría"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT categoria_id FROM categoria WHERE nombre = ?', (categoria,))
        result = cursor.fetchone()
        return result[0] if result else None

    def get_regla_id(self, regla):
        """
          Obtener el id de la regla
        """
        cursor = self.conn.cursor()
        cursor.execute('SELECT regla_id FROM reglas WHERE descripcion = ?', (regla,))
        result = cursor.fetchone()
        if result is None:
            return None
        return result[0]

    def associate_rule_keywords(self, regla_desc, keyword_pairs):
        """keyword_pairs: list of (palabra, operador, posicion) e.g., [('A', '+', 1), ('B', '+', 2)]"""
        regla_id = self.get_regla_id(regla_desc)
        if not regla_id:
            return
        cursor = self.conn.cursor()
        data = []
        for palabra, operador, posicion in keyword_pairs:
            palabra_id = self.get_keyword_id(palabra)  # Add get_keyword_id method if needed
            if palabra_id:
                data.append((regla_id, palabra_id, posicion, operador))
        cursor.executemany(
            'INSERT OR REPLACE INTO regla_palabras_clave (regla_id, palabra_id, posicion, operador) VALUES (?, ?, ?, ?)',
            data
        )
        self.conn.commit()

    def close_db(self):
        """Cierra conexión"""
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def delete_regla(self, regla):
        cursor = self.conn.cursor()
        query = "DELETE FROM reglas WHERE descripcion = ?"
        cursor.execute(query, (regla,))
        self.conn.commit()
