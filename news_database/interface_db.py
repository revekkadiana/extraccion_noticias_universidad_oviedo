from news_database.core_db import NewsCoreDatabase
from news_database.utils import get_domain

class NewsInterfaceDatabase(NewsCoreDatabase):
    ALL_OPTION = "Todas"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_db()

    # --- Consultas básicas ---
    def fetch_categories(self):
        """Obtener todas las categorías de la base de datos."""
        c = self.conn.cursor()
        c.execute("SELECT nombre FROM categoria ORDER BY nombre;")
        return [row[0] for row in c.fetchall()]


    def fetch_keywords_for_categories(self, category_names):
        """Obtener palabras clave para categorías específica."""
        c = self.conn.cursor()
        if not category_names or category_names == [NewsInterfaceDatabase.ALL_OPTION]:
            c.execute("SELECT DISTINCT palabra FROM palabras_clave")
        else:
            c.execute("""
                SELECT DISTINCT pc.palabra
                FROM palabras_clave pc
                JOIN palabras_clave_categoria pcc ON pc.palabra_id = pcc.palabra_id
                JOIN categoria c ON pcc.categoria_id = c.categoria_id
                WHERE c.nombre IN ({})
            """.format(','.join(['?'] * len(category_names))), category_names)
        return [row[0] for row in c.fetchall()]


    def fetch_articles(self, categories=None, keywords=None, start_date=None, end_date=None):
        c = self.conn.cursor()
        query = """
            SELECT a.titulo, a.url, f.nombre, a.fecha_publicacion
            FROM articulos a
            JOIN fuentes f ON a.fuente = f.fuente_id
            {}
            {}
            {}
            ORDER BY a.fecha_publicacion DESC
        """
        where_clauses = []
        params = []
        if categories and NewsInterfaceDatabase.ALL_OPTION not in categories:
            # Build a subquery for categories
            subquery = """
                EXISTS (
                    SELECT 1 FROM palabras_clave_categoria pcc
                    JOIN categoria c ON pcc.categoria_id = c.categoria_id
                    WHERE c.nombre IN ({})
                    AND pcc.palabra_id IN (
                        SELECT palabra_id FROM palabra_clave_articulos pca
                        WHERE pca.url = a.url
                    )
                )
            """.format(','.join(['?'] * len(categories)))
            where_clauses.append(subquery)
            params.extend(categories)
        if keywords and NewsInterfaceDatabase.ALL_OPTION not in keywords:
            # Build a subquery for keywords
            subquery = """
                EXISTS (
                    SELECT 1 FROM palabra_clave_articulos pca
                    JOIN palabras_clave pc ON pca.palabra_id = pc.palabra_id
                    WHERE pc.palabra IN ({})
                    AND pca.url = a.url
                )
            """.format(','.join(['?'] * len(keywords)))
            where_clauses.append(subquery)
            params.extend(keywords)
        if start_date:
            where_clauses.append("a.fecha_publicacion >= ?")
            params.append(start_date)
        if end_date:
            #where_clauses.append("a.fecha_publicacion <= ?")
            where_clauses.append("a.fecha_publicacion < ?")
            params.append(end_date)
        where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        query = query.format(where, "", "")
        c.execute(query, params)
        return c.fetchall()


    # --- Métodos para administración ---

    def add_category(self, nombre):
        cursor = self.conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO categoria (nombre) VALUES (?)", (nombre,))
        self.conn.commit()
        if cursor.rowcount == 0:
            raise ValueError(f"La categoría '{nombre}' ya existe.")

    def delete_category(self, nombre):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM categoria WHERE nombre = ?", (nombre,))
        self.conn.commit()

    def add_keyword(self, palabra):
        cursor = self.conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO palabras_clave (palabra) VALUES (?)", (palabra,))
        self.conn.commit()
        if cursor.rowcount == 0:
            raise ValueError(f"La palabra clave '{palabra}' ya existe.")

    def associate_keyword_category(self, palabra, categoria):
        cursor = self.conn.cursor()
        cursor.execute("SELECT palabra_id FROM palabras_clave WHERE palabra = ?", (palabra,))
        palabra_id = cursor.fetchone()
        cursor.execute("SELECT categoria_id FROM categoria WHERE nombre = ?", (categoria,))
        categoria_id = cursor.fetchone()
        if palabra_id and categoria_id:
            cursor.execute("""
                INSERT OR IGNORE INTO palabras_clave_categoria (palabra_id, categoria_id)
                VALUES (?, ?)
            """, (palabra_id[0], categoria_id[0])) #(palabra_id['palabra_id'], categoria_id['categoria_id']))
            self.conn.commit()
        else:
            raise ValueError("Palabra o categoría no encontrada")

    def fetch_sources(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT fuente_id, nombre, url_home FROM fuentes ORDER BY nombre")
        #return [(row['fuente_id'], row['nombre'], row['url_home']) for row in cursor.fetchall()]
        return [(row[0], row[1], row[2]) for row in cursor.fetchall()]

    def add_source(self, fuente_id, nombre, url_home):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO fuentes (fuente_id, nombre, url_home) VALUES (?, ?, ?)
        """, (fuente_id, nombre, url_home))
        self.conn.commit()
        if cursor.rowcount == 0:
            raise ValueError(f"La fuente con ID '{fuente_id}' ya existe.")

    def fetch_sitemaps(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT url_relativa FROM sitemap ORDER BY url_relativa")
        #return [row['url_relativa'] for row in cursor.fetchall()]
        return [row[0] for row in cursor.fetchall()]

    def add_sitemap(self, url_relativa):
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO sitemap (url_relativa) VALUES (?)", (url_relativa,))
        self.conn.commit()
        if cursor.rowcount == 0:
            raise ValueError(f"El sitemap '{url_relativa}' ya existe.")

    def associate_sitemap_source(self, fuente_id, url_relativa):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO fuentes_sitemap (fuente_id, url_relativa) VALUES (?, ?)
        """, (fuente_id, url_relativa))
        self.conn.commit()

    def get_sitemaps_by_source_id(self, fuente_id):
        cursor = self.conn.cursor()
        query = """
            SELECT s.url_relativa
            FROM sitemap s
            JOIN fuentes_sitemap fs ON s.url_relativa = fs.url_relativa
            WHERE fs.fuente_id = ?
            ORDER BY s.url_relativa
        """
        cursor.execute(query, (fuente_id,))
        return [row[0] for row in cursor.fetchall()]

    def remove_keyword_from_category(self, palabra, categoria):
        cursor = self.conn.cursor()
        query = """
            DELETE FROM palabras_clave_categoria
            WHERE palabra_id = (SELECT palabra_id FROM palabras_clave WHERE palabra = ?)
              AND categoria_id = (SELECT categoria_id FROM categoria WHERE nombre = ?)
        """
        cursor.execute(query, (palabra, categoria))
        self.conn.commit()

    def delete_source(self, fuente_id):
        cursor = self.conn.cursor()
        query = "DELETE FROM fuentes WHERE fuente_id = ?"
        cursor.execute(query, (fuente_id,))
        self.conn.commit()


    def delete_sitemap(self, url_relativa):
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")  # Asegurar que FK está activado

        # Eliminar el sitemap por url_relativa
        cursor.execute("DELETE FROM sitemap WHERE url_relativa = ?", (url_relativa,))
        self.conn.commit()

    def fetch_all_keywords(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT palabra FROM palabras_clave ORDER BY palabra;")
        rows = cursor.fetchall()
        return [row[0] for row in rows] if rows else []


    def delete_keyword(self, palabra):
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        cursor.execute("DELETE FROM palabras_clave WHERE palabra = ?", (palabra,))
        self.conn.commit()


    def add_regla(self, regla):
        cursor = self.conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO reglas (descripcion) VALUES (?)', (regla.strip(),))
        self.conn.commit()
        if cursor.rowcount == 0:
            raise ValueError(f"La regla '{regla}' ya existe.")

    def exist_regla(self, regla):
        cursor = self.conn.cursor()
        cursor.execute('SELECT 1 FROM reglas WHERE descripcion = ?', (regla,))
        return cursor.fetchone() is not None


    ## Uso de stemming
    def add_keyword_stem(self, palabra):
        """Agrega palabra clave verificando stem único."""
        stem = self.get_stem(palabra.strip().lower())
        cursor = self.conn.cursor()

        # Verificar si ya existe stem
        cursor.execute("SELECT palabra_id FROM palabras_clave WHERE stem = ?", (stem,))
        existing = cursor.fetchone()

        if existing:
            raise ValueError(f"La palabra clave '{palabra}' o alguna de sus variaciones ya existe.")
            #return existing[0]  # Retorna ID existente

        # Insertar nuevo
        cursor.execute("""
            INSERT OR IGNORE INTO palabras_clave (palabra, stem)
            VALUES (?, ?)
        """, (palabra, stem))

        self.conn.commit()
        if cursor.rowcount == 0:
            raise ValueError(f"La palabra clave '{palabra}' ya existe.")


    def bulk_insert_palabras_clave_stem(self, palabras_originales):
        """
        Inserta palabras clave únicas por stem.

        Returns:
            dict: {
                'insertadas': [lista_palabras_nuevas],
                'duplicadas': [lista_palabras_que_ya_existian],
                'total_insertadas': int,
                'total_duplicadas': int
            }
        """
        cursor = self.conn.cursor()

        # Obtener stems existentes
        cursor.execute("SELECT stem FROM palabras_clave WHERE stem IS NOT NULL")
        stems_existentes = {row[0] for row in cursor.fetchall()}

        insertadas = []
        duplicadas = []

        for palabra in set(palabras_originales):
            stem = self.get_stem(palabra.strip().lower())
            if stem and stem not in stems_existentes:
                cursor.execute("""
                    INSERT INTO palabras_clave (palabra, stem) VALUES (?, ?)
                """, (palabra.strip(), stem))
                insertadas.append(palabra)
            elif stem:
                duplicadas.append(palabra)

        self.conn.commit()

        return {
            'insertadas': insertadas,
            'duplicadas': duplicadas,
            'total_insertadas': len(insertadas),
            'total_duplicadas': len(duplicadas)
        }


    def associate_keyword_stem_category(self, palabra, categoria):
        cursor = self.conn.cursor()
        stem = self.get_stem(palabra.strip().lower())
        cursor.execute("SELECT palabra_id FROM palabras_clave WHERE stem = ?", (stem,))
        palabra_id = cursor.fetchone()
        cursor.execute("SELECT categoria_id FROM categoria WHERE nombre = ?", (categoria,))
        categoria_id = cursor.fetchone()
        if palabra_id and categoria_id:
            cursor.execute("""
                INSERT OR IGNORE INTO palabras_clave_categoria (palabra_id, categoria_id)
                VALUES (?, ?)
            """, (palabra_id[0], categoria_id[0])) #(palabra_id['palabra_id'], categoria_id['categoria_id']))
            self.conn.commit()
        else:
            raise ValueError("Palabra o categoría no encontrada")


    def remove_keyword_stem_from_category(self, palabra, categoria):
        cursor = self.conn.cursor()
        stem = self.get_stem(palabra.strip().lower())
        query = """
            DELETE FROM palabras_clave_categoria
            WHERE palabra_id = (SELECT palabra_id FROM palabras_clave WHERE stem = ?)
              AND categoria_id = (SELECT categoria_id FROM categoria WHERE nombre = ?)
        """
        cursor.execute(query, (stem, categoria))
        self.conn.commit()
