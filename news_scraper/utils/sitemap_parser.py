import scrapy
import gzip
from urllib.parse import urljoin
from scrapy import Request
from scrapy.selector import Selector
from scrapy.http import TextResponse
from news_scraper.utils.utils import *
from news_scraper.constants import INVALID_URL_WORDS, SITEMAP_NAMESPACES
from news_database.news_db import NewsDatabase

class SitemapParser:
    def __init__(self, from_date, news_db=None):
        self.from_date = from_date
        self.news_db = news_db


    def parse_sitemap(self, response, domain=None):
        """
        Procesa un sitemap para extraer URLs de noticias.
        """
        # Procesar sitemaps anidados
        yield from self._process_nested_sitemaps(response, domain)

        # Verificar que el archivo tenga noticias recientes
        if not self._has_recent_news(response):
            #print(f"No se tienen noticias recientes en {response.url}")
            return

        # Extraer datos de cada tag <url>
        yield from self._extract_url_data(response, domain)


    def _process_nested_sitemaps(self, response, domain=None):
        """
        Procesa los sitemaps anidados dentro de un sitemap.
        """
        if isinstance(response, Selector):
            # Si es un selector, usar sus métodos directamente
            sitemap_selectors = response.xpath('//ns:sitemap', namespaces=SITEMAP_NAMESPACES)
        elif isinstance(response, TextResponse):
            # Si es una TextResponse, acceder a su selector
            sitemap_selectors = response.selector.xpath('//ns:sitemap', namespaces=SITEMAP_NAMESPACES)
        else:
            print(f"La respuesta no es un TextResponse ni un Selector. {type(response)}")
            return

        for sitemap in sitemap_selectors:
            sitemap_loc = sitemap.xpath('./ns:loc/text()', namespaces=SITEMAP_NAMESPACES).get()

            # Omitir sitemaps antiguos
            last_mod = sitemap.xpath('./ns:lastmod/text()', namespaces=SITEMAP_NAMESPACES).get()
            if last_mod:
                lastmod_norm = normalize_date(last_mod)
                if lastmod_norm.date() < self.from_date:
                    continue

            # Algunas sitemaps no contiene la url completa, agregar base_url
            if not is_full_url(sitemap_loc):
                sitemap_loc = urljoin(domain, sitemap_loc)

            # Verificar que la url es un archivo xml
            file_extension = get_url_extension(sitemap_loc)

            if file_extension.lower() != '.gz' and is_valid_sitemap_url(sitemap_loc, INVALID_URL_WORDS):
                #print(f'Se encontró otro xml dentro del archivo actual: {sitemap_loc}') # varias salidas
                if sitemap_loc:
                    # Explorar recursivamente otros archivos xml
                    yield scrapy.Request(sitemap_loc, callback=lambda response: self.parse_sitemap(response, domain) )
            elif file_extension.lower() == '.gz'  and is_valid_sitemap_url(sitemap_loc, INVALID_URL_WORDS):
                print(f"Se encontró archivo comprimido {sitemap_loc}")
                yield scrapy.Request(sitemap_loc, callback=lambda response: self.parse_sitemap_gz(response, domain) )


    def parse_sitemap_gz(self, response, domain=None):
        compressed_file = response.body
        decompressed_file = gzip.decompress(compressed_file).decode("utf-8")
        selector = Selector(text=decompressed_file, type="xml")
        yield from self.parse_sitemap(selector, domain)


    def _has_recent_news(self, response):
        if isinstance(response, Selector):
            # Si es un selector, usar sus métodos directamente
            selector_list = response.xpath('//ns:url', namespaces=SITEMAP_NAMESPACES)
        elif isinstance(response, TextResponse):
            # Si es una TextResponse, acceder a su selector
            selector_list = response.selector.xpath('//ns:url', namespaces=SITEMAP_NAMESPACES)
        else:
            print(f"La respuesta no es un TextResponse ni un Selector. {type(response)}")
            return

        if len(selector_list) < 1:
            return False

        selector_list = self._sort_selector_by_date(selector_list)
        #start_index = 0 if len(selector_list) == 1 else 1
        start_index = 0
        first_publication_date = self._get_publication_date(selector_list[start_index])
        last_publication_date = self._get_publication_date(selector_list[-1])

        # Validar que ambas fechas existan
        #if not first_publication_date or not last_publication_date:
        #    return False

        # Validar que la primera y última fecha no sean menores que la fecha límite
        if first_publication_date and last_publication_date:
            #if not (first_publication_date >= self.from_date and last_publication_date >= self.from_date):
            if first_publication_date < self.from_date and last_publication_date < self.from_date:
                return False

        # Verificar que las urls no hayan sido rastreadas anteriormente
        first_url = selector_list[start_index].xpath('./ns:loc/text()', namespaces=SITEMAP_NAMESPACES).get()
        last_url = selector_list[-1].xpath('./ns:loc/text()', namespaces=SITEMAP_NAMESPACES).get()
        crawled_first_url = self.news_db.is_crawled_url(first_url)
        crawled_last_url = self.news_db.is_crawled_url(last_url)

        if crawled_first_url and crawled_last_url:
            return False
        return True

    def _extract_url_data(self, response, domain):
        """
        Extraer data de cada tag <url> en los mapas de sitio.
        """
        selector_list = response.xpath('//ns:url', namespaces=SITEMAP_NAMESPACES)


        # Encontrar urls no rastreadas
        crawled_urls = []
        explore_selector_list = []
        for url in selector_list:
            loc = url.xpath('./ns:loc/text()', namespaces=SITEMAP_NAMESPACES).get()
            # Si ya fue rastreada la url continuar con las siguientes
            if self.news_db.is_crawled_url(loc):
                continue
            explore_selector_list.append(url)
            crawled_urls.append(loc)
            
            # Agregar las urls no exploradas a la base de datos
            if len(crawled_urls) > 5:
                self.news_db.bulk_insert_crawled_urls(crawled_urls)
                crawled_urls.clear()


        # Extraer el contenido de las urls no exploradas
        for url in explore_selector_list:
            loc = url.xpath('./ns:loc/text()', namespaces=SITEMAP_NAMESPACES).get()
            fecha_publicacion = self._get_publication_date(url, normalize=False)
            fecha_publicacion_norm = normalize_date(fecha_publicacion) if fecha_publicacion else None

            if fecha_publicacion and fecha_publicacion_norm.date() < self.from_date:
                continue

            #if loc and is_full_url(loc) and fecha_publicacion and fecha_publicacion_norm.date() >= self.from_date:
            if loc and is_full_url(loc):
                titulo = url.xpath('./news:news/news:title/text()', namespaces=SITEMAP_NAMESPACES).get() or ""
                fuente = get_domain(domain)
                # Solicitud para obtener información de la página actual
                yield scrapy.Request(
                    url=loc,
                    callback=self.parse_article,
                    meta={
                        'fuente': fuente,
                        'url': loc,
                        'titulo': titulo,
                        'fecha_publicacion': fecha_publicacion_norm,
                    }
                )

    def parse_article(self, response):
        """
          Extracción del contenido principal de un articulo
        """
        texto = self._extract_text(response)

        if not texto: # Si no se encuentra texto
            return

        # Obtener metadata
        fuente = response.meta['fuente']
        url = response.meta['url']
        titulo = response.meta['titulo']
        fecha_publicacion = response.meta['fecha_publicacion']

        # Extrar título de la página
        if titulo is None or titulo.strip() == "":
            titulo = self._extract_title(response)

        # Extraer fecha
        if not fecha_publicacion:
            fecha_publicacion = self._extract_publication_date(response)

        # Si no se encuentra titulo ni fecha entonces se considera inválido
        if not titulo or not fecha_publicacion:
            return

        if fuente == "www.feb.es": #caso especial con fecha distanta al as demás
            dayfirst = False
        else:
            dayfirst = True

        # Normalizar fecha
        fecha_publicacion_norm = normalize_date(fecha_publicacion, dayfirst=dayfirst) if fecha_publicacion else None

        # Validar fecha de publicación
        if not fecha_publicacion_norm:
            return

        if fecha_publicacion_norm and fecha_publicacion_norm.date() < self.from_date:
            return


        yield {
            'fuente': fuente,
            'url': url.strip(),
            'titulo': titulo.strip(),
            'fecha_publicacion': fecha_publicacion_norm,
            'texto': texto,
        }


    def _extract_text(self, response):
        """
          Extracción del texto principal del artículo (personalizable por sitio)
        """
        selectors = [
            'p::text',
            'p span::text',
            'div.cuerpo::text'
        ]
        for selector in selectors:
            if '::' in selector:  # CSS
                text = ' '.join(response.css(selector).getall()).strip()
            else:  # XPath
                text = ' '.join(response.xpath(selector).getall()).strip()
            if text:
                return text
        return ""

    def _extract_title(self, response):
        """
          Extracción de título de respaldo usando patrones comunes
        """

        selectors = [
            'meta[property="og:title"]::attr(content)',
            'title::text',
            'h2.titular::text',
            'h1 span::text',
            'h3.entry-title::text',
            'h1 a::text',
            'h1::text',
        ]
        for selector in selectors:
            title = ' '.join(response.css(selector).getall())
            if title:
                return title.strip()
        return ""


    def _extract_publication_date(self, response):
        """
          Extracción de fecha de publicación usando patrones comunes
        """

        selectors = [
            'meta[property="article:published_time"]::attr(content)',
            'time::attr(datetime)',
            '//p[@class="firma"]/text()[2]',
            'p.firma::text',
            'h2.date-header span::text',
            'ul.post-tags li:first-child::text',
            '.entry-meta-date a::text',
            '.item-metadata.posts-date a::text',
            '//div[@class="fDescripcionA2"]//i[contains(@class, "icon-clock")]/following-sibling::text()[1]',
            'span.fecha-noticia::text',
            'div.single-post-meta span.date::text',
            '//div[contains(@class, "meta mb autor")]//i[contains(@class, "fa-calendar-o")]/following-sibling::text()',
            'span.fw-bold::text',
            'span.date::text',
            'div.fecha::text'
        ]

        for selector in selectors:
            if '::' in selector and '//' not in selector:  # CSS
                date = response.css(selector).get()
            else:  # XPath
                date = response.xpath(selector).get()
            if date:
                return date.strip()
        return ""



    def _get_publication_date(self, selector, normalize=True):
        """
        Obtiene la fecha de publicación de un selector.
        """
        fecha = selector.xpath('./news:news/news:publication_date/text()', namespaces=SITEMAP_NAMESPACES).get()
        if not fecha:
            fecha = selector.xpath('./ns:lastmod/text()', namespaces=SITEMAP_NAMESPACES).get()

        if not fecha:
            return None

        return normalize_date(fecha).date() if normalize else fecha


    def _get_source(self, selector, domain):
        """
        Obtiene la fuente de la noticia.
        """
        fuente = selector.xpath('./news:news/news:publication/news:name/text()', namespaces=SITEMAP_NAMESPACES).get()
        if domain:
            domain_base = get_base_url(domain)
            source_name = self.news_db.get_source_name(domain_base)
            if source_name:
                fuente = source_name
        return fuente or ""


    def _sort_selector_by_date(self, selector_list):
        selectors_with_dates = []
        for sel in selector_list:
            date_obj = self._get_publication_date(sel, normalize=True)
            if date_obj: # Solo si la fecha es válida (no None)
                selectors_with_dates.append((date_obj, sel))
            else:
                selectors_with_dates.append((datetime.min.date(), sel))

        if not selectors_with_dates:
            return []

        dates_only = [item[0] for item in selectors_with_dates]
        is_already_sorted = all(dates_only[i] >= dates_only[i+1] for i in range(len(dates_only) - 1))

        if not is_already_sorted:
            #print("La lista de selectores no está ordenada, procediendo a ordenar...")
            selectors_with_dates.sort(key=lambda x: x[0], reverse=True)
            sorted_selector_list = [item[1] for item in selectors_with_dates]
            return sorted_selector_list
        else:
            #print("La lista de selectores ya está ordenada por fecha (decreciente).")
            return [item[1] for item in selectors_with_dates]