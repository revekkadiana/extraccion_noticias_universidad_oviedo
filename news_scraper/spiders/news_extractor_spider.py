import scrapy
from newspaper import build, Config, Article
import requests
from urllib.parse import urljoin, urlparse
import os
import time
import re
from datetime import datetime
import gzip
from scrapy.selector import Selector
from scrapy.http import TextResponse
from news_scraper.utils.sitemap_parser import SitemapParser
from news_scraper.constants import INVALID_URL_WORDS
from news_scraper.utils.utils import *
from news_database.news_db import NewsDatabase
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.project import get_project_settings

class NewsUrlExtractorSpider(scrapy.Spider):
    # Nombre de spider usado al momento de ejecutar
    name = 'news_extractor'

    def __init__(self, fecha_inicio=None, *args, **kwargs):
        super(NewsUrlExtractorSpider, self).__init__(*args, **kwargs)

        # Fecha de inicio válida para filtrar urls
        self.from_date = self._initialize_start_date(fecha_inicio)
        print(f"Fecha de inicio: {self.from_date}")

        settings = get_project_settings()
        db_path = settings.get('DATABASE_PATH')

        self.news_db = NewsDatabase(db_path)
        self.sitemap_parser = SitemapParser(self.from_date, self.news_db)
        # Remover articulos anteriores a limit_date
        #self.news_db.remove_old_articles(self.from_date)

        # Actualizar espacio de la base de datos
        #self.news_db.vacuum_database()


    def _initialize_start_date(self, fecha_inicio=None):
        if fecha_inicio:
            return datetime.strptime(fecha_inicio, '%Y-%m-%d').date()
        return subtract_days_from_date(get_current_date(), days=1)


    async def start(self):
        news_urls = self.news_db.get_news_urls()
        for url in news_urls:
            yield scrapy.Request(url=url, callback=self.parse, errback=self.handle_error_start_request)


    def start_requests(self):
        self.start()


    def handle_error_start_request(self, failure):
        self.logger.error(f"Request fallida: {failure}")
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error(f"HTTP Error {response.status} en URL: {response.url}")
            yield from self._get_news_urls(response.url)


    def parse(self, response):
        robots_url = urljoin(response.url, "/robots.txt")
        print(f'Robots URL: {robots_url}')
        yield scrapy.Request(robots_url, callback=self.parse_robots,
                             meta={'domain': response.url},
                             errback=lambda failure: self.handle_error(failure, response.url))


    def parse_robots(self, response):
        if response.status != 200:
            print(f"Error al acceder a la url: {response.status}.")
            return

        domain = response.meta['domain']
        domain_base = get_base_url(domain)
        robots_text = response.text
        urls_sitemap = extract_sitemap_urls(robots_text, INVALID_URL_WORDS)

        if len(urls_sitemap) > 0:
            print(f"Se encontraron los siguientes enlaces xml: {urls_sitemap}")
            yield from self._process_urls_sitemap(urls_sitemap, domain)

        elif self.news_db.source_has_sitemap(domain_base):
            # Si no hay sitemaps en robots.txt, buscar en base de datos de sitemaps
            print("No se encontraron enlaces xml en robots.txt.", end=" ")
            yield from self._process_sitemap_from_metadata(domain_base, domain)

        else:
            print("No se encontraron enlaces xml. Accediendo a enlaces con librería Newspaper...")
            # Si no se encuentra ningún mapa del sitio,
            # utilizar Newspaper para obtener las URL de los artículos de noticias
            yield from self._get_news_urls(domain)


    def _process_urls_sitemap(self, urls_sitemap, domain):
        for enum, sitemap_url in enumerate(urls_sitemap):
            print(f"Accediendo al siguiente enlace... {sitemap_url}")

            if get_url_extension(sitemap_url).lower() == '.gz' and is_valid_sitemap_url(sitemap_url, INVALID_URL_WORDS):
                # Archivo comprimido en .gz
                print(f"Se encontró archivo comprimido {sitemap_url}")
                yield scrapy.Request(sitemap_url,
                                     callback=lambda response: self.sitemap_parser.parse_sitemap_gz(response, sitemap_url),
                                     errback=(lambda failure: self.handle_error(failure, sitemap_url)) if enum == 0 else None)
            else:
                # Archivo xml
                yield scrapy.Request(sitemap_url,
                                     callback=lambda response: self.sitemap_parser.parse_sitemap(response, domain),
                                     errback=(lambda failure: self.handle_error(failure, domain))  if enum == 0 else None )


    def _process_sitemap_from_metadata(self, domain_base, domain):
        sitemap_names = self.news_db.get_sitemaps_for_source_simple(domain_base)
        for sitemap_name in sitemap_names:
            url_sitemap = urljoin(domain_base, sitemap_name)
            print(f"Accediendo al siguiente enlace especificado... {url_sitemap}")
            yield scrapy.Request(url_sitemap,
                                 callback=lambda response: self.sitemap_parser.parse_sitemap(response, domain_base) )


    def handle_error(self, failure, domain=None):
        if failure and failure.value and failure.value.response:
            print(f"Request fallida: {failure.value.response.status} para {failure.request.url}")
        else:
            print(f"Request fallida para {failure.request.url}")
        print(f"Accediendo a enlaces con librería Newspaper...")
        if domain is None:
            domain = failure.request.url
        yield from self._get_news_urls(domain)


    def _configure_newspaper(self):
        """
        Configura la librería Newspaper.
        """
        all_user_agents = self.settings.getlist('USER_AGENTS') # Use getlist for list settings
        user_agent = None
        if all_user_agents:
            # Se puede mejorar para probar más de un user-agent
            user_agent = all_user_agents[0]

        config = Config()
        config.request_timeout = 5
        config.language = 'es'
        config.thread_timeout_seconds = 5
        config.memoize_articles = False
        config.fetch_images = False
        config.follow_meta_refresh = True
        config.number_threads = 4
        config.browser_user_agent = user_agent
        config.headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
        return config


    def _get_news_urls(self, domain):
        config = self._configure_newspaper()
        try:
            print(f'Extrayendo noticias con librería Newspaper para url: {domain}')
            start_time = time.time()
            paper = build(domain, config=config)
            print(f"--- {(time.time() - start_time):.2f}s segundos ---")
            articulos_urls = set(paper.article_urls())
            print(f'Se encontraron {len(articulos_urls)} enlaces de noticias.')

            #fuente = self._get_source_from_domain(domain)
            fuente = get_domain(domain)

            crawled_urls = []
            for articulo_url in articulos_urls:
                if not self.news_db.is_crawled_url(articulo_url):
                    crawled_urls.append(articulo_url)
                    yield scrapy.Request(
                        url=articulo_url,
                        callback=self.sitemap_parser.parse_article,
                        meta={
                            'fuente': fuente,
                            'url': articulo_url,
                            'titulo': '',
                            'fecha_publicacion': None,
                        }
                    )

            if len(crawled_urls) > 0:
                self.news_db.bulk_insert_crawled_urls(crawled_urls)

        except Exception as e:
            print(f"No se pudieron obtener noticias de {domain}")


    def _get_source_from_domain(self, domain):
        if domain is None:
            return ""

        domain_base = get_base_url(domain)
        source_name = self.news_db.get_source_name(domain_base)
        if source_name:
            return source_name
        return ""
