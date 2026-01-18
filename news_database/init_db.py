from urllib.parse import urljoin, urlparse
from news_database.news_db import NewsDatabase
from news_database.utils import get_base_url, get_domain, split_rule_text
import pandas as pd
import os

# Metadata: nombre y sitemap
# Urls deben de terminar en /
METADATA_URLS={
    'https://www.elcomercio.es/': {'nombre': 'El Comercio'},
    'https://www.lne.es/': {'nombre': 'La Nueva España'},
    'https://www.lavanguardia.com/': {'nombre': 'La Vanguardia',
                              'sitemap': ['sitemap-google-news.xml',
                                          'sitemap-news-agencias.xml'] +
                              [f'sitemap-news-agencias-{i}.xml' for i in range(1, 10)]},
    'https://www.larazon.es/': {'nombre': 'La Razón'},
    'https://www.rtpa.es/': {'nombre': 'Radiotelevisión del Principado de Asturias (RTPA)',
                              'sitemap': 'sitemap-noticias.xml'},
    'https://www.europapress.es/': {'nombre': 'Europa Press'},
    'https://www.20minutos.es/': {'nombre': '20 Minutos',
                                  'sitemap': 'sitemap-google-news.xml'},
    'https://www.elperiodico.com/' : {'nombre': 'El Periódico',
                                      'sitemap': 'google-news.xml'},
    'https://www.eldiario.es/' : {'nombre': 'ElDiario.es'},
    'https://www.elconfidencial.com/': {'nombre': 'El Confidencial',
                                        'sitemap': 'newsitemap_4.xml'},
    'https://www.culturalgijonesa.org/': {'nombre': 'Cultural Gijonesa'},
    'https://www.elespanol.com/' : {'nombre': 'El Español',
                                    'sitemap': 'sitemap_google_news.xml'},
    'https://www.nortes.me/': {'nombre': 'Nortes'},
    'https://www.tribunasalamanca.com/': {'nombre': 'Tribuna Salamanca'},
    'https://migijon.com/': {'nombre': 'Mi Gijón'},
    'https://www.infobae.com/' : {'nombre': 'Infobae',
                                  'sitemap': 'arc/outboundfeeds/news-sitemap2/'},
    'https://www.telecinco.es/' : {'nombre': 'Telecinco'},
    'https://www.laprovincia.es/' : {'nombre': 'La Provincia'},
    'https://www.laopiniondemalaga.es/': {'nombre': 'La Opinión de Málaga'},
    'https://www.elfielato.es/': {'nombre': 'El Fielato y El Nora'},
    'https://www.teleprensa.com/': {'nombre': 'Teleprensa'},
    'https://forbes.es/': {'nombre': 'Forbes'},
    'https://itg.es/': {'nombre': 'ITG centro tecnológico'},
    'https://www.eldia.es/': {'nombre': 'El Dia'},
    'https://www.nationalgeographic.com.es/': {'nombre': 'Historia National Geographic'},
    'https://iymagazine.es/': {'nombre': 'IyMagazine'},
    'https://www.lacerca.com/': {'nombre': 'La Cerca'},
    'https://www.tribunaavila.com/': {'nombre': 'Tribuna de Ávila'},
    'https://www.latribunadealbacete.es/': {'nombre': 'La Tribuna de Albacete'},
    'https://www.eldiariomontanes.es/': {'nombre': 'El Diario Montañés'},
    "https://www.larioja.com/":{"nombre": "La Rioja"},
    "https://www.almerianoticias.es/":{"nombre": "Almería Noticias"},
    "https://news.ual.es/":{"nombre": "UALNEWS"},
    "https://www.farodevigo.es/":{"nombre": "Faro de Vigo"},
    "https://www.diariocordoba.com/": {'nombre': 'Diario Córdoba'},
    "https://www.diariodeibiza.es/": {'nombre': 'Diario de Ibiza'},
    "https://segoviaudaz.es/": {'nombre': 'SegoviAudaz'},
    'https://www.noticiasde.es/': {'nombre': 'NoticiasDe'},
    'https://www.redaccionmedica.com/': {'nombre': 'Redacción Médica'},
    'https://cronicadeandalucia.com/': {'nombre': 'Crónica de Andalucía'},
    'https://www.diariodevalderrueda.es/': {'nombre': 'Diario de Valderrueda'},
    'https://galiciadigital24horas.com/': {'nombre': 'Galicia Digital 24 Horas'},
    'https://www.laescena.es/': {'nombre': 'La Escena'},
    'https://valencia24horas.com/': {'nombre': 'Valencia 24 Horas'},
    'https://www.eleconomista.es/': {'nombre': 'El Economista'},
    'https://www.ondacero.es/': {'nombre': 'Onda Cero'},
    'https://www.laopinioncoruna.es/': {'nombre': 'La Opinión A Coruña'},
    'https://economia3.com/': {'nombre': 'Economía 3'},
    'https://inmediaciones.org/': {'nombre': 'Inmediaciones' },
    'https://www.sport.es/': {'nombre': 'Sport'},
    'https://ort-ort.com/': {'nombre': 'ORT Noticias del Occidente de Asturias'},
    'https://ethic.es/': {'nombre': 'Ethic'},
    'https://www.tribunaavila.com/': {'nombre': 'Tribuna Ávila'},
    'https://www.laopiniondezamora.es/': {'nombre': 'La Opinión de Zamora'},
    'https://www.elcorreogallego.es/': {'nombre': 'El Correo Gallego'},
    'https://www.informacion.es/': {'nombre': 'INFORMACIÓN'},
    'https://www.elperiodicoextremadura.com/': {'nombre': 'El Periódico Extremadura'},
    'https://www.laopiniondemurcia.es/': {'nombre': 'La Opinión de Murcia'},
    'https://www.levante-emv.com/': {'nombre': 'Levante-EMV' },
    'https://www.diariodemallorca.es/': {'nombre': 'Diario de Mallorca'},
    'https://www.elperiodicodearagon.com/': {'nombre': 'El Periódico de Aragón'},
    'https://www.latribunadealbacete.es/': {'nombre': 'La Tribuna de Albacete'},
    'https://www.diariosur.es/': {'nombre': 'Diario Sur'},
    'https://www.malagahoy.es/': {'nombre': 'Málaga Hoy'},
    'https://www.superdeporte.es/': {'nombre': 'Superdeporte'},
    'https://www.elperiodicomediterraneo.com/': {'nombre': 'El Periódico Mediterráneo'},
    'https://www.salamanca24horas.com/': {'nombre': 'Salamanca 24 Horas' },
    'https://www.lacerca.com/': {'nombre': 'La Cerca'},
    'https://deportesmenorca.com/': {'nombre': 'Deportes Menorca'},
    'https://menorcaaldia.com/': {'nombre': 'Menorca al Día'},
    'https://www.lanzadigital.com/': {'nombre': 'Lanza Digital'},
    'https://www.menorca.info/': {'nombre': 'MENORCA'},
    'https://www.lacomarcadepuertollano.com/': {'nombre': 'La Comarca de Puerto Llano'},
    'https://www.gciencia.com/': {'nombre': 'GCiencia'},
    'https://www.eldebate.com/': {'nombre': 'El Debate', 'sitemap': 'sitemap-index.xml' },
    'https://www.magisnet.com/': {'nombre': 'Magisnet', 'sitemap': 'sitemap_index.xml'},
    'https://www.corresponsables.com/': {'nombre': 'Corresponsables',
                                          'sitemap': [f'post-sitemap{i}.xml' for i in range(30, 50)]},
    'https://www.diariodepontevedra.es/': {'nombre': 'Diario de Pontevedra'},
    'https://www.infoclm.es/': {'nombre': 'Periódico digital de Castilla-La Mancha'},
    'https://mariskalrock.com/': {'nombre': 'MariskalRock'},
    'https://salamancartvaldia.es/': {'nombre': 'Salamanca al Día'},
    'https://diariofarma.com/': {'nombre': 'Diario Farma'},
    'https://valenciaplaza.com/': {'nombre': 'Valencia Plaza'},
    'https://www.mundiario.com/': {'nombre': 'Mundiario'},
    'https://www.farmaventas.es/': {'nombre': 'Farmaventas',
                                    'sitemap': 'sitemap.xml'},
    'https://elportaluco.com/': {'nombre': 'El Portaluco'},
    'https://entomelloso.com/': {'nombre': 'Tomelloso'},
    'https://www.granadadigital.es/': {'nombre': 'Granada Digital',
                                        'sitemap': 'sitemap_index.xml'},
    'https://www.lavozdegalicia.es/': {'nombre': 'La Voz de Galicia'},
    'https://www.asturiasmundial.com/': {'nombre': 'Asturias Mundial'},
    'https://www.lagacetadesalamanca.es/': {'nombre': 'La Gaceta de Salamanca'},
    'https://noticias.juridicas.com/': {'nombre': 'Noticias Jurídicas'},
    'https://fernandezrozas.com/': {'nombre': 'Blog de José Carlos Fernández Rozas',
                                    'sitemap': 'news-sitemap.xml'},
    'https://www.abc.es/': {'nombre': 'ABC', 'sitemap': 'sitemap.xml'},
    'https://enfoques.gal/':{'nombre': 'enfoques.gal'},
    'https://periodicodeltalento.com/': {'nombre': 'Talento Periódico'},
    'https://www.vozpopuli.com/': {'nombre': 'Voz Populi'},
    'https://www.diariodelpuerto.com/': {'nombre': 'Diario del Puerto'},
     #newspaper
    'https://www.lavozdeasturias.es/': {'nombre': 'La Voz de Asturias'},
    'https://cualia.es/': {'nombre': 'Cualia'},
    'http://www.gentedigital.es/' : {'nombre': 'Gente Digital'},
    'https://www.8directo.com/':{'nombre': '8directo'},
    'https://www.lanuevacronica.com/': {'nombre': 'La Nueva Crónica'},
    'https://elpais.com/': {'nombre': 'El Pais'},
    'https://www.elperiodicodecanarias.es/': {'nombre': 'El Periodico de Canarias'},
    'https://www.immedicohospitalario.es/': {'nombre': 'IM Médico'},
    'https://bilbao24horas.com/': {'nombre': 'Bilbao 24 Horas'},
    'https://www.diariodeleon.es/': {'nombre': 'Diario de León'},
    'https://cronicaeconomica.com/': {'nombre': 'Crónica Económica'},
    'https://ahoraleon.com/': {'nombre': 'Ahora León'},
    "https://www.feb.es/": {'nombre': 'Federación Española de Baloncesto'},
    'https://www.consalud.es/': {'nombre': 'ConSalud'},
    'https://www.rrhhdigital.com/': {'nombre': 'RRHH Digital'},
    'https://www.diariomedico.com/': {'nombre': 'Diario Médico'},
    'https://vivirediciones.es/': {'nombre': 'Vivir Ediciones'},
    'https://www.granadahoy.com/': {'nombre': 'Granada Hoy'},
    'https://www.espormadrid.es/': {'nombre': 'Es por madrid'},
    'https://www.granadaesnoticia.com/': {'nombre': 'Granada es noticia'},
    'https://novaciencia.es/': {'nombre': 'Nova Ciencia'},
    'https://www.industriaquimica.es/': {'nombre': 'Industria Química'},
    'https://www.bolsamania.com/': {'nombre': 'Bolsamania',
                                    'sitemap': 'sitemap.xml'},
    'https://www.rutapesquera.com/': {'nombre': 'Ruta Pesquera'},
    'https://www.santanderdigital24horas.com/': {'nombre': 'Santander Digital 24 Horas'},
    'https://isanidad.com/': {'nombre': 'iSanidad'},
    'https://www.vocesdecuenca.com/': {'nombre': 'Voces de Cuenca'}
}

def get_news_sources_information():
    sitemap_data = []
    fuentes_data = []
    fuentes_sitemap_data = []

    for news_url, metadata in METADATA_URLS.items():
        base_url = get_base_url(news_url)
        domain_url = get_domain(news_url)
        news_name = metadata.get("nombre", "")
        sitemap_names = metadata.get("sitemap", None)
        fuentes_data.append( (domain_url, news_name, base_url))
        if sitemap_names:
            if isinstance(sitemap_names, str):
              sitemap_names = [sitemap_names]
            for sitemap_name in sitemap_names:
                sitemap_url = urljoin(base_url, sitemap_name)
                sitemap_data.append( (sitemap_url,) )
                fuentes_sitemap_data.append( (domain_url, sitemap_url) )

    return sitemap_data, fuentes_data, fuentes_sitemap_data


def initialize_news_sources(news_db):
    sitemap_data, fuentes_data, fuentes_sitemap_data = get_news_sources_information()
    news_db.bulk_insert_sitemap(sitemap_data)
    news_db.bulk_insert_fuentes(fuentes_data)
    news_db.bulk_insert_fuentes_sitemap(fuentes_sitemap_data)
    print(f"Se insertaron {len(fuentes_data)} fuentes correctamente.")
    

def initialize_keywords_categories_rules_db(ruta_excel, news_db):
    palabras_clave_df = pd.read_excel(ruta_excel)

    lista_categorias = list(palabras_clave_df.columns)

    # Obtener palabras por categorías y reglas
    reglas = set()
    palabras_clave = set()
    palabras_clave_categoria = {}
    regla_keywords_map = {}  # {regla: [(palabra, operador, posicion), ...]}

    for categoria in lista_categorias:
        reglas_categoria = set(palabras_clave_df[categoria].dropna())
        reglas.update(reglas_categoria)

        palabras_categoria = set()
        for regla in reglas_categoria:
            palabras_clave_regla, operator = split_rule_text(regla)
            palabras_clave.update(set(palabras_clave_regla))

            # Map keywords to rule with position and operator
            if regla not in regla_keywords_map:
                regla_keywords_map[regla] = []
            for pos, palabra in enumerate(palabras_clave_regla, 1):
                regla_keywords_map[regla].append((palabra, operator if operator is not None else '-', pos))

            if operator == 'o':
                palabras_categoria.update(palabras_clave_regla)
            else:
                # La primera palabra es la asignada a la categoria (AND case)
                # Tambien funciona para palabras clave solas
                palabra_clave_principal = palabras_clave_regla[0]
                palabras_categoria.add(palabra_clave_principal)

        palabras_clave_categoria[categoria] = list(palabras_categoria)

    # Insertar categorías y palabras clave en base de datos
    news_db.bulk_insert_categorias(lista_categorias)
    news_db.bulk_insert_palabras_clave_stem(palabras_clave)

    # Insertar relación palabras clave y categorías
    for categoria, palabras in palabras_clave_categoria.items():
        if len(palabras) == 0:
            continue
        categoria_id = news_db.get_category(categoria)
        palabras_clave_ids = news_db.get_keyword_ids_stemmed(palabras)
        lista_categoria_palabra = [(ids_tupla, categoria_id) for ids_tupla in palabras_clave_ids if ids_tupla]
        news_db.bulk_insert_palabra_clave_categorias(lista_categoria_palabra)

    print(f"Se insertaron {len(palabras_clave)} palabras clave y {len(palabras_clave_categoria)} categorias correctamente.")

    # Insertar reglas
    news_db.bulk_insert_regla(reglas)
    print(f"Se insertaron {len(reglas)} reglas correctamente.")

    # Insertar relaciones regla-palabras_clave
    for regla, keyword_list in regla_keywords_map.items():
        news_db.associate_rule_keywords(regla, keyword_list)
    print(f"Se asociaron reglas con {sum(len(kw) for kw in regla_keywords_map.values())} keywords.")


if __name__ == "__main__":

    # Obtener ruta absoluta del directorio donde está el script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # Construir ruta absoluta al archivo Excel en la misma carpeta
    ruta_excel = os.path.join(base_dir, "clipping-palabras-clave.xlsx")
    # Construye la ruta completa para la base de datos dentro de este directorio
    db_path = os.path.join(base_dir, 'news.db')

    # Crear tablas de la base de datos
    news_db = NewsDatabase(db_path=db_path)
    news_db.create_tables()

    # Inicializar fuentes con sitemaps
    initialize_news_sources(news_db)

    # Inicializar palabras clave y categorias
    initialize_keywords_categories_rules_db(ruta_excel, news_db)

    news_db.close_db()

    print(f"Se inicializó correctamente la base de datos")
