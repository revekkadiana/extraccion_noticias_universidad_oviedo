import streamlit as st
import pandas as pd
from datetime import datetime,date, timedelta
from news_database.utils import get_domain, split_rule_text
from urllib.parse import urlparse, urlunparse
from thefuzz import fuzz
import subprocess
import threading
import time
import os
from datetime import datetime
from datetime import time as dt_time
import queue
import urllib.parse
import re
# Cargar modelo de spaCy para español
from unidecode import unidecode
from nltk.stem.snowball import SnowballStemmer
#import spacy

# Inicializar
stemmer = SnowballStemmer("spanish")

#nlp_fast = spacy.load("es_core_news_sm")
#nlp_fast.disable_pipes("ner", "parser")

APP_URL = "http://156.35.163.135:80"#"https://localhost:8508"

#-------- Scrapy tab ---------
LOG_FILE = "news_scraper/output.log"
output_queue = queue.Queue()

def run_scrapy_and_log(q):
    start_time = datetime.now()
    with open(LOG_FILE, "w", encoding="utf-8") as log_file:
        #cmd = ["scrapy", "crawl", "news_extractor", "-L", "DEBUG", "-s", "ROBOTSTXT_OBEY=False"]
        cmd = ["scrapy", "crawl", "news_extractor", "-s", "LOG_ENABLED=False", "-s", "ROBOTSTXT_OBEY=False"]
        process = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT, text=True)
        process.wait()
    end_time = datetime.now()
    elapsed = end_time - start_time
    q.put(elapsed)
    q.put(None)  # Señal de fin

def scrapy_tab():
    st.subheader("Ejecutar extractor de artículos")

    if 'scrapy_running' not in st.session_state:
        st.session_state['scrapy_running'] = False
    if 'elapsed_time' not in st.session_state:
        st.session_state['elapsed_time'] = None

    if st.session_state.scrapy_running:
        st.info("El extractor está ejecutándose, por favor espera...")

        try:
            elapsed = output_queue.get_nowait()
            if elapsed is not None:
                st.session_state.elapsed_time = elapsed
            else:
                # Fin de proceso
                st.session_state.scrapy_running = False
                st.success("Artículos extraídos exitosamente.")
                time.sleep(2)
                st.rerun()
        except queue.Empty:
            pass

    else:
        if st.button("Iniciar extracción de artículos"):
            st.session_state.scrapy_running = True
            st.session_state.elapsed_time = None
            threading.Thread(target=run_scrapy_and_log, args=(output_queue,), daemon=True).start()
            st.rerun()

    if not st.session_state.scrapy_running and os.path.exists(LOG_FILE):
        with open(LOG_FILE, "rb") as f:
            st.download_button(
                label="Descargar archivo de log",
                data=f,
                file_name="output.log",
                mime="text/plain"
            )

    if st.session_state.elapsed_time:
        st.success(f"Extracción finalizada en: {str(st.session_state.elapsed_time).split('.')[0]} (HH:MM:SS)")


#-------- Agrupar titulos similares ---------
def remove_url_fragment(url):
    parsed_url = urlparse(url)
    return urlunparse(parsed_url._replace(fragment=''))


'''
# Lematizacion
def preprocesar_titulos(titulos_list, batch_size=100):
    """
    Normaliza título en español:
    - Minúsculas y elimina acentos
    - Elimina puntuación
    - Elimina stopwords, espacios
    - Lematizacion por lotes
    """
    clean_titles = []
    for title in titulos_list:
        title = unidecode(title.lower().strip())
        title = re.sub(r'[^\w\s]', ' ', title)  # Remover puntuación
        clean_titles.append(title)

	  # Batch processing
    titulos_norm_list = []
    for i in range(0, len(clean_titles), batch_size):
        batch = clean_titles[i:i+batch_size]
        docs = list(nlp_fast.pipe(batch, batch_size=batch_size))
        for doc in docs:
            lemmas = [token.lemma_.lower() for token in doc 
                     if not token.is_punct and not token.is_space 
                     and not token.is_stop and not token.like_num 
                     and token.lemma_.strip()]
            titulos_norm_list.append(" ".join(lemmas))
    
    return titulos_norm_list
'''


def preprocesar_titulos(titulos_list):
    """
    Normaliza título en español:
    - Minúsculas y elimina acentos
    - Elimina puntuación
    - Elimina palabras con pocos caracteres
    - Elimina stopwords
    - Stemming
    """
    stopwords = {'el', 'la', 'de', 'que', 'y', 'en', 'del', 'a', 'se', 'no', 
                'directo', 'urgente', 'última', 'hora', 'breaking', 'los', 'las'}
    
    titulos_norm_list = []
    for title in titulos_list:
        # Limpieza básica
        title = unidecode(title.lower().strip()) # Normaliza caracteres
        title = re.sub(r'[^\w\s]', ' ', title)   # Elimina puntuacion 
        words = re.findall(r'\b[a-z]{3,}\b', title) # Extrae palabras >= 3
        
        # Stemming
        stemmed_words = [stemmer.stem(w) for w in words if w not in stopwords]
        titulos_norm_list.append(' '.join(stemmed_words))
    
    return titulos_norm_list


def agrupar_similares(articulos, umbral=70):
    # ordenar articulos por titulo para encontrar mas rapido los similares
    #articulos = articulos.sort_values('Titulo').reset_index(drop=True)
    articulos = articulos.sort_values(['Fecha de Publicación', 'Titulo'], ascending=[False, True]).reset_index(drop=True)

    titulos_list = articulos['Titulo'].tolist()
    titulos_norm_list = preprocesar_titulos(titulos_list)
    titulos_norm = {idx: titulos_norm_list[idx] for idx in articulos.index}
    
    # Agrupamiento
    grupos = []
    for index, titulo_norm in titulos_norm.items():
        art = articulos.loc[index]
        agregado = False
        for grupo in grupos:
            primer_norm = titulos_norm[grupo[0].name]
            score = fuzz.token_set_ratio(titulo_norm, primer_norm)
            if score >= umbral:
                grupo.append(art)
                agregado = True
                break
        if not agregado:
            grupos.append([art])
    return grupos


def mostrar_articulos_con_grupos(articulos, umbral=80):
    articulos = pd.DataFrame(articulos, columns=['Titulo', 'URL', 'Fuente', 'Fecha de Publicación'])

    # Convertir a tipo datetime usando UTC para tener un formato estandarizado
    articulos['Fecha de Publicación'] = pd.to_datetime(articulos['Fecha de Publicación'], format='mixed', errors='coerce', utc=True)
    # Convertir a zona horaria de españa
    articulos['Fecha de Publicación'] = articulos['Fecha de Publicación'].dt.tz_convert('Europe/Madrid')
    articulos['Fecha de Publicación'] = articulos['Fecha de Publicación'].dt.strftime('%Y-%m-%d')

    grupos = agrupar_similares(articulos, umbral)

    # Añadir columna 'Grupo' a cada artículo
    data_con_grupos = []
    for i, grupo in enumerate(grupos, start=1):
        for art in grupo:
            art_copy = art.copy()
            art_copy['Grupo'] = f"Grupo {i}"
            data_con_grupos.append(art_copy)

    df_grupos = pd.DataFrame(data_con_grupos)
    df_grupos.reset_index(drop=True, inplace=True)

    st.dataframe(
        df_grupos,
        column_config={
            "URL": st.column_config.LinkColumn(
                label="URL",
                help="Haz clic para ir al articulo",
                display_text="🔗",
                #width='small'
                ),
            "Grupo": None
        }
    )


#-------- Compartir enlace ---------
def share_url(current_params):
    # --- Botones para Compartir Enlace ---
    st.markdown("---")

    # Construir la URL completa para compartir
    query_string = urllib.parse.urlencode(current_params, doseq=True)
    #base_url = "https://localhost:8501" # o "https://tu-app-streamlit.streamlit.app"
    base_url = APP_URL
    share_url = f"{base_url}?{query_string}"

    st.markdown("Compartir enlace:")

    col_link, col_mail, col_wa, col_spacer = st.columns([0.6, 0.05, 0.05, 0.3])

    with col_link:
        st.text_input("URL para compartir:", value=share_url, disabled=False, label_visibility="collapsed")

    with col_mail:
        mail_subject = urllib.parse.quote("Noticias")
        mail_body = urllib.parse.quote(f"Mira estas noticias que encontré para ti: {share_url}")
        mail_to_link = f"mailto:?subject={mail_subject}&body={mail_body}"
        st.markdown(f"[![Correo](https://img.icons8.com/ios-filled/30/000000/filled-message.png)]({mail_to_link})", unsafe_allow_html=True)

    with col_wa:
        whatsapp_message = urllib.parse.quote(f"Mira estas noticias: {share_url}")
        whatsapp_link = f"https://wa.me/?text={whatsapp_message}"
        st.markdown(f"[![WhatsApp](https://img.icons8.com/ios-filled/30/000000/whatsapp--v1.png)]({whatsapp_link})", unsafe_allow_html=True)

    with col_spacer:
        st.empty()


#-------- Buscar artículos ---------
def get_default_date_values():
    # Fecha de hoy
    hoy = date.today()
    # Fecha de ayer
    ayer = hoy - timedelta(days=1)

    default_start_date_str = st.query_params.get("start_date", ayer.strftime('%Y-%m-%d'))
    default_end_date_str = st.query_params.get("end_date", hoy.strftime('%Y-%m-%d'))

    try:
        default_start_date = date.fromisoformat(default_start_date_str)
    except ValueError:
        default_start_date = ayer

    try:
        default_end_date = date.fromisoformat(default_end_date_str)
    except ValueError:
        default_end_date = hoy

    return default_start_date, default_end_date


def search_articles_filter(news_db):
    ALL_OPTION = news_db.ALL_OPTION

    # --- Leer parámetros de la URL  ---
    default_categories = st.query_params.get_all("categories")
    if len(default_categories) == 0:
        default_categories = [ALL_OPTION]

    default_keywords = st.query_params.get_all("keywords")
    if len(default_keywords) == 0:
        default_keywords = [ALL_OPTION]

    default_start_date, default_end_date = get_default_date_values()

    if 'selected_categories' not in st.session_state:
        st.session_state.selected_categories = default_categories

    if 'selected_keywords' not in st.session_state:
        st.session_state.selected_keywords = default_keywords


    col1, col2 = st.columns(2)
    categories = news_db.fetch_categories()


    with col1:
        def radio_select():
            if 'radio_categories' in st.session_state and st.session_state.radio_categories == 'Selección Individual':
                    st.session_state.selected_keywords = []

        # Default values
        radio_cat_options = ["Todas", "Selección Individual"]
        default_radio_cat = st.query_params.get("radio_cat", "")
        default_radio_cat_idx = radio_cat_options.index(default_radio_cat) if default_radio_cat in radio_cat_options else 0

        mode_cat = st.radio("Elegir categorías:", radio_cat_options,
                        key = 'radio_categories',
                        on_change = radio_select,
                        horizontal=True,
                        index = default_radio_cat_idx)

        if mode_cat == "Todas":
            selected_categories = [ALL_OPTION]
        else:
            selected_categories = st.multiselect("",
                                                 options=categories,
                                                 key="selected_categories",
                                                 label_visibility ="collapsed")

    keywords = news_db.fetch_keywords_for_categories(selected_categories)

    # Filtrar las palabras clave predeterminadas solo a aquellas que aún están presentes en las opciones de palabras clave
    #filtered_default_keywords = [kw for kw in st.session_state.selected_keywords if kw in keywords]

    with col2:
        # Default values
        radio_key_options = ["Todas", "Selección Individual"]
        default_radio_kw = st.query_params.get("radio_key", "")
        default_radio_kw_idx = radio_key_options.index(default_radio_kw) if default_radio_kw in radio_key_options else 0

        mode_key = st.radio("Elegir palabras clave:", radio_key_options,
                         key='radio_keyword', horizontal=True,
                         index = default_radio_kw_idx)

        if mode_key == "Todas":
            selected_keywords = [ALL_OPTION]#keywords
        else:
            selected_keywords = st.multiselect("",
                                               options=keywords,
                                               key="selected_keywords",
                                               label_visibility ="collapsed")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input('Fecha de inicio', value=default_start_date)
    with col2:
        end_date = st.date_input('Fecha de fin', value=default_end_date)
    start_datetime = datetime.combine(start_date, dt_time.min)
    end_datetime = datetime.combine(end_date, dt_time.min) + timedelta(days=1)
    start_iso = start_datetime.strftime('%Y-%m-%d %H:%M:%S') if start_datetime else None
    end_iso = end_datetime.strftime('%Y-%m-%d %H:%M:%S') if end_datetime else None

    # --- Actualizar parámetros de la URL (igual que antes) ---
    current_params = {}
    if selected_categories and ALL_OPTION not in selected_categories:
        current_params["categories"] = selected_categories
    if selected_keywords and ALL_OPTION not in selected_keywords:
        current_params["keywords"] = selected_keywords
    if start_iso:
        current_params["start_date"] = start_date
    if end_iso:
        current_params["end_date"] = end_date
    current_params["radio_key"] = mode_key
    current_params["radio_cat"] = mode_cat

    if st.button('Mostrar Noticias'):
        if not selected_categories:
            st.warning("Por favor seleccione una categoria.")
        elif not selected_keywords:
            st.warning("Por favor seleccione una palabra clave.")
        else:
            if current_params != dict(st.query_params):
                st.query_params.clear()
                st.query_params.update(current_params)
            articles = news_db.fetch_articles(
                categories=selected_categories if ALL_OPTION not in selected_categories else None,
                keywords=selected_keywords if ALL_OPTION not in selected_keywords else None,
                start_date=start_iso,
                end_date=end_iso
            )
            if articles:
                mostrar_articulos_con_grupos(articles)
            else:
                st.write('No se encontraron noticias relacionadas.')
    share_url(current_params)


def show_search_results(results):
    articles_list = []
    for metadata in results:
        articles_list.append([metadata['titulo'], metadata['url'], metadata.get('fuente'), metadata['fecha_publicacion']])
    articulos = pd.DataFrame(articles_list, columns=['Titulo', 'URL', 'Fuente', 'Fecha de Publicación'])

    st.dataframe(
        articulos,
        column_config={
            "URL": st.column_config.LinkColumn(
                label="URL",
                help="Haz clic para ir al articulo",
                display_text="🔗",
                #width='small'
                )
        }
    )


def semantic_search_articles(news_db, search_manager):

    default_semantic_query = st.query_params.get("query", "")
    default_top_k_index = int(st.query_params.get("top_k_index", '0'))

    # Fecha de hoy
    default_start_date, default_end_date = get_default_date_values()

    col_query, col_top_k = st.columns([0.9, 0.1])

    with col_query:
        semantic_query = st.text_input("Ingrese texto para búsqueda semántica", value=default_semantic_query)

    with col_top_k:
        options = [5, 10, 20, 30]
        top_k = st.selectbox("", options=options, index=default_top_k_index, label_visibility="hidden")
        selected_index = options.index(top_k)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input('Fecha de inicio', value=default_start_date, key='start_dt_semantic')
    with col2:
        end_date = st.date_input('Fecha de fin', value=default_end_date, key='end_dt_semantic')

    # --- Actualizar parámetros de la URL ---
    current_params = {}
    if semantic_query:
        current_params["query"] = semantic_query
    current_params["top_k_index"] = selected_index
    if start_date:
        current_params["start_date"] = start_date
    if end_date:
        current_params["end_date"] = end_date

    if st.button("Buscar"):
        if current_params != dict(st.query_params):
            st.query_params.clear()
            st.query_params.update(current_params)

        if semantic_query.strip():
            articles = search_manager.retrieve_related_news(semantic_query, k = top_k, date_from=start_date, date_to=end_date)
            if articles:
                show_search_results(articles)
            else:
                st.write('No se encontraron noticias relacionadas.')
        else:
            st.warning("Por favor ingrese un texto para la búsqueda semántica.")

    share_url(current_params)


def search_articles(news_db, search_manager):
    st.subheader("Buscar Artículos")

    search_type = st.tabs(["Filtro", "Búsqueda Semántica"])
    with search_type[0]:  # Filter tab
        try:
            search_articles_filter(news_db)
        except Exception as e:
            st.error(f"Error al realizar la búsqueda: {e}")

    with search_type[1]:  # Semantic Search tab
        try:
            semantic_search_articles(news_db, search_manager)
        except Exception as e:
            st.error(f"Error al realizar la búsqueda: {e}")


def manage_categories(news_db):
    st.subheader("Administrar Categorías")
    categories = news_db.fetch_categories()

    # Usar expander con dataframe para mostrar las categorías existentes
    with st.expander("Ver categorías existentes"):
        if categories:
            df_categories = pd.DataFrame({'Nombre de Categoría': categories})
            st.dataframe(df_categories, hide_index=True, use_container_width=True)
        else:
            st.info("No hay categorías creadas aún.")

    new_cat = st.text_input("Agregar nueva categoría")
    if st.button("Agregar categoría"):
        if new_cat.strip():
            try:
                news_db.add_category(new_cat.strip())
                st.success(f"Categoría '{new_cat}' agregada.")
                time.sleep(2)
                st.rerun() # recargamos la pagina para ver los cambios
            except ValueError as ve:
                st.warning(str(ve))
                time.sleep(2)
                st.rerun()
            except Exception as e:
                st.error(f"Error al agregar categoría: {e}")

    st.markdown("---")
    st.markdown("### Eliminar categoría")

    options = ["-- Seleccione una categoría --"] + categories
    cat_to_delete = st.selectbox("Eliminar categoría", options=options)

    if cat_to_delete != "-- Seleccione una categoría --" and st.button("Eliminar categoría"):
        try:
            news_db.delete_category(cat_to_delete)
            st.success(f"Categoría '{cat_to_delete}' eliminada.")
            time.sleep(2)
            st.rerun()
        except Exception as e:
            st.error(f"Error al eliminar categoría: {e}")


def manage_keywords(news_db):
    st.subheader("Administrar Palabras Clave")

    # Selector de acción en la página principal
    action = st.radio(
        "Selecciona la acción",
        options=["Agregar palabra clave", "Asociar a categorías", "Eliminar asociación o palabra clave"],
        index=0,
        horizontal=True
    )

    categories = news_db.fetch_categories() or []
    all_keywords = news_db.fetch_all_keywords() or []

    if action == "Agregar palabra clave":

        with st.expander("Ver palabras clave existentes"):
            if all_keywords:
                df_keywords = pd.DataFrame({'Palabra Clave': all_keywords})
                st.dataframe(df_keywords, hide_index=True, use_container_width=True)
            else:
                st.info("No hay palabras clave creadas aún.")

        #new_keyword = st.text_input("Agregar nueva palabra clave")
        new_keyword = st.text_input("Agregar nueva palabra clave (ej: 'A', 'A + B + C', 'A or B or C')")
        if st.button("Agregar palabra clave"):
            if not new_keyword.strip():
                st.warning("Debe ingresar una palabra clave válida.")
            elif new_keyword.strip() in all_keywords:
                st.warning("La palabra clave ya existe.")
            else:
                try:
                    regla = new_keyword.strip()
                    palabras_clave_regla, operator = split_rule_text(regla)

                    or_operator = ' o ' in regla
                    and_operator = '+' in regla
                    if or_operator and and_operator:
                      st.warning("No se pueden combinar operadores '+' con ' o ' en la palabra clave")
                      return

                    if len(palabras_clave_regla) == 1:
                        news_db.add_keyword_stem(palabras_clave_regla[0])
                        st.success(f"Palabra clave '{palabras_clave_regla[0]}' agregada correctamente.")
                    else:
                        resultado = news_db.bulk_insert_palabras_clave_stem(palabras_clave_regla)
                        if resultado['total_insertadas'] > 0:
                            st.success(f"**{resultado['total_insertadas']}** nuevas palabras clave insertadas")
                            for p in resultado['insertadas']:
                                st.write(f"• {p}")

                        if resultado['total_duplicadas'] > 0:
                            st.info(f"**{resultado['total_duplicadas']}** palabras clave ya existen")
                            for p in resultado['duplicadas']:
                                st.write(f"• {p}")

                    # Agregar regla
                    news_db.add_regla(regla)

                    # Asociar palabras clave a regla con operador y posicion
                    keyword_pairs = [(kw, operator if operator is not None else '-', pos)
                                     for pos, kw in enumerate(palabras_clave_regla, 1)]
                    news_db.associate_rule_keywords(regla, keyword_pairs)
                    #st.success(f"Regla '{regla}' agregada con {len(palabras_clave_regla)} palabras clave.")

                    time.sleep(2)
                    st.rerun()
                except ValueError as ve:
                    st.warning(str(ve))
                    time.sleep(2)
                    st.rerun()
                except Exception as e:
                    st.error(f"Error al agregar palabra clave: {e}")

    elif action == "Asociar a categorías":
        st.markdown("### Asociar palabra clave a categorías")
        if not all_keywords:
            st.info("No hay palabras clave para asociar. Agrega alguna primero.")
        else:
            kw_to_associate = st.selectbox("Seleccionar palabra clave", options=all_keywords, key="kw_to_remove")
            cats_to_associate = st.multiselect("Seleccionar categorías", options=categories)
            if st.button("Asociar"):
                if not cats_to_associate:
                    st.warning("Debe seleccionar al menos una categoría.")
                else:
                    try:
                        for cat in cats_to_associate:
                            news_db.associate_keyword_stem_category(kw_to_associate, cat)
                            #news_db.associate_keyword_category(kw_to_associate, cat)
                        st.success(f"Palabra clave '{kw_to_associate}' asociada a las categorías seleccionadas.")
                        time.sleep(2)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al asociar palabra clave: {e}")

    else:
        st.markdown("### Eliminar palabra clave o asociación")

        delete_option = st.radio("¿Qué deseas eliminar?", options=["Eliminar asociación con categoría", "Eliminar palabra clave completa"])

        if delete_option == "Eliminar asociación con categoría":
            if not categories or not all_keywords:
                st.info("No hay categorías o palabras clave para eliminar asociación.")
            else:
                selected_cat = st.selectbox("Seleccionar categoría", options=categories, key="remove_assoc_cat")

                if selected_cat:
                    keywords = news_db.fetch_keywords_for_categories([selected_cat]) or []

                    with st.expander(f"Palabras clave asociadas a '{selected_cat}'"):
                        if keywords:
                            kw_to_remove = st.selectbox("Seleccionar palabra clave para eliminar asociación", options=keywords, key="remove_assoc_kw")
                            if st.button("Eliminar asociación"):
                                try:
                                    news_db.remove_keyword_stem_from_category(kw_to_remove, selected_cat)
                                    st.success(f"Asociación de '{kw_to_remove}' con '{selected_cat}' eliminada.")
                                    time.sleep(2)
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error al eliminar asociación: {e}")
                        else:
                            st.info("No hay palabras clave asociadas a esta categoría.")

        else:  # Eliminar palabra clave completa
            if not all_keywords:
                st.info("No hay palabras clave para eliminar.")
            else:
                kw_to_delete = st.selectbox("Seleccionar palabra clave para eliminar", options=all_keywords, key="delete_kw")
                if st.button("Eliminar palabra clave completa"):
                    try:
                        news_db.delete_keyword(kw_to_delete.strip())
                        st.success(f"Palabra clave '{kw_to_delete}' eliminada completamente.")
                        time.sleep(2)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al eliminar palabra clave: {e}")


def remove_keyword_association(news_db):
    #st.markdown("### Eliminar asociación palabra clave - categoría")

    categories = news_db.fetch_categories() or []
    selected_cat = st.selectbox("Seleccionar categoría", options=categories, key="remove_assoc_cat")

    if selected_cat:
        keywords = news_db.fetch_keywords_for_categories([selected_cat]) or []

        with st.expander(f"Palabras clave asociadas a '{selected_cat}'"):
            if keywords:
                kw_to_remove = st.selectbox("Seleccionar palabra clave para eliminar asociación", options=keywords, key="remove_assoc_kw")
                if st.button("Eliminar asociación"):
                    try:
                        news_db.remove_keyword_from_category(kw_to_remove, selected_cat)
                        st.success(f"Asociación de '{kw_to_remove}' con '{selected_cat}' eliminada.")
                        st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Error al eliminar asociación: {e}")
            else:
                st.info("No hay palabras clave asociadas a esta categoría.")


def manage_sources(news_db):
    st.subheader("Administrar Fuentes")

    fuentes = news_db.fetch_sources()
    import pandas as pd
    if fuentes:
        df_fuentes = pd.DataFrame(fuentes, columns=['ID', 'Nombre', 'URL Home'])

        st.dataframe(
            df_fuentes,
            column_config={
                "URL Home": st.column_config.LinkColumn("Fuente", help="Haz clic para ir a la fuente"),
                "ID": None
            }
        )

    else:
        st.info("No hay fuentes registradas.")

    st.markdown("### Agregar nueva fuente")
    #new_id = st.text_input("ID de la fuente")
    new_name = st.text_input("Nombre de la fuente")
    new_url = st.text_input("URL home de la fuente")
    if st.button("Agregar fuente"):
        try:
            new_id = get_domain(new_url)
        except Exception as e:
            st.error(f"Error al obtener id: {e}")

        if new_id.strip() and new_name.strip():
            try:
                news_db.add_source(new_id.strip(), new_name.strip(), new_url.strip())
                st.success(f"Fuente '{new_name}' agregada.")
                time.sleep(3)
                st.rerun()
            except ValueError as ve:
                st.warning(str(ve))
                time.sleep(3)
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")


    # --- Sección para eliminar fuente ---
    if fuentes:
        st.markdown("---")
        st.markdown("### Eliminar fuente")
        fuentes_dic = {f[1]: f[0] for f in fuentes} #f[1] -> nombres, f[0] -> ids
        fuente_display = list(fuentes_dic.keys())

        fuente_seleccionada = st.selectbox("Selecciona la fuente a eliminar", options=fuente_display)

        if st.button("Eliminar fuente"):
            fuente_id_a_eliminar = fuentes_dic.get(fuente_seleccionada)
            try:
                news_db.delete_source(fuente_id_a_eliminar)
                st.success(f"Fuente con ID '{fuente_id_a_eliminar}' eliminada correctamente.")
                time.sleep(3)
                st.rerun()
            except Exception as e:
                st.error(f"Error al eliminar la fuente: {e}")


def manage_sitemaps(news_db):
    st.subheader("Administrar Mapas de Sitio")

    fuentes = news_db.fetch_sources()
    fuente_nombres = [f[1] for f in fuentes]
    selected_fuente = st.selectbox("Seleccionar fuente para mostrar mapas de sitio", options=fuente_nombres)

    # Obtener el id de la fuente seleccionada
    fuente_id = None
    for f in fuentes:
        if f[1] == selected_fuente:
            fuente_id = f[0]
            break

    if fuente_id is None:
        st.error("Fuente no encontrada.")
        return

    # Obtener mapas de sitio asociados a la fuente seleccionada
    mapas = news_db.get_sitemaps_by_source_id(fuente_id)

    if mapas:
        import pandas as pd
        df_mapas = pd.DataFrame(mapas, columns=['URL Relativa'])
        st.dataframe(df_mapas, use_container_width=True)
    else:
        st.info("No hay mapas de sitio asociados a esta fuente.")

    new_sitemap = st.text_input("Agregar nueva URL relativa de sitemap")
    if st.button("Agregar sitemap"):
        if new_sitemap.strip():
            try:
                news_db.add_sitemap(new_sitemap.strip())
                news_db.associate_sitemap_source(fuente_id, new_sitemap.strip())
                st.success(f"Sitemap '{new_sitemap}' agregado y asociado a la fuente '{selected_fuente}'.")
                time.sleep(3)
                st.rerun()
            except ValueError as ve:
                st.warning(str(ve))
                time.sleep(3)
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    if mapas:
        st.markdown("---")
        st.markdown("### Eliminar sitemap")
        sitemap_seleccionado = st.selectbox("Seleccionar sitemap para eliminar", options=mapas)

        if st.button("Eliminar sitemap"):
            try:
                news_db.delete_sitemap(sitemap_seleccionado)
                st.success(f"Sitemap '{sitemap_seleccionado}' eliminado correctamente.")
                time.sleep(3)
                st.rerun()
            except Exception as e:
                st.error(f"Error al eliminar sitemap: {e}")


def user_interface(news_db, search_manager):
    search_articles(news_db, search_manager)


def admin_interface(news_db, search_manager):
    st.header("Panel de Administración")

    admin_tabs = st.tabs(["Categorías", "Palabras Clave", "Fuentes", "Sitemaps", "Buscar Artículos", "Extraer Artículos"])

    with admin_tabs[0]:
        manage_categories(news_db)

    with admin_tabs[1]:
        manage_keywords(news_db)

    with admin_tabs[2]:
        manage_sources(news_db)

    with admin_tabs[3]:
        manage_sitemaps(news_db)

    with admin_tabs[4]:
        search_articles(news_db, search_manager)

    with admin_tabs[5]:
        scrapy_tab()
