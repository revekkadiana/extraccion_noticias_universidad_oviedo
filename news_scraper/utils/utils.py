from urllib.parse import urljoin, urlparse
import re
import os
import datetime
from datetime import datetime, timedelta
import dateutil
from dateutil import parser as date_parser
from zoneinfo import ZoneInfo

def get_base_url(url):
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}/"

def get_domain(url):
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
    return urlparse(url).netloc.lower()

def is_full_url(url):
    parsed_url = urlparse(url)
    return all([parsed_url.scheme, parsed_url.netloc])


def is_valid_sitemap_url(url, invalid_url_words):
    for invalid_word in invalid_url_words:
        if invalid_word in url:
            return False

    pattern = r'\b(1[0-9]{3}|20[0-1][0-9]|202[0-4])\b' # Matches 1000-1999, 2000-2019, and 2020-2024
    match_url = re.search(pattern, url)
    if match_url:
        return False
    else:
        return True


def normalize_url(url, base_url=None):
    if not is_full_url(url) and base_url:
        return urljoin(base_url, url)
    return url


def get_url_extension(url):
    parsed_url = urlparse(url)
    path = parsed_url.path
    _, extension = os.path.splitext(path)
    return extension


def extract_sitemap_urls(robots_text, invalid_url_words={}):
    sitemap_urls = set()
    for line in robots_text.splitlines():
        if line.lower().startswith('sitemap:'):
            sitemap_url = line.split(':', 1)[1].strip()
            if is_valid_sitemap_url(sitemap_url, invalid_url_words):
                sitemap_urls.add(sitemap_url)
    return sitemap_urls


def normalize_date(date_string, formato_entrada=None, dayfirst=None):
    if not date_string or not isinstance(date_string, str):
        if isinstance(date_string, datetime) or isinstance(date_string, datetime.date):
            return date_string
        print(f"Invalid input: '{date_string}', type: {type(date_string)}")
        return None

    processed_str = date_string.lower()

    # Remover "a las"
    processed_str = re.sub(r'\ba las\b', ' ', processed_str)

    tz_match = re.search(r'(cet|cest)', processed_str)
    tz_abbr = tz_match.group(0) if tz_match else None
    if tz_abbr:
        processed_str = processed_str.replace(tz_abbr, 'T')

    # 1. Remover día de la semana
    processed_str = re.sub(r'^(lunes|martes|miércoles|jueves|viernes|sábado|domingo),\s*', '', processed_str)

    # 2. Remover del/de
    processed_str = re.sub(r'\sdel?\s', ' ', processed_str)

    # 3. Remover componentes de tiempo (preservar separador ISO T)
    if 't' not in processed_str:
        processed_str = re.sub(r'(\d{1,2}:\d{2})(?:\s*h\.?|hrs?)', r'\1', processed_str)

    # 4. Eliminar guiones
    processed_str = re.sub(r'(\d{1,2}:\d{2})\s*-\s*', r'\1 ', processed_str)

    # 5. Convertir meses de español a ingles
    month_translations = {
        'enero': 'january', 'ene': 'jan',
        'febrero': 'february', 'feb': 'feb',
        'marzo': 'march', 'mar': 'mar',
        'abril': 'april', 'abr': 'apr',
        'mayo': 'may', 'may': 'may',
        'junio': 'june', 'jun': 'jun',
        'julio': 'july', 'jul': 'jul',
        'agosto': 'august', 'ago': 'aug',
        'septiembre': 'september', 'setiembre': 'september', 'sep': 'sep', 'set': 'sep',
        'octubre': 'october', 'oct': 'oct',
        'noviembre': 'november', 'nov': 'nov',
        'diciembre': 'december', 'dic': 'dec',
    }

    for spanish, english in month_translations.items():
        processed_str = processed_str.replace(spanish, english)

    try:
        if re.match(r'^\d{1,2}/\d{1,2}/\d{2,4}', processed_str): # format DD/MM/YYYY
            if dayfirst is not None:
                parsed_date = dateutil.parser.parse(processed_str, dayfirst=dayfirst)
            else:
                parsed_date = dateutil.parser.parse(processed_str, dayfirst=True)
        else:
            parsed_date = dateutil.parser.parse(processed_str)
        parsed_date = parsed_date.replace(tzinfo=ZoneInfo("Europe/Madrid"))
        return parsed_date
    except (ValueError, TypeError) as e:
        print(f"Formato de fecha inválido: '{date_string}' - Error: {str(e)}")
        return None


def get_current_date():
    return datetime.now().date()


def subtract_days_from_date(date: datetime.date, days: int) -> datetime.date:
    return date - timedelta(days=days)
