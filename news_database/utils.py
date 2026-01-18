from urllib.parse import urljoin, urlparse

def get_base_url(url):
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}/"

def get_domain(url):
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
    return urlparse(url).netloc.lower()
    
def split_rule_text(rule):
    rule = rule.strip()
    partes = []
    operator = None
    if '+' in rule:
        partes = [part.strip() for part in rule.split('+')]
        operator = '+'
    elif ' o ' in rule:
        partes = [part.strip() for part in rule.split(' o ')]
        operator = 'o'
    else:
        partes = [rule]
    return partes, operator