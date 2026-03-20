import requests
from warcio.archiveiterator import ArchiveIterator
from LOG_MESSAGE import INFO, WARNING

CC_url = "https://data.commoncrawl.org/"


def get_wet_response(wet_url):
    """
    Ouvre un stream HTTP vers un fichier WET de Common Crawl.
    Retourne la response (stream), ou None si erreur.
    Rien n'est écrit sur disque.
    """
    full_url = CC_url + wet_url
    print(INFO + f"Requête WET : {full_url}")

    response = requests.get(full_url, stream=True)
    if response.status_code != 200:
        print(WARNING + f"Erreur HTTP {response.status_code} pour {full_url}")
        return None

    print(INFO + "Réponse HTTP 200 OK")
    return response


def get_wet_page(response, page_wanted):
    """Utilitaire de debug : retourne le contenu d'un enregistrement WET."""
    stream = ArchiveIterator(response.raw)
    for page_index, record in enumerate(stream):
        if page_index == page_wanted:
            if record.rec_type == "conversion":
                return record.content_stream().read().decode("utf-8", errors="replace")
            else:
                raise ValueError(f"Type inattendu : {record.rec_type}")
    return None
