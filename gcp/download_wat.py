import requests
from warcio.archiveiterator import ArchiveIterator
from LOG_MESSAGE import INFO, WARNING

CC_url = "https://data.commoncrawl.org/"


def wet_to_wat_url(wet_url):
    """Convertit une URL WET en URL WAT correspondante."""
    return wet_url.replace("/wet/", "/wat/").replace(".warc.wet.gz", ".warc.wat.gz")


def get_wat_response(wat_url):
    """
    Ouvre un stream HTTP vers un fichier WAT de Common Crawl.
    Retourne la response (stream), ou None si erreur.
    Rien n'est écrit sur disque.
    """
    full_url = CC_url + wat_url
    print(INFO + f"Requête WAT : {full_url}")

    response = requests.get(full_url, stream=True)
    if response.status_code != 200:
        print(WARNING + f"Erreur HTTP {response.status_code} pour {full_url}")
        return None

    print(INFO + "Réponse HTTP 200 OK")
    return response


def get_wat_page(response, page_wanted):
    """Utilitaire de debug : retourne le contenu d'un enregistrement WAT."""
    stream = ArchiveIterator(response.raw)
    for page_index, record in enumerate(stream):
        if page_index == page_wanted:
            if record.rec_type == "metadata":
                return record.content_stream().read().decode("utf-8", errors="replace")
            else:
                raise ValueError(f"Type inattendu : {record.rec_type}")
    return None
