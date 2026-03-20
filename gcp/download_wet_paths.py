import requests
import gzip
import io
from CC_name import get_CC_names
from LOG_MESSAGE import INFO, WARNING

CC_url = "https://data.commoncrawl.org/"


def download_wet_paths(CC_archive_name):
    """
    Télécharge et décompresse les paths WET en mémoire.
    Retourne une liste de chemins (strings) sans rien écrire sur disque.
    """
    wet_paths_url = CC_url + f"crawl-data/{CC_archive_name}/wet.paths.gz"
    print(INFO + f"Téléchargement des paths WET : {wet_paths_url}")

    response = requests.get(wet_paths_url)
    if response.status_code != 200:
        print(WARNING + f"Erreur HTTP {response.status_code} pour {wet_paths_url}")
        return []

    compressed = io.BytesIO(response.content)
    with gzip.open(compressed, "rb") as fgz:
        wet_paths = [line.decode("utf-8").strip() for line in fgz if line.strip()]

    print(INFO + f"{len(wet_paths)} paths WET chargés pour {CC_archive_name}")
    return wet_paths


def get_all_wet_urls(min_year=2024, max_year=2025):
    """
    Retourne la liste complète des URLs WET pour la période donnée.
    Tout se passe en mémoire, rien n'est écrit sur disque.
    """
    CC_archive_names = get_CC_names(min_year=min_year, max_year=max_year)
    all_urls = []
    for name in CC_archive_names:
        all_urls += download_wet_paths(name)
    print(INFO + f"Total URLs WET disponibles : {len(all_urls)}")
    return all_urls
