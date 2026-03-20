import requests
import gzip
import io
from CC_name import get_CC_names
from LOG_MESSAGE import INFO, WARNING

CC_url = "https://data.commoncrawl.org/"


def download_wat_paths(CC_archive_name):
    """
    Télécharge et décompresse les paths WAT en mémoire.
    Retourne une liste de chemins (strings) sans rien écrire sur disque.
    """
    wat_paths_url = CC_url + f"crawl-data/{CC_archive_name}/wat.paths.gz"
    print(INFO + f"Téléchargement des paths WAT : {wat_paths_url}")

    response = requests.get(wat_paths_url)
    if response.status_code != 200:
        print(WARNING + f"Erreur HTTP {response.status_code} pour {wat_paths_url}")
        return []

    compressed = io.BytesIO(response.content)
    with gzip.open(compressed, "rb") as fgz:
        wat_paths = [line.decode("utf-8").strip() for line in fgz if line.strip()]

    print(INFO + f"{len(wat_paths)} paths WAT chargés pour {CC_archive_name}")
    return wat_paths


def get_all_wat_urls(min_year=2024, max_year=2025):
    """
    Retourne la liste complète des URLs WAT pour la période donnée.
    Tout se passe en mémoire, rien n'est écrit sur disque.
    """
    CC_archive_names = get_CC_names(min_year=min_year, max_year=max_year)
    all_urls = []
    for name in CC_archive_names:
        all_urls += download_wat_paths(name)
    print(INFO + f"Total URLs WAT disponibles : {len(all_urls)}")
    return all_urls
