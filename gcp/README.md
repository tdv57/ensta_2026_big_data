# Google Cloud Platform

## Introduction

Pour augmenter notre capacité de calcul et de stockage, nous avons décidé de passer sur une infrastructure cloud. Pour cela nous avons utilisé Google Cloud Platform (GCP) qui possède 300$ de crédits offerts pour effectuer des calculs dans le cloud.

## Code

Cette décision a été prise lors du checkpoint en classe. Comme nous avions précédemment pris deux chemins différents avec Hadoop MapReduce en java et PySpark en python, nous avons décidé de nous accorder sur un chemin pour le cloud computing. Nous avons donc choisi de nous pencher sur la méthode via PySpark étant donné qu'elle a l'avantage de streamer les données et de ne stocker que les résultats, elle évite d'avoir à ingérer des centaines de Go.

Un des problèmes auquel nous avons fait face a été de traduire tous les chemins locaux entre les écritures et lectures de fichiers parquets. Une fois cela réussi, nous avons pu ajouté un script bash qui regroupe le pipeline pour qu'il soit prêt à être lancé sur le cloud en une commande, il permet de créer les workers, d'envoyer les travaux et n'oublie pas d'éteindre les workers pour ne pas que nos crédits fondent.

Pour pouvoir lancer le code, il faut tout d'abord satisfaire les requirements, il suffit ensuite de lancer le script.
```bash
git clone https://github.com/tdv57/ensta_2026_big_data.git
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cd gcp
chmod +x deploy_and_run.sh
./deploy_and_run.sh
```

Si jamais vous souhaitez modifier l'url de départ, finale ou le pas cela se fait dans les paramètres au début du script `deploy_and_run.sh`.
