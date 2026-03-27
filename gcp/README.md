# Google Cloud Platform

## Introduction

Pour augmenter notre capacité de calcul et de stockage, nous avons décidé de passer sur une infrastructure cloud. Pour cela nous avons utilisé Google Cloud Platform (GCP) qui possède 300$ de crédits offerts pour effectuer des calculs dans le cloud.

## Code

Cette décision a été prise lors du checkpoint en classe. Comme nous avions précédemment pris deux chemins différents avec Hadoop MapReduce en java et PySpark en python, nous avons décidé de nous accorder sur un chemin pour le cloud computing. Nous avons donc choisi de nous pencher sur la méthode via PySpark étant donné qu'elle a l'avantage de streamer les données et de ne stocker que les résultats, elle évite d'avoir à ingérer des centaines de Go.

Un des problèmes auquel nous avons fait face a été de traduire tous les chemins locaux entre les écritures et lectures de fichiers parquets. Une fois cela réussi, nous avons pu ajouté un script bash qui regroupe le pipeline pour qu'il soit prêt à être lancé sur le cloud en une commande, il permet de créer les workers, d'envoyer les travaux et n'oublie pas d'éteindre les workers pour ne pas que nos crédits fondent.

Pour pouvoir lancer le code, il faut tout d'abord satisfaire les requirements, il suffit ensuite de lancer le script.
```bash
# Récupérer le projet
git clone https://github.com/tdv57/ensta_2026_big_data.git
cd ensta_2026_big_data
```

```bash
# Créer un venv pour satisfaire les dépendances
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

```bash
# Se déplacer dans la partie gcp et préparer l'exécutable
cd gcp
chmod +x deploy_and_run.sh
```

```bash
# Déployer le code et le lancer dans le cloud
./deploy_and_run.sh
```

Si jamais vous souhaitez modifier l'url de départ, finale ou le pas cela se fait dans les paramètres au début du script `deploy_and_run.sh`.

## Résultats
Les résultats s'affichent directement à la fin du pipeline. Cependant pour ne pas avoir à tout calculer une seconde fois, il est possible de télécharger en local les fichiers parquets de résultats pour pouvoir rapidement relire les résultats sans avoir à tout calculer de nouveau.

```bash
# Pour voir les différents fichiers parquets
gcloud storage ls gs://ensta-bigdata-2024/final_parquet/
```

```bash
# Pour télécharger les fichiers parquets finaux en local
gcloud storage cp -r gs://ensta-bigdata-2024/final_parquet/ ./resultats_0_900000_900
```

Dans ce répertoire github, plusieurs résultats on été uploadés pour permettre de les lire sans avoir à les calculer soi-même. Le nom des répertoires suivent le modèle suivant : `resultats_{URL_DEPART}_{URL_FINALE}_{PAS}`.

À tout moment, on peut se déplacer dans l'un des dossiers résultats et lancer le fichier python de lecture des résultats. Ce fichier est copié dans chaque dossier résultat. Cela peut sembler redondant mais c'est pour qu'il ne soit pas copié dans le cloud lors de l'appel à `gcloud storage cp *.py gs://${GCS_BUCKET#gs://}/code/` du pipeline bash.
```bash
# Ce fichier python lit automatiquement les résultats situés dans ./final_parquet/
python3 read_results.py
```

## GCP

Pour vérifier qu'aucune VM ne tourne et que les crédits ne sont pas consommés, ou au contraire pour vérifier que les VMs sont allumées, à tout moment on peut lancer une des commandes suivantes
```bash
# Vérifier les clusters actifs
gcloud dataproc clusters list --region=us-central1

# Vérifier les VMs actives
gcloud compute instances list

# Vérifier les jobs en cours
gcloud dataproc jobs list --region=us-central1
```

Enfin pour éteindre les VMs si le script ne l'a pas bien fait (à cause d'un crash au milieu par exemple), on peut lancer cette commande.
```bash
gcloud dataproc clusters delete ensta-cluster --region=us-central1 --quiet
```

## Conclusion
Ce projet nous a permis de répondre à notre problématique, ainsi que de mieux comprendre en pratique comment on peut utiliser une infrastructure cloud pour effectués des calculs distants. 
