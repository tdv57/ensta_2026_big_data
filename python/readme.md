## Pour télécharger le dataset en local : 

1) lancer **init.sh** 
2) lancer sur deux terminaux différents **./launch_write_wet_parquet_files.sh** et **./launch_write_wat_parquet_files.sh**  
En cas d'une erreur ou d'un arrêt du téléchargement avant la fin (wat ou wet):  
    a) Il faut regarder wet_urls_downloaded   
    b) Supprimer tout les fichiers parquet dans le dossier wet_parquet qui ne contiennent pas les intervalles d'urls (normalement il n'y a au plus une suppression de dossier à faire)   
    c) Relancer le script.  
3) une fois les deux scripts terminés lancer **"python3 write_final_parquet.py"** pour avoir le dataset final on appellera final_parquet les fichiers parquet qui font la jointure entre les fichiers wet et wat.
4) pour lire les fichiers il faut modifier le main des fonctions **read_{wet,wat,final}_parquet_files.py** et les lancer avec python3  

## Pour télécharger le dataset via google cloud platform:

Nous appelerons `${GCP_BUCKET}` le nom du bucket sous le format suivant:  
gs://nom_bucket  
  
  
### 1) lancer ./init.sh  
### 1) Créer un projet et un google cloud storage   
### 3) Créer un cluster via la console gcp (dataproc -> cluster -> create cluster)   
Attention: il faut faire en sorte de permettre des requêtes vers internet en décochant l'option "Adresse IP interne uniquement" dans l'onglet "Personnaliser le cluster"  
  
### 4) Voici les commandes à copié collé pour initialiser les scripts sur gcp:  
[ATTENTION] si vous voulez refaire un téléchargement il faudra supprimer les dossier wet_parquet wat_parquet final_parquet ainsi que remettre à zéro le fichier wet_parquet_extra_info sous peine de mélanger les nouvelles données avec les anciennes   
  
1° gcloud storage cp   python/{CC_name.py,wet_parquet_extra_info,LOG_MESSAGE.py,download_warc.py,download_warc_paths.py,download_wat.py,download_wat_paths.py,download_wet.py,download_wet_paths.py,write_wet_parquet_files.py,write_wat_parquet_files.py,write_gcp_final_parquet_files.py,run_wet_gcp.py,run_wat_gcp.py}  
2° gcloud storage touch ${GCP_BUCKET}/wet_parquet_extra_info  
3° gcloud storage cp -r python/{python_packages.zip,wat_paths/,wet_paths/}  ${GCP_BUCKET}
  
   
### 5) payload pour lancer le job sur le cluster:
gcloud dataproc jobs submit pyspark ${GCP_BUCKET}/${fichier à lancer} 
--cluster=`${nom cluster}`  \   
--region=`${region du cluster}`  \     
--py-files `${GCP_BUCKET}`/python_packages.zip,`${GCP_BUCKET}`/CC_name.py,`${GCP_BUCKET}`/LOG_MESSAGE.py,`${GCP_BUCKET}`/download_wat.py,`${GCP_BUCKET}`/download_wat_paths.py,`${GCP_BUCKET}`/download_wet.py,`${GCP_BUCKET}`/download_wet_paths.py,`${GCP_BUCKET}`/write_wet_parquet_files.py,`${GCP_BUCKET}`/write_wat_parquet_files.py,`${GCP_BUCKET}`/download_warc.py,`${GCP_BUCKET}`/download_warc_paths.py \  
-- `${GCP_BUCKET}` `${first url}` `${last url}` `${pas}`

si on veut créer les fichiers wet entre les urls 0 à 900000(le max) avec un pas de 1000 (donc on veut traiter 900 urls et entre chaque url traitée on en saute 1000)
on passe la commande suivante : 

gcloud dataproc jobs submit pyspark `${GCP_BUCKET}`/run_wet_gcp.py 
--cluster=`${nom cluster}` 
--region=`${région du cluster}` 
--py-files `${GCP_BUCKET}`/python_packages.zip,`${GCP_BUCKET}`/CC_name.py,`${GCP_BUCKET}`/LOG_MESSAGE.py,`${GCP_BUCKET}`/download_wat.py,`${GCP_BUCKET}`/download_wat_paths.py,`${GCP_BUCKET}`/download_wet.py,`${GCP_BUCKET}`/download_wet_paths.py,`${GCP_BUCKET}`/write_wet_parquet_files.py,`${GCP_BUCKET}`/write_wat_parquet_files.py,`${GCP_BUCKET}`/download_warc.py,`${GCP_BUCKET}`/download_warc_paths.py 
-- `${GCP_BUCKET}` 0 900000 1000

### 6) Pour créer sur gcp le dataset de fichier "final_parquet" qui est la jointure entre les wet_parquet et les wat_parquet il faut exécuter les 3 commandes suivantes:

**CONSEIL**: commencer avec first_url=0 last_url=10 pas=1.  
  
1) gcloud dataproc jobs submit pyspark `${GCP_BUCKET}`/run_wet_gcp.py 
--cluster=`${nom cluster}` \
--region=`${région du cluster}` \   
--py-files `${GCP_BUCKET}`/python_packages.zip,`${GCP_BUCKET}`/CC_name.py,`${GCP_BUCKET}`/LOG_MESSAGE.py,`${GCP_BUCKET}`/download_wat.py,`${GCP_BUCKET}`/download_wat_paths.py,`${GCP_BUCKET}`/download_wet.py,`${GCP_BUCKET}`/download_wet_paths.py,`${GCP_BUCKET}`/write_wet_parquet_files.py,`${GCP_BUCKET}`/write_wat_parquet_files.py,`${GCP_BUCKET}`/download_warc.py,`${GCP_BUCKET}`/download_warc_paths.py  \  
-- `${GCP_BUCKET}` `${first url}` `${last url}` `${pas}`

2) gcloud dataproc jobs submit pyspark ${GCP_BUCKET}/run_wat_gcp.py 
--cluster=`${nom cluster}` \
--region=`${région du cluster}` \ 
--py-files `${GCP_BUCKET}`/python_packages.zip,`${GCP_BUCKET}`/CC_name.py,`${GCP_BUCKET}`/LOG_MESSAGE.py,`${GCP_BUCKET}`/download_wat.py,`${GCP_BUCKET}`/download_wat_paths.py,`${GCP_BUCKET}`/download_wet.py,`${GCP_BUCKET}`/download_wet_paths.py,`${GCP_BUCKET}`/write_wet_parquet_files.py,`${GCP_BUCKET}`/write_wat_parquet_files.py,`${GCP_BUCKET}`/download_warc.py,`${GCP_BUCKET}`/download_warc_paths.py  \  
-- `${GCP_BUCKET}` `${first url}` `${last url}` `${pas}`

3) gcloud dataproc jobs submit pyspark `${GCP_BUCKET}`/write_gcp_final_parquet_files.py 
--cluster=`${nom cluster}` \  
--region=`${région du cluster}`  \   
--py-files `${GCP_BUCKET}`/python_packages.zip,`${GCP_BUCKET}`/CC_name.py,`${GCP_BUCKET}`/LOG_MESSAGE.py,`${GCP_BUCKET}`/download_wat.py,`${GCP_BUCKET}`/download_wat_paths.py,`${GCP_BUCKET}`/download_wet.py,`${GCP_BUCKET}`/download_wet_paths.py,`${GCP_BUCKET}`/write_wet_parquet_files.py,`${GCP_BUCKET}`/write_wat_parquet_files.py,`${GCP_BUCKET}`/download_warc.py,`${GCP_BUCKET}`/download_warc_paths.py \  
-- `${GCP_BUCKET}` `${first url}` `${last url}` `${pas}`

4) gcloud storage cp -r ${GCP_BUCKET}/final_parquet .


## Explication des fonctions : 

### modification des targets

Les targets sont insensibles à la casse.  
Pour modifier les targets, il faut modifier le fichier **write_wet_parquet_files.py** dans lequel TARGETS indique les targets. Il est possible d'ajouter des targets en rajoutant une liste de string, ou alors d'augmenter la couverture d'une target en rajoutant des string dans la liste voulue. (par exemple si target = [["biden"],["emmanuel macron","le président français"]] alors une target va compter les occurences "biden" et une autre target comptera la somme des occurences "emmanuel macron" avec les occurences "le président français".  

### download_{warc,wet,wat}_paths.py

Les programmes **download_{warc,wet,wat}_paths.py** vont écrire un fichier gz contenant toutes les urls qu'il faut contacter pour trouver les fichiers {wat,warc,wet}.
Ces urls sont ensuite écrites dans des fichiers txt dans le dossier {wet,wat,warc}_paths 

### download_{warc,wet,wat}.py

Les programmes **download_{warc,wat,wet}.py** donnent la possibilité de charger les urls, les requêtes et les pages des fichiers {warc,wat,wet}.
Nous n'utiliserons généralement que le chargement des urls.
  
Le programme **download_wat** possède une fonction nommée **get_wat_urls(wat_from_wet = True)** qui charge les urls dans une liste et renvoit cette liste. L'argument wat_from_wet permet de télécharger les fichiers qui sont reliés aux fichiers wet (par défaut les urls ne sont pas coordonnées dans common crawl ce qui peut amener à télécharger des fichiers wat qui ne sont pas reliés aux fichiers wet).
Les fonctions **get_gcp_{wat,wet,warc}_urls** ne sont qu'une variante pour s'adapter à la sémantique de gcp.  
  
Les fonctions **gcp_build_df_urls(spark_session, bucket_path,first_url, last_url, pas)** créent un dataframe contenant les urls à contacter. le dataframe contient les urls qui survivent au slicing suivant: all_urls[first_url:last_url:pas].   

### write_{wet,wat}_parquet_files.py

Les programmes **write_`{wet,wat}`_parquet_files.py** donnent la possibilité de transformer les fichiers {wet,wat} en fichier parquet.
la fonction **write_`{wet,wat}`_parquet_files** et la fonction **`{wet,wat}`_urls_to_parquet** présentent un goulot d'étranglement qui implique une utilisation non optimale de spark et limite la capacité à partager le travail entre plusieurs workers. Ce goulot est laissé volontairement car il est corrigé dans la fonction **gcp_write_{wet,wat}_parquet_files** et permet de montrer l'évolution du code tout au long du projet.
  
la fonction **gcp_write_`{wet,wat}`_parquet_files** et la fonction **gcp_`{wet,wat}`_urls_to_parquet** ne présentent plus ce problème.
Voici comment le goulot d'étranglement a été enlevé dans la fonction **gcp_write_`{wet,wat}`_parquet_files** :  
les urls menant aux fichiers `{wet,wat}` sont chargées dans un dataframe via la fonction **gcp_build_df_urls**.
Nous transformons le dataframe des urls en RDD sur lequel nous utilisons la fonction MapPartition qui permet de partitionner les urls et de les traiter en parallèle sur plusieurs workers.
Dans la fonction annexe **gcp_`{wet,wat}`_urls_to_parquet** nous utilisons également l'objet Session de request pour utiliser un cache et ainsi diminuer le nombre de requête effectué tout en gagnant potentiellement du temps sur le téléchargement aussi la fonction utilise le mot clé yield qui permet de faire un retour sous forme de flux au lieu d'attendre de retourner tous les résultats d'un coup.

pour récupérer les informations nécessaires à la construction des fichiers parquets nous utilisons les fonctions annexes suivantes:  
  
**Pour les fichiers wat:**  
  
**is_response(metadata_json)** vérifie que les métadonnées correspondent à une réponse, des métadonnées peuvent correspondre à une requête dans ce cas là nous sautons ces métadonnées.  
**get_title(metadata_json)** permet d'obtenir sur les métadonnées le titre de la page.  
**get_uri_host_path(metadata_json)** permet d'obtenir l'uri le host et le path de la page.
le champ WARC-Record-ID et le champ WARC-Refers-To sont directement extraits dans la fonction **wat_urls_to_parquet**
  
**Pour les fichiers wet:**  
  
**count_occurence(targets, text)** compte les occurences de chaque target dans le text via la fonction count (appliqué comme cela: text.count(target)), les targets sont insensibles à la casse.
les champs WARC-Record-ID, WARC-Refers-To, YEAR, MONTH, DAY sont directement extraits dans la fonction **wet_urls_to_parquet**  

### write_final_parquet_files.py

Ce programme contient la fonction **write_final_parquet_files(spark_session, first_url_processed_in_parquet,last_url_processed_in_parquet, pas)**   
Cette fonction permet de créer les fichiers parquet appelés "final_parquet" qui sont les fichiers parquet représentant la jointure entre les fichiers wet et wat.  
Pour bien comprendre les arguments à mettre, si jamais dans votre fichier wet_parquet vous avez : wet_parquet_files_100000_200000.parquet wet_parquet_files_200000_300000.parquet (et la même chose dans wat_parquet)   
alors il faut appeler la fonction avec :   
first_url_processed=100000, last_url_processed = 300000, pas = 100000   
Signifiant que la plus petite url traitée est la numéro 100000 la plus grande 300000 et qu'il y a un pas de 100000.

### read_final_parquet_files.py

Ce programme charge les fichiers final_parquet dans un dataframe et calcule des statistiques dessus.  
  
La fonction **get_n_occurence_by_months(df_final, n_occurence)** renvoit un dictionnaire qui relie chaque mois à une liste comportant le nombre de page comportant au moins n_occurence de chaque target.  
les clés sont ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"]  
Chaque liste est de taille égale au nombre de targets.    
  
La fonction **get_n_occurence_by_TLD(df_final, n_occurence)** renvoit un dictionnnaire qui relie chaque tdl à une liste comportant le nombre de page comportant au moins n_occurence de chaque target.
les clés sont [".ru", ".com", ".fr", ".uk", ".de", ".jp", ".cn", ".us", ".br", ".in"].  
Chaque liste est de taille égale au nombre de targets.  
  
La fonction **get_total_n_occurences(df_final, n_occurence)** renvoit une liste comportant le nombre de page total comportant au moins n_occurence de chaque target.  
  
La fonction **get_best_k_host_for_n_occurences(df_final, n_occurences, k_best, target)** renvoit une liste de tuple pour la TARGET target de taille k_best, où le tuple d'index k-1 représente le kème host ayant le plus de page comportant au moins n_occurence de la TARGET target. le tuple est dans le format suivant: (host, count).  




