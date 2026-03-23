## Pour télécharger le dataset en local : 

1) lancer init.sh 
2) lancer sur deux terminaux différents ./launch_write_wet_parquet_files.sh et ./launch_write_wat_parquet_files.sh
En cas d'une erreur ou d'une fin du téléchargement avant la fin (wat ou wet):
    a) Il faut regarder wet_urls_downloaded 
    b) Supprimer tout les fichiers parquet dans le dossier wet_parquet qui ne contiennent pas les intervalles d'urls (normalement il n'y a qu'une ou zéro supression de dossier à faire) 
    c) Relancer le script.
3) une fois les deux scripts terminés lancer "python3 write_final_parquet.py" pour avoir le dataset final 
4) pour lire les fichiers il faut modifier le main des fonctions read_{wet,wat,final}_parquet_files.py et les lancer avec python3

##Pour télécharger le dataset via google cloud platform:

Nous appelerons \$\{GCP_BUCKET\} le nom de votre bucket sous le format suivant:
gs://nom_bucket


lancer ./init.sh
Créer un projet et un google cloud storage 
Créer un cluster via la console gcp (dataproc -> cluster -> create cluster) 
Attention: il faut faire en sorte de permettre des requêtes vers internet en décochant l'option "Adresse IP interne uniquement" dans l'onglet "Personnaliser le cluster"
mettre sur le cluster les dossiers et fichiers suivants:

\$\{GCP\_BUCKET\}/CC_name.py
\$\{GCP\_BUCKET\}/LOG_MESSAGE.py
\$\{GCP\_BUCKET\}/download_warc.py
\$\{GCP\_BUCKET\}/download_warc_paths.py
\$\{GCP\_BUCKET\}/download_wat.py
\$\{GCP\_BUCKET\}/download_wat_paths.py
\$\{GCP\_BUCKET\}/download_wet.py
\$\{GCP\_BUCKET\}/download_wet_paths.py
\$\{GCP\_BUCKET\}/python_packages.zip
\$\{GCP\_BUCKET\}/write_wet_parquet_files.py
\$\{GCP\_BUCKET\}/write_wat_parquet_files.py
\$\{GCP\_BUCKET\}/write_gcp_final_parquet_files.py
\$\{GCP\_BUCKET\}/wat_paths/
\$\{GCP\_BUCKET\}/wet_paths/
\$\{GCP\_BUCKET\}/run_wet_gcp.py
\$\{GCP\_BUCKET\}/run_wat_gcp.py

et créer le fichier suivant
\$\{GCP_BUCKET\}/wet_parquet_extra_info (le fichier récoltera le nombre de pages webs lues et le nombre de pages webs ne présentant aucune occurence de chaque target)

Voici les commandes à copié collé pour initialiser les scripts sur gcp:
[ATTENTION] si vous voulez refaire un téléchargement il faudra supprimer les dossier wet_parquet wat_parquet final_parquet ainsi que remettre à zéro le fichier wet_parquet_extra_info sous peine de mélanger les nouvelles données avec les anciennes 

1° gcloud storage cp python/\{CC\_name.py,LOG\_MESSAGE.py,download\_warc.py,download\_warc\_paths.py,download\_wat.py,download\_wat\_paths.py,download\_wet.py,download\_wet\_paths.py,write\_wet\_parquet\_files.py,write\_wat\_parquet\_files.py,write\_gcp\_final\_parquet\_files.py,run\_wet\_gcp.py,run\_wat\_gcp.py\}
2° gcloud storage touch \$\{GCP\_BUCKET\}/wet\_parquet\_extra\_info
3° gcloud storage cp -r python/\{python\_packages.zip,wat\_paths/,wet\_paths/\}


payload pour lancer le job sur le cluster:
gcloud dataproc jobs submit pyspark \$\{GCP\_BUCKET\}/\$\{fichier à lancer\} \
--cluster=\$\{nom cluster\}  \   
--region=\$\{region du cluster\}  \   
--py-files \$\{GCP\_BUCKET\}/python\_packages.zip,\$\{GCP\_BUCKET\}/CC\_name.py,\$\{GCP\_BUCKET\}/LOG\_MESSAGE.py,\$\{GCP\_BUCKET\}/download\_wat.py,\$\{GCP\_BUCKET\}/download\_wat\_paths.py,\$\{GCP\_BUCKET\}/download\_wet.py,\$\{GCP\_BUCKET\}/download\_wet\_paths.py,\$\{GCP\_BUCKET\}/write\_wet\_parquet\_files.py,\$\{GCP\_BUCKET\}/write\_wat\_parquet\_files.py,\$\{GCP\_BUCKET\}/download\_warc.py,\$\{GCP\_BUCKET\}/download\_warc\_paths.py \
-- \$\{GCP\_BUCKET\} \$\{first url\} \$\{last url\} \$\{pas\}

si on veut créer les fichiers wet entre les urls 0 à 900000(le max) avec un pas de 1000 (donc on veut traiter 900 urls et entre chaque url traitée on en saute 1000)
on passe la commande suivante : 

gcloud dataproc jobs submit pyspark \$\{GCP\_BUCKET\}/run\_wet\_gcp.py \
--cluster=\$\{nom cluster\} \
--region=\$\{région du cluster\} \
--py-files \$\{GCP\_BUCKET\}/python\_packages.zip,\$\{GCP\_BUCKET\}/CC\_name.py,\$\{GCP\_BUCKET\}/LOG\_MESSAGE.py,\$\{GCP\_BUCKET\}/download\_wat.py,\$\{GCP\_BUCKET\}/download\_wat\_paths.py,\$\{GCP\_BUCKET\}/download\_wet.py,\$\{GCP\_BUCKET\}/download\_wet\_paths.py,\$\{GCP\_BUCKET\}/write\_wet\_parquet\_files.py,\$\{GCP\_BUCKET\}/write\_wat\_parquet\_files.py,\$\{GCP\_BUCKET\}/download\_warc.py,\$\{GCP\_BUCKET\}/download\_warc\_paths.py \
-- \$\{GCP\_BUCKET\} 0 900000 1000

Pour créer sur gcp le dataset de fichier "final\_parquet" qui est la jointure entre les wet\_parquet et les wat\_parquet il faut exécuter les 3 commandes suivantes:

1) gcloud dataproc jobs submit pyspark \$\{GCP\_BUCKET\}/run\_wet\_gcp.py \
--cluster=\$\{nom cluster\} \
--region=\$\{région du cluster\} \
--py-files \$\{GCP\_BUCKET\}/python\_packages.zip,\$\{GCP\_BUCKET\}/CC\_name.py,\$\{GCP\_BUCKET\}/LOG\_MESSAGE.py,\$\{GCP\_BUCKET\}/download\_wat.py,\$\{GCP\_BUCKET\}/download\_wat\_paths.py,\$\{GCP\_BUCKET\}/download\_wet.py,\$\{GCP\_BUCKET\}/download\_wet\_paths.py,\$\{GCP\_BUCKET\}/write\_wet\_parquet\_files.py,\$\{GCP\_BUCKET\}/write\_wat\_parquet\_files.py,\$\{GCP\_BUCKET\}/download\_warc.py,\$\{GCP\_BUCKET\}/download\_warc\_paths.py \
-- \$\{GCP\_BUCKET\} \$\{first url\} \$\{last url\} \$\{pas\}

2) gcloud dataproc jobs submit pyspark \$\{GCP\_BUCKET\}/run\_wat\_gcp.py \
--cluster=\$\{nom cluster\} \
--region=\$\{région du cluster\} \
--py-files \$\{GCP\_BUCKET\}/python\_packages.zip,\$\{GCP\_BUCKET\}/CC\_name.py,\$\{GCP\_BUCKET\}/LOG\_MESSAGE.py,\$\{GCP\_BUCKET}/download\_wat.py,\${GCP\_BUCKET\}/download\_wat\_paths.py,\$\{GCP\_BUCKET\}/download\_wet.py,\$\{GCP\_BUCKET\}/download\_wet\_paths.py,\$\{GCP\_BUCKET\}/write\_wet\_parquet\_files.py,\$\{GCP\_BUCKET\}/write\_wat\_parquet\_files.py,\$\{GCP\_BUCKET\}/download\_warc.py,\$\{GCP\_BUCKET\}/download\_warc\_paths.py \
-- \$\{GCP\_BUCKET\} \$\{first url\} \$\{last url\} \$\{pas\}

3) gcloud dataproc jobs submit pyspark \$\{GCP\_BUCKET\}/write\_gcp\_final\_parquet\_files.py \
--cluster=\$\{nom cluster\} \
--region=\$\{région du cluster\} \
--py-files \$\{GCP\_BUCKET\}/python\_packages.zip,\$\{GCP\_BUCKET\}/CC\_name.py,\$\{GCP\_BUCKET\}/LOG\_MESSAGE.py,\$\{GCP\_BUCKET\}/download\_wat.py,\$\{GCP\_BUCKET\}/download\_wat\_paths.py,\$\{GCP\_BUCKET\}/download\_wet.py,\$\{GCP\_BUCKET\}/download\_wet\_paths.py,\$\{GCP\_BUCKET\}/write\_wet\_parquet\_files.py,\$\{GCP\_BUCKET\}/write\_wat\_parquet\_files.py,\$\{GCP\_BUCKET\}/download\_warc.py,\$\{GCP\_BUCKET\}/download\_warc\_paths.py \
-- \$\{GCP\_BUCKET\} \$\{first url\} \$\{last url\} \$\{pas\}

4) gcloud storage cp -r \$\{GCP\_BUCKET\}/final\_parquet .


## Explication des fonctions : 

Les programmes download\_\{warc,wet,wat\}\_paths.py vont écrire un fichier gz contenant toutes les urls qu'il faut contacter pour trouver les fichers \{wat,warc,wet\}.
Ces urls sont ensuite écrites dans des fichiers txt ayant le nom \{wet,wat,warc\}\_paths 

Les programmes download\_\{warc,wat,wet\}.py donne la possibilité de charger les urls, les requêtes et les pages des fichiers \{warc,wat,wet\}.
Nous n'utiliserons généralement que le chargement des urls.
Le programme download\_wat possède une fonction nommée get\_wat\_urls(wat\_from\_wet = True), wat\_from\_wet permet de télécharger les fichiers qui sont reliés aux fichiers wet (par défaut les urls ne sont pas coordonnées ce qui peut amener à télécharger des fichiers wat qui ne sont pas reliés aux fichiers wet)
Les fonctions get\_gcp\_\{wat,wet,warc\}\_urls ne sont qu'une variante pour s'adapter à la sémantique de gcp 

Les programmes write\{wat\_wet\}\_parquet\_files.py donne la possibilité de créer les datasets.
la fonction write\_\{wet,wat\}\_parquet\_files et la fonction \{wet,wat\}\_urls\_to\_parquet présentent un goulot d'étranglement qui implique une utilisation non optimale de spark et limite la capacité à partager le travail entre plusieurs workers.
la fonction gcp\_write\_\{wet,wat\}\_parquet\_files et la fonction gcp\_\{wet,wat\}\_urls\_to\_parquet ne présentent plus ce problème.

Avec la version gcp 
les urls menant aux fichiers \{wat,wet\} sont chargées dans un dataframe ce qui permet de diviser le traitement des urls entre différents workers.
Nous transformons le dataframe des urls en RDD sur lequel nous utilisons la fonction MapPartition qui permet de partitionner les urls et de les traiter en parallèle en plusieurs blocs.
Nous utilisons également l'objet Session de request pour utiliser un cache de réponse. La fonction yield permet de faire un retour sous forme de flux au lieu de retourner tout à la fois.



