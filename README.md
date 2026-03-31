
**Pour chaque dossier présent dans le repo un readme a été inclus pour expliquer ce que fait chaque partie et comment lancer les programmes de chaque partie.**  
  
## CONTEXTE DU PROJET

Le but du projet est de compté le nombre de pages sur internet parlant d'une cible. Dans notre cas nous recherchons les pages internet qui comportent des occurences des mots "Trump" "Biden" et "Harris" et qui datent de l'année 2024.  
Nous essayons de voir si un candidat a eu une couverture plus importante que les autres candidats et si l'on peut relier les évènements de la campagne présidentielle américaine de 2024 avec le nombre de page sur internet parlant des candidats.  

## DATASET

Le dataset utilisé est celui du common crawl. C'est un dataset en constante évolution dans lequel on vient scrapter et insérer les pages web rencontrées ainsi que leur contenu.  
Le dataset est composé de trois types de fichiers: wat, wet et warc.  
La taille complète des fichiers wat compressés sous format gz est d'environ 230 Mo * 900000.  
La taille complète des fichiers wet compressés sous format gz est d'environ 100 Mo * 900000.  
Les fichiers warc ne sont pas téléchargés. Ils représentent les pages html complètes des pages web.  
Les fichiers wet représentent le texte brut qui s'affiche sur la page web.  
Les fichiers wat contiennent des métadonnées liées à la page web et au scraping de la page.  
  
Pour de plus amples informations sur la structure des fichiers vous pouvez vous référer à cette page qui donne des informations très complètes:  
https://dmorgan.info/posts/common-crawl-python/  
  
## METHODOLOGIE

Le pipeline est suivant:  
1) Conta


