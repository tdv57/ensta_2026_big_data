
**Pour chaque dossier présent dans les différents dossier un readme a été inclus pour expliquer ce que fait chaque partie et comment lancer les programmes de chaque partie.**  
  
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
1) Contacter une url qui transmet un dossier gz comportant les urls à contacter pour avoir les fichiers wat, wet et warc.
2) Les fichiers sont gardés en ram et ne sont jamais écrit sur le disque
3) On télécharge et traite les fichiers un par un. A chaque fois qu'on traite un fichier un script parse la page:
   Pour tous les types de fichiers: on récupère le WARD-ID et le WARC-REFERS-TO (ce dernier attribut permet de relier le contenu wet d'une page au contenu wat de cette même page).  
   Si le fichier est un fichier wet: on récupère le nombre d'occurence pour chaque cible, on récupère également le jour, le mois et l'année du scraping de la page. il est à noter que l'ont enregistre pas les lignes qui ne comportent aucune occurence pour chaque ligne. exemple: (1, 0, 1) est sauvegardé (1 occurence trump et 1 biden) tandis que (0, 0, 0) cette ligne n'est pas sauvegardée.  
   Si le fichier est un fichier wat: on récupère le titre de la page ainsi que l'URI le host et le path.
4) Une fois ces informations récupérées on les stocke dans une liste. Une fois qu'un nombre de lignes assez grand ont été atteint on écrit ce paquet de lignes dans un fichier parquet. Le fichier wet_parquet_extra_info contient les bornes des urls téléchargées ainsi que le nombre de pages rencontrées et le nombre de pages sans occurence et qui ne se retrouvent donc pas dans les fichiers parquet.
   
## Résultats 

Les résultats sont nombreux, on peut voir qu'en règle générale le nombre de page comportant l'occurence trump domine les deux autres candidats, cependant le nombre de sites comportant l'occurence trump ne représente que 1% des sites analysées (Nombre total de page = 26 271 933).

<img width="1318" height="655" alt="image" src="https://github.com/user-attachments/assets/3b223a77-62c3-4ca0-9fad-00e202b1091b" />

Il est également possible de relier le nombre de site comportant au moins 1 et 4 occurences de biden et Trump avec des évènements de la campagne américaine.

<img width="1023" height="511" alt="image" src="https://github.com/user-attachments/assets/3de5e9d6-ec45-4bd8-a59a-b56e76aa0455" />

<img width="1021" height="516" alt="image" src="https://github.com/user-attachments/assets/60689376-72ac-4796-b1cb-2ffc4447efe9" />

<img width="999" height="517" alt="image" src="https://github.com/user-attachments/assets/57ea25a7-a507-4e3a-b885-d2c531ca2f15" />

<img width="1007" height="519" alt="image" src="https://github.com/user-attachments/assets/5703dbae-67ab-4470-a877-eee737963c29" />
