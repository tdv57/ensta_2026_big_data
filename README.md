
**Pour chaque dossier présent dans les différents dossier un readme a été inclus pour expliquer ce que fait chaque partie et comment lancer les programmes de chaque partie.**  
  
## CONTEXTE DU PROJET

Le but du projet est de compter le nombre de pages sur internet parlant d'une cible. Dans notre cas nous recherchons les pages internet qui comportent des occurences des mots "Trump" "Biden" et "Harris" et qui datent de l'année 2024.  
Nous essayons de voir si un candidat a eu une couverture plus importante que les autres candidats et si l'on peut relier les évènements de la campagne présidentielle américaine de 2024 avec le nombre de page sur internet parlant des candidats.  

## DATASET

Le dataset utilisé est celui du common crawl. C'est un dataset en constante évolution dans lequel on vient scrapter et insérer les pages web rencontrées ainsi que leur contenu.  
Le dataset est composé de trois types de fichiers: wat, wet et warc.  
La taille complète des fichiers wat compressés sous format gz est d'environ 230 Mo * 900000.  
La taille complète des fichiers wet compressés sous format gz est d'environ 100 Mo * 900000.
Ce qui nous amène à un dataset d'environ 300 To de données. Nous avons décidé de nous concentrer sur 1/1000e du dataset pour des soucis de temps, c'est-à-dire 300 Go.
Les fichiers warc ne sont pas téléchargés. Ils représentent les pages html complètes des pages web.  
Les fichiers wet représentent le texte brut qui s'affiche sur la page web.  
Les fichiers wat contiennent des métadonnées liées à la page web et au scraping de la page.  
  
Pour de plus amples informations sur la structure des fichiers vous pouvez vous référer à cette page qui donne des informations très complètes:  
https://dmorgan.info/posts/common-crawl-python/  
  
## METHODOLOGIE

Le pipeline est le suivant:  
1) Contacter une url qui transmet un dossier gz comportant les urls à contacter pour avoir les fichiers wat, wet et warc.
2) Les fichiers sont gardés en ram et ne sont jamais écrit sur le disque
3) On télécharge et traite les fichiers un par un. A chaque fois qu'on traite un fichier un script parse la page:
   Pour tous les types de fichiers: on récupère le WARD-ID et le WARC-REFERS-TO (ce dernier attribut permet de relier le contenu wet d'une page au contenu wat de cette même page).  
   Si le fichier est un fichier wet: on récupère le nombre d'occurence pour chaque cible, on récupère également le jour, le mois et l'année du scraping de la page. il est à noter que l'ont enregistre pas les lignes qui ne comportent aucune occurence pour chaque ligne. exemple: (1, 0, 1) est sauvegardé (1 occurence trump et 1 biden) tandis que (0, 0, 0) cette ligne n'est pas sauvegardée.  
   Si le fichier est un fichier wat: on récupère le titre de la page ainsi que l'URI le host et le path.
4) Une fois ces informations récupérées on les stocke dans une liste. Une fois qu'un nombre de lignes assez grand ont été atteint on écrit ce paquet de lignes dans un fichier parquet. Le fichier wet_parquet_extra_info contient les bornes des urls téléchargées ainsi que le nombre de pages rencontrées et le nombre de pages sans occurence et qui ne se retrouvent donc pas dans les fichiers parquet.
   
## Résultats 

Les résultats sont nombreux, on peut voir qu'en règle général le nombre de page comportant l'occurence trump domine les deux autres candidats, cependant le nombre de sites comportant l'occurence trump ne représente que 1% des sites analysées (Nombre total de page = 26 271 933).  
  
<img width="1318" height="655" alt="image" src="https://github.com/user-attachments/assets/3b223a77-62c3-4ca0-9fad-00e202b1091b" />
  
Il est également possible de relier le nombre de site comportant au moins 1 et 4 occurences de biden et Trump avec des évènements de la campagne américaine.
  
<img width="1023" height="511" alt="image" src="https://github.com/user-attachments/assets/3de5e9d6-ec45-4bd8-a59a-b56e76aa0455" />
  
<img width="1021" height="516" alt="image" src="https://github.com/user-attachments/assets/60689376-72ac-4796-b1cb-2ffc4447efe9" />
  
Pour Joe Biden on peut expliquer le pic en juillet par le fait que ce le mois durant lequel il c'est retiré de la présidentielle, tandis que la perte du nombre de page web parlant de biden après juillet peut être expliqué par le fait que Kamala Harris a plus pris le pas sur Joe Biden.
  
<img width="999" height="517" alt="image" src="https://github.com/user-attachments/assets/57ea25a7-a507-4e3a-b885-d2c531ca2f15" />
  
<img width="1007" height="519" alt="image" src="https://github.com/user-attachments/assets/5703dbae-67ab-4470-a877-eee737963c29" />
  
On peut expliquer le pic en juillet par le fait que Donald Trump a été victime d'une tentative d'assassinat, de plus les pics de novembre et décembre peuvent être expliqués par le fait que Donald Trump a été élu président.  
  
On peut également regarder le nombre de sites comportant au moins 1 ou 4 occurences pour chaque cible par tld:
  
<img width="1662" height="805" alt="image" src="https://github.com/user-attachments/assets/679ae4f6-3df9-4602-98de-356c99225045" />
  
<img width="1774" height="713" alt="image" src="https://github.com/user-attachments/assets/958fedea-eafe-4e65-aff8-8b36d9787654" />
  
On peut également regarder les sites qui comportent le plus de pages comportant au moins 1 ou 4 occurences:  
  
Les sites comportant le plus d'occurences de Harris montrent que les résultats pour la candidate sont fragiles (Harris est un mot commun en Angleterre et aux USA).  

**Résultats pour 1 occurence pour Harris:**

<img width="747" height="622" alt="image" src="https://github.com/user-attachments/assets/7c023673-ca7d-4629-8d1c-33f7abc7d0d3" />

**Résultats pour 4 occurences pour Harris:**

<img width="690" height="622" alt="image" src="https://github.com/user-attachments/assets/c75f4602-4bb4-4f25-859b-4c437089d964" />
  
Pour biden on peut observer que l'augmentation du nombre d'occurences minimales permet d'avoir des résultats plus solide:   
  
**Résultats pour 1 occurence pour Biden:**  

<img width="747" height="577" alt="image" src="https://github.com/user-attachments/assets/f4e1fc4d-aafe-4ed7-be6a-4ab6665880b6" />

**Résultats pour 4 occurences pour Biden:**

<img width="697" height="576" alt="image" src="https://github.com/user-attachments/assets/5c5216f3-201b-4d1d-9da2-55573eb9a259" />

**Résultats pour 1 occurence pour Trump:**

<img width="751" height="577" alt="image" src="https://github.com/user-attachments/assets/90b93e44-8cb8-4f69-8c60-5462819be2a0" />
  
**Résultats pour 4 occurences pour Trump:**
  
<img width="690" height="576" alt="image" src="https://github.com/user-attachments/assets/29e25aaa-ee63-4a19-9bc3-833e91ee1629" />

## Dépendances

Pour télécharger les packages nécessaires pour les scripts du dossier python et gcp il faut créer un environnement virtuel puis installer les packages dans le fichier `requirements.txt`.


