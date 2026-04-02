
**Pour chaque dossier présent dans les différents dossier un readme a été inclus pour expliquer ce que fait chaque partie et comment lancer les programmes de chaque partie.**  
  
## CONTEXTE DU PROJET

Le projet a pour objectif de quantifier le nombre de pages web traitant d’une thématique donnée. Dans notre cas, il s’agit d’identifier et de comptabiliser les pages contenant des occurrences des mots « Trump », « Biden » et « Harris », publiées au cours de l’année 2024.  
  
L’objectif est d’analyser si l’un des candidats a bénéficié d’une couverture médiatique plus importante que les autres, et d’examiner dans quelle mesure les événements de la campagne présidentielle américaine de 2024 peuvent être corrélés avec le volume de pages en ligne mentionnant ces candidats.  

## RUN LE PROJET

Comme dit en préambule un readme a été ajouté dans chaque dossier pour expliquer comment faire tourner le code à l'intérieur du dossier.  

## Dépendances

Pour télécharger les packages nécessaires pour les scripts du dossier python et gcp il faut créer un environnement virtuel puis installer les packages dans le fichier `requirements.txt`.

## DATASET

Le dataset utilisé est celui du common crawl. C'est un dataset en constante évolution dans lequel on vient scrapter et insérer les pages web rencontrées ainsi que leur contenu.  
Le dataset est composé de trois types de fichiers: wat, wet et warc.  
La taille complète des fichiers wat compressés sous format gz est d'environ 230 Mo * 900000.  
La taille complète des fichiers wet compressés sous format gz est d'environ 100 Mo * 900000.  
La taille complète des fichiers parquet issus des fichiers wet font 41 508 Ko.  
La taille complète des fichiers parquet issus des fichiers wat font 63 0000 Ko.  
La complète des fichiers parquet final (jointure des parquet issus des fichiers wet et wat) font 
Ce qui nous amène à un dataset d'environ 300 To de données. Nous avons décidé de nous concentrer sur 1/1000e du dataset pour des soucis de temps, c'est-à-dire 300 Go.
Les fichiers warc ne sont pas téléchargés. Ils représentent les pages html complètes des pages web.  
Les fichiers wet représentent le texte brut qui s'affiche sur la page web.  
Les fichiers wat contiennent des métadonnées liées à la page web et au scraping de la page.  
  
Pour de plus amples informations sur la structure des fichiers vous pouvez vous référer à cette page qui donne des informations très complètes:  
https://dmorgan.info/posts/common-crawl-python/  
  
## METHODOLOGIE

**Description du pipeline:**  
1) Une requête est envoyée vers une URL fournissant une archive au format .gz, contenant une liste d’URLs à contacter pour récupérer les fichiers WAT, WET et WARC.  
2) Les fichiers sont traités directement en mémoire (RAM) et ne sont jamais écrits sur le disque.  
3) Les fichiers sont téléchargés et traités un par un. Lors du traitement, un script analyse leur contenu :  
   Pour tous les types de fichiers, on extrait le WARC-Record-ID ainsi que le champ WARC-Refers-To, ce dernier permettant de relier le contenu WET d’une page à son contenu WAT correspondant.  
   Pour les fichiers WET :
       On compte le nombre d’occurrences de chaque cible (ex : « Trump », « Biden », « Harris »).
       On extrait également la date de collecte (jour, mois, année).
       Les lignes ne contenant aucune occurrence (par exemple : (0, 0, 0)) ne sont pas conservées. En revanche, une ligne comme (1, 0, 1) est enregistrée.   
   Si le fichier est un fichier wat:  
       On récupère le titre de la page ainsi que des informations sur l’URL : URI, host et path.  
4) Les données extraites sont stockées temporairement dans une liste. Lorsqu’un volume suffisant de lignes est atteint, celles-ci sont écrites dans un fichier au format Parquet.
   Un fichier supplémentaire, wet_parquet_extra_info, contient des informations complémentaires telles que :
       les bornes des URLs traitées,
       le nombre total de pages analysées,
       le nombre de pages ne contenant aucune occurrence (et donc absentes des fichiers Parquet).  
   
## Résultats 

Les résultats obtenus sont nombreux. De manière générale, on observe que le nombre de pages contenant une occurrence du mot « Trump » est supérieur à celui des deux autres candidats. Toutefois, ces pages ne représentent qu’environ 1 % de l’ensemble des pages analysées (nombre total de pages : 26 271 933).  

**Nombre total de pages comportant au moins 1 occurence de la cible**  
<img width="1318" height="655" alt="image" src="https://github.com/user-attachments/assets/3b223a77-62c3-4ca0-9fad-00e202b1091b" />
    
Il est également possible de mettre en relation le nombre de sites contenant au moins 1 ou 4 occurrences de « Biden » et « Trump » avec certains événements marquants de la campagne présidentielle américaine.  

**Nombre de site comportant au moins 1 occurence de biden réparti par mois**  
<img width="1023" height="511" alt="image" src="https://github.com/user-attachments/assets/3de5e9d6-ec45-4bd8-a59a-b56e76aa0455" />

**Nombre de site comportant au moins 4 occurences de biden réparti par mois**  
<img width="1021" height="516" alt="image" src="https://github.com/user-attachments/assets/60689376-72ac-4796-b1cb-2ffc4447efe9" />


Concernant Joe Biden, le pic observé en juillet peut s’expliquer par le fait qu’il s’agit du mois durant lequel il s’est retiré de la course présidentielle. La baisse du nombre de pages web mentionnant Biden après cette période peut s’expliquer par la prise de relais progressive de Kamala Harris dans l’attention médiatique.  

**Nombre de site comportant au moins 1 occurence de trump réparti par mois**  
  
<img width="999" height="517" alt="image" src="https://github.com/user-attachments/assets/57ea25a7-a507-4e3a-b885-d2c531ca2f15" />

**Nombre de site comportant au moins 4 occurences de trump réparti par mois**  
  
<img width="1007" height="519" alt="image" src="https://github.com/user-attachments/assets/5703dbae-67ab-4470-a877-eee737963c29" />
  
Pour Donald Trump, le pic observé en juillet peut être associé à la tentative d’assassinat dont il a été victime. Par ailleurs, les pics de novembre et décembre s’expliquent probablement par son élection à la présidence.  
On peut également regarder le nombre de sites comportant au moins 1 ou 4 occurences pour chaque cible par tld:  

On peut également analyser la répartition des sites contenant au moins 1 ou 4 occurrences par domaine de premier niveau (TLD) :  

**Nombre de site comportant au moins 1 occurence pour chaque cible réparti par tld**  
  
<img width="1662" height="805" alt="image" src="https://github.com/user-attachments/assets/679ae4f6-3df9-4602-98de-356c99225045" />

**Nombre de site comportant au moins 4 occurences pour chaque cible réparti par tld**  
<img width="1774" height="713" alt="image" src="https://github.com/user-attachments/assets/958fedea-eafe-4e65-aff8-8b36d9787654" />
  
Il est aussi pertinent d’examiner les sites comportant le plus grand nombre de pages avec au moins 1 ou 4 occurrences.  
  
Les résultats concernant Harris montrent une certaine fragilité : le terme « Harris » étant un nom courant dans les pays anglophones, il introduit du bruit dans les données.  

**Résultats pour 1 occurence pour Harris:**

<img width="747" height="622" alt="image" src="https://github.com/user-attachments/assets/7c023673-ca7d-4629-8d1c-33f7abc7d0d3" />

**Résultats pour 4 occurences pour Harris:**

<img width="690" height="622" alt="image" src="https://github.com/user-attachments/assets/c75f4602-4bb4-4f25-859b-4c437089d964" />
  
Pour Biden, on observe que l’augmentation du seuil minimal d’occurrences permet d’obtenir des résultats plus robustes et plus pertinents.  
  
**Résultats pour 1 occurence pour Biden:**  

<img width="747" height="577" alt="image" src="https://github.com/user-attachments/assets/f4e1fc4d-aafe-4ed7-be6a-4ab6665880b6" />

**Résultats pour 4 occurences pour Biden:**  

<img width="697" height="576" alt="image" src="https://github.com/user-attachments/assets/5c5216f3-201b-4d1d-9da2-55573eb9a259" />

**Résultats pour 1 occurence pour Trump:**  

<img width="751" height="577" alt="image" src="https://github.com/user-attachments/assets/90b93e44-8cb8-4f69-8c60-5462819be2a0" />
  
**Résultats pour 4 occurences pour Trump:**  
  
<img width="690" height="576" alt="image" src="https://github.com/user-attachments/assets/29e25aaa-ee63-4a19-9bc3-833e91ee1629" />




