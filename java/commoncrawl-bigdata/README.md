# CommonCrawl — Analyse Élection US 2024

Pipeline MapReduce analysant les fichiers WAT de Common Crawl pour compter
les pages web mentionnant **Trump**, **Harris** et **Biden**.
Répartition par pays (TLD) et par langue.

---

## Prérequis

- Hadoop 3.4.1 avec `$HADOOP_HOME` défini
- Java 11+, Maven 3.x

---

## 1. Démarrer Hadoop

Dans deux terminaux séparés :

```bash
# Terminal 1
"$HADOOP_HOME/bin/hdfs" namenode

# Terminal 2
"$HADOOP_HOME/bin/hdfs" datanode
```

---

## 2. Initialiser HDFS (première fois uniquement)

```bash
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "/user/$USER"
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /hdfs/crawl/input
```

---

## 3. Charger les données WAT dans HDFS

Le script télécharge les fichiers WAT directement depuis Common Crawl
et les place dans HDFS **sans stockage temporaire local**.
Modifier `N_FILES` dans le script pour ajuster le nombre de fichiers (~220 MB chacun).

```bash
./load_wat_to_hdfs.sh
```

---

## 4. Compiler et lancer

```bash
mvn clean package -q

"$HADOOP_HOME/bin/hdfs" dfs -put -f $PWD/election_keywords.csv /user/$USER/election_keywords.csv
"$HADOOP_HOME/bin/hdfs" dfs -rm -r /hdfs/crawl/output 2>/dev/null || true

"$HADOOP_HOME/bin/hadoop" jar target/commoncrawl-analysis-1.0-SNAPSHOT.jar \
    bigdata.job.CrawlAnalysisDriver \
    -files $PWD/election_keywords.csv \
    /hdfs/crawl/input \
    /hdfs/crawl/output
```

> ⚠️ Le `put -f` avant chaque lancement est obligatoire — sans lui Hadoop
> charge une ancienne version du CSV depuis HDFS et les résultats sont vides.

---

## 5. Lire les résultats

```bash
# Totaux par candidat
"$HADOOP_HOME/bin/hdfs" dfs -cat /hdfs/crawl/output/job2_company_totals/part-r-00000

# Répartition par langue
"$HADOOP_HOME/bin/hdfs" dfs -cat /hdfs/crawl/output/job3_language_dist/part-r-00000

# Détail par (candidat, pays, langue)
"$HADOOP_HOME/bin/hdfs" dfs -cat /hdfs/crawl/output/job1_detail/part-r-00000
```

---

## Crawls 2024 disponibles

Modifier `CRAWL` dans `load_wat_to_hdfs.sh` pour changer de période :

| Crawl | Période |
|---|---|
| `CC-MAIN-2024-42` | Octobre 2024 — dernière ligne droite |
| `CC-MAIN-2024-46` | Novembre 2024 — résultats post-élection |
| `CC-MAIN-2024-51` | Décembre 2024 |

---

## Structure

```
src/main/java/bigdata/
├── job/CrawlAnalysisDriver.java          orchestration des 3 jobs
├── mapper/CrawlAnalysisMapper.java       parse WAT + détecte candidats
├── mapper/CompanyTotalMapper.java        total par candidat
├── mapper/LanguageDistributionMapper.java répartition par langue
├── reducer/CrawlAnalysisReducer.java     agrégation (aussi Combiner)
├── model/WatRecord.java                  parsing JSON des fichiers WAT
└── util/CompanyRegistry.java             chargement et matching mots-clés
src/main/resources/
└── election_keywords.csv                 mots-clés Trump / Harris / Biden
load_wat_to_hdfs.sh                       script de chargement HDFS
```
