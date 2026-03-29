#!/bin/bash
# =============================================================================
# load_wat_to_hdfs.sh
# Télécharge des fichiers WAT de Common Crawl et les charge dans HDFS
# via un pipe (aucun stockage temporaire local)
# =============================================================================

set -e  # arrêter le script si une commande échoue

# --- Configuration ---
CRAWL="CC-MAIN-2024-46"           # crawl à utiliser
N_FILES=5                          # nombre de fichiers à charger
HDFS_INPUT="/hdfs/crawl/input"     # répertoire HDFS de destination

# --- Vérifications préalables ---
echo "=== Vérification de l'environnement ==="

if [ -z "$HADOOP_HOME" ]; then
    echo "ERREUR : \$HADOOP_HOME n'est pas défini"
    exit 1
fi
echo "HADOOP_HOME : $HADOOP_HOME"

# Vérifier que HDFS tourne
"$HADOOP_HOME/bin/hdfs" dfs -ls / > /dev/null 2>&1 || {
    echo "ERREUR : HDFS ne répond pas. Lance start-dfs.sh d'abord."
    exit 1
}
echo "HDFS : OK"

# Créer le répertoire d'input HDFS si besoin
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "$HDFS_INPUT"
echo "Répertoire HDFS : $HDFS_INPUT"

# --- Récupérer la liste des fichiers WAT ---
echo ""
echo "=== Récupération de la liste des fichiers WAT pour $CRAWL ==="

WAT_PATHS_URL="https://data.commoncrawl.org/crawl-data/$CRAWL/wat.paths.gz"
echo "URL : $WAT_PATHS_URL"

# Télécharger et décompresser la liste, garder les N premiers
mapfile -t PATHS < <(curl -s "$WAT_PATHS_URL" | zcat | head -n "$N_FILES")

echo "Fichiers à charger : ${#PATHS[@]}"

# --- Chargement dans HDFS ---
echo ""
echo "=== Chargement dans HDFS ==="

SUCCESS=0
FAIL=0

for path in "${PATHS[@]}"; do
    filename=$(basename "$path")
    url="https://data.commoncrawl.org/$path"

    echo ""
    echo "--- [$((SUCCESS + FAIL + 1))/$N_FILES] $filename ---"
    echo "Source : $url"

    # Vérifier si le fichier existe déjà dans HDFS
    if "$HADOOP_HOME/bin/hdfs" dfs -test -e "$HDFS_INPUT/$filename" 2>/dev/null; then
        echo "Déjà présent dans HDFS, on passe."
        SUCCESS=$((SUCCESS + 1))
        continue
    fi

    # Télécharger et piper directement dans HDFS
    if wget --progress=dot:mega -qO- "$url" | \
       "$HADOOP_HOME/bin/hdfs" dfs -put - "$HDFS_INPUT/$filename"; then
        echo "OK : $filename chargé dans HDFS"
        SUCCESS=$((SUCCESS + 1))
    else
        echo "ECHEC : $filename"
        # Supprimer le fichier partiel s'il existe
        "$HADOOP_HOME/bin/hdfs" dfs -rm -f "$HDFS_INPUT/$filename" 2>/dev/null || true
        FAIL=$((FAIL + 1))
    fi
done

# --- Résumé ---
echo ""
echo "=== Résumé ==="
echo "Succès : $SUCCESS / $N_FILES"
echo "Échecs : $FAIL / $N_FILES"
echo ""
echo "Contenu de $HDFS_INPUT :"
"$HADOOP_HOME/bin/hdfs" dfs -du -h "$HDFS_INPUT"

echo ""
echo "=== Prêt à lancer le job ==="
echo "cp src/main/resources/election_keywords.csv ."
echo "mvn clean package -q"
echo "\"\$HADOOP_HOME/bin/hadoop\" jar target/commoncrawl-analysis-1.0-SNAPSHOT.jar \\"
echo "    bigdata.job.CrawlAnalysisDriver \\"
echo "    -files election_keywords.csv \\"
echo "    $HDFS_INPUT \\"
echo "    /hdfs/crawl/output"
