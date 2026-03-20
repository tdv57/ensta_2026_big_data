#!/bin/bash
# =============================================================================
# deploy_and_run.sh
# Script complet pour déployer et lancer le pipeline sur GCP Dataproc
# =============================================================================
set -e

# --- Configuration ---
GCS_BUCKET="gs://ensta-bigdata-2024"
CLUSTER_NAME="ensta-cluster"
REGION="us-central1"
FIRST_URL=0
LAST_URL=900000    
PAS=900 
SCRIPT_START=$(date +%s)

# --- Étape 1 : Uploader le code dans GCS ---
echo "=== Upload du code dans GCS ==="
gcloud storage cp *.py          gs://${GCS_BUCKET#gs://}/code/
gcloud storage cp init_dataproc.sh gs://${GCS_BUCKET#gs://}/code/
echo "Code uploadé."

# --- Étape 2 : Créer le cluster Dataproc ---
echo "=== Création du cluster Dataproc ==="
REGIONS=("us-central1" "us-east1" "europe-west4" "us-west1")
CLUSTER_CREATED=false

for R in "${REGIONS[@]}"; do
    echo "Tentative de création dans $R..."
    if gcloud dataproc clusters create $CLUSTER_NAME \
        --region=$R \
        --num-workers=4 \
        --master-machine-type=e2-standard-4 \
        --worker-machine-type=e2-standard-4 \
        --master-boot-disk-size=100GB \
        --worker-boot-disk-size=100GB \
        --image-version=2.1-debian11 \
        --initialization-actions=$GCS_BUCKET/code/init_dataproc.sh \
        --metadata="GCS_BUCKET=$GCS_BUCKET" 2>/dev/null; then
        REGION=$R
        CLUSTER_CREATED=true
        echo "Cluster créé dans $R"
        break
    else
        echo "Pas de ressources dans $R, essai suivant..."
    fi
done

if [ "$CLUSTER_CREATED" = false ]; then
    echo "ERREUR : impossible de créer le cluster dans aucune région."
    exit 1
fi

# --- Étape 3 : Lancer le job WET ---
echo "=== Lancement du job WET (Trump/Harris/Biden occurrences) ==="
gcloud dataproc jobs submit pyspark $GCS_BUCKET/code/run_wet.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --files=$GCS_BUCKET/code/write_wet_parquet_files.py,$GCS_BUCKET/code/download_wet.py,$GCS_BUCKET/code/download_wet_paths.py,$GCS_BUCKET/code/CC_name.py,$GCS_BUCKET/code/LOG_MESSAGE.py \
    --properties="spark.executorEnv.GCS_BUCKET=$GCS_BUCKET,spark.yarn.appMasterEnv.GCS_BUCKET=$GCS_BUCKET" \
    -- $FIRST_URL $LAST_URL $PAS
echo "Job WET terminé."

JOB_WET_END=$(date +%s)
echo "Job WET termine en $((JOB_WET_END - JOB_WET_START))s."

# --- Étape 4 : Lancer le job WAT ---
echo "=== Lancement du job WAT (titre, host, URI) ==="
gcloud dataproc jobs submit pyspark $GCS_BUCKET/code/run_wat.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --files=$GCS_BUCKET/code/write_wat_parquet_files.py,$GCS_BUCKET/code/download_wat.py,$GCS_BUCKET/code/download_wet_paths.py,$GCS_BUCKET/code/CC_name.py,$GCS_BUCKET/code/LOG_MESSAGE.py \
    --properties="spark.executorEnv.GCS_BUCKET=$GCS_BUCKET,spark.yarn.appMasterEnv.GCS_BUCKET=$GCS_BUCKET" \
    -- $FIRST_URL $LAST_URL $PAS
echo "Job WAT terminé."

JOB_WAT_END=$(date +%s)
echo "Job WAT termine en $((JOB_WAT_END - JOB_WAT_START))s."

# --- Étape 5 : Lancer la jointure finale ---
echo "=== Lancement de la jointure WAT + WET ==="
gcloud dataproc jobs submit pyspark $GCS_BUCKET/code/write_final_parquet.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --files=$GCS_BUCKET/code/LOG_MESSAGE.py \
    --properties="spark.executorEnv.GCS_BUCKET=$GCS_BUCKET,spark.yarn.appMasterEnv.GCS_BUCKET=$GCS_BUCKET" \
    -- $FIRST_URL $LAST_URL
echo "Jointure terminée."

JOB_JOIN_END=$(date +%s)

# --- Étape 6 : Lancer l'analyse finale ---
echo "=== Analyse des résultats ==="
gcloud dataproc jobs submit pyspark $GCS_BUCKET/code/read_final_parquet_files.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --files=$GCS_BUCKET/code/LOG_MESSAGE.py \
    --properties="spark.executorEnv.GCS_BUCKET=$GCS_BUCKET,spark.yarn.appMasterEnv.GCS_BUCKET=$GCS_BUCKET"
echo "Analyse terminée."

# --- Étape 7 : Supprimer le cluster ⚠️ ---
echo "=== Suppression du cluster (stop la facturation) ==="
gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet
echo "Cluster supprimé."

SCRIPT_END=$(date +%s)
TOTAL=$((SCRIPT_END - SCRIPT_START))
N_EFFECTIVE=$(( ($LAST_URL - $FIRST_URL) / $PAS ))

echo "=============================================="
echo " RESUME FINAL"
echo "=============================================="
echo " Duree totale    : $((TOTAL/3600))h $(((TOTAL%3600)/60))m $((TOTAL%60))s"
echo " Job WET         : $((JOB_WET_END - JOB_WET_START))s"
echo " Job WAT         : $((JOB_WAT_END - JOB_WAT_START))s"
echo " Jointure        : $((JOB_JOIN_END - JOB_JOIN_START))s"
echo "----------------------------------------------"
echo " Fichiers traites      : $N_EFFECTIVE WET + $N_EFFECTIVE WAT"
echo " Volume source estime  : ~$(( N_EFFECTIVE * 420 )) MB (~$(( N_EFFECTIVE * 420 / 1024 )) GB)"
echo "----------------------------------------------"
echo " Taille des resultats dans GCS :"
echo -n "   wet_parquet   : " && gcloud storage du -s gs://ensta-bigdata-2024/wet_parquet/ | awk '{printf "%.1f MB\n", $1/1024/1024}'
echo -n "   wat_parquet   : " && gcloud storage du -s gs://ensta-bigdata-2024/wat_parquet/ | awk '{printf "%.1f MB\n", $1/1024/1024}'
echo -n "   final_parquet : " && gcloud storage du -s gs://ensta-bigdata-2024/final_parquet/ | awk '{printf "%.1f MB\n", $1/1024/1024}'
echo "=============================================="

echo ""
echo "=== Pipeline terminé ==="
echo "Résultats dans : $GCS_BUCKET/final_parquet/"
echo "Pour voir les fichiers :"
echo "  gcloud storage ls $GCS_BUCKET/final_parquet/"
