from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from LOG_MESSAGE import INFO, DEBUG, ERROR, WARNING
import download_warc as dwarc
import download_wat as dwat 
import download_wet as dwet 
from warcio.archiveiterator import ArchiveIterator



# Créer un parquet file à partir des wet files 

# Faire un champ pour compter les occurences de chaque variable 

# Faire un champ texte brut

# Entrée: tableau des targets exemple = [["trump"],["harris", "kamala harris"]] et le texte brut
# Sortie : tableau du nombre d'occurence pour chaque target [2, 4] par exemple
def count_occurence(targets, text) : 
    text_lower = text.lower()
    occurence = []
    for target in targets :
        s = 0 
        for target_word in target : 
            s += text_lower.count(target_word.lower())
        occurence.append(s)
    return occurence

def wet_response_to_parquet(spark_session, schema, wet_response, targets) : 
    BATCH_SIZE = 100
    rows = []
    wet_stream = ArchiveIterator(wet_response.raw)
    print(INFO + "writing parquet file")
    for record in wet_stream : 
        warc_id = record.rec_headers.get_header("WARC-Record-ID")
        text = record.content_stream().read().decode("utf-8", errors="ignore")
        counts = count_occurence(targets, text)
        if (counts != [0,0]):
            print(INFO + "fichier comportant la target trouvé")
            print(text)
            rows.append((warc_id, *counts))
        if len(rows) >= BATCH_SIZE:
            df = spark_session.createDataFrame(spark_session.sparkContext.parallelize(rows), schema=schema)
            df.write.mode("append").parquet("wet_output_spark.parquet")
            rows = []
    if len(rows) >= BATCH_SIZE:
        df = spark_session.createDataFrame(spark_session.sparkContext.parallelize(rows), schema=schema)
        df.write.mode("append").parquet("wet_output_spark.parquet")


def main() : 
    TARGETS = [["trump"], ["harris"]]
    spark_session = SparkSession.builder.getOrCreate()

    schema_struct_type =  \
    [ StructField("WARC_ID", StringType(), True)] + [StructField(f"Target{n_target}", IntegerType(), True) for n_target in range(len(TARGETS))]

    schema = StructType(schema_struct_type)

    wet_urls = dwet.get_wet_urls()
    print(INFO + f"Creating parquet files for target {TARGETS}")
    wet_response_to_parquet(spark_session=spark_session,targets=TARGETS, schema=schema, wet_response=dwet.get_wet_response(wet_urls[0]))
    spark_session.stop()

if __name__ == "__main__" : 
    main()

