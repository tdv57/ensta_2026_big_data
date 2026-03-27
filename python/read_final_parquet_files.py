from pyspark.sql import SparkSession
import os 
from LOG_MESSAGE import INFO, DEBUG, ERROR, WARNING
from pyspark.sql.functions import count

final_parquet = os.path.abspath("final_parquet")



def get_n_occurence_by_months(df_final, n_occurence):
    months = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"]
    occurences = dict()
    print(INFO + f"nombre de ligne avec {n_occurence} occurences de chaque target par mois")
    for i in range(0,12):
        df_march = df_final.filter(df_final.MONTH == i+1)
        count_TRUMP = df_march.filter(df_final.Target0 >= n_occurence).count()
        count_HARRIS = df_march.filter(df_final.Target1 >= n_occurence).count()
        count_BIDEN = df_march.filter(df_final.Target2 >= n_occurence).count()
        print(INFO + f"Pour le mois {months[i]}")
        print(f"Biden = {count_BIDEN}")
        print(f"Trump = {count_TRUMP}")
        print(f"Harris = {count_HARRIS}")
        occurences[months[i]] = [count_TRUMP, count_HARRIS, count_BIDEN]
    return occurences

def get_n_occurence_by_TLD(df_final, n_occurence):
    TLDs = [".ru", ".com", ".fr", ".uk", ".de", ".jp", ".cn", ".us", ".br", ".in"]


    occurences = dict()
    for TLD in TLDs:
        print(INFO + f"fin d'url traité : {TLD}")
        df_TLD_TRUMP = df_final.filter(df_final.HOST.endswith(TLD)).filter(df_final.Target0 >= n_occurence).count()
        df_TLD_HARRIS = df_final.filter(df_final.HOST.endswith(TLD)).filter(df_final.Target1 >= n_occurence).count()
        df_TLD_BIDEN = df_final.filter(df_final.HOST.endswith(TLD)).filter(df_final.Target2 >= n_occurence).count()

        print(f"Trump = {df_TLD_TRUMP}") 
        print(f"Harris = {df_TLD_HARRIS}") 
        print(f"Biden = {df_TLD_BIDEN}")
        occurences[TLD] = [df_TLD_TRUMP, df_TLD_HARRIS, df_TLD_BIDEN]
    return occurences 

def get_total_n_occurences(df_final, n_occurences):
    total_occurences_TRUMP = df_final.filter(df_final.Target0 >= n_occurences).count()
    total_occurences_HARRIS = df_final.filter(df_final.Target1 >= n_occurences).count()
    total_occurences_BIDEN = df_final.filter(df_final.Target2 >= n_occurences).count()

    print(INFO + f"total n_occurences = {n_occurences} for each target")
    print(f"Trump = {total_occurences_TRUMP}")
    print(f"Harris = {total_occurences_HARRIS}")
    print(f"Biden = {total_occurences_BIDEN}")
    return [total_occurences_TRUMP, total_occurences_HARRIS, total_occurences_BIDEN]

def get_best_k_host_for_n_occurences(df_final, n_occurences, k_best, target):
  if target == 0:
    result = df_final.filter(df_final.Target0 >= n_occurences)
  elif target == 1:
    result = df_final.filter(df_final.Target1 >= n_occurences)
  else:
    result = df_final.filter(df_final.Target2 >= n_occurences)

  result = (result.groupBy("host") \
  .agg(count("*").alias("count"))
  .orderBy("count", ascending=False) \
  .limit(k_best)
  )
  return [(row["host"], row["count"]) for row in result.collect()]

def main():
    spark_session = SparkSession.builder \
    .getOrCreate()

    df_final = spark_session.read.option("recursiveFileLookup", "true") \
               .option("mergeSchema", "true") \
               .parquet(final_parquet)
    
    # get_total_n_occurences(df_final,1)
    
    # get_n_occurence_by_TLD(df_final,1)

    # get_total_n_occurences(df_final,8)
    
    # get_n_occurence_by_TLD(df_final,8)

    # get_n_occurence_by_months(df_final, 8)

    res = get_best_k_host_for_n_occurences(df_final=df_final, n_occurences=1, k_best=20, target=2)
    print(res)
if __name__ == "__main__":
    main()