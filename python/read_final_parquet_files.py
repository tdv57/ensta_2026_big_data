from pyspark.sql import SparkSession
import os 
from LOG_MESSAGE import INFO, DEBUG, ERROR, WARNING

final_parquet = os.path.abspath("final_parquet")



def get_occurence_by_months(df_final):
    months = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"]
    occurences = dict()
    for i in range(1,13):
        df_march = df_final.filter(df_final.MONTH == i)
        count_TRUMP = df_march.filter(df_final.Target0 > 0).count()
        count_HARRIS = df_march.filter(df_final.Target1 > 0).count()
        count_BIDEN = df_march.filter(df_final.Target2 > 0).count()
        print(INFO + f"Pour le mois {months[i]}")
        print(f"Biden = {count_BIDEN}")
        print(f"Trump = {count_TRUMP}")
        print(f"Harris = {count_HARRIS}")
        occurences[months[i]] = [count_TRUMP, count_HARRIS, count_BIDEN]
    return occurences

def get_occurence_by_TLD(df_final):
    TLDs = [".ru", ".com", ".fr", ".uk", ".de", ".jp", ".cn", ".us", ".br", ".in"]

    df_final.limit(10).show()
    occurences = dict()
    for TLD in TLDs:
        print(INFO + f"fin d'url traité : {TLD}")
        df_TLD_TRUMP = df_final.filter(df_final.HOST.endswith(TLD)).filter(df_final.Target0 > 0).count()
        df_TLD_HARRIS = df_final.filter(df_final.HOST.endswith(TLD)).filter(df_final.Target1 > 0).count()
        df_TLD_BIDEN = df_final.filter(df_final.HOST.endswith(TLD)).filter(df_final.Target2 > 0).count()

        print(df_TLD_TRUMP) 
        print(df_TLD_HARRIS) 
        print(df_TLD_BIDEN)
        occurences[TLD] = [df_TLD_TRUMP, df_TLD_HARRIS, df_TLD_BIDEN]
    return occurences 

def get_total_occurences(df_final):
    total_occurences_TRUMP = df_final.filter(df_final.Target0 > 0).count()
    total_occurences_HARRIS = df_final.filter(df_final.Target1 > 0).count()
    total_occurences_BIDEN = df_final.filter(df_final.Target2 > 0).count()

    print(INFO + "total occurences for each target")
    print(f"Trump = {total_occurences_TRUMP}")
    print(f"Harris = {total_occurences_HARRIS}")
    print(f"Biden = {total_occurences_BIDEN}")
    return [total_occurences_TRUMP, total_occurences_HARRIS, total_occurences_BIDEN]
#df_final.filter(df.MONTH == "")

def main():
    spark_session = SparkSession.builder \
    .getOrCreate()

    df_final = spark_session.read.option("recursiveFileLookup", "true") \
               .option("mergeSchema", "true") \
               .parquet(final_parquet)
    
    get_total_occurences(df_final)

    
if __name__ == "__main__":
    main()