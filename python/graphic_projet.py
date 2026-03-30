from read_final_parquet_files import get_total_n_occurences, get_n_occurence_by_TLD, get_n_occurence_by_months
import matplotlib.pyplot as plt 
from pyspark.sql import SparkSession 
import os 
from LOG_MESSAGE import INFO, ERROR, WARNING, DEBUG

def main():
    final_parquet = os.path.abspath("final_parquet")
    spark_session = SparkSession.builder \
    .getOrCreate()

    df_final = spark_session.read.option("recursiveFileLookup", "true") \
               .option("mergeSchema", "true") \
               .parquet(final_parquet)
    TLDs = [".ru", ".com", ".fr", ".uk", ".de", ".jp", ".cn", ".us", ".br", ".in"]
    # print(INFO + str(3067442 + 3247176 + 2805816 + 3002925 + 2938109 + 2661855 + 2749574 + 3017719 + 2781317))

    # [total_trump, total_harris, total_biden] = get_total_n_occurences(df_final=df_final,n_occurences=1)
    # plt.bar(["Trump","Harris","Biden"], [total_trump, total_harris, total_biden])
    # plt.title("Nombre de page comportant au moins 1 occurence de la target")
    # plt.xlabel("targets")
    # plt.ylabel("nombre de page comportant au moins une occurence de la target")
    # plt.show()



    # temp = get_n_occurence_by_months(df_final=df_final, n_occurence=1)
    # print(temp)
    # occurence_1_by_month = [v[0] for v in temp.values()]
    # mois = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"]
    # print(INFO + "test occurence")
    # plt.bar(mois,occurence_1_by_month)
    # plt.title("Nombre de page comportant au moins une occurence de Trump")
    # plt.xlabel("mois")
    # plt.ylabel("Nombre de pages")
    # plt.show()

    # temp = get_n_occurence_by_months(df_final=df_final, n_occurence=4)
    # print(temp)
    # occurence_1_by_month = [v[0] for v in temp.values()]
    # mois = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"]
    # print(INFO + "test occurence")
    # plt.bar(mois,occurence_1_by_month)
    # plt.title("Nombre de page comportant au moins 4 occurence de Trump")
    # plt.xlabel("mois")
    # plt.ylabel("Nombre de pages")
    # plt.show()


    # temp = get_n_occurence_by_months(df_final=df_final, n_occurence=1)
    # print(temp)
    # occurence_1_by_month = [v[1] for v in temp.values()]
    # mois = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"]
    # print(INFO + "test occurence")
    # plt.bar(mois,occurence_1_by_month)
    # plt.title("Nombre de page comportant au moins une occurence de Harris")
    # plt.xlabel("mois")
    # plt.ylabel("Nombre de pages")
    # plt.show()

    # temp = get_n_occurence_by_months(df_final=df_final, n_occurence=4)
    # print(temp)
    # occurence_1_by_month = [v[1] for v in temp.values()]
    # mois = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"]
    # print(INFO + "test occurence")
    # plt.bar(mois,occurence_1_by_month)
    # plt.title("Nombre de page comportant au moins 4 occurence de Harris")
    # plt.xlabel("mois")
    # plt.ylabel("Nombre de pages")
    # plt.show()


    # temp = get_n_occurence_by_months(df_final=df_final, n_occurence=1)
    # print(temp)
    # occurence_1_by_month = [v[2] for v in temp.values()]
    # mois = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"]
    # print(INFO + "test occurence")
    # plt.bar(mois,occurence_1_by_month)
    # plt.title("Nombre de page comportant au moins une occurence de Biden")
    # plt.xlabel("mois")
    # plt.ylabel("Nombre de pages")
    # plt.show()


    # temp = get_n_occurence_by_months(df_final=df_final, n_occurence=4)
    # print(temp)
    # occurence_1_by_month = [v[2] for v in temp.values()]
    # mois = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"]
    # print(INFO + "test occurence")
    # plt.bar(mois,occurence_1_by_month)
    # plt.title("Nombre de page comportant au moins 4 occurence de Biden")
    # plt.xlabel("mois")
    # plt.ylabel("Nombre de pages")
    # plt.show()

    # occurence_1_by_tld = get_n_occurence_by_TLD(df_final=df_final, n_occurence=1)
    # labels = ["Trump", "Harris", "Biden"]

    # # création des sous-graphiques
    # fig, axes = plt.subplots(2, 5, figsize=(15, 6))  # 2 lignes, 5 colonnes
    # axes = axes.flatten()  # pour itérer facilement

    # for i, (tld, values) in enumerate(occurence_1_by_tld.items()):
    #     axes[i].bar(labels, values)
    #     axes[i].set_title(tld)

    # plt.tight_layout()
    # plt.show()


    occurence_1_by_tld = get_n_occurence_by_TLD(df_final=df_final, n_occurence=4)
    labels = ["Trump", "Harris", "Biden"]

    # création des sous-graphiques
    fig, axes = plt.subplots(2, 5, figsize=(15, 6))  # 2 lignes, 5 colonnes
    axes = axes.flatten()  # pour itérer facilement

    for i, (tld, values) in enumerate(occurence_1_by_tld.items()):
        axes[i].bar(labels, values)
        axes[i].set_title(tld)

    plt.tight_layout()
    plt.show()
if __name__ == "__main__":
    main()