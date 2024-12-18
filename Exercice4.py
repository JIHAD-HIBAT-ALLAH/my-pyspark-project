from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max,avg, min as _min , when , mean, lit

spark = SparkSession.builder.appName("Analyse des produits").getOrCreate()
produits = spark.read.csv("d:/dell/Documents/produits.csv", header=True, inferSchema=True)
produits_les_plus_chers = produits.orderBy(col("Prix").desc()).limit(3)
produits_les_plus_chers.show()
statistiques_par_categorie = produits.groupBy("Catégorie") \
    .agg(avg("Prix").alias("Prix_moyen"), _sum("Prix").alias("Prix_total"))

statistiques_par_categorie.show()

spark.stop()

