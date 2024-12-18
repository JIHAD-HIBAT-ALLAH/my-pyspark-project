
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max

spark = SparkSession.builder.appName("Analyse des ventes").getOrCreate()

ventes = spark.read.csv("d:/dell/Documents/ventes.csv", header=True, inferSchema=True)

ventes = ventes.withColumn("Chiffre_d_affaires", col("Quantité") * col("Prix_unitaire"))

chiffre_affaires_total = ventes.agg(_sum("Chiffre_d_affaires").alias("Chiffre_affaires_total")).collect()[0][0]

produit_plus_vendu = ventes.groupBy("Produit").agg(_sum("Quantité").alias("Total_Vendu")) \
    .orderBy(col("Total_Vendu").desc()).first()

print(f"Chiffre d'affaires total : {chiffre_affaires_total} €")
print(f"Produit le plus vendu : {produit_plus_vendu['Produit']} ({produit_plus_vendu['Total_Vendu']} unités)")
spark.stop()
