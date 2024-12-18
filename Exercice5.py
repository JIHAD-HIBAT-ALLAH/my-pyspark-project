from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max,avg, min as _min , when , mean, lit

spark = SparkSession.builder.appName("Analyse des transactions").getOrCreate()
transactions = spark.read.csv("d:/dell/Documents/transactions.csv", header=True, inferSchema=True)
depenses_totales = transactions.groupBy("Client").agg(_sum("Montant").alias("Dépenses_totales"))
client_plus_depensier = depenses_totales.orderBy(col("Dépenses_totales").desc()).first()
depenses_totales.show()
print(f"Client ayant dépensé le plus : {client_plus_depensier['Client']} ({client_plus_depensier['Dépenses_totales']} €)")

spark.stop()
