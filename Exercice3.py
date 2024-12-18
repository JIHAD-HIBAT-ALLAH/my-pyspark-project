from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max,avg, min as _min , when , mean, lit

spark = SparkSession.builder.appName("Analyse des clients").getOrCreate()
clients = spark.read.csv("d:/dell/Documents/clients.csv", header=True, inferSchema=True)
moyenne_age = clients.agg(avg("Âge")).collect()[0][0]
clients = clients.fillna({"Âge": moyenne_age})
clients = clients.fillna({"Ville": "Inconnue"})
clients = clients.filter(col("Revenu").isNotNull())

clients.show()

spark.stop()
