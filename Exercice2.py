from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max,avg, min as _min , when


spark = SparkSession.builder.appName("Analyse des utilisateurs").getOrCreate()

utilisateurs = spark.read.json("d:/dell/Documents/utilisateurs.json")

utilisateurs = utilisateurs.withColumn("âge", col("âge").cast("int"))
utilisateurs = utilisateurs.filter(col("ville").isNotNull() & col("âge").isNotNull())
age_moyen = utilisateurs.agg(avg("âge").alias("Age_Moyen")).collect()[0][0]
utilisateurs_par_ville = utilisateurs.groupBy("ville").count()

utilisateur_le_plus_jeune = utilisateurs.orderBy(col("âge").asc()).first()
print(f"Âge moyen : {age_moyen:.1f} ans")
utilisateurs_par_ville.show()

if utilisateur_le_plus_jeune:
    print(f"Plus jeune utilisateur : {utilisateur_le_plus_jeune['nom']} ({utilisateur_le_plus_jeune['âge']} ans)")
else:
    print("Aucun utilisateur trouvé.")
spark.stop()
