# Databricks notebook source
# MAGIC %md
# MAGIC # Partycjonowanie i zapis do pliku

# COMMAND ----------

# DBTITLE 1,Pobranie przykładowego DF
import pyspark.sql.functions as F

df = spark.read.format("parquet").load(f'file:/Workspace/Repos/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/pl-pyspark-and-databricks/data/parquet/netflix_titles.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partycjonowanie
# MAGIC
# MAGIC Każdy tworzony DF jest automatycznie partycjonowany przez Sparka. Ilość partycji jest wtedy ustala na podstawie wielkości buforu, sektora dysku (domyślnie jest 128MB) i samej wielkości danych. Każda partycja oznacza oddzielny plik (part) podczas zapisywania.
# MAGIC
# MAGIC Aby zweryfikować ilość partycji możemy wykonać ```df.rdd.getNumPartitions()```

# COMMAND ----------

# DBTITLE 1,Wyświetlenie ilości partycji przykładowego DF
print(df.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC Aby zmienić ilość partycji (czyli plików) należy skorzystać jednej z dwóch metod:
# MAGIC
# MAGIC - coalesce(n) - pozwala jedynie redukować ilość partycji.
# MAGIC - repartition(n) - ustawia ilość partycji na n.
# MAGIC
# MAGIC > ✅ Partycjonowanie wykona się, jeżeli dane na to pozwolą (np. ciężko utworzyć dwie partycje z jednego wiersza)

# COMMAND ----------

# DBTITLE 1,Przykład
# coalesce
print(f'Ilość partcji po coalesce: {df.coalesce(2).rdd.getNumPartitions()}')

# repartition
print(f'Ilość partycji po repartition: {df.repartition(8).rdd.getNumPartitions()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Zapis do pliku
# MAGIC Po podaniu DataFrame (i ewentualnie dodanie ```coalesce/repartition```) możemy wykonać zapis do plików. Podczas zapisywania zostanie utworzony katalog o nazwie podanej w ścieżce. Sam zapisywanie składa się z metod:
# MAGIC
# MAGIC 1. ```write``` - inicjuje zapis
# MAGIC 2. ```mode``` - sposób zapisu
# MAGIC     - _overwrite_ - nadpisuje wszystko w podanej ścieżce
# MAGIC     - _append_ - dodaje dane do wskazanej ścieżki
# MAGIC     - _ignore_ - Jeżeli katalog istnieje, nie wykonuj zapisu
# MAGIC     - _error_ - Jeżeli katalog istnieje, zwróć błąd (domyślna)
# MAGIC 3. ```format("nazwa")``` - format zapisu danych
# MAGIC 4. ```options(k1=v1, k2=v2, ..)``` - dodatkowe opcje zapisu (np. nadpisywanie schematu)
# MAGIC 5. ```save("ścieżka")``` - ścieżka do katalogu, gdzie mają być zapisane dane.
# MAGIC
# MAGIC > ✅ Jeżeli zapis zakończył się powodzeniem, to w katalogu pojawi się plik o nazwie **_SUCCESS**

# COMMAND ----------

# DBTITLE 1,Przykład
df.repartition(8).write.mode("overwrite").format("csv").options(header=False).save("tutaj/sciezka/do/katalogu")
