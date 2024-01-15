# Databricks notebook source
# MAGIC %md
# MAGIC # Grupowanie i agregacja
# MAGIC
# MAGIC Grupowanie pozwala na dotarczanie danych w postaci agregacji informacji z wybranych kolumn. Możemy wykonywać agregacje na całym DataFrame (cała tabela) lub na poszczególnych kolumnach. Aby zastosować ```HAVING``` znany z SQL należy wykonywać metodę ```filter()``` po agregacji.

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.read.format("csv").options(header=True).load(f'file:/Workspace/Repos/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/pl-pyspark-and-databricks/data/csv/noble_prizes.csv')

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funkcje agregujące
# MAGIC
# MAGIC Do funkcji agregujących należą:
# MAGIC - ```sum(col)``` - sumowanie
# MAGIC - ```min(col)``` - wartość minimalna/najstarsza data
# MAGIC - ```max(col)``` - wartość maksymalna/najmłodasz data
# MAGIC - ```avg(col)``` - średnia
# MAGIC - ```collect_list(col)``` - zebranie "elementów" do listy.
# MAGIC - ```collect_set(col)``` - zebranie "elementów do listy, bez duplikatów

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregacja - cały DataFrame
# MAGIC Aby móc wykonywać agregację na całym DataFrame należy użyć metody ```select()```.

# COMMAND ----------

# DBTITLE 1,Przykłady
# Średnia wartość nagrody Nobla (bez dopasowania)
display(
  df.select(F.avg(F.col("prizeAmount")).alias("avg_prize_amount"))
)

# Najwyższe i najmniejsze ID
display(
  df.select(
    F.min(F.col("id")).alias("min_id"),
    F.max(F.col("id")).alias("max_id")
    )
)

# Wyświetl dostępne kategorie w liście. Bez duplikatów
display(
  df.select(F.collect_set(F.col("category")).alias("category_set"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregacja - metoda groupBy
# MAGIC
# MAGIC Jeżeli chcemy wykonywać grupowanie na podstawie wybranych kolumn, musimy skorzystać z metody ``groupBy(col1, col2, ...)```. Następnie wykonujemy interesującą nas agregację lub korzystamy z metody ```agg(agg_func1, agg_func1, ...)```, jeżeli mamy więcej niż jedną.
# MAGIC
# MAGIC > ✅ Dobrą praktyką jest zawsze korzystanie z ```agg()```, nawet dla pojedynczej agregacji. Spark 3.x+ ma zoptymalizowaną metodę, a pozwala to na standardyzację pracy.

# COMMAND ----------

# DBTITLE 1,Przykłady
# Wypisz na liście wszystkich laureatów z każdej kategorii. Pomiń puste kategorie
display(
    df.groupBy(F.col("category"))
    .agg(F.collect_set(F.col("name")).alias("name_list"))
    .filter((F.col("category").isNotNull()))
)

# Wyświetl najmniejszą i największą nagrodę przyznaną w każdym roku, zależnie czy jest to ogranizacja czy osoba.
display(
    df.groupBy(F.col("ind_or_org"), F.col("awardYear"))
    .agg(
        F.max(F.col("prizeAmount")).alias("max_prize"),
        F.min(F.col("prizeAmount")).alias("min_rpize")
         )
    .orderBy(F.col("awardYear").desc())
)
