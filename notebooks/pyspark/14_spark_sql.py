# Databricks notebook source
# MAGIC %md
# MAGIC # Spark SQL
# MAGIC
# MAGIC PySpark pozwala wykonywać w ramach DataFrame wszelkie polecenia SQL. Wbudowany on ma translator z języka SQL na metody/funkcje PySpark.

# COMMAND ----------

# DBTITLE 1,Przykładowe dane
import pyspark.sql.functions as F

df = spark.read.format("csv").options(header=True).load(f'file:/Workspace/Repos/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/pl-pyspark-and-databricks/data/csv/countries.csv')

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Przygotowanie danych
# MAGIC
# MAGIC Jeżeli nasza tabela nie jest dostępna w przypadku Databricks pod Unity Catalog (nie została zarejestrowana w tej bazie danych) to należy na początku utworzyć z naszego DataFrame widok za pomocą metody ```createOrReplaceTempVies(nazwa_widoku)```. Następnie możemy wykonywać w ramach niego polecenia SQL.
# MAGIC
# MAGIC W przypadku Databricks mamy dwie opcje:
# MAGIC
# MAGIC - Korzystamy z metody ```spark.sql(zapytanie_w_str)```, które zwraca DF, który możemy zapisać do zmiennej.
# MAGIC - Wykorzystać nową komórkę w formacie SQL (wystarczy na początku napisać ```%sql```). Databricks po wykonaniu takiej komórki automatycznie pod koniec działania utworzy DataFrame o nazwie ```_sqldf```, który może wykorzystać w przyszłości.
# MAGIC
# MAGIC ✅ Wszystkie funkcje z ```pyspark.sql.functions``` są dostępne dla składni SQL. Wyjątkiem możemy nazwać col - tutaj dostęp do kolumny mamy po nazwie.

# COMMAND ----------

# DBTITLE 1,Tworzenie widoku
df.createOrReplaceTempView("v_countries")

# COMMAND ----------

# DBTITLE 1,Przykład - spark.sql
countries_starts_with_A = spark.sql("SELECT * FROM v_countries WHERE substring(name, 1, 1) == 'A'")
display(countries_starts_with_A)

# COMMAND ----------

# DBTITLE 1,Przykład - użycie %sql
# MAGIC %sql
# MAGIC -- Wszystkie kraje na P
# MAGIC SELECT * FROM v_countries WHERE substring(name, 1, 1) == "P";

# COMMAND ----------

# DBTITLE 1,Wykorzystanie _sqldf
display(_sqldf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funkcje użytkownika
# MAGIC
# MAGIC Aby móc wykorzystać UDF w ramach składni SQL należy wstępnie zarejestrować funkcję wewnątrz sparka przy pomocy ```spark.udf.register(nazwa_funkcji, funkcja_UDF)```. Zwracany typ danych to obiekt z modułu ```pyspark.sql.types```

# COMMAND ----------

# DBTITLE 1,Przygotowanie danych
import pyspark.sql.functions as F

df = spark.read.format("parquet").load(f'file:/Workspace/Repos/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/pl-pyspark-and-databricks/data/parquet/netflix_titles.parquet')

# COMMAND ----------

# 1. Stworzenie funkcji
def list2str(array: list) -> str:
  if array is None:
    return ""
  else:
    return ",".join(array)


# 2. Utworzenie UDF
list2str_UDF = F.udf(lambda col: list2str(col))

# 3. Rejestrowanie jej dla Spark SQL
spark.udf.register( "list_to_str", list2str_UDF)

# 4. Wykorzystanie w SQL
df.createOrReplaceTempView("netflix")

display(spark.sql("SELECT cast, list_to_str(cast) AS list_to_str FROM netflix"))
