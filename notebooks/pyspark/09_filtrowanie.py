# Databricks notebook source
# MAGIC %md
# MAGIC # Filtrowanie danych
# MAGIC
# MAGIC PySpark pozwala za pomocą ```.filter()``` lub ```.where()``` filtrować wiersze z naszego DataFrame. Przejmuje jak argument warunek logiczny, które ma zwrócić True/False. Możemy wykorzystywać wszystkie funkcje dostępne dla PySpark.
# MAGIC
# MAGIC > ✅ Dobrą praktyką jest umieszczanie warunków ```()```, nawet w przypadku pojedynczego.

# COMMAND ----------

# DBTITLE 1,Załadowanie danych
import pyspark.sql.functions as F

df = spark.read.format("parquet").load(f'file:/Workspace/Repos/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/pl-pyspark-and-databricks/data/parquet/netflix_titles.parquet')

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Podstawowe operacje matematyczne
# MAGIC
# MAGIC - Daty podajemy jako łańcuchy znakowe w formacie ```"yyyy-MM-dd"```
# MAGIC - Podstawowe operacji, czyli większy, mniejszy, równy itd..

# COMMAND ----------

# DBTITLE 1,Przykłady
# Filmy po 2020
display( df.filter( (F.col("release_year") > 2020) ) )

# Tylko "TV Show"
display( df.filter( (F.col("type") == "TV Show") ) )

# Pozycje, które wyszły w innym miesiącu niż wrzesień (dowolny rok)
display( df.filter( (F.month(F.col("date_added")) != 9) ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Łączenie i negacja warunków
# MAGIC
# MAGIC Możemy łączyć warunki oraz negować dla metody ```filter()``` za pomocą znaków specjalnych:
# MAGIC
# MAGIC - ```&``` oznacza AND
# MAGIC - ```|``` oznacza OR
# MAGIC - ```~``` oznacza NOT (umieszczamy go zaraz przed nawiasem warunku)

# COMMAND ----------

# DBTITLE 1,Przykłady
# Tylko filmy zaczynające się na literę W
display(
    df.filter( (F.col("type") == "Movie") & (F.col("title").startswith("W")) )
)

# Filmy, gdzie rating to R lub PG
display(
    df.filter( (F.col("rating") == "R") | (F.col("rating") == "PG") )
)

# Negacja warunku: "listed_in" musi zawierać ponad 2 wpisy
display(
    df.filter( ~(F.size(F.col("listed_in")) > 2)  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BETWEEN / ISIN
# MAGIC
# MAGIC W przypadku _IS NOT IN_ oraz _NOT BETWEEN_ należy wykorzystać negacje: ```~```.
# MAGIC
# MAGIC - ```between(l, h)``` - zwraca True, jeżeli wartość kolumny znajduje się w zakresie ```<l, h>```.
# MAGIC - ```isin(lista)``` - wyświetli wiersze, gdzie kolumna zawiera wartość z listy.

# COMMAND ----------

# DBTITLE 1,Przykłady
# Data dodania pomiędzy 2019-01-01, a 2019-03-31
display(
    df.filter(
        (F.col("date_added").between("2019-01-01", "2019-03-31"))
    )
)

# Tytuł zaczyna się na F, H lub Z
display(
    df.filter(
        (F.substring(F.col("title"), 1, 1).isin(["F", "H", "Z"]))
    )
)

# Tytuł nie jest klasifikowany jako: TV-14, TV-Y lub R
display(
    df.filter(
        ~(F.col("title").isin(["TV-14", "TV-Y", "R"]))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Praca z nullami i operator LIKE
# MAGIC
# MAGIC - ```like()``` działa podobnie jak w SQL. **%** oznacza dowolną ilość dowolnych znaków, natomiast **_** dokładnie jeden dowolny znak. Negacja za pomocą operator ```~```.
# MAGIC - ```isNull()``` oraz ```isNotNull()``` pozwala filtrować **null** z kolumny.
# MAGIC
# MAGIC > ⚠️ Wielkość liter ma znaczenie!

# COMMAND ----------

# DBTITLE 1,Przykłady
# Wszystkie filmy zaczynające się na słowo "Jaws"
display(df.filter((F.col("title").like("Jaws%"))))

# Wyświetl filmy, które nie posiadają reżysera
display(df.filter((F.col("director").isNull())))
