# Databricks notebook source
# MAGIC %md
# MAGIC # Kolekcje danych
# MAGIC
# MAGIC Posiadamy typy danych, które pozwalają na przechowywanie kolekcji jako wartość dla danej kolumny. Natomiast PySpark dostarcza metody do tworzenia kolekcji podczas wykonywania np. ```withColumn```

# COMMAND ----------

# DBTITLE 1,Struktura danych
from pyspark.sql.types import (
    StructType,
    StructField,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    CharType,
    StringType,
    BooleanType,
    DateType,
    TimestampType,
    ArrayType,
    MapType,
)
import pyspark.sql.functions as F
from datetime import datetime

schema = StructType(
    [
        StructField("FIRST", StringType(), False),
        StructField("LAST", StringType(), False),
        StructField("SKILLS", StringType(), False),
        StructField("SALARY", DoubleType(), False),
        StructField("POSITION", StringType(), False),
        StructField("MAX_ON_POS", DoubleType(), False),
    ]
)

example_df = spark.createDataFrame(
    [
        ("Adam", "Nowak", "Java;SQL;Python;Scala;SQL", 2500.0, "Junior", 5000.0),
        ("Jan", "Kowalski", "Azure;Python;Spark;PyTorch", 5000.0, "Regular", 6000.0),
        ("Sebastian", "Ostatni", "Azure;GCP;Git;Linux;Bash;Azure", 8000.0, "Senior", 8000.0)
    ],
    schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Listy i zbiory
# MAGIC
# MAGIC Do tworzenia listy/zbioru PySpark posiada metody:
# MAGIC - ```split(txt, separator)``` - utwórz listę z łańcucha znakowe na podstawie podanego separatora.
# MAGIC - ```array(arg1, arg2, arg3, ...)``` - utwórz listę z podanych argumentów.

# COMMAND ----------

# DBTITLE 1,Przykłady
display(
  example_df
  .withColumn("TECHNOLOGIES", F.split(F.col("SKILLS"), ";"))
  .withColumn("SALARY_AND_POS", F.array(F.col("SALARY"), F.col("POSITION")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mapy
# MAGIC
# MAGIC Dostępne są dwie opcje:
# MAGIC - ```create_map(k1, v1, k2, v2, ...)``` - tworzy mapę na podstawie zestawu argumentów. Nieparzyste argumenty to klucze, parzyste wartości. Zatem pierwszy i drugi argument tworzy pierwszą parę klucz:wartość, kolejne dwa drugą itd..
# MAGIC - ```struct(arg1, arg2, arg3, ...)``` - tworzy mapę z podanych argumentów. Kluczem jest nazwa kolumny/alias funkcji.

# COMMAND ----------

# DBTITLE 1,Przykłady
display(
    example_df.withColumn(
        "DETAILS",
        F.create_map(
            F.lit("position_level"),
            F.col("POSITION"),
            F.lit("current_salary"),
            F.col("SALARY"),
            F.lit("main"),
            F.split(F.col("SKILLS"), ";").getItem(0),
        ),
    ).withColumn("SALARY_RANGE", F.struct(F.col("SALARY"), F.col("MAX_ON_POS")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metoda explode
# MAGIC
# MAGIC Metoda ```explode(kolekcja)``` pozwala na rozpakowanie wskazanej jako argument kolekcji. Zostaną dodane nowe wiersze (czyli pozostałe kolumny zostaną zduplikowane), a w przypadku mapy powstaną dwie kolumny: key oraz value
# MAGIC
# MAGIC > ⚠️ Możemy użyć jednego ```explode()``` na select.

# COMMAND ----------

# Wykorzystanie DataFrame z poprzedniego przykładu
example_df = (
    example_df.withColumn(
        "DETAILS",
        F.create_map(
            F.lit("position_level"),
            F.col("POSITION"),
            F.lit("current_salary"),
            F.col("SALARY"),
            F.lit("main"),
            F.split(F.col("SKILLS"), ";").getItem(0),
        ),
    )
    .withColumn("SALARY_RANGE", F.struct(F.col("SALARY"), F.col("MAX_ON_POS")))
    .withColumn("TECHNOLOGIES", F.split(F.col("SKILLS"), ";"))
)

# COMMAND ----------

# DBTITLE 1,Przykład
# Przed użyciem explode
display(
    example_df
    .select(
        F.col("FIRST"),
        F.col("DETAILS"),
        F.col("TECHNOLOGIES")
    )
)

display(
    example_df
    .select(
        F.col("FIRST"),
        F.explode(F.col("TECHNOLOGIES"))
    )
)

display(
    example_df
    .select(
        F.col("FIRST"),
        F.explode(F.col("DETAILS"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
