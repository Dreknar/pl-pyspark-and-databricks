# Databricks notebook source
# MAGIC %md
# MAGIC # Wartości unikatowe oraz praca z kolumnami

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
    DataType,
    TimestampType,
    ArrayType,
    MapType,
)
import pyspark.sql.functions as F
from datetime import datetime

schema = StructType(
    [
        StructField("ID", IntegerType(), False),
        StructField("LOGIN", StringType(), False),
        StructField("IS_ACTIVE", BooleanType(), False),
        StructField("LOGIN_AT", TimestampType(), True),
        StructField("TAGS", ArrayType(StringType(), False), False),
        StructField("METADATA", MapType(StringType(), StringType(), True), True),
        StructField("ORGANIZATION", StringType(), False),
    ]
)

example_df = spark.createDataFrame(
    [
        (
            1,
            "u001",
            True,
            datetime(2023, 12, 1, 12, 36, 23),
            ["SQL", "Scala", "Python", "Java"],
            {"posts": "120", "stars": "200", "2FA_activated": "True"},
            "R&D"
        ),
        (
            2,
            "u002",
            False,
            datetime(2023, 12, 2, 15, 44, 13),
            ["Azure", "GCP", "AWS", "C#"],
            {"2FA_activated": "True", "is_banned": True, "ban_reason": "Fake news"},
            "Collection"
        ),
        (3, "u003", False, None, ["Linux", "Red Hat"], None, "R&D"),
                (
            4,
            "u004",
            True,
            datetime(2023, 1, 10, 9, 12, 44),
            ["SQL", "Scala", "Python", "Java"],
            {"posts": "600", "stars": "2500", "2FA_activated": "True"},
            "R&D"
        ),
                                (
            5,
            "u005",
            True,
            datetime(2023, 1, 15, 9, 12, 44),
            ["SQL", "Oracle", "MySQL", "MS SQL Server"],
            {"posts": "300", "stars": "120", "2FA_activated": "False"},
            "Databases"
        ),
                
    ],
    schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dodawanie/Usuwanie kolumny
# MAGIC
# MAGIC DataFrame w PySpark pozwala na dynamiczną zmianę struktury za pomocą funkcji:
# MAGIC
# MAGIC - ```drop(nazwa_kolumny, nazwa_kolumny, ...)``` - usuwa podane kolumny.
# MAGIC - ```withColumn(nazwa_kolumny, wartość)``` - dodaje nową kolumnę do DataFrame. Wartość może być wynik funkcji.
# MAGIC
# MAGIC > ✅ ```withColumn``` pozwala również na nadpisywanie wartości istniejących już kolumn. Wystarczy, że jako nazwę podamy istniejącą kolumnę.

# COMMAND ----------

# DBTITLE 1,Przykład
display(
    example_df
    .withColumn("IS_OLD_ACC", F.lit(True))
    .withColumn("ID", F.col("ID") + 100)
)

display(
    example_df
    .drop("METADATA")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtrowanie duplikatów
# MAGIC
# MAGIC Aby wyświetlić unikatowe wiersze z naszego DataFrame musimy wykonać na nim metodę ```distinct()```. Podobnie jak w SQL wyświetlamy tylko te wiersze, które są unikatowe w danym wyniku, zatem ```distinct()``` po poleceniu ```select()``` będzie zwracał inny zbiór niż wykonany bezpośrednio na DataFrame.
# MAGIC
# MAGIC Drugim sposobem jest wykorzystanie metody ```dropDuplicates([nazwa_kolumny, nazwa_kolumny, ...])``` na DataFrame. PySpark będzie skanował zbiór danych i zostawiał wystąpienie każdego wiersza według kolumn podanych jako argumenty funkcji.
# MAGIC
# MAGIC > ⚠️ Kolejność wierszy przy korzystaniu ```dropDuplicates``` ma znaczenie! Warto wcześniej posortować zbiór przed dodaniem metody.
# MAGIC > ⚠️ ```distinct``` nie działa, kiedy DataFrame posiada kolekcje danych.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Przykłady
display(example_df.select(F.col("ORGANIZATION"), F.col("IS_ACTIVE")).distinct())

display(example_df.select(F.col("ORGANIZATION")).distinct())

display(
  example_df.dropDuplicates(["ORGANIZATION"])
)

display(
  example_df.orderBy(F.col("ID").desc()).dropDuplicates(["ORGANIZATION"])
)

# COMMAND ----------

# DBTITLE 1,Błąd dla distinct
display(example_df.distinct())
