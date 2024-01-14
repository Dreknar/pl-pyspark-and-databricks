# Databricks notebook source
# MAGIC %md
# MAGIC # Selekcja i sortowanie
# MAGIC
# MAGIC DataFrame przypomina ten z modułu pandas, więc część poleceń będzie wspólna np. odczytywanie wybranej kolumny. Nie jest to rekomendowane, ponieważ DataFrame PySpark jest oddzielną implementacją i posiada ona swój zestaw funkcji i narzędzi. Przykładowo jest to metoda ```col()``` pozwalająca odczytywać wartości wybranej kolumny. W przeciwieństwie do klasycznego rozwiązania: ```df[nazwa_kolumny]``` mamy dostęp do dodatkowych opcji/narzędzi.
# MAGIC
# MAGIC ✅ Zalecane jest stosowanie metody ```col()``` podczas pracy z danymi (poza pojedynczymi wyjątkami)

# COMMAND ----------

# DBTITLE 1,Import funkcji
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

# COMMAND ----------

# DBTITLE 1,Przykładowy DataFrame
schema = StructType(
    [
        StructField("ID", IntegerType(), False),
        StructField("LOGIN", StringType(), False),
        StructField("IS_ACTIVE", BooleanType(), False),
        StructField("LOGIN_AT", TimestampType(), True),
        StructField("TAGS", ArrayType(StringType(), False), False),
        StructField("METADATA", MapType(StringType(), StringType(), True), True),
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
        ),
        (
            2,
            "u002",
            False,
            datetime(2023, 12, 2, 15, 44, 13),
            ["Azure", "GCP", "AWS", "C#"],
            {"2FA_activated": "True", "is_banned": True, "ban_reason": "Fake news"},
        ),
        (3, "u003", False, None, ["Linux", "Red Hat"], None),
                (
            4,
            "u004",
            True,
            datetime(2023, 1, 10, 9, 12, 44),
            ["SQL", "Scala", "Python", "Java"],
            {"posts": "600", "stars": "2500", "2FA_activated": "True"},
        ),
    ],
    schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wyświetlanie danych

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT
# MAGIC Metoda ```select()``` pozwala na wyświetlanie kolumn oraz wyniku funkcji wykonywanych w ramach danego DataFrame. Jako argumenty podajemy funkcje/kolumny, które mają zostać wyświetlone.

# COMMAND ----------

# DBTITLE 1,Przykład
display(
    example_df.select(F.col("LOGIN"), F.col("TAGS"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pobieranie wartości z kolekcji
# MAGIC Jeżeli natomiast chcemy pobrać wartość pod danym indeksem/kluczem z naszej kolekcji danych należy dodać informacje o indeksie po podaniu kolumny (zupełnie jak w Pythonie) lub wykonać metodę ```getItem(klucz_lub_index)```. PySpark wstawi ```null```, jeżeli nie znajdzie wartości.

# COMMAND ----------

# DBTITLE 1,Przykład
display(
    example_df.select(
        F.col("TAGS").getItem(2),
        F.col("METADATA").getItem("is_banned")
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aliasy
# MAGIC Do każdej metody ```pyspark.sql.functions```, która zwraca wartość pojedynczą wartość możemy wykonać ```alias(nazwa)``` w celu ustawienia własnej nazwy kolumny.

# COMMAND ----------

# DBTITLE 1,Przykład
display(
    example_df.select(
        F.col("TAGS").getItem(2).alias("random_tech"),
        F.col("METADATA").getItem("is_banned").alias("is_banned"),
        F.col("LOGIN").alias("username")
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pseudokolumna
# MAGIC Metoda ```lit``` pozwala na stworzenie pseudokolumny ze stałą wartością, którą podajemy jako argument.

# COMMAND ----------

display(
    example_df.select(
        F.lit("Hi!").alias("greetings"),
        F.col("LOGIN")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sortowanie danych

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funkcje sort() i orderBy()
# MAGIC
# MAGIC Sortowanie danych w PySpark możemy wykonać za pomocą dwóch metod:
# MAGIC - ```sort([col1, col2, col3, ...], ascending=[True|False, True|False, True|False, ...])```; W tej sytuacji należy pamiętać że wartości dla klucza _ascending_ odpawiadają kolejności podawania kolumn. Przykładowo _ascending[0]_ dotyczy pierwszego argumentu.
# MAGIC - ```orderBy(col(nazwa), col(nazwa).desc(), ...)```; Tutaj zamieniamy kolejność sortowania poprzez wykonanie metody ```desc()``` na kolumnie. Tutaj też możemy używać funkcji jak np. ```getItem()```
# MAGIC
# MAGIC > ✅ Kolejność argumentów dla powyższych funkcji wpływa na wynik sortowania!

# COMMAND ----------

# DBTITLE 1,Przykłady
display(
    example_df.select(
        F.col("ID"),
        F.col("LOGIN")
    ).sort("ID", ascending=[False])
)

display(
    example_df.select(
        F.col("ID"),
        F.col("LOGIN")
    ).orderBy(F.col("ID").desc())
)

display(
    example_df.select(
        F.col("ID"),
        F.col("LOGIN"),
        F.col("METADATA")
    ).orderBy(
        F.col("METADATA").getItem("stars").desc(),
        F.col("METADATA").getItem("posts")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dodatkowe funkcje
# MAGIC
# MAGIC Na DataFrame możemy również wykonać funkcje:
# MAGIC - ```count()``` - zwraca ilość wierszy (jest to liczba, nie DataFrame)
# MAGIC - ```limit(n)``` - ogranicza DataFrame do n wierszy (może być to odczyt losowy!)

# COMMAND ----------

# DBTITLE 1,Przykłady
display(example_df.count())

display(example_df.select(F.col("TAGS")).limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC
