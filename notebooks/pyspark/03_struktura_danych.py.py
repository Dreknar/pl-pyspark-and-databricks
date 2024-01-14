# Databricks notebook source
# MAGIC %md
# MAGIC # Struktury danych
# MAGIC
# MAGIC Podczas tworzenia Data Frame PySpark automatycznie utworzy strukturę (schemat) danych dla niego. Podobnie jak konwersja danych - będzie on wybierał typ danych, który jest najbardziej "wszechstronny" dla podanych wartości.
# MAGIC
# MAGIC Możemy wyświetlić schemat danych wykonująć na df metodę ```printSchema()```

# COMMAND ----------

# DBTITLE 1,Wyświetlenie schematu
from datetime import datetime

rows = [
  (1, "Example001", datetime(2024, 1, 5, 12, 36, 22), True),
  (2, None, datetime(2024, 1, 6, 16, 0, 0), False)
  ]
columns = ["ID", "DOCUMENT_NAME", "LAST_UPDATED_AT", "IS_VALID"]

df = spark.createDataFrame(rows, columns)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Jak możemy zauważyć, poza nazwą kolumny i jego typu danych dostaniemy również informację, czy kolumna moze być pusta. Domyślnie każda kolumna jest ustawiona na ```True``` dla ```nullable```. Aby ustawić własny schemat danych i/lub nie chcemy, aby PySpark automatycznie tworzył własny schemat, możemy zbudować swój własny za pomocą ```StructField``` oraz ```StructType```. Schemat podczas tworzenia DF podajemy zamiast listy nazw kolumn.
# MAGIC
# MAGIC > ✅ Tworzenie własnych schematów jest dobrą praktyką, ponieważ w przypadku zmianu schematu danych otrzymaty odpowiedni komunikat/błąd.

# COMMAND ----------

# MAGIC %md
# MAGIC ## StructType i StructField
# MAGIC Aby stworzyć własny schemat danych musimy zaimportować na początku niezbędne obiekty z modułu ```pyspark.sql.types```
# MAGIC
# MAGIC Dostępne mamy obiekty reprezentujące:
# MAGIC
# MAGIC - int -> ByteType, ShortType, IntegerType, LongType (różnica polega na wielkości liczby całkowitej)
# MAGIC - float -> FloatType, DoubleType (różnica polega na poziomie precyzji)
# MAGIC - bool -> BooleanType
# MAGIC - str -> CharType, StringType (Char to pojedynczy znak)
# MAGIC - datetime -> DateType, TimestampType
# MAGIC - list -> ArrayType
# MAGIC - dict -> MapType

# COMMAND ----------

# DBTITLE 1,Import obiektów
from pyspark.sql.types import (
    StructType,  # Pozwala na stworzenie struktury
    StructField,  # Kolumna w strukturze Data Frame
    # Typy liczbowe
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    # Znaki (łańcuchy znakowe)
    CharType,
    StringType,
    # Typ logiczny
    BooleanType,
    # Data i czas
    DateType,
    TimestampType,
    # Kolekcje danych
    ArrayType,
    MapType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC Aby stworzyć własny schemat musimy stworzyć obiekt klasy ```StructType```, który jako argument przyjmuje listę kolumn zbudowanych za pomocą obiektu ```StructType```. ```StructType``` przyjmuje trzy argumenty: nazwę kolumny, typ danych (odpowiedni obiekt) oraz True/False informujący, czy dana kolumna może być pusta.
# MAGIC
# MAGIC Dodatkowo:
# MAGIC - Dla ```ArrayType``` podajemy jako pierwszy argument typ danych dla wartości oraz drugi argumenty True/False informujący, czy lista może zawierać ```null``` (Domyślnie False)
# MAGIC - Dla ```MapType``` podajemy dodatkowo trzy argumenty: ```(typ_danych_klucza, typ_danych_wartosci, czy_wartosci_moga_byc_null)```. Domyślnie ```czy_wartosci_moga_byc_null``` jest ustawione na False.

# COMMAND ----------

# DBTITLE 1,Tworzenie schematu
schema = StructType(
    [
        StructField("ID", IntegerType(), False),
        StructField("LOGIN", StringType(), False),
        StructField("IS_ACTIVE", BooleanType(), False),
        StructField("LOGIN_AT", TimestampType(), True),
        StructField("TAGS", ArrayType(StringType(), False)),
        StructField("METADATA", MapType(StringType(), IntegerType(), True))
    ]
)

# COMMAND ----------

# DBTITLE 1,Implementacja schematu w DF
from datetime import datetime

rows = [
    (1, "Example001", True, datetime(2023, 12, 23, 12, 46, 0), ["CSS", "SQL", "JAVA"], {"logins": 100, "bans": 0}),
    (2, "Non-Active", True, None, ["JAVA"], {"logins": None, "bans": None})
]

df = spark.createDataFrame(rows, schema)

display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aktualizacja schematu
# MAGIC
# MAGIC Jeżeli chcemy zaktualizować schemat to musimy utworzyć nowy Data Frame.
