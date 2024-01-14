# Databricks notebook source
# MAGIC %md
# MAGIC # Funkcje PySparka
# MAGIC
# MAGIC PySpark dostarczą ogromną ilość funkcji, które współpracują ze strukturą DataFrame (kolumnami). Wszystkie opisane poniżej funkcje znajdują się w ```pyspark.sql.functions```.
# MAGIC
# MAGIC > ✅ Argumenty podane w ```[arg]``` w ten sposób są opcjonalne dla danej funkcji.

# COMMAND ----------

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
        StructField("HIRE_DATE", DateType(), False),
        StructField("LAST_LOGIN_AT", TimestampType(), False),
        StructField("METADATA", MapType(StringType(), StringType(), True))    
    ]
)

example_df = spark.createDataFrame(
    [
        ("Adam", "Nowak", "Java;SQL;Python;Scala;SQL", 2500.0, "Junior", 5000.0, datetime(2023, 10, 1), datetime(2023, 12, 22, 16, 2, 11), {"posts": "120", "stars": "200", "2FA_activated": "True"}),
        ("Jan", "Kowalski", "Azure;Python;Spark;PyTorch", 5000.0, "Regular", 6000.0, datetime(2023, 11, 1), datetime(2024, 1, 6, 17, 30, 0), {"posts": "600", "stars": "2500", "2FA_activated": "True"}),
        ("Sebastian", "Ostatni", "Azure;GCP;Git;Linux;Bash;Azure", 8000.0, "Senior", 8000.0, datetime(2023, 11, 15), datetime(2024, 1, 4, 12, 15, 0), {"posts": "500", "stars": "1250", "2FA_activated": "False"})
    ],
    schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funkcje tekstowe (String)
# MAGIC
# MAGIC | Funkcja                           | Opis                                                                                                                                                                                                                 |
# MAGIC |-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
# MAGIC | concat_ws(sep, col1, col2, ...)   | Łączy argumenty (łańcuchy znakowe) w jeden za pomocą podanego separatora                                                                                                                                             |
# MAGIC | concat(col1, col2, ...)   | Łączy argumenty (łańcuchy znakowe) w jeden                                                                                                                                             |
# MAGIC | length(col)                       | Zwraca długość podanego tekstu w kolumnie.                                                                                                                                                                           |
# MAGIC | lower(col)                        | Zwraca tekst małymi literami                                                                                                                                                                                         |
# MAGIC | upper(col)                        | Zwraca tekst wielkimi literami                                                                                                                                                                                       |
# MAGIC | locate(substr, col, [pos])        | Zwraca pozycję substringa w tekście. Opcjonalnie szuka od podane pozycji                                                                                                                                             |
# MAGIC | ltrim(col)                        | Usuwa białe znaki z lewej strony                                                                                                                                                                                     |
# MAGIC | rtrim(col)                        | Usuwa białe znaki z prawej strony                                                                                                                                                                                    |
# MAGIC | trim(col)                         | Usuń białe znake z lewej i prawej strony                                                                                                                                                                             |
# MAGIC | repeat(col, n)                    | Zwraca _n_ wielokrotność tekstu kolumny                                                                                                                                                                              |
# MAGIC | split(col, sep)                   | Stwórz listę z łańcucha znakowego na podstawie podanego separatora                                                                                                                                                   |
# MAGIC | substring(col, start, len)        | Zwraca łańcuch znakowy zbudowany od pozycji _start_ łańcucha znakowego _col_. Opcjonalnie możemy podać _len_, aby ustalić długość podłańcucha znakowego                                                              |
# MAGIC | regexp_extract(col, exp, grpID)   | Zwróć łańuch znakowy na pasujący do wzorca. Zwraca pierwsze dopasowanie. Jeżeli wyrażenie regularne składa się z kilku grup - definiujemy _groupID_. Jeżeli dopasowanie nie zostanie znalezion -> zwracany jest null |
# MAGIC | regexp_replace(col, pattern, rep) | Zamień w łańcuchu znakowym wskazany tekst na _rep_ dopasowany do wzorca _pattern_                                                                                                                                    |

# COMMAND ----------

# DBTITLE 1,Przykłady
display(
  example_df
  .select(
    F.concat_ws(" ", F.col("FIRST"), F.col("LAST")).alias("concat_ws"),
    F.concat(F.col("FIRST"), F.lit("--"), F.col("LAST")).alias("concat"),
    F.upper(F.col("POSITION")).alias("upper"),
    F.lower(F.col("POSITION")).alias("lower"),
  )
)

display(
  example_df
  .select(
    F.split(F.col("SKILLS"), ";").alias("split"),
    F.regexp_extract(F.col("SKILLS"), "(Java|Python)", 1).alias("regexp_extract"),
    F.regexp_replace(F.col("SKILLS"), "Java", "JVM").alias("regexp_replace")
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funkcje daty i czasu
# MAGIC
# MAGIC > ✅ Więcej informacji o wzorca daty/czasu możemy przeczytać [tutaj](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)
# MAGIC
# MAGIC | Funkcja                                    | Opis                                                                                                                                                                                    |
# MAGIC |--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
# MAGIC | current_date()                             | Zwraca aktualną datę                                                                                                                                                                    |
# MAGIC | current_timestamp()                             | Zwraca aktualną datę i czas                                                                                                                                                                    |
# MAGIC | unix_timestamp([col])                                | Zwraca datę jako EPOCH. Jeżeli nie podamy kolumny - zwraca EPOCH dla aktualnej daty/czasu.|
# MAGIC | from_unixtime([col])                                | Konwertuje sekundu z EPOCH (1970-01-01 00:00:00 UTC) na ciąg znaków reprezentujący znacznik czasu tego momentu w bieżącej strefie czasowej systemu w formacie yyyy-MM-dd HH:mm:ss|
# MAGIC | date_format(date, format)                  | Konwertuje datę/łańcuch znakowy daty do łańcucha znakowego do podanego formatu                                                                                                          |
# MAGIC | to_date(col, [format])                     | Konwertuje datę do wskazanego formatu. Jeżeli nie podamy formatu -> konwertuje do domyślnego                                                                                            |
# MAGIC | add_months(date, n)                        | Dodaj n miesięcy do daty                                                                                                                                                                |
# MAGIC | date_add(date, days)                       | Zwraca datę po dodaniu _days_ do _date_                                                                                                                                                 |
# MAGIC | date_sub(date, days)                       | Zwraca datę po odjęciu _days_ do _date_                                                                                                                                                 |
# MAGIC | datediff(start, end)                       | Zwraca różnicę (dni) pomiędzy datami                                                                                                                                                    |
# MAGIC | months_between(end, start)                 | Zwraca różnicę (miesiące) pomiędzy datami                                                                                                                                               |
# MAGIC | date_trunc(option, col) lub trunc(option, col) | Zwraca datę "ściętą" do _option_. Przykładowo: podając "yyyy" ścina do roku.  Dostępne opcje: (year, yyyy, yy), (month, mon, mm), (day, dd), second, minute, hour, week, month, quarter |
# MAGIC | year(col)                                  | Zwraca rok z daty                                                                                                                                                                       |
# MAGIC | month(col)                                 | Zwraca miesiac z daty                                                                                                                                                                   |
# MAGIC | day(col)                                   | Zwraca dzien z daty                                                                                                                                                                     |
# MAGIC | hour(col)                                  | Zwraca godzine z daty                                                                                                                                                                   |
# MAGIC | minute(col)                                | Zwraca minuty z daty                                                                                                                                                                    |
# MAGIC | second(col)                                | Zwraca sekundy z daty                                                                                                                                                                   |

# COMMAND ----------

# DBTITLE 1,Przykłady
display(
    example_df.select(
        F.current_date().alias("current_date"),
        F.current_timestamp().alias("current_timestamp"),
        F.date_format(F.col("HIRE_DATE"), "d.M.y").alias("date_format"),
        F.date_add(F.col("HIRE_DATE"), 30).alias("date_add"),
        F.add_months(F.col("HIRE_DATE"), 12).alias("add_months"),
        F.year(F.col("HIRE_DATE")).alias("year"),
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Funkcje listy
# MAGIC
# MAGIC | Funkcja                  | Opis                                                                                                            |
# MAGIC |--------------------------|-----------------------------------------------------------------------------------------------------------------|
# MAGIC | array_contains(col, val) | Zwraca True, jeżeli _val_ znajduje się na liście, w przeciwnym razie False. Jeżeli lista jest pusta zwraca null |
# MAGIC | array_distinct(col)      | Zwraca listę z unikatowymi wartościami                                                                          |
# MAGIC | array_max(col)           | Zwraca maksymalną wartość z listy                                                                               |
# MAGIC | array_min(col)           | Zwraca minimalną wartość z listy                                                                                |
# MAGIC | size(col)                | Zwraca długość listy                                                                                       |
# MAGIC | sort_array(col, [order]) | Sortuje podaną listę. Domyślnie order jest True, powodując sortowanie rosnące.                                  |

# COMMAND ----------

# DBTITLE 1,Przykłady
display(
    example_df
    .withColumn(("TECHNOLOGIES"), F.split(F.col("SKILLS"), ";").alias("split"))
    .select(
        F.col("TECHNOLOGIES").alias("base"),
        F.array_contains(F.col("TECHNOLOGIES"), "Java").alias("array_contains"),
        F.array_distinct(F.col("TECHNOLOGIES")).alias("array_distinct"),
        F.size(F.col("TECHNOLOGIES")).alias("size"),
        F.sort_array(F.col("TECHNOLOGIES")).alias("sort_array")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funkcje mapy
# MAGIC
# MAGIC | Funkcja         | Opis                      |
# MAGIC |-----------------|---------------------------|
# MAGIC | size(col)       | Zwraca długość listy/mapy |
# MAGIC | map_keys(col)   | Zwraca listę kluczy       |
# MAGIC | map_values(col) | Zwraca listę wartości     |

# COMMAND ----------

# DBTITLE 1,Przykłady
display(
    example_df
    .select(
        F.map_keys(F.col("METADATA")).alias("map_keys"),
        F.map_values(F.col("METADATA")).alias("map_values"),
        F.size(F.col("METADATA")).alias("size")
    )
)
