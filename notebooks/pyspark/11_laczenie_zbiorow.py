# Databricks notebook source
# MAGIC %md
# MAGIC # Łączenie DataFrame (zbiorów)
# MAGIC
# MAGIC DataFrame możemy łączyć na dwa sposoby:
# MAGIC - Łączenie (merge), czyli metody ```union``` oraz ```unionAll```
# MAGIC - Rozszerzanie DataFrame, czyli złączenia z kategorii ```join```

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

emp_schema = StructType(
    [
        StructField("EMP_ID", IntegerType(), False),
        StructField("FIRST", StringType(), False),
        StructField("LAST", StringType(), False),
        StructField("SALARY", DoubleType(), False),
        StructField("DEP_ID", IntegerType(), True),
    ]
)

emp_df = spark.createDataFrame(
    [
        (1, "Adam", "Nowak", 7500.0, 1),
        (2, "Jan", "Kowalski", 6000.0, 1),
        (3, "Michał", "Trzeci", 3600.0, 2),
        (4, "Ewa", "Nazwisko", 9560.0, 2),
        (5, "Elżbieta", "Ostatnia", 6300.0, 3),
        (5, "Joanna", "Bezdomna", 6300.0, None),
    ],
    emp_schema,
)

dep_schema = StructType(
    [
        StructField("ID", IntegerType(), False),
        StructField("NAME", StringType(), False),
        StructField("CITY", StringType(), False),
    ]
)

dep_df = spark.createDataFrame(
    [
        (1, "Alpha", "Warsaw"),
        (2, "Gamma", "Lodz"),
        (3, "Omega", "Gdansk"),
        (4, "Lambda", "Poznan"),
    ],
    dep_schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## JOIN
# MAGIC
# MAGIC Aby wykonać wykonać złączenie dwóch DataFrame na jednym z nich wykonujemy metodę ```join(df, warunek_zlaczenia, typ_zlaczenia)```. Poniżej tabela typów złączeń dostępnych w PySpark.
# MAGIC
# MAGIC | Metoda join                        | Odpowiednik w SQL |
# MAGIC |------------------------------------|-------------------|
# MAGIC | inner                              | INNER JOIN        |
# MAGIC | outer, full, fullouter, full_outer | FULL OUTER JOIN   |
# MAGIC | left, leftouter, left_outer        | LEFT JOIN         |
# MAGIC | right, rightouter, right_outer     | RIGHT JOIN        |
# MAGIC | cross                              |                   |
# MAGIC | anti, leftanti, left_anti          | Elementy tylko DF na którym wykonujemy ```join```                  |
# MAGIC | semi, leftsemi, left_semi          | Elementy tylko DF na którym wykonujemy ```join``` mające dopasowanie do dołącznego DF                  |
# MAGIC
# MAGIC Warunek złączenia możemy podać na dwa sposoby:
# MAGIC - Warunek logiczny: ```(df1.col1 == df2.col1) [& (df1.col2 == df2.col2) ...]```. 
# MAGIC - Lista kolumn: ```[col1, col2, col3, ...]```. W tej sytuacji kolumny po których łączymy **muszą mieć tą samą nazwę w obu DataFrame!!** Plusem tego rozwiązania jest fakt, żekolumny biorące udział w złączeniu nie będą duplikowane. Oznacza to, że ze złączenia zostaną wybrane tylko kolumny DataFrame na którym wykonujemy metodę ```join```.

# COMMAND ----------

# DBTITLE 1,Przykłady
# inner join - złączenie logiczne
display(emp_df.join(dep_df, (emp_df.DEP_ID == dep_df.ID), "inner"))

# inner join - złączenie za pomocą listy
display(emp_df.join(dep_df.withColumnRenamed("ID", "DEP_ID"), ["DEP_ID"], "inner"))

# złączenie anti
display(dep_df.join(emp_df, (emp_df.DEP_ID == dep_df.ID), "semi"))

# złączenie semi
display(emp_df.join(dep_df, (emp_df.DEP_ID == dep_df.ID), "anti"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tworzenia zbiorów - UNION/UNION ALL
# MAGIC
# MAGIC Zasada ```union(df)``` i ```unionAll(df)``` w PySpark jest taka sama jak w SQL. ```unionAll(df)``` dodatkowo nie pomija duplikatów.
# MAGIC
# MAGIC Podczas korzystania z tych metod należy pamiętać, że:
# MAGIC - Ilość kolumn każdego łączonego DF musi być taka sama
# MAGIC - Nazwy kolumn są pobierane z pierwszego DF, na którym wykonywany jest ```union()/unionAll()```

# COMMAND ----------

# DBTITLE 1,Przykład
# Przykład union
display(
    emp_df
    .select(F.col("FIRST"), F.col("LAST"))
    .union(
        dep_df.select(F.col("NAME"), F.col("CITY"))
    )
)
