# Databricks notebook source
# MAGIC %md
# MAGIC # Typy danych
# MAGIC
# MAGIC PySpark posiada spory zestaw typów danych wykorzystywanych podczas pracy z DataFrame. Poza typami prostymi jak np. liczby całkowite, łańcuchy znakowe mamy również dostęp do kolekcji danych oraz daty i czasu. Jako, że PySpark urachamia wszystko za pomocą JVM (wykorzystując Scalę) musimy pamiętać o odpowiednim zapisie danych dla danego typu. Nowsza wersja Databricks dodatkowo podczas wyświetlania danych będzie pokazywać obok nazwy kolumn typ danych.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Podstawowe typy danych
# MAGIC
# MAGIC Do podstawowych typów danych możemy zaliczyć:
# MAGIC
# MAGIC - Liczby całkowite (int)
# MAGIC - Liczby zmiennoprzecinkowe (float)
# MAGIC - Łańcuchy znakowe (str)
# MAGIC - Wartość logiczna (bool)

# COMMAND ----------

# DBTITLE 1,Przykład
rows = [(1, "My first movie!", 4.7, True)]
columns = ["ID", "TITLE", "RATING", "IS_PUBLISHED"]

display(spark.createDataFrame(rows, columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data i czas
# MAGIC Możemy przechowywać informacje na temat daty i czasu w podobym sposób jak robimy to w SQL (typy danych DATE i TIMESTAMP). Aby zapisać wartość w takim formacie musimy skorzystać z modułu ```datetime```. Dodatkowo należy pamiętać o:
# MAGIC
# MAGIC - Domyślny format dla DateType: ```yyyy-MM-dd```
# MAGIC - Domyślny format dla TimestampType: ```yyyy-MM-dd HH:mm:ss.SSSS```
# MAGIC - DateType jest uzupełniony o czas ```00:00:00.000+00:00```
# MAGIC - Jeżeli podana wartość nie może zostać przedstawiona jako DateType/TimestampType, PySpark zwróci ```null```

# COMMAND ----------

# DBTITLE 1,Przykład
from datetime import datetime

rows = [(1, datetime(2024, 1, 5), datetime(2024, 1, 5, 12, 36, 22))]
columns = ["ORDER_ID", "ORDER_DATE", "ORDER_TIMESTAMP"]

display(spark.createDataFrame(rows, columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kolekcje danych
# MAGIC W pojedynczym polu możemy zapisać dane w postaci listy lub słownika (mapy). Należy pamiętać, że podane przez nas kolekcje danych muszą być zrozumiałe przez JVM i języka Scala. 
# MAGIC
# MAGIC Dlatego dobrą praktyką jest:
# MAGIC
# MAGIC - Lista przechowuje informacje tylko w jednym typie danych
# MAGIC - Słownik (mapa) ma wspólny typ danych dla kluczy, jak i dla wartości. Dla kluczy najlepiej wykorzystać str/int, natomiast wartość może być dowolna (akceptowalne są również inne kolekcje danych).
# MAGIC
# MAGIC > ⚠️ DateType/TimestampType dla słownika jako wartość będzie konwertowany na łańuch znakowy

# COMMAND ----------

# DBTITLE 1,Przykład
rows = [
  (1, "user001", ["SQL", "Python", "Azure"], {"created_at": datetime(2023, 12, 10, 11, 38, 0), "last_login_at": datetime(2024, 1, 8, 23, 58, 12)})
]
columns = ["ID", "LOGIN", "TECHNOLOGIES", "LOGIN_TIMES"]

display(spark.createDataFrame(rows, columns))
