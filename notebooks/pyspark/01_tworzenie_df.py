# Databricks notebook source
# MAGIC %md
# MAGIC # Tworzenie DataFrame
# MAGIC PySpark wykorzystuje głównie "ramki danych" do swojej pracy. Wszystkie dane zapisywane są w formacie przypominającym tabelę: mamy kolumny i wiersze. Do tworzenia własnego Data Frame, bez udziału danych zewnętrznych (plików) wykorzystujemy polecenie ```spark.createDataFrame```. Pierwszym argument to lista krotek, które są wierszami naszego df, a drugi to lista kolumn (a dokładnie ich nazw).

# COMMAND ----------

# DBTITLE 1,Przykładowy DF
rows = [(1, "Adam", "Nowak"), (2, "Jan", "Kowalski")]
columns = ["ID", "FIRST_NAME", "LAST_NAME"]

df = spark.createDataFrame(rows, columns)

# COMMAND ----------

# DBTITLE 1,Wyświetlenie zawartości Data Frame
display(df)
