# Databricks notebook source
# MAGIC %md
# MAGIC # All in one
# MAGIC Notebook zawiera wszystkie notebooki jako całość.

# COMMAND ----------

# MAGIC %md
# MAGIC # Dobre praktyki
# MAGIC
# MAGIC - Dostarczamy w miarę możliwości własny schemat dla danych. Pozwoli nam to uniknąć błędów podczas zaczytywanie danych.
# MAGIC - Kilka funkcji modułu ```pyspark.sql.functions``` ma taką samą nazwę jak funkcje Pythona. Rekomendowane jest używanie aliasów lub importowanie modułu z aliasem np. ```import pyspark.sql.functions as F```
# MAGIC - W celu optymalizacji pamięci warto nadpisywać DataFrame podczas jego transformacji, ewentualnie usuwać niepotrzebne za pomocą polecenia ```del```.
