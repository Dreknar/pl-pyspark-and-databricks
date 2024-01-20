# Databricks notebook source
# MAGIC %md
# MAGIC # Ogólne
# MAGIC Zbiór przydatnych poleceń z dbutils.

# COMMAND ----------

# DBTITLE 1,Ogólna pomoc
dbutils.help()

# COMMAND ----------

# DBTITLE 1,Pobierz nazwę użytkownika
dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# COMMAND ----------

# DBTITLE 1,Pobierz nazwę notebooka (pełna ścieżka)
dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
