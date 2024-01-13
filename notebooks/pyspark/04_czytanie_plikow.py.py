# Databricks notebook source
# MAGIC %md
# MAGIC # Czytanie pliku
# MAGIC Najczęściej dane dla naszych ramek będziemy dostarczać za pomocą plików/źródeł zewnętrznych. PySpark w Databricks pozwala na czytanie z różnych formatów danych. Do najpopularniejszych należą:
# MAGIC
# MAGIC - CSV
# MAGIC - JSON
# MAGIC - XML
# MAGIC - PARQUET
# MAGIC - DELTA
# MAGIC
# MAGIC > ⚠️ Format delta został pominięty, posiada swój własny notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Różnice pomiędzy formatami
# MAGIC
# MAGIC |                                    | CSV                          | XML                                 | JSON                                | PARQUET                                         |
# MAGIC |------------------------------------|------------------------------|-------------------------------------|-------------------------------------|-------------------------------------------------|
# MAGIC | Łatwy do odczytu/podglądu          | ✅                            | ✅                                   | ✅                                   |                                                 |
# MAGIC | Możliwość podziału                 | ✅                            | ✅                                   | ✅                                   | ✅                                               |
# MAGIC | Zawiera informacje o typach danych |                              |                                     | ✅                                   | ✅                                               |
# MAGIC | Wysoka szybkość odczytu            | ✅                            |                                     |                                     | ✅                                               |
# MAGIC | Zawiera informacje zagnieżdżone    |                              | ✅                                   | ✅                                   | ✅                                               |
# MAGIC | Domyślny schemat: kolumny          | Manualny (wymagany nagłówek) | Automatyczny (na podstawie odczytu) | Automatyczny (na podstawie odczytu) | Automatyczny (natychmiastowy, zawiera metadane) |
# MAGIC | Domyślny schemat: typy danych      | Manualny (wymagany nagłówek) | Automatyczny (na podstawie odczytu) | Automatyczny (na podstawie odczytu) | Automatyczny (natychmiastowy, zawiera metadane) |
# MAGIC
# MAGIC > ⚠️ DF zbudowany na podstawie plików płaskich ignoruje dostarczony schemat (np. ustawi dla wszystkich kolumn ```nullable=True```)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Odczyt z pliku
# MAGIC
# MAGIC Dane z pliku możemy odczytywać na dwa sposoby:
# MAGIC
# MAGIC - Odczyt bezpośredni, za pomocą metody: ```spark.read.csv()```. Wtedy wszystkie elementy dodatkowe parametry przekazujemy za pomocą flag metody
# MAGIC - (Rekomendowany) Odczyt za pomocą metod: ```format```, ```options```, ```load```. Jest to uniwersalny zapis, dostępny w każdej wersji Sparka.
# MAGIC     - Argumenty dla ```options``` przekazujemy za pomocą flag, zależnie od wybranego formatu.
# MAGIC     - ```load``` zawiera ścieżkę do pliku **lub listę ścieżek, jeżeli chcemy zaczytać wiele plików do pojedynczego DataFrame**.
# MAGIC     - ```format``` zawiera informacje o formacie danych (rozszerzenie)
# MAGIC
# MAGIC Przykładowo: ```spark.read.format("csv").options(header=True).load("my-data/file.csv")```
# MAGIC
# MAGIC > ⚠️ Możemy zamiast pliku wskazać katalog, wtedy PySpark weźmie pod uwagę wszystki pliki z rozszerzeniem wskazanym w ```format```. Działa również z listą katalogów.

# COMMAND ----------

# DBTITLE 1,Ścieżka do data
DATA_DIR = f'file:/Workspace/Repos/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/pl-pyspark-and-databricks/data'

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV
# MAGIC
# MAGIC Dostępne opcje:
# MAGIC - **delimiter** - seperator kolumn. Domyślnie ```,```.
# MAGIC - **header** - czy plik zawiera nagłówek. Domyślnie ```False```.
# MAGIC - **quotes** - w jaki sposób są prezentowane łańcuchy znakowy (wtedy jest ignorowany delimiter dla tekstu). Domyślnie jest to ```"```.
# MAGIC - **nullValue** - sposób reprezentacji nulll wpliku. Domyślnie puste pole.

# COMMAND ----------

# DBTITLE 1,Przykład
display(
    spark.read.format("csv")
    .options(header=True, delimiter=",", quotes="\"")
    .load(f'{DATA_DIR}/csv/netflix_titles.csv')
)

display(
    spark.read.format("csv")
    .options(header=True, delimiter=",", quotes="\"", nullValue="TV Show")
    .load(f'{DATA_DIR}/csv/netflix_titles.csv')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### XML
# MAGIC
# MAGIC Pliki XML są możliwe do odczytu dzięki stworzonej biblioteki przez zespół Databricks. W tym wypadku jako format podajemy hasło ```com.databricks.spark.xml``` lub ```xml```. Więcej informacji na oficjalnym [repozytorium](https://github.com/databricks/spark-xml)
# MAGIC
# MAGIC > ⚠️ Biblioteka ```com.databricks.spark.xml``` musi zostać zainstalowana na klastrze. Należy posać dla pakietu Maven nazwę: ```com.databricks:spark-xml_2.12:0.17.0```
# MAGIC
# MAGIC Dostępne opcje:
# MAGIC - **rowTag** - nazwa roota. Wymagane!. Jeżeli dane są bardziej zagnieżdzone - dodajemy kolejne wywołania metody ```options```.

# COMMAND ----------

# DBTITLE 1,Przykład
display(
    spark.read
    .format("xml")
    .options(rowTag="states")
    .options(rowTag="state")
    .load(f'{DATA_DIR}/xml/states.xml')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON
# MAGIC
# MAGIC Dostępne opcje:
# MAGIC - **multiline** - jeżeli nasz plik zawiera wiele JSONów (lista słowników) nalezy ustawić ```True```. Domyślnie ```False```

# COMMAND ----------

# DBTITLE 1,Przykład
display(
    spark.read.format("json")
    .options(multiline=True)
    .load(f'{DATA_DIR}/json/noble_prize_award.json')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### PARQUET

# COMMAND ----------

# DBTITLE 1,Przykład
display(
    spark.read.format("parquet")
    .load(f'{DATA_DIR}/parquet/netflix_titles.parquet')
)
