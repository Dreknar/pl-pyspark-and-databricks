# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Table
# MAGIC
# MAGIC Możemy nazwać, że delta to "rozbudowany" parquet. Dostarcza on dodatkowe funkcjonalności jak: walidację schematu, transakcyjność oraz nowe polecenie z zakresu DML: ```MERGE INTO```. Delta jest automatycznie zainstalowana i wspierana przez klastry Databricks (nie musimy nic dodatkowo instalować).
# MAGIC
# MAGIC Oficjalna strona projektu dostępna pod tym [linkiem](https://delta.io/).

# COMMAND ----------

# DBTITLE 1,Tworzenie bazy danych
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo_db;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tworzenie tabeli
# MAGIC
# MAGIC W przypadku Databricks domyślnie będzie tworzona tabela w formacie delta. Mamy dostępną klasyczną składnie SQL. Delta wprowadza również atrybuty jal constrainty czy PRIMARY/FOREIGN KEY. Jest również możliwość tworzenia tabeli na podstawie danych zapisanych na dysku.
# MAGIC
# MAGIC - [Tworzenie tabeli - składnia (oficjalna dokumentacja)](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-table-using)
# MAGIC
# MAGIC > ✅ Niezależnie od wybranej opcji dobrą praktyką jest podawanie lokalizacji danych (```LOCATION```) oraz partycjonowanie danych (```PARTITIONED BY```).
# MAGIC
# MAGIC > ⚠️ Kolejność podawania kolumn, którą biorą udział w tworzeniu partycji ma znaczenie! Dodatkowo wartości w kolumnach nie mogą kończyć się znakiem specjalnym np. kropką.
# MAGIC
# MAGIC > ⚠️ Nie wybieramy nigdy do partycji kolumny, które mają PRIMARY KEY lub wartości unikatowe. Powoduje to duży rozkład danych, co ostatnie wydłuża czas odczytywania danych.
# MAGIC
# MAGIC - [Typy danych - oficjalna dokumentacja](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-datatypes)
# MAGIC - Funkcje: [Agregujące](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-functions-builtin#aggregate-functions) [Analityczne](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-functions-builtin#analytic-window-functions) [Listy](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-functions-builtin#array-functions) [Mapy/Słownika](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-functions-builtin#map-functions) [Rankingowe](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-functions-builtin#ranking-window-functions)
# MAGIC - [Funkcje dla delta SQL - oficjalna dokumentacja](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-functions-builtin)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Przykład: składnia SQL
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo_db.tbl1 (
# MAGIC   -- nazwa_kolumny typ_danych|funkcja
# MAGIC   col1 STRING,
# MAGIC   col2 DOUBLE,
# MAGIC   col3 DATE,
# MAGIC   col4 TIMESTAMP,
# MAGIC   col5 DOUBLE GENERATED ALWAYS AS (col2 * 1.23),
# MAGIC   col6 INT NOT NULL
# MAGIC )
# MAGIC PARTITIONED BY (col3)
# MAGIC LOCATION '/FileStore/demo/db/tbl1.delta'

# COMMAND ----------

# DBTITLE 1,Przykład: DF + SQL
DATA_DIR = f'file:/Workspace/Repos/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/pl-pyspark-and-databricks/data'
df = spark.read.parquet(f'{DATA_DIR}/parquet/netflix_titles.parquet')

df.write.partitionBy("release_year").format("delta").mode("overwrite").save("/FileStore/demo/db/netflix-tbl.delta")

dbutils.fs.ls("/FileStore/demo/db/netflix-tbl.delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo_db.netflix_demo USING DELTA LOCATION '/FileStore/demo/db/netflix-tbl.delta';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_db.netflix_demo LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Przykład - sprawdzenie struktury
# MAGIC %sql
# MAGIC DESCRIBE FORMATTED demo_db.netflix_demo; 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modyfikacja tabeli
# MAGIC
# MAGIC - [ALTER TABLE - oficjalna dokumentacja](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-alter-table)
# MAGIC
# MAGIC Nie ma możliwości usunięcia kolumny, jeżeli:
# MAGIC - Bierze udział w tworzeniu partycji
# MAGIC - Jest na niej nałożony PRIMARY KEY
# MAGIC - Inna kolumna jest od niej zależna (np. używa w ```CONSTRAINT``` lub ```GENERATED AS```)

# COMMAND ----------

# DBTITLE 1,Przykład: dodanie kolumny
# MAGIC %sql
# MAGIC ALTER TABLE demo_db.tbl1 ADD COLUMN col_x INT;

# COMMAND ----------

# DBTITLE 1,Przykład: constraint check
# MAGIC %sql
# MAGIC ALTER TABLE demo_db.tbl1 ADD CONSTRAINT check_col3_range CHECK (col3 >= 100 AND col3 <= 1000);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usunięcie tabeli
# MAGIC
# MAGIC Pamiętamy również o usunięciu danych (pliki nie są usuwanie przy poleceniu ```DROP```)

# COMMAND ----------

# DBTITLE 1,Przykład
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS demo_db.tbl1;

# COMMAND ----------

dbutils.fs.rm('/FileStore/demo/db/tbl1.delta', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Podstawowe polecenia DML
# MAGIC
# MAGIC > ✅ Poniżej polecenia oparte o SQL, natomiast można również modyfikować dane poprzez wykonywanie zapisu danych na DataFrame.
# MAGIC
# MAGIC > 🔺 Dane usuwamy tylko za pomocą SQL. Z powodu tworzenia logów usunięcie danych poprzez usunięcie plików powoduje uszkodzenie tabeli!!!

# COMMAND ----------

# DBTITLE 1,Przykład: INSERT
# MAGIC %sql
# MAGIC INSERT INTO demo_db.tbl1(col1,col2,col3,col4,col6) VALUES ('A', 2.25, '2024-01-01', '20204-01-01 12:00:30', 120);

# COMMAND ----------

# DBTITLE 1,Przykład: UPDATE
# MAGIC %sql
# MAGIC UPDATE demo_db.tbl1 SET col6 = 120 WHERE col1 = 'A';

# COMMAND ----------

# DBTITLE 1,Przykład: DELETE
# MAGIC %sql
# MAGIC DELETE FROM demo_db.tbl1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Polecenie MERGE INTO
# MAGIC
# MAGIC Polecenie MERGE INTO to prawdziwy kombajn dla delta table. Pozwala on aktualizować tabelę na podstawie innego DataFrame. Na podstawie warunku złączenia możemy zdecydować co należy wykonać, jeżeli dane zostały/nie zostały znalezione w danym zbiorze. ```SOURCE``` oznacza aktualizowaną tabelę, natomiast ```TARGET``` DataFrame na podstawie którego wykonujemy aktualizację.
# MAGIC
# MAGIC > [Oficjalna dokumentacja MERGE INTO](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-merge-into)
# MAGIC
# MAGIC Ogólna składnia
# MAGIC ```delta
# MAGIC MERGE INTO src
# MAGIC USING trg ON trg.col = src.col -- Warunek złączenia. Można rozbudować o inne funkcje
# MAGIC -- Po MATCHED możemy dodać BY SOURCE|TARGET, kiedy interesuje nas dopasowanie/brak dopasowanie po wybranej stronie. Można rozbudować o kolejne funkcje.
# MAGIC WHEN NOT MATCHED THEN -- Kiedy dane nie zostały dopasowane
# MAGIC     -- polecenie DML np. INSERT *, UPDATE SET src.col = trg.col * 100
# MAGIC WHEN MATCHED THEN -- Kiedy dane zostały dopasowane
# MAGIC     --- polecenie DML np. DELETE *
# MAGIC ```
# MAGIC
# MAGIC > ✅ Rekomendowane jest używanie ```%sql``` w przypadku MERGE INTO niż poleceń Python podczas pracy w Databricks.
# MAGIC
# MAGIC > ⚠️ Możemy umieścić po jednym ```WHEN MATCHED``` i ```WHEN NOT MATCHED```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Przykład poniższa pokazuje:
# MAGIC 1. Przygotowanie danych. ```SOURCE``` zawiera tylko dane z 2020-2021, a ```TARGET``` 2019-2020.
# MAGIC 2. Dla danych z:
# MAGIC     - 2019 roku -> dodaj je. ```WHEN NOT MATCHED```
# MAGIC     - 2020 roku -> zaktualizuj poprzez wykonanie na tytule funkcji UPPER ```WHEN MATCHED```

# COMMAND ----------

# DBTITLE 1,Przygotowanie danych
# MAGIC %sql
# MAGIC DELETE FROM demo_db.netflix_demo WHERE release_year NOT IN (2021,2020);

# COMMAND ----------

import pyspark.sql.functions as F
df_update = spark.read.parquet(f'{DATA_DIR}/parquet/netflix_titles.parquet').filter( (F.col("release_year").isin([2019, 2020])) )
df_update.createOrReplaceTempView("df_update")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO demo_db.netflix_demo AS src
# MAGIC USING df_update AS trg ON trg.show_id = src.show_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE
# MAGIC   SET src.title = UPPER(src.title)
# MAGIC WHEN NOT MATCHED BY TARGET THEN
# MAGIC   INSERT * 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_db.netflix_demo WHERE release_year = 2020 LIMIT 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_db.netflix_demo WHERE release_year = 2019 LIMIT 2;
