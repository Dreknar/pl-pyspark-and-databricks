# Databricks notebook source
# MAGIC %md
# MAGIC # UDF i mapowanie

# COMMAND ----------

# DBTITLE 1,Przygotowanie danych
from pyspark.sql import Row
import pyspark.sql.functions as F

df = spark.read.format("parquet").load(f'file:/Workspace/Repos/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/pl-pyspark-and-databricks/data/parquet/netflix_titles.parquet')

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## UDF
# MAGIC
# MAGIC UDF (User Defined Function) pozwala na rozbudowywanie funkcjaności PySpark o funkcje użytkownika. Jest to szczególnie przydatne w sytuacji, kiedy chcemy wykorzystać funkcje Pythona w ramach kolumn DataFrame - domyślnie nnie można iterować po kolumnach. W przypadku UDF należy dostarczyć dwa elementy:
# MAGIC
# MAGIC - Stworzyć funkcję, która będzie zwracała interesujący nas wynik.
# MAGIC - Zarejestrować funkcję jako UDF w sparku za pomocą funkcji ```udf```, która przyjmuje argument w postaci **lambdy**.
# MAGIC
# MAGIC > ⚠️ UDF nie jest zoptymalizowany pod działanie na klastrze Sparka niż funkcje znajdujące się w module ```pyspark.sql.functions```. Rekomendowane jest wykonanie testów w przypadku sytuacji, które możemy obsłużyć za pomocą prostego UDF lub złożonego zestawu funkcji PySpark. Może się okazać, że jedno rozwiązanie będzie optymalniejsze dla danego scenariusza niż drugie. Społeczność PySpark mimo wszystko rekomuneduje unikanie dużej ilość UDF.
# MAGIC
# MAGIC > ✅ Należy również pamiętać, że błędy znajdziemy najczęściej podczas finałowego wykonania UDF/mapowania, więc należy uzbroić się w cierpliwość.

# COMMAND ----------

# DBTITLE 1,Przykładowe wykorzystanie UDF
"""
Funkcja zamienia listę na łańcuch znakowy.
Zwróć pustą listę, jak pole jest null
"""

# 1. Stworzenie funkcji
def list2str(array: list) -> str:
  if array is None:
    return ""
  else:
    return ",".join(array)


# 2. Utworzenie UDF
list2str_UDF = F.udf(lambda col: list2str(col))

# 3. Wykorzystanie UDF jak funkcji z pyspark.sql.functions
display(
  df.select(
    F.col("cast"), list2str_UDF(F.col("cast")).alias("list_to_string")
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mapowanie
# MAGIC
# MAGIC Mapowanie to potężne narzędzia pozwalające zamienić jeden DataFrame w drugi, dodając również możliwość manipulowania i transformowania danych według naszych potrzeb. Mapowanie wykorzystuje ```UDF```. Dobrą praktyką jest zwracanie za pomocą funkcji UDF wiersza dla nowego DataFrame, czyli obiektu klasy ```Row```.
# MAGIC
# MAGIC Posiadamy dwie metody mapujące:
# MAGIC
# MAGIC - ```map()``` - mapuje jeden wiersz na drugi 
# MAGIC - ```flatMap()``` - dodatkowo "rozpakowuje" listę wierszy zwracaną przez funkcję. Przykładowo: jeżeli funkcja zwróci listę trzy elementową to w nowym DataFrame wiersz zostanie zapisany jeden pod drugim.
# MAGIC
# MAGIC
# MAGIC Aby móc przeprowadzić mapowanie należy wstępnie Daframe przestawić jako rdd, a następnie wykonać na nim metodę ```map```/```flatMap```. Wynik metody ostatecznie nie jest jeszcze DataFrame, więc musimy wykonać na nim metodę ```toDF(schema)```. Czyli kompletne polecenia wygląda następująco:
# MAGIC
# MAGIC - ```nazwa_df.rdd.map(funkcja_mapujaca).toDF(schemat)```
# MAGIC - ```nazwa_df.rdd.flatMap(funkcja_mapujaca).toDF(schemat)```
# MAGIC
# MAGIC > ⚠️ Kolejność kolumn zwracanych przez mapowanie musi być taka sama jak podano w schemacie!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row
# MAGIC
# MAGIC Obiekt Row rreprezntuje każdy wierszy dostępny w DataFrame. Wszystkie informacje są przestawione w postaci ```klucz=wartość```, czyli: ```Row(col1=val1, col2=val2, col3=val3, ...)```.
# MAGIC
# MAGIC Funkcja pod ```map()``` powinna zwracać pojedynczy obiekt ```Row()```, natomiast dla ```flatMap()``` zwracamy listę obiektów ```Row```.
# MAGIC
# MAGIC Dodatkowo wykorzystując obiekt ```Row``` z modułu ```pyspark.sql``` możemy odwoływać się do wybranych informacji podczas pisania funkcji mapującej w postaci ```arg_funkcji.nazwa_kolumny```.
# MAGIC
# MAGIC > ✅ Jeżeli jest problem podczas wykorzystywania wartości pól wiersza, warto stosować funkcje konwertujące na nich.

# COMMAND ----------

# DBTITLE 1,Przykład użycia map()
"""
Chcemy stworzyć nowy DF na podstawie powyższego z następującymy kolumnami:
"ID" show_id bez litery s na początku,
"title" w formacie <tytuł> (duration),
created_by jako lista składająca się z director i country
"""

# 1. Tworzymy funkcje mapującą
def map_to_details(r: Row) -> Row:
    new_ID = str(r.show_id)[1:]
    new_title = f'{r.title} ({r.duration})'
    new_created_by = [r.director, r.country]

    return Row(
        ID = new_ID,
        title = new_title,
        created_by = new_created_by
    )
    
# 2. Wykonanie mapowania
new_df = df.rdd.map(lambda c: map_to_details(c))

# 3. Konwertowanie do DataFrame
new_df = new_df.toDF(["ID", "title", "created_by"])
display(new_df)

# COMMAND ----------

# DBTITLE 1,Przykład użycia flatMap()
"""
Przykładowy DataFrame zawiera nazwę pliku oraz jego wielkość w MB. Nasz system pozwala przechowywać pliki maksymalnie o wielkości 128MB. Funkcja musi "rozbić" pliki na część, gdzie każdy z nich może mieć maksymalnie 128MB.
Nowy DataFrame zawiera informacje o: nazwa pliku, id części, wielkość części.
"""

# 0. Tworzenie DataFrame
files_df = spark.createDataFrame([("example.exe", 258.0)], ["file_name", "size_in_MB"])

# 1. Tworzenie funkcji pod flatMap. Funkcja zwraca listę wierszy, ponieważ te wiersze reprezntują ten jeden plik podzielony na części
def split_file(r: Row) -> list:
    parts = []
    input_size = float(r.size_in_MB)

    part_id = 1 # Początkowe ID dla części plików

    while input_size > 0:
        if input_size >= 128.0:
            part_size = 128.0
        else:
            part_size = input_size
        
        input_size -= part_size
        
        parts.append(Row(file = r.file_name, part = f'part-{part_id}', file_size = part_size))
        part_id += 1
    
    return parts

# 2. Wykonanie flatMap
files_in_part_df = files_df.rdd.flatMap(lambda c: split_file(c))

# 3. Tworzenie df
files_in_part_df = files_in_part_df.toDF(["file", "part", "file_size"])
display(files_in_part_df)
