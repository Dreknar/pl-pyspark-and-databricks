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

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

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
    DataType,
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

# COMMAND ----------

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
# MAGIC > ⚠️ Biblioteka ```com.databricks.spark.xml``` musi zostać zainstalowana na klastrze. Należy podać dla pakietu Maven nazwę: ```com.databricks:spark-xml_2.12:0.17.0```
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

# COMMAND ----------

# MAGIC %md
# MAGIC # Selekcja i sortowanie
# MAGIC
# MAGIC DataFrame przypomina ten z modułu pandas, więc część poleceń będzie wspólna np. odczytywanie wybranej kolumny. Nie jest to rekomendowane, ponieważ DataFrame PySpark jest oddzielną implementacją i posiada ona swój zestaw funkcji i narzędzi. Przykładowo jest to metoda ```col()``` pozwalająca odczytywać wartości wybranej kolumny. W przeciwieństwie do klasycznego rozwiązania: ```df[nazwa_kolumny]``` mamy dostęp do dodatkowych opcji/narzędzi.
# MAGIC
# MAGIC ✅ Zalecane jest stosowanie metody ```col()``` podczas pracy z danymi (poza pojedynczymi wyjątkami)

# COMMAND ----------

# DBTITLE 1,Import funkcji
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
    DataType,
    TimestampType,
    ArrayType,
    MapType,
)
import pyspark.sql.functions as F
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Przykładowy DataFrame
schema = StructType(
    [
        StructField("ID", IntegerType(), False),
        StructField("LOGIN", StringType(), False),
        StructField("IS_ACTIVE", BooleanType(), False),
        StructField("LOGIN_AT", TimestampType(), True),
        StructField("TAGS", ArrayType(StringType(), False), False),
        StructField("METADATA", MapType(StringType(), StringType(), True), True),
    ]
)

example_df = spark.createDataFrame(
    [
        (
            1,
            "u001",
            True,
            datetime(2023, 12, 1, 12, 36, 23),
            ["SQL", "Scala", "Python", "Java"],
            {"posts": "120", "stars": "200", "2FA_activated": "True"},
        ),
        (
            2,
            "u002",
            False,
            datetime(2023, 12, 2, 15, 44, 13),
            ["Azure", "GCP", "AWS", "C#"],
            {"2FA_activated": "True", "is_banned": True, "ban_reason": "Fake news"},
        ),
        (3, "u003", False, None, ["Linux", "Red Hat"], None),
                (
            4,
            "u004",
            True,
            datetime(2023, 1, 10, 9, 12, 44),
            ["SQL", "Scala", "Python", "Java"],
            {"posts": "600", "stars": "2500", "2FA_activated": "True"},
        ),
    ],
    schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wyświetlanie danych

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT
# MAGIC Metoda ```select()``` pozwala na wyświetlanie kolumn oraz wyniku funkcji wykonywanych w ramach danego DataFrame. Jako argumenty podajemy funkcje/kolumny, które mają zostać wyświetlone.

# COMMAND ----------

# DBTITLE 1,Przykład
display(
    example_df.select(F.col("LOGIN"), F.col("TAGS"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pobieranie wartości z kolekcji
# MAGIC Jeżeli natomiast chcemy pobrać wartość pod danym indeksem/kluczem z naszej kolekcji danych należy dodać informacje o indeksie po podaniu kolumny (zupełnie jak w Pythonie) lub wykonać metodę ```getItem(klucz_lub_index)```. PySpark wstawi ```null```, jeżeli nie znajdzie wartości.

# COMMAND ----------

# DBTITLE 1,Przykład
display(
    example_df.select(
        F.col("TAGS").getItem(2),
        F.col("METADATA").getItem("is_banned")
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aliasy
# MAGIC Do każdej metody ```pyspark.sql.functions```, która zwraca wartość pojedynczą wartość możemy wykonać ```alias(nazwa)``` w celu ustawienia własnej nazwy kolumny.

# COMMAND ----------

# DBTITLE 1,Przykład
display(
    example_df.select(
        F.col("TAGS").getItem(2).alias("random_tech"),
        F.col("METADATA").getItem("is_banned").alias("is_banned"),
        F.col("LOGIN").alias("username")
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pseudokolumna
# MAGIC Metoda ```lit``` pozwala na stworzenie pseudokolumny ze stałą wartością, którą podajemy jako argument.

# COMMAND ----------

display(
    example_df.select(
        F.lit("Hi!").alias("greetings"),
        F.col("LOGIN")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sortowanie danych

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funkcje sort() i orderBy()
# MAGIC
# MAGIC Sortowanie danych w PySpark możemy wykonać za pomocą dwóch metod:
# MAGIC - ```sort([col1, col2, col3, ...], ascending=[True|False, True|False, True|False, ...])```; W tej sytuacji należy pamiętać że wartości dla klucza _ascending_ odpawiadają kolejności podawania kolumn. Przykładowo _ascending[0]_ dotyczy pierwszego argumentu.
# MAGIC - ```orderBy(col(nazwa), col(nazwa).desc(), ...)```; Tutaj zamieniamy kolejność sortowania poprzez wykonanie metody ```desc()``` na kolumnie. Tutaj też możemy używać funkcji jak np. ```getItem()```
# MAGIC
# MAGIC > ✅ Kolejność argumentów dla powyższych funkcji wpływa na wynik sortowania!

# COMMAND ----------

# DBTITLE 1,Przykłady
display(
    example_df.select(
        F.col("ID"),
        F.col("LOGIN")
    ).sort("ID", ascending=[False])
)

display(
    example_df.select(
        F.col("ID"),
        F.col("LOGIN")
    ).orderBy(F.col("ID").desc())
)

display(
    example_df.select(
        F.col("ID"),
        F.col("LOGIN"),
        F.col("METADATA")
    ).orderBy(
        F.col("METADATA").getItem("stars").desc(),
        F.col("METADATA").getItem("posts")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dodatkowe funkcje
# MAGIC
# MAGIC Na DataFrame możemy również wykonać funkcje:
# MAGIC - ```count()``` - zwraca ilość wierszy (jest to liczba, nie DataFrame)
# MAGIC - ```limit(n)``` - ogranicza DataFrame do n wierszy (może być to odczyt losowy!)

# COMMAND ----------

# DBTITLE 1,Przykłady
display(example_df.count())

display(example_df.select(F.col("TAGS")).limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC # Wartości unikatowe oraz praca z kolumnami

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
    DataType,
    TimestampType,
    ArrayType,
    MapType,
)
import pyspark.sql.functions as F
from datetime import datetime

schema = StructType(
    [
        StructField("ID", IntegerType(), False),
        StructField("LOGIN", StringType(), False),
        StructField("IS_ACTIVE", BooleanType(), False),
        StructField("LOGIN_AT", TimestampType(), True),
        StructField("TAGS", ArrayType(StringType(), False), False),
        StructField("METADATA", MapType(StringType(), StringType(), True), True),
        StructField("ORGANIZATION", StringType(), False),
    ]
)

example_df = spark.createDataFrame(
    [
        (
            1,
            "u001",
            True,
            datetime(2023, 12, 1, 12, 36, 23),
            ["SQL", "Scala", "Python", "Java"],
            {"posts": "120", "stars": "200", "2FA_activated": "True"},
            "R&D"
        ),
        (
            2,
            "u002",
            False,
            datetime(2023, 12, 2, 15, 44, 13),
            ["Azure", "GCP", "AWS", "C#"],
            {"2FA_activated": "True", "is_banned": True, "ban_reason": "Fake news"},
            "Collection"
        ),
        (3, "u003", False, None, ["Linux", "Red Hat"], None, "R&D"),
                (
            4,
            "u004",
            True,
            datetime(2023, 1, 10, 9, 12, 44),
            ["SQL", "Scala", "Python", "Java"],
            {"posts": "600", "stars": "2500", "2FA_activated": "True"},
            "R&D"
        ),
                                (
            5,
            "u005",
            True,
            datetime(2023, 1, 15, 9, 12, 44),
            ["SQL", "Oracle", "MySQL", "MS SQL Server"],
            {"posts": "300", "stars": "120", "2FA_activated": "False"},
            "Databases"
        ),
                
    ],
    schema,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dodawanie/Usuwanie kolumny
# MAGIC
# MAGIC DataFrame w PySpark pozwala na dynamiczną zmianę struktury za pomocą funkcji:
# MAGIC
# MAGIC - ```drop(nazwa_kolumny, nazwa_kolumny, ...)``` - usuwa podane kolumny.
# MAGIC - ```withColumn(nazwa_kolumny, wartość)``` - dodaje nową kolumnę do DataFrame. Wartość może być wynik funkcji.
# MAGIC
# MAGIC > ✅ ```withColumn``` pozwala również na nadpisywanie wartości istniejących już kolumn. Wystarczy, że jako nazwę podamy istniejącą kolumnę.

# COMMAND ----------

# DBTITLE 1,Przykład
display(
    example_df
    .withColumn("IS_OLD_ACC", F.lit(True))
    .withColumn("ID", F.col("ID") + 100)
)

display(
    example_df
    .drop("METADATA")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtrowanie duplikatów
# MAGIC
# MAGIC Aby wyświetlić unikatowe wiersze z naszego DataFrame musimy wykonać na nim metodę ```distinct()```. Podobnie jak w SQL wyświetlamy tylko te wiersze, które są unikatowe w danym wyniku, zatem ```distinct()``` po poleceniu ```select()``` będzie zwracał inny zbiór niż wykonany bezpośrednio na DataFrame.
# MAGIC
# MAGIC Drugim sposobem jest wykorzystanie metody ```dropDuplicates([nazwa_kolumny, nazwa_kolumny, ...])``` na DataFrame. PySpark będzie skanował zbiór danych i zostawiał wystąpienie każdego wiersza według kolumn podanych jako argumenty funkcji.
# MAGIC
# MAGIC > ⚠️ Kolejność wierszy przy korzystaniu ```dropDuplicates``` ma znaczenie! Warto wcześniej posortować zbiór przed dodaniem metody.
# MAGIC > ⚠️ ```distinct``` nie działa, kiedy DataFrame posiada kolekcje danych.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Przykłady
display(example_df.select(F.col("ORGANIZATION"), F.col("IS_ACTIVE")).distinct())

display(example_df.select(F.col("ORGANIZATION")).distinct())

display(
  example_df.dropDuplicates(["ORGANIZATION"])
)

display(
  example_df.orderBy(F.col("ID").desc()).dropDuplicates(["ORGANIZATION"])
)

# COMMAND ----------

# DBTITLE 1,Błąd dla distinct
display(example_df.distinct())
