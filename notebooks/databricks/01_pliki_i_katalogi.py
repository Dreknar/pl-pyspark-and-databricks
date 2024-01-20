# Databricks notebook source
# MAGIC %md
# MAGIC # Pliki i katalogi
# MAGIC
# MAGIC Databricks dostarcza własny system plików do zarządzania danymi, które są dostępne w ramach tego środowiska. Sam system nazywa się _Databricks File System_ (DBFS). Należy pamiętać, że ścieżki możemy podawać na dwa sposoby:
# MAGIC
# MAGIC 1. POSIX-style paths: w tej sytuacji dostęp do danych mamy za pomocą ścieżke dobrze nam znanym z UNIX/LINUX (czyli zaczynające się od ```/```).
# MAGIC
# MAGIC ![POSIX-style paths](https://learn.microsoft.com/en-us/azure/databricks/_static/images/files/posix-paths.png)
# MAGIC
# MAGIC 2. URI-style paths: ścieżki są definiowane jak odwołania sie po HTTP (zaczynają się np. ```file:```)
# MAGIC
# MAGIC ![URI-style paths](https://learn.microsoft.com/en-us/azure/databricks/_static/images/files/uri-paths-azure.png)
# MAGIC
# MAGIC > ✅ Należy pamiętać o obu opcjach, ponieważ nie są one równoważne. Przykładowo Spark w Databricks preferuje zapis POSIXowy do danych montowanych (mnt), natomiast dostęp do danych z tego repozytorium mamy za pomocą zapisu URI.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materiały dodatkowe (dokumentacja)
# MAGIC
# MAGIC - [What is the Databricks File System (DBFS)?](https://learn.microsoft.com/en-us/azure/databricks/dbfs/)
# MAGIC - [Work with files on Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/files/)
# MAGIC - [FileStore](https://learn.microsoft.com/en-us/azure/databricks/dbfs/filestore)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dbutils - moduł fs

# COMMAND ----------

# DBTITLE 1,help
#Wyświetlenie pomocy dla modułu fs
dbutils.fs.help()

# COMMAND ----------

# DBTITLE 1,ls
# Wyświetla zawartość wskazanego katalogu
dbutils.fs.ls("ścieżka/do/katalogu")

# COMMAND ----------

# DBTITLE 1,cp
rekurencja = True # Domyślnie False

# Kopiuje plik/katalog w nowe miejsce. W przypadku trzeciego argumentu ustawiamy True dla katalogów, który domyślnie jest False.
dbutils.fs.cp("ścieżka/do/pliku/lub/katalogu/źródłowego", "ścieżka/do/katalogu/docelowego", rekurencja)

# COMMAND ----------

# DBTITLE 1,head
n = 100 # Domyślnie 65536

# Wyświetla n bajtów z pliku tekstowego. n domyślnie to: 65536. Wyświetlany tekst jest enkodowany w UTF-8/
dbutils.fs.head("ścieżka/do/pliku", n)

# COMMAND ----------

# DBTITLE 1,mkdirs
# Tworzy katalog w podanej ścieżce. Brakujące katalogi w ścieżce również sa tworzone. Tak jak bashu opcja -p dla polecenia mkdir
dbutils.fs.mkdirs("ścieżka/do/katalogu/i/nazwa")

# COMMAND ----------

# DBTITLE 1,mv
rekurencja = True # Domyślnie False

# Przenosi plik/katalog w nowe miejsce. W przypadku trzeciego argumentu ustawiamy True dla katalogów, który domyślnie jest False.
dbutils.fs.mv("ścieżka/do/pliku/lub/katalogu/źródłowego", "ścieżka/do/katalogu/docelowego", rekurencja)

# COMMAND ----------

# DBTITLE 1,rm
rekurencja = True # Domyślnie False

# Usuwa plik/katalog. W przypadku drugiego argumentu ustawiamy True dla katalogów, który domyślnie jest False.
dbutils.fs.mv("ścieżka/do/pliku/lub/katalogu/źródłowego", rekurencja)

# COMMAND ----------

# DBTITLE 1,put
nadpisz = True # Domyślnie False

# Zapisuje podany tekst do pliku. Jeżeli plik istnieje to otrzymamy błąd. Możemy ustawić trzeci argument na True - w tej sytuacji zostanie napidsana zawartość istniejącego pliku. Wykorzystane jest enkodowanie UTF-8.
dbutils.fs.put("ścieżka/do/pliku", "Tekst do zapisania", nadpisz)
