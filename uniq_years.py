# Для Задачи 1. Определим какие года пристуствуют в датасете
from pyspark.sql import SparkSession
from pyspark.sql.functions import year

# Инициализация SparkSession
spark = SparkSession.builder.appName("list_years").getOrCreate()
# Загрузка данных
df = spark.read.csv('/home/petr0vsk/WorkSQL/Netology_Spark/Z_2/covid-data.csv', header=True, inferSchema=True)
# Извлечение года из даты и выбор уникальных значений
df_years = df.select(year("date").alias("year")).distinct()
# Сортировка годов по возрастанию
df_years = df_years.sort("year")
# Вывод списка годов в консоль
df_years.show()
# Останавливаем сессию
spark.stop()
