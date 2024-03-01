# Для Задачи 2. Определим список стран и не-стран колонки 
from pyspark.sql import SparkSession
# Инициализация SparkSession
spark = SparkSession.builder.appName("List Countries and Non-Countries").getOrCreate()
# Загрузка данных
df = spark.read.csv('/home/petr0vsk/WorkSQL/Netology_Spark/Z_2/covid-data.csv', header=True, inferSchema=True)
# Получение уникальных значений в колонке 'location'
unique_locations = df.select("location").distinct()
# Вывод списка уникальных значений
unique_locations.show(unique_locations.count(), truncate=False)
# Сохранение в CSV 
output_path = '/home/petr0vsk/WorkSQL/Netology_Spark/Z_2/unique_locations'
unique_locations.write.csv(output_path, header=True, mode="overwrite")
# Останавливаем сессию Spark
spark.stop()
