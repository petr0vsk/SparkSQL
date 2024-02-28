# Нетология ДЗ
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, max
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
# Инициализация SparkSession
spark = SparkSession.builder.appName("covid_analysis").getOrCreate()
# Загрузка данных
df = spark.read.csv('/home/petr0vsk/WorkSQL/Netology_Spark/Z_2/covid-data.csv', header=True, inferSchema=True)
############################################################################
# Задача 1
# Фильтрация данных на 31 марта
df_filtered = df.filter(df['date'] == '2020-03-31')
# Группировка по странам с расчетом максимального значения общего количества случаев
df_grouped = df_filtered.groupBy("iso_code", "location").agg({"total_cases": "max"}).withColumnRenamed("max(total_cases)", "total_cases_on_31_march")
# Сортировка по убыванию количества случаев
df_sorted = df_grouped.orderBy(desc("total_cases_on_31_march"))
# Выбор 15 стран с наибольшим количеством случаев
top_15_countries = df_sorted.limit(15)
# Показ результатов
top_15_countries.show()
# Путь, куда будет сохранен файл
output_path = '/home/petr0vsk/WorkSQL/Netology_Spark/Z_2/top_15_countries'
# Сохранение DataFrame в CSV, если уже есть перезапишем
top_15_countries.write.csv(output_path, header=True, mode="overwrite")
##############################################################################
# Задача 2
# Загрузка данных
# Фильтрация данных за последнюю неделю марта 2021 года
df_filtered = df.filter((col("date") >= "2021-03-24") & (col("date") <= "2021-03-31"))
# Группировка по стране и дате, не агрегируем данные, чтобы сохранить детализацию по дням
df_grouped = df_filtered.select("location", "date", "new_cases")
# Находим день с максимальным количеством новых случаев для каждой страны
windowSpec = Window.partitionBy("location").orderBy(desc("new_cases"))
df_max_cases_per_country = df_grouped.withColumn("rank", rank().over(windowSpec)).filter(col("rank") == 1).drop("rank")
# Сортировка по убыванию количества новых случаев и выборка топ-10
top_10_days = df_max_cases_per_country.orderBy(desc("new_cases")).limit(10)
# Вывод результатов
top_10_days.show()
# Сохранение в CSV 
output_path = '/home/petr0vsk/WorkSQL/Netology_Spark/Z_2/top_10_countries'
top_10_days.write.csv(output_path, header=True, mode="overwrite")

