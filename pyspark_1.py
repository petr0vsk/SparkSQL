from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
# Инициализация SparkSession
spark = SparkSession.builder.appName("covid_analysis").getOrCreate()
# Загрузка данных
df = spark.read.csv('/home/petr0vsk/WorkSQL/Netology_Spark/Z_2/covid-data.csv', header=True, inferSchema=True)
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
