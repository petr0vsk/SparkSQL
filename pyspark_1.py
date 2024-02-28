# Нетология ДЗ
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, max, lag, desc, coalesce, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, coalesce
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
################################################################################
# Задача 3
# Фильтрация данных для России за последнюю неделю марта 2021 года
df_filtered = df.filter((col("location") == "Russia") & (col("date") >= "2021-03-24") & (col("date") <= "2021-03-31"))
# Оконная функция для сортировки по дате и получения количества новых случаев за предыдущий день
windowSpec = Window.partitionBy("location").orderBy("date")
df_with_lag = df_filtered.withColumn("new_cases_yesterday", lag("new_cases").over(windowSpec))
# Расчет дельты между количеством новых случаев сегодня и вчера
# Расчет дельты между количеством новых случаев сегодня и вчера с заменой null на 0
df_with_delta = df_with_lag.withColumn("delta", coalesce(col("new_cases") - col("new_cases_yesterday"), lit(0)))
# Выбор необходимых колонок для выходного датасета
output_df = df_with_delta.select(
    col("date").alias("число"),
    coalesce(col("new_cases_yesterday"), lit(0)).alias("кол-во новых случаев вчера"),
    col("new_cases").alias("кол-во новых случаев сегодня"),
    col("delta").alias("дельта")
)

# Вывод результатов
output_df.show()
# Сохранение в CSV 
output_path = '/home/petr0vsk/WorkSQL/Netology_Spark/Z_2/last_week_March'
output_df.write.csv(output_path, header=True, mode="overwrite")



