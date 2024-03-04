# Нетология Домашнее задание по теме «Spark SQL»
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, max, lag, desc, coalesce, lit, format_number, dayofmonth, month, when
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, coalesce
# Инициализация SparkSession
spark = SparkSession.builder.appName("covid_analysis").getOrCreate()
# Загрузка данных
df = spark.read.csv('/home/petr0vsk/WorkSQL/Netology_Spark/Z_2/covid-data.csv', header=True, inferSchema=True)
## Задача 1 ##########################################################################
# Для расчета процента переболевших населения страны без прямых данных о населении, 
# мы можем использовать колонку total_cases_per_million, 
# так как она отражает общее количество случаев на миллион населения. 
# Используя эту колонку, мы можем оценить процент переболевших относительно общего числа населения, 
# предполагая что 1 миллион в этой колонке соответствует 1 миллиону населения. 
# Это позволит нам обойтись без явных данных о населении каждой страны.

# Фильтрация данных на 31 марта
# Фильтрация данных на 31 марта 2021 года
df_filtered = df.filter(df['date'] == '2021-03-31')
# Преобразование колонки total_cases_per_million в числовой формат
df_filtered = df_filtered.withColumn("total_cases_per_million", df_filtered["total_cases_per_million"].cast("float"))
# Расчет процента переболевших, используя total_cases_per_million
df_filtered = df_filtered.withColumn("percentage_infected", col("total_cases_per_million") / 10000)
# Округление процента переболевших до двух знаков после запятой
df_filtered = df_filtered.withColumn("percentage_infected", format_number("percentage_infected", 2))
# Группировка по странам и вычисление максимального процента переболевших среди всех 31 марта
df_grouped = df_filtered.groupBy("iso_code", "location").agg(max("percentage_infected").alias("max_percentage_infected"))

# Сортировка стран по убыванию процента переболевших и выбор топ-15
df_top15 = df_grouped.sort(col("max_percentage_infected").desc()).limit(15)
# Применение алиасов к колонкам для итогового датасета
top_15_countries = df_top15.select(
    col("iso_code").alias("iso_code"),
    col("location").alias("страна"),
    col("max_percentage_infected").alias("процент переболевших")
)
top_15_countries.show()
# Путь, куда будет сохранен файл
output_path = '/home/petr0vsk/WorkSQL/Netology_Spark/Z_2/top_15_countries'
# Сохранение DataFrame в CSV, если уже есть перезапишем
top_15_countries.write.csv(output_path, header=True, mode="overwrite")

### Задача 2 ###########################################################################
#  Решаем исходя из предположения что задача заключается 
# в поиске максимального единичного значения новых случаев за день в пределах последней недели марта.
# Убираем из результата не-страны с помощью списка исключений
exclusions = ['World', 'Europe', 'Asia', 'North America', 'South America', 'European Union', 'Africa'] 

# Фильтрация данных за последнюю неделю марта 2021 года, исключая не-страновые записи
df_filtered = df.filter(
    (col("date") >= "2021-03-24") & 
    (col("date") <= "2021-03-31") &
    (~df["location"].isin(exclusions))
)
# Группировка по стране и дате, не агрегируем данные, чтобы сохранить детализацию по дням
df_grouped = df_filtered.select("location", "date", "new_cases")
# Находим день с максимальным количеством новых случаев для каждой страны
windowSpec = Window.partitionBy("location").orderBy(desc("new_cases"))
df_max_cases_per_country = df_grouped.withColumn("rank", rank().over(windowSpec))\
    .filter(col("rank") == 1)\
    .drop("rank")
# Применение алиасов к колонкам
df_max_cases_per_country = df_max_cases_per_country.select(
    col("date").alias("число"),
    col("location").alias("страна"),
    col("new_cases").alias("кол-во новых случаев")
)
# Сортировка по убыванию количества новых случаев и выборка топ-10
top_10_days = df_max_cases_per_country.orderBy(desc("new_cases")).limit(10)
# Вывод результатов
top_10_days.show()
# Сохранение в CSV 
output_path = '/home/petr0vsk/WorkSQL/Netology_Spark/Z_2/top_10_countries'
top_10_days.write.csv(output_path, header=True, mode="overwrite")

### Задача 3 ##############################################################################
# Фильтрация данных для России за последнюю неделю марта 2021 года, включая день перед началом последней недели марта
#df_filtered = df.filter((col("location") == "Russia") & (col("date") >= "2021-03-24") & (col("date") <= "2021-03-31"))
df_filtered = df.filter((col("location") == "Russia") &  (col("date") >= "2021-03-23") & (col("date") <= "2021-03-31"))
# Оконная функция для сортировки по дате
windowSpec = Window.partitionBy("location").orderBy("date")
# Расчет количества новых случаев за предыдущий день
df_with_lag = df_filtered.withColumn("new_cases_yesterday", lag("new_cases").over(windowSpec))
# Расчет дельты
df_with_delta = df_with_lag.withColumn("delta", col("new_cases") - coalesce(col("new_cases_yesterday"), lit(0)))
# Фильтрация, чтобы убрать лишнюю строку (23 марта)
df_final = df_with_delta.filter(col("date") >= "2021-03-24")
# Выбор необходимых колонок для выходного датасета
output_df = df_final.select(
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

# Останавливаем сессию
spark.stop()

