
"""Я обрав датасет Airbnb NYC 2019, бо він поєднує географічні, категоріальні та числові дані в реалістичному бізнес-контексті, що дає простір для змістовних трансформацій і агрегацій."""

file_path = "/Volumes/workspace/default/workspace/Airbnb_NYC_2019_dataset.csv"

df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True,
    multiLine=True,      # дозволяє полям з переносами рядків всередині лапок
    escape='"',          # правильна обробка лапок всередині тексту
    quote='"'
)

df.printSchema()
print("Total rows:", df.count())


from pyspark.sql.functions import col, sum as spark_sum, when

df.select([
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df.columns
]).show()

from pyspark.sql.functions import lit

# reviews_per_month порожній ЛОГІЧНО означає "відгуків ще не було" -> 0, а не середнє
df = df.fillna({"reviews_per_month": 0.0})

# name/host_name - текстові поля, пропуск не критичний -> заповнюємо заглушкою
df = df.fillna({"name": "Unknown", "host_name": "Unknown"})

# last_review залишаємо NULL - це дата, "заповнення" середнім тут не має сенсу

from pyspark.sql.functions import when, col, round as spark_round

# Загальна вартість за мінімальний термін оренди
df = df.withColumn("price_per_min_stay", spark_round(col("price") * col("minimum_nights"), 2))

# Категорія ціни
df = df.withColumn(
    "price_category",
    when(col("price") < 100, "Budget")
    .when((col("price") >= 100) & (col("price") < 300), "Mid-range")
    .otherwise("Luxury")
)

# Флаг "активний" лістинг (є хоч один відгук)
df = df.withColumn("has_reviews", when(col("number_of_reviews") > 0, True).otherwise(False))

df.select("price", "minimum_nights", "price_per_min_stay", "price_category", "has_reviews").show(10)

df_filtered = df.filter(
    (col("neighbourhood_group").isin("Manhattan", "Brooklyn")) &
    (col("has_reviews") == True) &
    (col("price") > 0)
)

print("Filtered rows:", df_filtered.count())
df_filtered.select("name", "neighbourhood_group", "price", "price_category").show(10)

df.createOrReplaceTempView("airbnb")

result = spark.sql("""
    SELECT 
        neighbourhood_group,
        room_type,
        COUNT(*) AS num_listings,
        ROUND(AVG(price), 2) AS avg_price,
        ROUND(AVG(number_of_reviews), 1) AS avg_reviews,
        ROUND(AVG(availability_365), 0) AS avg_availability
    FROM airbnb
    WHERE neighbourhood_group IS NOT NULL
    GROUP BY neighbourhood_group, room_type
    ORDER BY avg_price DESC
""")
result.show()


result.write.format("delta").mode("overwrite").saveAsTable("airbnb_price_by_area_and_type")

display(result)

sample_df = df.filter(col("price") < 500).sample(0.05)  # семпл для швидкості й читабельності
display(sample_df.select("price", "number_of_reviews", "neighbourhood_group", "room_type"))

category_dist = spark.sql("""
    SELECT neighbourhood_group, price_category, COUNT(*) AS count
    FROM airbnb
    WHERE neighbourhood_group IS NOT NULL
    GROUP BY neighbourhood_group, price_category
""")
display(category_dist)