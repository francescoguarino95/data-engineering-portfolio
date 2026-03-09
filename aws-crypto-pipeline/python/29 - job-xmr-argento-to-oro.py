from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, to_date, date_trunc, avg
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("job-xmr-argento-to-oro", {})

price_path = "s3://cryptodata-argento-professionai/xmr/price/"
trend_path = "s3://cryptodata-argento-professionai/xmr/trend/"

price = spark.read.parquet(price_path)
trend = spark.read.parquet(trend_path)

# Normalizziamo nomi colonne
if "Date" in price.columns: price = price.withColumnRenamed("Date", "date")
if "Price" in price.columns: price = price.withColumnRenamed("Price", "price")

# XMR trend dovrebbe già avere google_trend, ma normalizziamo comunque
if "GoogleTrend" in trend.columns: trend = trend.withColumnRenamed("GoogleTrend", "google_trend")
if "monero_interesse" in trend.columns: trend = trend.withColumnRenamed("monero_interesse", "google_trend")
if "settimana" in trend.columns: trend = trend.withColumnRenamed("settimana", "date")

price = price.withColumn("date", to_date(col("date")))
trend = trend.withColumn("date", to_date(col("date")))

# Media mobile 10 giorni (daily)
w = Window.orderBy(col("date")).rowsBetween(-9, 0)
price = price.withColumn("price_ma10", avg(col("price")).over(w))

# Settimanale
price_week = price.withColumn("week_start", to_date(date_trunc("week", col("date")))) \
                 .groupBy("week_start") \
                 .agg(avg("price_ma10").alias("price_ma10"))

trend_week = trend.withColumnRenamed("date", "week_start")

# Join finale
final_df = price_week.join(trend_week, on="week_start", how="inner") \
                    .select(
                        col("week_start").alias("date"),
                        col("price_ma10"),
                        col("google_trend")
                    ) \
                    .orderBy("date")

out_path = "s3://cryptodata-oro-professionai/xmr/"
final_df.write.mode("overwrite").parquet(out_path)

job.commit()