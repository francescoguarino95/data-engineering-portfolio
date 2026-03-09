from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, to_date, date_trunc, avg
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("job-btc-argento-to-oro", {})

# 1) Leggiamo i parquet da Argento
price_path = "s3://cryptodata-argento-professionai/btc/price/"
trend_path = "s3://cryptodata-argento-professionai/btc/trend/"

price = spark.read.parquet(price_path)
trend = spark.read.parquet(trend_path)

# 2) Normalizziamo nomi colonne (prezzi)
if "Date" in price.columns:
    price = price.withColumnRenamed("Date", "date")
if "Price" in price.columns:
    price = price.withColumnRenamed("Price", "price")

price = price.withColumn("date", to_date(col("date")))

# 3) Media mobile 10 giorni sul prezzo (daily)
w = Window.orderBy(col("date")).rowsBetween(-9, 0)
price = price.withColumn("price_ma10", avg(col("price")).over(w))

# 4) Granularità settimanale (per join con trend)
price_week = (
    price.withColumn("week_start", to_date(date_trunc("week", col("date"))))
         .groupBy("week_start")
         .agg(avg("price_ma10").alias("price_ma10"))
)

# 5) Trend: in Argento ha colonne Date + google_trend
if "Date" in trend.columns:
    trend = trend.withColumnRenamed("Date", "week_start")

# Se per caso il nome fosse diverso (es. interesse bitcoin), lo gestiamo
if "interesse bitcoin" in trend.columns:
    trend = trend.withColumnRenamed("interesse bitcoin", "google_trend")

trend = trend.withColumn("week_start", to_date(col("week_start")))

# 6) Join finale
final_df = (
    price_week.join(trend, on="week_start", how="inner")
              .select(
                  col("week_start").alias("date"),
                  col("price_ma10"),
                  col("google_trend")
              )
              .orderBy("date")
)

# 7) Scriviamo su bucket Oro
out_path = "s3://cryptodata-oro-professionai/btc/"
final_df.write.mode("overwrite").parquet(out_path)

job.commit()