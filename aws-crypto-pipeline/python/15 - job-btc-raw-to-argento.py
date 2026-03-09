import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType

# Inizializzazione Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("job-btc-raw-to-argento", {})


# 1. Leggiamo i dati RAW dal Data Catalog

# Prezzo BTC
btc_price_df = glueContext.create_dynamic_frame.from_catalog(
    database="cryptodata_raw_db",
    table_name="btc_eur_historical_data_csv"
).toDF()

# Google Trend BTC
btc_trend_df = glueContext.create_dynamic_frame.from_catalog(
    database="cryptodata_raw_db",
    table_name="google_trend_bitcoin_csv"
).toDF()


# 2. Sistemiamo colonne data e prezzo (perché Glue ha messo col0, col1 ecc), le restanti non servono

btc_price_df = btc_price_df.withColumnRenamed("col0", "Date") \
                           .withColumnRenamed("col1", "Price")

# Convertiamo Date in formato data
btc_price_df = btc_price_df.withColumn("Date", to_date(col("Date"), "dd/MM/yyyy"))

# Convertiamo Price in double
btc_price_df = btc_price_df.withColumn("Price", col("Price").cast(DoubleType()))


# 3. Pulizia: rimuoviamo prezzi = -1

btc_price_df = btc_price_df.filter(col("Price") != -1)


# 4 Sistemiamo trend

btc_trend_df = btc_trend_df.withColumnRenamed("settimana", "Date") \
                           .withColumnRenamed("interesse_bitcoin", "GoogleTrend")

btc_trend_df = btc_trend_df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))


# 5. Salviamo in Parquet su bucket Argento

btc_price_df.write.mode("overwrite").parquet(
    "s3://cryptodata-argento-professionai/btc/price/"
)

btc_trend_df.write.mode("overwrite").parquet(
    "s3://cryptodata-argento-professionai/btc/trend/"
)

job.commit()