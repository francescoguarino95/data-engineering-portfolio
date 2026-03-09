from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("job-xmr-raw-to-argento", {})

# 1) Leggiamo i RAW dal Data Catalog
xmr_price_df = glueContext.create_dynamic_frame.from_catalog(
    database="cryptodata_raw_db",
    table_name="xmr_eur_kraken_historical_data_csv"
).toDF()

xmr_trend_df = glueContext.create_dynamic_frame.from_catalog(
    database="cryptodata_raw_db",
    table_name="google_trend_monero_csv"
).toDF()

# 2) Teniamo solo date e price e sistemiamo i tipi

# Le colonne sono state lette bene, quindi non abbiamo bisogno di rinominarle
xmr_price_df = xmr_price_df.select("date", "price")

# Cast: date (dd/MM/yyyy) e price double (già double, ma facciamo cast per sicurezza)
xmr_price_df = xmr_price_df.withColumn("date", to_date(col("date"), "dd/MM/yyyy"))
xmr_price_df = xmr_price_df.withColumn("price", col("price").cast(DoubleType()))

# Pulizia: rimuoviamo i prezzi mancanti (-1) e righe sporche
xmr_price_df = xmr_price_df.filter(col("price") != -1).dropna(subset=["date", "price"])

# 3) Trend: rinominiamo e castiamo la data
xmr_trend_df = xmr_trend_df.withColumnRenamed("settimana", "date") \
                           .withColumnRenamed("monero_interesse", "google_trend")

xmr_trend_df = xmr_trend_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
                           .dropna(subset=["date", "google_trend"])

# 4) Scrittura su Argento (Parquet)
xmr_price_df.write.mode("overwrite").parquet(
    "s3://cryptodata-argento-professionai/xmr/price/"
)

xmr_trend_df.write.mode("overwrite").parquet(
    "s3://cryptodata-argento-professionai/xmr/trend/"
)

job.commit()