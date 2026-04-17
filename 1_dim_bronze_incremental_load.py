# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

brand_schema = StructType([
    StructField("brand_code", StringType(), False),
    StructField("brand_name", StringType(), True),
    StructField("category_code", StringType(), True),
])

raw_data_path = "/Volumes/ecommerce/source_data/raw/brands/*.csv"
df_brands = spark.read.option('header', "true").option("delimiter", ",").schema(brand_schema).csv(raw_data_path)
df_brands = df_brands.withColumn("_source_file", col("_metadata.file_path")) \
                     .withColumn("ingested_at", current_timestamp())

table_name = f"{catalog_name}.bronze.brz_brands"
table_exists = spark.catalog.tableExists(table_name)

if table_exists:
    delta_table = DeltaTable.forName(spark, table_name)
    delta_table.alias("target").merge(
        df_brands.alias("source"),
        "target.brand_code = source.brand_code"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"✅ Incremental merge completed for {table_name}")
else:
    df_brands.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
    print(f"✅ Bronze table created: {table_name}")

# COMMAND ----------

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),
    StructField("length_cm", StringType(), True),
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True)
])

# -----------------------------
# Read raw data
# -----------------------------
raw_data_path = "/Volumes/ecommerce/source_data/raw/products/*.csv"

df_products = (
    spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .schema(products_schema)
        .csv(raw_data_path)
)

# -----------------------------
# Add Bronze metadata
# -----------------------------
df_products = (
    df_products
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("ingested_at", current_timestamp())
)

# -----------------------------
# Target table
# -----------------------------
table_name = f"{catalog_name}.bronze.brz_products"
table_exists = spark.catalog.tableExists(table_name)

# -----------------------------
# MERGE logic
# -----------------------------
if table_exists:
    delta_table = DeltaTable.forName(spark, table_name)

    (
        delta_table.alias("target")
        .merge(
            df_products.alias("source"),
            "target.product_id = source.product_id"
        )
        .whenMatchedUpdate(set={
            "sku": "source.sku",
            "category_code": "source.category_code",
            "brand_code": "source.brand_code",
            "color": "source.color",
            "size": "source.size",
            "material": "source.material",
            "weight_grams": "source.weight_grams",
            "length_cm": "source.length_cm",
            "width_cm": "source.width_cm",
            "height_cm": "source.height_cm",
            "rating_count": "source.rating_count",

            # 🔑 map to EXISTING target columns
            "file_name": "source._source_file",
            "ingest_timestamp": "source.ingested_at"
        })
        .whenNotMatchedInsert(values={
            "product_id": "source.product_id",
            "sku": "source.sku",
            "category_code": "source.category_code",
            "brand_code": "source.brand_code",
            "color": "source.color",
            "size": "source.size",
            "material": "source.material",
            "weight_grams": "source.weight_grams",
            "length_cm": "source.length_cm",
            "width_cm": "source.width_cm",
            "height_cm": "source.height_cm",
            "rating_count": "source.rating_count",
            "file_name": "source._source_file",
            "ingest_timestamp": "source.ingested_at"
        })
        .execute()
    )

    print(f"✅ Incremental merge completed for {table_name}")

else:
    (
        df_products
            .withColumnRenamed("_source_file", "file_name")
            .withColumnRenamed("ingested_at", "ingest_timestamp")
            .write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
    )

    print(f"✅ Bronze table created: {table_name}")

# COMMAND ----------

df_customers = (
    df_customers
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("ingested_at", current_timestamp())
)

# -----------------------------
# Target table
# -----------------------------
table_name = f"{catalog_name}.bronze.brz_customers"
table_exists = spark.catalog.tableExists(table_name)

# -----------------------------
# MERGE logic
# -----------------------------
if table_exists:
    delta_table = DeltaTable.forName(spark, table_name)

    (
        delta_table.alias("target")
        .merge(
            df_customers.alias("source"),
            "target.customer_id = source.customer_id"
        )
        .whenMatchedUpdate(set={
            "phone": "source.phone",
            "country_code": "source.country_code",
            "country": "source.country",
            "state": "source.state",

            # 🔑 map to EXISTING target metadata columns
            "file_name": "source._source_file",
            "ingest_timestamp": "source.ingested_at"
        })
        .whenNotMatchedInsert(values={
            "customer_id": "source.customer_id",
            "phone": "source.phone",
            "country_code": "source.country_code",
            "country": "source.country",
            "state": "source.state",
            "file_name": "source._source_file",
            "ingest_timestamp": "source.ingested_at"
        })
        .execute()
    )

    print(f"✅ Incremental merge completed for {table_name}")

else:
    (
        df_customers
            .withColumnRenamed("_source_file", "file_name")
            .withColumnRenamed("ingested_at", "ingest_timestamp")
            .write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
    )

    print(f"✅ Bronze table created: {table_name}")

# COMMAND ----------

date_schema = StructType([
    StructField("date", StringType(), False),
    StructField("year", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("week_of_year", IntegerType(), True),
])

# -----------------------------
# Read raw data
# -----------------------------
raw_data_path = "/Volumes/ecommerce/source_data/raw/date/*.csv"

df_calendar = (
    spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .schema(date_schema)
        .csv(raw_data_path)
)

# -----------------------------
# Add source-side metadata
# -----------------------------
df_calendar = (
    df_calendar
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("ingested_at", current_timestamp())
)

# -----------------------------
# Target table
# -----------------------------
table_name = f"{catalog_name}.bronze.brz_calendar"
table_exists = spark.catalog.tableExists(table_name)

# -----------------------------
# INSERT-ONLY MERGE
# -----------------------------
if table_exists:
    delta_table = DeltaTable.forName(spark, table_name)

    (
        delta_table.alias("target")
        .merge(
            df_calendar.alias("source"),
            "target.date = source.date"
        )
        .whenNotMatchedInsert(values={
            "date": "source.date",
            "year": "source.year",
            "day_name": "source.day_name",
            "quarter": "source.quarter",
            "week_of_year": "source.week_of_year",
            "_source_file": "source._source_file",
            "_ingested_at": "source.ingested_at"   # 🔑 CRITICAL FIX
        })
        .execute()
    )

    print(f"✅ Calendar incremental load completed for {table_name}")

else:
    (
        df_calendar
            .withColumnRenamed("ingested_at", "_ingested_at")
            .write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
    )

    print(f"✅ Bronze table created: {table_name}")