# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DataType, TimestampType, FloatType
from pyspark.sql.functions import monotonically_increasing_id, current_timestamp, input_file_name
from delta.tables import DeltaTable

import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = 'ecommerce'

# Define schema for the data file
brand_schema = StructType([
    StructField("brand_code", StringType(), False),
    StructField("brand_name", StringType(), True),
    StructField("category_code", StringType(), True),
])

# COMMAND ----------

raw_data_path = "/Volumes/ecommerce/source_data/raw/brands/*.csv"

df = spark.read.option('header', "true").option("delimeter", ",").schema(brand_schema).csv(raw_data_path)

# add metadata columns
df = df.withColumn("_source_file", F.col("_metadata.file_path")) \
       .withColumn("ingested_at", F.current_timestamp())

display(df.limit(5))     

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_brands")

# COMMAND ----------

category_schema = StructType([
    StructField("category_code", StringType(), False),
    StructField("category_name", StringType(), True)
])

# Load data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/category/*.csv"

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(category_schema).csv(raw_data_path)

# Add metadata columns
df_raw = df_raw.withColumn("_ingested_at", F.current_timestamp()) \
               .withColumn("_source_file", F.col("_metadata.file_path"))


# Write raw data to the Bronze layer (catalog: ecommerce, schema: bronze, table: brz_category)
df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_category")   

# COMMAND ----------

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand_code", StringType(), True),
    StructField("color", StringType(), True),
    StructField("size", StringType(), True),
    StructField("material", StringType(), True),
    StructField("weight_grams", StringType(), True),  #datatype is string due to incoming data contain anamolies
    StructField("length_cm", StringType(), True),     #datatype is string due to incoming data contain anamolies
    StructField("width_cm", FloatType(), True),
    StructField("height_cm", FloatType(), True),
    StructField("rating_count", IntegerType(), True),
    StructField("file_name", StringType(), False),
    StructField("ingest_timestamp", TimestampType(), False)
])

# Load data using the schema defined
raw_data_path = "/Volumes/ecommerce/source_data/raw/products/*.csv"

df = spark.read.option("header", "true").option("delimiter", ",").schema(products_schema).csv(raw_data_path) \
    .withColumn("file_name", F.col("_metadata.file_path")) \
    .withColumn("ingest_timestamp", F.current_timestamp())

# Write raw data to the Bronze layer (catalog: ecommerce, schema: bronze, table: brz_products)
df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_products")    

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True)
])

# Load data using the schema defined
raw_data_path ="/Volumes/ecommerce/source_data/raw/customers/*.csv"

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(customers_schema).csv(raw_data_path) \
    .withColumn("file_name", F.col("_metadata.file_path")) \
    .withColumn("ingest_timestamp", F.current_timestamp())

# Write raw data to the Bronze layer (catalog: ecommerce, schema: bronze, table: brz_customers)
df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_customers")  

# COMMAND ----------


# Define schema for the data file
date_schema = StructType([
    StructField("date", StringType(), True),           # Raw date in string format
    StructField("year", IntegerType(), True),          # Year
    StructField("day_name", StringType(), True),       # Day name (can be mixed case)
    StructField("quarter", IntegerType(), True),       # Quarter
    StructField("week_of_year", IntegerType(), True),  # Week of year (can be negative)
])

# Load data using the schema defined
raw_data_path = f"/Volumes/ecommerce/source_data/raw/date/*.csv" 

df_raw = spark.read.option("header", "true").option("delimiter", ",").schema(date_schema).csv(raw_data_path)

# Add metadata columns
df_raw = df_raw.withColumn("_ingested_at", F.current_timestamp()) \
               .withColumn("_source_file", F.col("_metadata.file_path"))


# Write raw data to the Bronze layer (catalog: ecommerce, schema: bronze, table: brz_calendar) 
df_raw.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_calendar")  

   

             

# COMMAND ----------

from pyspark.sql.functions import col

# --- Define raw paths for each CSV table ---
raw_paths = {
    "brz_products": "/Volumes/ecommerce/source_data/raw/products/*.csv",
    "brz_category": "/Volumes/ecommerce/source_data/raw/category/*.csv",
    "brz_brand": "/Volumes/ecommerce/source_data/raw/brands/*.csv",
    "brz_customers": "/Volumes/ecommerce/source_data/raw/customers/*.csv",
    "brz_calendar": "/Volumes/ecommerce/source_data/raw/date/*.csv"
}

# --- Define schemas for each table (reuse your existing schemas) ---
schemas = {
    "brz_products": products_schema,
    "brz_category": category_schema,
    "brz_brand": brand_schema,
    "brz_customers": customers_schema,
    "brz_calendar": date_schema
}

# --- Define the proposed primary key for each table ---
primary_keys = {
    "brz_products": ["product_id"],
    "brz_category": ["category_code"],
    "brz_brand": ["brand_code"],
    "brz_customers": ["customer_id"],
    "brz_calendar": ["date"]
}

# --- Loop through all tables and check uniqueness ---
for table_name, path in raw_paths.items():
    df = spark.read.option("header", "true").schema(schemas[table_name]).csv(path)
    
    key_cols = primary_keys[table_name]
    key_str = " + ".join(key_cols)  # For composite keys
    
    duplicates = df.groupBy(key_cols).count().filter(col("count") > 1)
    
    print(f"\nChecking table: {table_name} | Key: {key_str}")
    if duplicates.count() == 0:
        print("✅ Key is unique!")
    else:
        print("❌ Duplicates found:")
        duplicates.show(truncate=False)


# COMMAND ----------

raw_data_path = "/Volumes/ecommerce/source_data/raw/products/*.csv"

df = spark.read.option("header", "true").schema(products_schema).csv(raw_data_path)
df.groupBy("product_id").count().filter("count > 1").show()
