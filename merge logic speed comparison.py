# Databricks notebook source
# ============================================================
# CELL 1: Imports
# ============================================================
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, current_timestamp, lit, when
from delta.tables import DeltaTable
import time

catalog_name = 'ecommerce'

# COMMAND ----------

# ============================================================
# CELL 2: TEST 1 - Full Overwrite (OLD METHOD - Slow)
# ============================================================
print("=" * 60)
print("TEST 1: Full Overwrite Method (Baseline)")
print("=" * 60)

# Read source data
brand_schema = StructType([
    StructField("brand_code", StringType(), False),
    StructField("brand_name", StringType(), True),
    StructField("category_code", StringType(), True),
])

raw_data_path = "/Volumes/ecommerce/source_data/raw/brands/*.csv"
df_brands = spark.read.option('header', "true").option("delimiter", ",").schema(brand_schema).csv(raw_data_path)
df_brands = df_brands.withColumn("_source_file", col("_metadata.file_path")) \
                     .withColumn("ingested_at", current_timestamp())

# Time the full overwrite
start_time = time.time()

df_brands.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_brands_test_overwrite")

overwrite_time = time.time() - start_time

print(f"\n✅ Full Overwrite completed in: {overwrite_time:.2f} seconds")
print(f"   Rows written: {df_brands.count()}")

# COMMAND ----------

# ============================================================
# CELL 3: TEST 2 - Incremental MERGE (NEW METHOD - Fast)
# ============================================================
print("\n" + "=" * 60)
print("TEST 2: Incremental MERGE Method (Optimized)")
print("=" * 60)

# First, create the table if it doesn't exist
table_name = f"{catalog_name}.bronze.brz_brands_test_merge"
print(f"   Table name: {table_name}")  # Debug - check the name

# Check if table exists using try-except
try:
    existing_df = spark.table(table_name)
    table_exists = True
    print(f"   ✅ Table already exists with {existing_df.count()} rows")
except Exception as e:
    table_exists = False
    print(f"   ℹ️  Table does not exist yet - will create it")

if not table_exists:
    # Initial load
    print("   Creating initial table...")
    df_brands.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)
    print("   ✅ Initial table created")

# Now simulate an incremental load (re-reading same data)
print("\n   Reading new data for MERGE test...")
df_brands_new = spark.read.option('header', "true").option("delimiter", ",").schema(brand_schema).csv(raw_data_path)
df_brands_new = df_brands_new.withColumn("_source_file", col("_metadata.file_path")) \
                             .withColumn("ingested_at", current_timestamp())

# Time the MERGE
print("   Starting MERGE operation...")
start_time = time.time()

delta_table = DeltaTable.forName(spark, table_name)
delta_table.alias("target").merge(
    df_brands_new.alias("source"),
    "target.brand_code = source.brand_code"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

merge_time = time.time() - start_time

print(f"\n✅ Incremental MERGE completed in: {merge_time:.2f} seconds")
print(f"   Rows processed: {df_brands_new.count()}")

# COMMAND ----------

# ============================================================
# CELL 4: RESULTS COMPARISON
# ============================================================
print("\n" + "=" * 60)
print("PERFORMANCE COMPARISON")
print("=" * 60)

print(f"\n📊 Results:")
print(f"   Full Overwrite:     {overwrite_time:.2f} seconds")
print(f"   Incremental MERGE:  {merge_time:.2f} seconds")

if overwrite_time > merge_time:
    speedup = ((overwrite_time - merge_time) / overwrite_time) * 100
    time_saved = overwrite_time - merge_time
    print(f"\n✅ MERGE is {speedup:.1f}% faster!")
    print(f"   Time saved: {time_saved:.2f} seconds")
else:
    slowdown = ((merge_time - overwrite_time) / overwrite_time) * 100
    print(f"\n⚠️  MERGE is {slowdown:.1f}% slower (expected for small datasets)")

print("\n💡 Note: MERGE benefits increase with larger datasets and when most")
print("   records already exist (fewer updates needed vs full rewrites)")



# COMMAND ----------

