# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

catalog_name = 'ecommerce'

# Check current state
print("BEFORE RUNNING YOUR CODE:")
df_before = spark.table(f"{catalog_name}.bronze.brz_brands")
df_before.filter(col("brand_code") == "ACME").show(truncate=False)

# Now run your bronze notebook (or just the brands cell)
# ...

# Check after
print("\nAFTER RUNNING YOUR CODE:")
df_after = spark.table(f"{catalog_name}.bronze.brz_brands")
df_after.filter(col("brand_code") == "ACME").show(truncate=False)

# Compare timestamps
print("\nDid the timestamp change? If YES → it was UPDATED!")


# COMMAND ----------

