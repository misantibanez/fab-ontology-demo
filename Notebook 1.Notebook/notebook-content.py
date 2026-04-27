# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1b7861d8-68c0-4518-be38-b0e3662c4d4b",
# META       "default_lakehouse_name": "OntologyLH",
# META       "default_lakehouse_workspace_id": "7f3d7df9-a6e1-4cc1-89e8-6bed6c28cc80",
# META       "known_lakehouses": [
# META         {
# META           "id": "1b7861d8-68c0-4518-be38-b0e3662c4d4b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
1+1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/FactSales.csv")
# df now is a Spark DataFrame containing CSV data from "Files/FactSales.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace 'YourTableName' with your desired table name
# Example: df.write.mode("overwrite").saveAsTable("FactSalesTable")
df.write.mode("overwrite").saveAsTable("FactSales")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/DimStore.csv")
# df now is a Spark DataFrame containing CSV data from "Files/DimStore.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace 'YourTableName' with your desired table name
# Example: df.write.mode("overwrite").saveAsTable("FactSalesTable")
df.write.mode("overwrite").saveAsTable("DimStore")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Freezer.csv")
# df now is a Spark DataFrame containing CSV data from "Files/Freezer.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace 'YourTableName' with your desired table name
# Example: df.write.mode("overwrite").saveAsTable("FactSalesTable")
df.write.mode("overwrite").saveAsTable("Freezer")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/FreezerTelemetry.csv")
# df now is a Spark DataFrame containing CSV data from "Files/FreezerTelemetry.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace 'YourTableName' with your desired table name
# Example: df.write.mode("overwrite").saveAsTable("FactSalesTable")
df.write.mode("overwrite").saveAsTable("FreezerTelemetry")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
