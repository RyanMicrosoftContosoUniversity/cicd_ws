# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%configure  
# MAGIC 
# MAGIC { 
# MAGIC     "defaultLakehouse": { 
# MAGIC         "name": {
# MAGIC                   "parameterName": "default_lakehouse_name",
# MAGIC                   "defaultValue": "bronze_lh"
# MAGIC         },
# MAGIC         "id": {
# MAGIC                   "parameterName": "default_lakehouse_id",
# MAGIC                   "defaultValue": "fc2a7f71-e48c-4fc1-b6a6-90672c99a015"
# MAGIC         },
# MAGIC         "workspaceId": {
# MAGIC                   "parameterName": "default_workspace_id",
# MAGIC                   "defaultValue": "d616705d-6d64-4f83-a586-051cc5ec8979"
# MAGIC         }
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = spark.sql("SHOW TABLES").select("tableName").rdd.flatMap(lambda x: x).collect()
for table in tables:
    print(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
