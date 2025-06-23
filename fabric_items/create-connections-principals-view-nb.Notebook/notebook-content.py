# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2ea67807-ba46-4e16-be19-2d3b14ea25bf",
# META       "default_lakehouse_name": "connections_lh",
# META       "default_lakehouse_workspace_id": "8c2e876f-d533-421d-8775-a5fb8c9d91b4",
# META       "known_lakehouses": [
# META         {
# META           "id": "2ea67807-ba46-4e16-be19-2d3b14ea25bf"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Create View to see Connections and Principals

# CELL ********************

connections_df = spark.sql("""
SELECT *
FROM connections
""")

display(connections_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

principals_df = spark.sql("""
SELECT *
FROM principals
""")

display(principals_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     connection_id,
# MAGIC     connection_name,
# MAGIC     gateway_id,
# MAGIC     connectivity_type,
# MAGIC     connection_path,
# MAGIC     connection_type,
# MAGIC     principals.principal_id,
# MAGIC     principal_type,
# MAGIC     principal_name,
# MAGIC     role
# MAGIC 
# MAGIC FROM connections
# MAGIC LEFT JOIN
# MAGIC principals
# MAGIC ON
# MAGIC connections.principal_id = principals.principal_id

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW dbo.connections_principals_view AS
# MAGIC SELECT
# MAGIC     connection_id,
# MAGIC     connection_name,
# MAGIC     gateway_id,
# MAGIC     connectivity_type,
# MAGIC     connection_path,
# MAGIC     connection_type,
# MAGIC     principals.principal_id,
# MAGIC     principal_type,
# MAGIC     principal_name,
# MAGIC     role
# MAGIC FROM dbo.connections
# MAGIC LEFT JOIN dbo.principals
# MAGIC ON connections.principal_id = principals.principal_id


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
