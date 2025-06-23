# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "34686a86-eb58-90dc-4dd2-809b89175e53",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

%pip install msgraph-sdk

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip show azure-identity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install --upgrade azure-identity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip show azure-identity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip show cryptography

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install cryptography==41.0.7 --force-reinstall


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from azure.identity import ClientSecretCredential

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import asyncio
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
client_id_secret = 'fuam-spn-client-id'
tenant_id_secret = 'fuam-spn-tenant-id'
client_secret_name = 'fuam-spn-secret'

# Replace with your Azure AD app details
tenant_id = notebookutils.credentials.getSecret(kv_uri, tenant_id_secret)
client_id = notebookutils.credentials.getSecret(kv_uri, client_id_secret)
client_secret = notebookutils.credentials.getSecret(kv_uri, client_secret_name)


# Authenticate using ClientSecretCredential
credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret
)

# Create Graph client
client = GraphServiceClient(credential)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

async def get_user(user_id:str):
    user = await client.users.by_user_id(user_id).get()
    if user:
        print(f"User Display Name: {user.display_name}")
        return user.display_name
    else:
        print("User not found.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Loop through Principal ID's and add user's name

# CELL ********************

connections_df = spark.read.format('delta').load('abfss://cicd_ws@onelake.dfs.fabric.microsoft.com/connections_lh.Lakehouse/Tables/connections')

display(connections_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

principal_id_list = [row['principal_id'] for row in connections_df.select('principal_id').collect()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# make a unique list
unique_list = list(dict.fromkeys(principal_id_list))

unique_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# loop through and get all users:
principal_full_list = []

for user in unique_list:
    if user is None:
        continue
    principal_name = await get_user(user)
    print(f'Principal Name: {principal_name}')

    principal_full_list.append({
        "principal_id": user,
        "principal_name": principal_name 
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# convert to a dataframe
principal_df = spark.createDataFrame(principal_full_list)

display(principal_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write Principals to Table

# CELL ********************

# Write to Delta table
lakehouse_path = 'abfss://cicd_ws@onelake.dfs.fabric.microsoft.com/connections_lh.Lakehouse/Tables'
principal_df.write.format('delta').mode('overwrite').save(f'{lakehouse_path}/principals')

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
