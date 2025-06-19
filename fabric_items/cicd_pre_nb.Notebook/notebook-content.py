# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "fdda9b38-f6f8-ad3f-421b-a9ddf07120a8",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # CICD Pre-Notebook
# The objective of this notebook is to check all connections that exist and validate there are no user mapping issues between them (ie duplicate connections)
# 
# The notebook is expected to be run by an admin or a principal with full access to all connections created

# CELL ********************

from sempy_labs import _connections as conn
import pandas as pd
from msgraph import GraphServiceClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential
import asyncio

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

# list connections
connections = conn.list_connections()

cxn_list = connections['Connection Id'].tolist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize an empty list to collect DataFrames
all_role_assignments = []

# Iterate through the list of connections
for cxn in cxn_list:
    connections_role_assignments = conn.list_connection_role_assignments(cxn)
    
    # Add the 'Connection ID' column to the DataFrame
    connections_role_assignments['Connection Id'] = cxn
    
    # Append to the list
    all_role_assignments.append(connections_role_assignments)

# Concatenate all DataFrames into one
combined_role_assignments = pd.concat(all_role_assignments, ignore_index=True)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

combined_role_assignments

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

connections

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

combined_role_assignments

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


merged_df = pd.merge(
    connections,
    combined_role_assignments,
    on='Connection Id',
    how='left'
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merged_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get a list of all the principal Ids 
principal_id_list = merged_df['Principal Id'].tolist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unique_list = list(dict.fromkeys(principal_id_list))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unique_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

async def get_user():
    user = await client.users.by_user_id('c65f2c4b-7fe6-4274-b8c9-2bcfbb6d784b').get()
    if user:
        print(f"User Display Name: {user.display_name}")
    else:
        print("User not found.")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = None

if loop and loop.is_running():
    # If there's a running loop, use `create_task`
    task = loop.create_task(get_user())
else:
    # Otherwise, run normally
    asyncio.run(get_user())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

await get_user()

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
