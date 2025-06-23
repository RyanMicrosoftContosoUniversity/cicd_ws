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
# 
# Note: Currently this needs to be split into two separate notebooks because semantic-link-labs is coupled to azure-identity azure-identity==1.7.1 and msgraph is coupled to azure-identity==1.23.0

# CELL ********************

from sempy_labs import _connections as conn
import pandas as pd
import asyncio
from azure.identity import ClientSecretCredential

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

# kv_uri = 'https://kvfabricprodeus2rh.vault.azure.net/'
# client_id_secret = 'fuam-spn-client-id'
# tenant_id_secret = 'fuam-spn-tenant-id'
# client_secret_name = 'fuam-spn-secret'

# # Replace with your Azure AD app details
# tenant_id = notebookutils.credentials.getSecret(kv_uri, tenant_id_secret)
# client_id = notebookutils.credentials.getSecret(kv_uri, client_id_secret)
# client_secret = notebookutils.credentials.getSecret(kv_uri, client_secret_name)


# # Authenticate using ClientSecretCredential
# credential = ClientSecretCredential(
#     tenant_id=tenant_id,
#     client_id=client_id,
#     client_secret=client_secret
# )

# # Create Graph client
# client = GraphServiceClient(credential)

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

# MARKDOWN ********************

# # Convert to PySpark DataFrame and Write to Lakehouse Table

# CELL ********************

spark_merged_df = spark.createDataFrame(merged_df)

display(spark_merged_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def mount_lakehouse(source_path:str, mount_path:str):
    """
    Mount a storage path to access as if storage is local
    source_path: abfss path
    mount_path: temporary directory where the data is stored
    """
    try:
        mssparkutils.fs.mount(source=source_path, mountPoint=mount_path)
        print("Mount successful")
    except Exception as e:
        print(f"Mount failed: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# mount the lakehouse
lh_path = 'abfss://cicd_ws@onelake.dfs.fabric.microsoft.com/connections_lh.Lakehouse/Tables'
mount_path = '/mnt/connections_lh'

mount_lakehouse(lh_path, mount_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# refactor columns
spark_merged_df.columns

column_mapping = {
    'Connection Id': 'connection_id',
    'Connection Name': 'connection_name',
    'Gateway Id': 'gateway_id',
    'Connectivity Type': 'connectivity_type',
    'Connection Path': 'connection_path',
    'Connection Type': 'connection_type',
    'Privacy Level': 'privacy_level',
    'Credential Type': 'credential_type',
    'Single Sign On Type': 'single_sign_on_type',
    'Connection Encryption': 'connection_encryption',
    'Skip Test Connection': 'skip_test_connection',
    'Connection Role Assignment Id': 'connection_role_assignment_id',
    'Principal Id': 'principal_id',
    'Principal Type': 'principal_type',
    'Role': 'role'
}


# Rename columns
for old_name, new_name in column_mapping.items():
    spark_merged_df = spark_merged_df.withColumnRenamed(old_name, new_name)

# Write to Delta table
lakehouse_path = 'abfss://cicd_ws@onelake.dfs.fabric.microsoft.com/connections_lh.Lakehouse/Tables'
spark_merged_df.write.format('delta').mode('overwrite').save(f'{lakehouse_path}/connections_test')



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
