# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # Create Deployment Pipeline Notebook
# This notebook will be used to:
# -   Create Deployment Pipeline via API: https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/create-deployment-pipeline?tabs=HTTP
# -   Assign Workspaces to a Stage via API: https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/assign-workspace-to-stage?tabs=HTTP
# -   Add AD Group to Deployment Pipeline via API: https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/add-deployment-pipeline-role-assignment?tabs=HTTP#add-a-group-role-assignment-to-a-deployment-pipeline-example

# CELL ********************

%pip install semantic-link-labs

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import os
import notebookutils
import pandas as pd
from sempy_labs import _deployment_pipelines as dp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# list deployment pipelines

dp.list_deployment_pipelines()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dp.list_deployment_pipeline_stages('devops-example-deployment-pipelines')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_api_token_via_akv(kv_uri:str, client_id_secret:str, tenant_id_secret:str, client_secret_name:str)->str:
    """
    Function to retrieve an api token used to authenticate with Microsoft Fabric APIs

    kv_uri:str: The uri of the azure key vault
    client_id_secret:str: The name of the key used to store the value for the client id in the akv
    tenant_id_secret:str: The name of the key used to store the value for the tenant id in the akv
    client_secret_name:str: The name of the key used to store the value for the client secret in the akv

    """
    client_id = notebookutils.credentials.getSecret(kv_uri, client_id_secret)
    tenant_id = notebookutils.credentials.getSecret(kv_uri, tenant_id_secret)
    client_secret = notebookutils.credentials.getSecret(kv_uri, client_secret_name)

    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    scope = 'https://analysis.windows.net/powerbi/api/.default'
    token = credential.get_token(scope).token

    return token


def create_deployment_pipeline(json_payload:dict, api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/create-deployment-pipeline?tabs=HTTP
    POST https://api.fabric.microsoft.com/v1/deploymentPipelines

    json_payload:dict: The json payload to create the deployment pipeline {
  "displayName": "My Deployment Pipeline Name",
  "description": "My deployment pipeline description",
  "stages": [
    {
      "displayName": "Development",
      "description": "Development stage description",
      "isPublic": false
    },
    {
      "displayName": "Test",
      "description": "Test stage description",
      "isPublic": false
    },
    {
      "displayName": "Production",
      "description": "Production stage description",
      "isPublic": true
    }
  ]
}
    api_token:str: The API token used to authenticate with the API
    """
    # validate payload is valid
    tup_resp = validate_deployment_pipeline_schema(json_payload)

    url = f'https://api.fabric.microsoft.com/v1/deploymentPipelines'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.post(url, headers=headers, json=json_payload)

    return response

def add_group_to_deployment_pipeline(deployment_pipeline_id:str, json_payload:dict, api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/add-deployment-pipeline-role-assignment?tabs=HTTP#add-a-group-role-assignment-to-a-deployment-pipeline-example
    POST https://api.fabric.microsoft.com/v1/deploymentPipelines/{deploymentPipelineId}/roleAssignments

    deployment_pipeline_id:str: The UUID of the Deployment Pipeline
    payload_type:str: One of user or group
    api_token:str: The API token used to authenticate with the API
    """
    # check payload_type for processing

    tup_resp = validate_group_role_assignment_schema(json_payload)

    if tup_resp[0]:
        print('Schema Successful')
    else:
        error_message=tup_resp[1]
        print(f'Schema Validation Failed: {error_message}')
        return {"error": error_message, "status_code": 400}

    url = f'https://api.fabric.microsoft.com/v1/deploymentPipelines/{deployment_pipeline_id}/roleAssignments'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }    

    response = requests.post(url, headers=headers, json=json_payload)

    return response

def assign_workspace_to_stage(deployment_pipeline_id:str, stage_id:str, workspace_id:str, api_token:str):
    """
    https://learn.microsoft.com/en-us/rest/api/fabric/core/deployment-pipelines/assign-workspace-to-stage?tabs=HTTP
    POST https://api.fabric.microsoft.com/v1/deploymentPipelines/{deploymentPipelineId}/stages/{stageId}/assignWorkspace

    deployment_pipeline_id:str UUID of the deployment pipeline
    stage_id: uuid of the stage
    workspace_id:str: The uuid of the workspace to be assigned
    api_token:str: The API token used to authenticate with the API



    The payload for assigning the workspace

    {
        "workspaceId": "4de5bcc4-2c88-4efe-b827-4ee7b289b496"
    }
    """
    url = f'POST https://api.fabric.microsoft.com/v1/deploymentPipelines/{deployment_pipeline_id}/stages/{stage_id}/assignWorkspace'

    headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
    }

    json_payload = {
        "workspaceId": f"{workspace_id}"
    }

    response = requests.post(url, headers=headers, json=json_payload)

    return response

def validate_group_role_assignment_schema(data):
    """
    Validates if data follows the schema:
    {
      "principal": {
        "id": str,
        "type": str  # should be one of the valid types
      },
      "role": str  # should be one of the valid roles
    }
    
    Args:
        data: Dictionary or JSON object to validate
        
    Returns:
        tuple: (is_valid, error_message)
    """
    # Check if data is a dictionary
    if not isinstance(data, dict):
        return False, "Data must be a dictionary"
    
    # Check if principal exists and is a dictionary
    if "principal" not in data:
        return False, "Missing 'principal' field"
    if not isinstance(data["principal"], dict):
        return False, "'principal' must be a dictionary"
    
    # Check if principal has id and type
    principal = data["principal"]
    if "id" not in principal:
        return False, "Missing 'principal.id' field"
    if not isinstance(principal["id"], str):
        return False, "'principal.id' must be a string"
    
    if "type" not in principal:
        return False, "Missing 'principal.type' field"
    if not isinstance(principal["type"], str):
        return False, "'principal.type' must be a string"
    
    # Validate principal type (add more valid types if needed)
    valid_principal_types = ["User", "Group", "ServicePrincipal"]
    if principal["type"] not in valid_principal_types:
        return False, f"'principal.type' must be one of: {', '.join(valid_principal_types)}"
    
    # Check if role exists and is a string
    if "role" not in data:
        return False, "Missing 'role' field"
    if not isinstance(data["role"], str):
        return False, "'role' must be a string"
    
    # Validate role (add more valid roles if needed)
    valid_roles = ["Admin", "User", "Reader"]
    if data["role"] not in valid_roles:
        return False, f"'role' must be one of: {', '.join(valid_roles)}"
    
    # All checks passed
    return True, "Schema is valid"

def validate_deployment_pipeline_schema(data):
    """
    Validates if data follows the deployment pipeline creation schema:
    {
      "displayName": str,
      "description": str,
      "stages": [
        {
          "displayName": str,
          "description": str,
          "isPublic": bool
        },
        ...
      ]
    }
    
    Args:
        data: Dictionary or JSON object to validate
        
    Returns:
        tuple: (is_valid, error_message)
    """
    # Check if data is a dictionary
    if not isinstance(data, dict):
        return False, "Data must be a dictionary"
    
    # Check required top-level fields
    required_fields = ["displayName", "description", "stages"]
    for field in required_fields:
        if field not in data:
            return False, f"Missing required field '{field}'"
    
    # Check displayName and description are strings
    if not isinstance(data["displayName"], str):
        return False, "'displayName' must be a string"
    if not isinstance(data["description"], str):
        return False, "'description' must be a string"
    
    # Check stages is a list
    if not isinstance(data["stages"], list):
        return False, "'stages' must be a list"
    
    # Check if stages is empty
    if len(data["stages"]) == 0:
        return False, "'stages' cannot be empty"
    
    # Validate each stage
    for i, stage in enumerate(data["stages"]):
        # Check if stage is a dictionary
        if not isinstance(stage, dict):
            return False, f"Stage at index {i} must be a dictionary"
        
        # Check required stage fields
        stage_required_fields = ["displayName", "description", "isPublic"]
        for field in stage_required_fields:
            if field not in stage:
                return False, f"Missing required field '{field}' in stage at index {i}"
        
        # Check stage field types
        if not isinstance(stage["displayName"], str):
            return False, f"'displayName' must be a string in stage at index {i}"
        if not isinstance(stage["description"], str):
            return False, f"'description' must be a string in stage at index {i}"
        if not isinstance(stage["isPublic"], bool):
            return False, f"'isPublic' must be a boolean in stage at index {i}"
    
    # Check for duplicate stage names (optional validation)
    stage_names = [stage["displayName"] for stage in data["stages"]]
    if len(stage_names) != len(set(stage_names)):
        return False, "Stage displayNames must be unique"
    
    # All checks passed
    return True, "Schema is valid"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

json_example = {
  "displayName": "Deployment Pipeline Test",
  "description": "My deployment pipeline description",
  "stages": [
    {
      "displayName": "Development",
      "description": "Development stage description",
      "isPublic": True
    },
    {
      "displayName": "Test",
      "description": "Test stage description",
      "isPublic": False
    },
    {
      "displayName": "Production",
      "description": "Production stage description",
      "isPublic": True
    }
  ]
}



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

token = get_api_token_via_akv(kv_uri, client_id_secret, tenant_id_secret, client_secret_name)

response = create_deployment_pipeline(json_example, token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add AD Group to Deployment Pipeline
principal_id = 'c65f2c4b-7fe6-4274-b8c9-2bcfbb6d784b'

add_user_payload = add_user_payload = {
  "principal": {
    "id": "c65f2c4b-7fe6-4274-b8c9-2bcfbb6d784b",
    "type": "User"
  },
  "role": "Admin"
}


group_add_response = deployment_pipeline_id = '36dbc745-0ba4-44d3-9036-0bbddc6733a8'
add_group_to_deployment_pipeline(deployment_pipeline_id, add_user_payload, token)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

group_add_response

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
