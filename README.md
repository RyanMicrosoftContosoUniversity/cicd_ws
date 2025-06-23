# cicd_ws

This repository contains resources and notebooks for managing CI/CD workflows, environment configurations, and deployment pipelines in Microsoft Fabric environments. The structure and main components are as follows:

## Repository Structure

- **fabric_items/**: Items visible in Microsoft Fabric
  

## Key Features

- **Environment Management**: YAML files for Python and Spark environment setup.
- **CI/CD Automation**: Notebooks for validating connections and automating deployment pipelines.
- **Integration**: Uses Microsoft Graph SDK and semantic-link-labs for advanced automation.
- **Lakehouse Support**: Metadata files for managing lakehouse connections.

## Usage

1. Review and update the environment YAML files as needed for your dependencies.
2. Use the provided notebooks to:
   - Validate and audit connections.
   - Interact with Microsoft Graph.
   - Create and manage deployment pipelines.
3. Adjust Spark compute settings in the YAML files for your workload requirements.

## Prerequisites

- Microsoft Fabric workspace access
- Python 3.11+
- Required Python packages (see environment.yml files)
- Appropriate Azure and Microsoft Graph permissions

---
For more details, see the comments and documentation within each notebook and configuration file.
