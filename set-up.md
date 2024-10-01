# Current Architecture:

## Azure Blob Storage: Storing raw data
## Azure SQL Database: Storing processed data
## Databricks with PySpark: For data transformation and processing
- Mount Blob Storage in Databricks
- Transform data and write to Azure SQL DB


## Azure Data Factory Integration:

Create a Linked Service in ADF for Databricks workspace
Create a pipeline in ADF with a Databricks Notebook Activity
Set up a trigger in ADF to run this pipeline on your desired schedule
