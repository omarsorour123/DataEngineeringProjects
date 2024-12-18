# ETL Pipeline for CSV Data Transformation and Loading into PostgreSQL

## Overview

This project demonstrates an ETL (Extract, Transform, Load) pipeline built to process CSV files. The pipeline extracts data from CSV files, performs various transformations using `pandas`, and then loads the cleaned and transformed data into a PostgreSQL database. Additionally, the pipeline includes a cloud integration feature to upload transformed data to Azure Blob Storage.

## Features

- **Extract**: Reads CSV files from a specified location.
- **Transform**: 
  - Fixes and changes column names.
  - Corrects and transforms data types.
  - Aggregates data as needed.
- **Load**: 
  - Loads the transformed data into a PostgreSQL database.
  - **Cloud Integration**: Transformed data can be saved as a CSV file and uploaded to Azure Blob Storage for secure and scalable storage.

## Requirements

This project uses **Poetry** for dependency management. Poetry simplifies dependency handling and package management for Python projects.

### To install dependencies, run:

```bash
poetry install
```
## Cloud Integration: Sending to Azure Blob Storage
### The pipeline supports uploading transformed data to Azure Blob Storage. This feature leverages Azure's secure, scalable, and cost-effective storage capabilities to store data in the cloud. The following configuration is required:
- Azure Storage Account URL
- Container Name
- Blob SAS Token