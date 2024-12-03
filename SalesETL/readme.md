# ETL Pipeline for CSV Data Transformation and Loading into PostgreSQL

## Overview

This project demonstrates an ETL (Extract, Transform, Load) pipeline built to process CSV files. The pipeline extracts data from CSV files, performs various transformations using `pandas`, and then loads the cleaned and transformed data into a PostgreSQL database.

## Features

- **Extract**: Reads CSV files from a specified location.
- **Transform**: 
  - Fixes and changes column names.
  - Corrects and transforms data types.
  - Aggregates data as needed.
- **Load**: Loads the transformed data into a PostgreSQL database.
  
## Requirements

This project uses **Poetry** for dependency management. Poetry simplifies dependency handling and package management for Python projects.

### To install dependencies, run:

```bash
poetry install
