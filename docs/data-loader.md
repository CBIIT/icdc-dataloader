---
layout: default
nav_order: 1
title: Data Loader
---
# Data Loader
This is the user documentation for the Data Loader module contained in the ICDC-Dataloader utility.

[![Codacy Badge](https://app.codacy.com/project/badge/Grade/f4d5afb8403642dbab917cb4aa4ef47d)](https://www.codacy.com/gh/CBIIT/icdc-dataloader?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=CBIIT/icdc-dataloader&amp;utm_campaign=Badge_Grade)
## Introduction
The Data Loader module is a versatile Python application used to load data into a Neo4j database. The application is capable of loading data from either a system directory or from an Amazon Web Services (AWS) S3 Bucket

The Data Loader can be found in this Github Repository: [ICDC-Dataloader](https://github.com/CBIIT/icdc-dataloader)
## Pre-requisites
* Python 3.6 or newer
* An initialized and running Neo4j database
* If loading from an AWS S3 bucket, AWS Command Line Interface (CLI)
## Dependencies
Run ```pip3 install -r requirements.txt``` to install dependencies. Or run ```pip install -r requirements.txt``` if you are using virtualenv. The dependencies included in ````requirements.txt```` are listed below:
* pyyaml
* neo4j - version 1.7.6
* boto3
* requests
## Inputs
* Neo4j endpoint and credentials
* YAML formatted schema file and properties files
* A Data Loader configuration file
* If loading from an AWS S3 bucket, the S3 folder and bucket name
* The dataset directory or a local temporary folder if loading from an AWS S3 bucket
## Outputs
The Data Loader module loads data into the specified Neo4j database but does not produce any outputs.
## Command Line Arguments
* **Neo4j URI**
    * Address of the target Neo4j endpoint
    * Command : ````-i/--uri <URI>````
    * Not required
    * Default Value : ````bolt://localhost:7687````
* **Neo4j Username**
    * Username to be used for the Neo4j database
    * Command : ````-u/--user <username>````
    * Not required
    * Default Value : ````neo4j````
* **Neo4j Password**
    * Password to be used for the Neo4j database
    * Command : ````-p/--password <password>````
    * Not required if specified in the configuration file
    * Default Value : ````N/A````
* **Schema File(s)**
    * The file path(s) of the YAML formatted schema file(s). Use multiple –schema arguments (one for each schema file), if more than one schema files are needed
    * Command : ````-s/--schema <schema1> -s/--schema <schema2> …````
    * Not required if specified in the configuration file
    * Default Value : ````N/A````
* **Properties File**
    * The file containing the properties for the specified schema
    * Command : ````--prop-file <properties file>````
    * Not required if specified in the configuration file
    * Default Value : ````N/A````
* **Configuration File**
    * The YAML file containing the configuration details for the Data Loader execution
    * Command : ````<configuration file>````
    * Required
    * Default Value : ````N/A````
* **Enable Cheat Mode**
    * Disables data validation before loading data
    * Command : ````-c/--cheat-mode````
    * Not required
    * Default Value : ````N/A````
* **Enable Dry Run**
    * Runs data validation only, disables loading data
    * Command : ````-d/--dry-run````
    * Not required
    * Default Value : ````N/A````
* **Wipe Database**
    * Clears all data in the database before loading the data
    * Command : ````--wipe-db````
    * Not required
    * Default Value : ````N/A````
* **Disable Backup**
    * Skips the backing up the database before loading the data
    * Command : ````--no-backup````
    * Not required
    * Default Value : ````N/A````
* **Enable Auto-Confirm**
    * Automatically confirms any confirmation prompts that are displayed during the data loading
    * Command : ````-y/--yes````
    * Not Required
    * Default Value : ````N/A````
* **Maximum Violations to Display**
    * The maximum number of violations(per data file) to be displayed in the console output during data loading
    * Command : ````-M/--max-violations <number>````
    * Not Required
    * Default Value : ````10````
* **S3 Bucket Name**
    * The name of the S3 bucket containing the data to be loaded
    * Command : ````-b/--bucket <bucket name>````
    * Not required if data is not being loaded from an S3 bucket
    * Default Value : ````N/A````
* **S3 Folder Name**
    * The name of the S3 folder containing the data to be loaded
    * Command : ````-f/--s3-folder <folder name>````
    * Not required if data is not being loaded from an S3 bucket
    * Default Value : ````N/A````
* **Mode Selection**
    * The loading mode, valid inputs are ````UPSERT_MODE````, ````NEW_MODE````, ````DELETE_MODE````
    * Command : ````-m/--mode <mode>````
    * Not Required
    * Default Value : ````UPSERT_MODE````
* **Enable No Parent IDs Mode**
    * Does not save parent node IDs in children nodes
    * Command : ````--no-parents````
    * Not Required
    * Default Value : ````N/A````
* **Dataset Directory**
    * The directory containing the data to be loaded, a temporary directory if loading from an S3 bucket
    * Command : ````--dataset <dir>````
    * Not required if specified in the configuration file
    * Default Value : ````N/A````
## Usage Example
Below is an example command to run the Model Converter:
````
python3 loader.py -p secret -s tests/data/icdc-model.yml -s tests/data/icdc-model-props.yml config/config.yml --prop-file config/props-icdc.yml -–dataset /data/Dataset-20191119
````
### Example Inputs
* **Neo4j Password**
    * ````secret````
* **Schema Files**
    * ````tests/data/icdc-model.yml````
    * ````tests/data/icdc-model-props.yml````
* **Configuration File**
    * ````config/config.yml````
* **Properties File**
    * ````config/props-icdc.yml````
* **Dataset Directory**
    * ````/data/Dataset-20191119````
