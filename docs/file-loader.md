---
layout: default
nav_order: 1
title: File Loader
---
# File Loader
This is the user documentation for the File Loader module contained in the ICDC-Dataloader utility.

[![Codacy Badge](https://app.codacy.com/project/badge/Grade/f4d5afb8403642dbab917cb4aa4ef47d)](https://www.codacy.com/gh/CBIIT/icdc-dataloader?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=CBIIT/icdc-dataloader&amp;utm_campaign=Badge_Grade)

## Introduction
The File Loader module processes incoming S3 files and then calls the Data Loader module to load the processed file data into a Neo4j database.

The File Loader can be found in this Github Repository: [ICDC-Dataloader](https://github.com/CBIIT/icdc-dataloader)

## Pre-requisites
* Python 3.6 or newer
* An initialized and running Neo4j database
* An AWS S3 bucket containing the data to be loaded
* AWS Command Line Interface (CLI)

## Dependencies
Run ```pip3 install -r requirements.txt``` to install dependencies. Or run ```pip install -r requirements.txt``` if you are using virtualenv. The dependencies included in ````requirements.txt```` are listed below:

* pyyaml
* neo4j - version 1.7.6
* boto3
* requests

## Inputs
* Neo4j endpoint and credentials
* YAML formatted schema file and properties files
* A File Loader configuration file
* S3 folder name
* S3 bucket name
* SQS name

## Outputs
The File Loader module loads data into the specified Neo4j database but does not produce any outputs.

## Command Line Arguments
* **Amazon Simple Queue Service (SQS) Name**
    * The name of the SQS queue
    * Command : ````-q/--queue <queue Name>````
    * Required
    * Default Value : ````N/A````
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
    * At least one is required
    * Default Value : ````N/A````
* **Properties File**
    * The file containing the properties for the specified schema
    * Command : ````--prop-file <properties file>````
    * Required
    * Default Value : ````N/A````
* **Configuration File**
    * The YAML file containing the configuration details for the Data Loader execution
    * Command : ````<configuration file>````
    * Required
    * Default Value : ````N/A````
* **Enable Dry Run**
    * Runs data validation only, disables loading data
    * Command : ````-d/--dry-run````
    * Not required
    * Default Value : ````N/A````
* **Maximum Violations to Display**
    * The maximum number of violations(per data file) to be displayed in the console output during data loading
    * Command : ````-M/--max-violations <number>````
    * Not Required
    * Default Value : ````10````
* **S3 Bucket Name**
    * The name of the S3 bucket containing the data to be loaded
    * Command : ````-b/--bucket <bucket name>````
    * Required
    * Default Value : ````N/A````
* **S3 Folder Name**
    * The name of the S3 folder containing the data to be loaded
    * Command : ````-f/--s3-folder <folder name>````
    * Required
    * Default Value : ````N/A````

## Usage Example
Below is an example command to run the File Loader:
````
file_loader.py -q example-queue -p secret -s tests/data/icdc-model.yml -s tests/data/icdc-model-props.yml config/config.yml --prop-file config/props-icdc.yml -b s3_bucket -f s3_folder
````

### Example Inputs
* **SQS Name**
    * ````example-queue````
* **Neo4j Password**
    * ````secret````
* **Schema Files**
    * ````tests/data/icdc-model.yml````
    * ````tests/data/icdc-model-props.yml````
* **Configuration File**
    * ````config/config.yml````
* **Properties File**
    * ````config/props-icdc.yml````
* **S3 Folder**
    * ````s3_folder````
* **S3 Bucket**
    * ````s3_bucket````
