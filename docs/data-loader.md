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
* AWS S3 bucket, if applicable
* If the Neo4j database is running remotely, SSH login permissions are required.
* On the system running the Neo4j database, the ````<Neo4j Home>/bin```` directory must be added to the ````PATH```` environment variable in order to perform the backup operation.

## Dependencies
Run ```pip3 install -r requirements.txt``` to install dependencies. Or run ```pip install -r requirements.txt``` if you are using virtualenv. The dependencies included in ````requirements.txt```` are listed below:

* pyyaml
* neo4j - version 1.7.6
* boto3
* requests

## Inputs
* A Data Loader configuration file
* Neo4j endpoint and credentials
* YAML formatted schema file and properties files
* If loading from an AWS S3 bucket, the S3 folder and bucket name
* The dataset directory or a local temporary folder if loading from an AWS S3 bucket

## Outputs
The Data Loader module loads data into the specified Neo4j database, and log messages to console as well as a log file inside ````tmp/```` folder.

## Data File Format Specifications
* Files must be in TSV format with ````.tsv```` or ````.txt```` extension
* Files must contain a ````type```` column indicates what node type of the record/node
* Any column with a ````parent_node_type.parent_id_field```` formatted heading will be used as parent of current record/node
* Any column with a ````relationship_type$field_name```` formatted heading will be treated as a property on that relationship
* All other columns will be treated as regular properties for the record/node

## Configuration File
All the inputs of Data Loader can be set in a YAML format configuration file by using the fields defined below. Using a configuration file can make your Data Loader command significantly shorter. 

An example configuration file can be found in ````config/data-loader-config.example.yml````

*  ````neo4j:uri````: Address of the target Neo4j endpoint
*  ````neo4j:user````: Username to be used for the Neo4j database
*  ````neo4j:password````: Password to be used for the Neo4j database
*  ````schema````: The file path(s) of the YAML formatted schema file(s)
*  ````prop_file````: The file containing the properties for the specified schema
*  ````cheat_mode````: Disables data validation before loading data
*  ````dry_run````: Runs data validation only, disables loading data
*  ````wipe_db````: Clears all data in the database before loading the data
*  ````no_backup````: Skips the backing up the database before loading the data
*  ````backup_folder````: Location to store database backup
*  ````no_confirmation````: Automatically confirms any confirmation prompts that are displayed during the data loading
*  ````max_violations````: The maximum number of violations (per data file) to be displayed in the console output during data loading
*  ````no_parents````: Does not save parent node IDs in children nodes
*  ````split_transactions````: Splits the database load operations into separate transactions for each file
*  ````s3_bucket````: The name of the S3 bucket containing the data to be loaded
*  ````s3_folder````: The name of the S3 folder containing the data to be loaded
*  ````loading_mode````: The loading mode to be used
*  ````dataset````: The directory containing the data to be loaded, a temporary directory if loading from an S3 bucket
*  ````verbose````: When set as true, print the whole list of permissive values when the value is non-permissive value in logs

## Command Line Arguments
All of command line arguments can be specified in the configuration file. If an argument is specified in both the configuration file and the command line then the command line value will be used.

* **Configuration File**
    * The YAML file containing the configuration details for the Data Loader execution
    * Command : ````<configuration file>````
    * Not Required
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
    * Not required if specified in the configuration file, or in environment variable ````NEO_PASSWORD````
    * Default Value : ````N/A````
* **Schema File(s)**
    * The file path(s) of the YAML formatted schema file(s). Use multiple –schema arguments (one for each schema file), if more than one schema files are needed
    * Command : ````-s/--schema <schema1> -s/--schema <schema2> …````
    * Not required if specified in the configuration file
    * Default Value : ````N/A````
* **Properties File**
    * The file containing additional properties for the specified schema such as type mappings, id field identifications, and plurals mappings.
    * Command : ````--prop-file <properties file>````
    * Not required if specified in the configuration file
    * Example files can be found under ````config/```` folder, such as  ````config/props-bento-ext.yml```` for Bento reference implementation, and ````config/props-ctdc.yml```` for CTDC etc.
    * Default Value : ````N/A````
* **Enable Cheat Mode**
    * Disables data validation before loading data
    * Command : ````-c/--cheat-mode````
    * Not required
    * Default Value : ````false````
* **Enable Dry Run**
    * Runs data validation only, disables loading data
    * Command : ````-d/--dry-run````
    * Not required
    * Default Value : ````false````
* **Wipe Database**
    * Clears all data in the database before loading the data
    * Command : ````--wipe-db````
    * Not required
    * Default Value : ````false````
* **Disable Backup**
    * Skips the backing up the database before loading the data
    * Command : ````--no-backup````
    * Not required
    * Default Value : ````false````
* **Database Backup Folder**
    * The folder where the database backup will be stored.
    * Command : ````--backup-folder````
    * Required unless the backup operation is disabled by the ````--no-backup```` command
    * Default Value : ````N/A````
* **Enable Auto-Confirm**
    * Automatically confirms any confirmation prompts that are displayed during the data loading
    * Command : ````-y/--yes````
    * Not Required
    * Default Value : ````false````
* **Maximum Violations to Display**
    * The maximum number of violations (per data file) to be displayed in the console output during data loading
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
    * The loading mode, valid inputs are ````upsert````, ````new````, ````delete````
    * Command : ````-m/--mode <mode>````
    * Not Required
    * Default Value : ````upsert````
* **Enable No Parent IDs Mode**
    * Does not save parent node IDs in children nodes
    * Command : ````--no-parents````
    * Not Required
    * Default Value : ````false````
* **Enable Split Transactions Mode**
    * Creates a separate database transactions for each file while loading
    * Command : ````--split-transactions````
    * Not Required
    * Default Value : ````false````
* **Dataset Directory**
    * The directory containing the data to be loaded, a temporary directory if loading from an S3 bucket
    * Command : ````--dataset <dir>````
    * Not required if specified in the configuration file
    * Default Value : ````N/A````
* **Enable Verbose Mode**
    * When set as true, print the whole list of permissive values when the value is non-permissive value in logs
    * Command : ````-v/--verbose````
    * Not required
    * Default Value : ````false````

## Usage Example
Below is an example command to run the Model Converter:
````
python3 loader.py config/config.yml -p secret -s tests/data/icdc-model.yml -s tests/data/icdc-model-props.yml --prop-file config/props-icdc.yml --no-backup -–dataset /data/Dataset-20191119
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
* **Disable Backup**
    * Set to ````true```` with ````--no-backup```` argument
* **Dataset Directory**
    * ````/data/Dataset-20191119````
