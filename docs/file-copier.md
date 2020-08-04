---
layout: default
nav_order: 1
title: File Loader
---
# File Copier
This is the user documentation for the File Copier module contained in the ICDC-Dataloader utility.

[![Codacy Badge](https://app.codacy.com/project/badge/Grade/f4d5afb8403642dbab917cb4aa4ef47d)](https://www.codacy.com/gh/CBIIT/icdc-dataloader?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=CBIIT/icdc-dataloader&amp;utm_campaign=Badge_Grade)

## Introduction
The File Copier copies files from a source URL to a designated AWS S3 Bucket. It has 3 modes of operation:

* **Master mode** - The File Copier will read all of the file information from the pre-manifest, push jobs onto the job queue, and then listen to the results queue for the loading results.
* **Slave mode** - The File Copier will grab jobs from the job queue, perform the copy job, and then push the job result to the result queue.
* **Solo mode** - The File Copier will read all of the file information from the pre-manifest and then copy all of the files to the destination S3 bucket.

The File Copier can be found in this Github Repository: [ICDC-Dataloader](https://github.com/CBIIT/icdc-dataloader)

## Pre-requisites
* Python 3.6 or newer
* An initialized destination AWS S3 bucket
* AWS Command Line Interface (CLI)
* Initialized Job and Result SQS FIFO Queues (````master```` and ````slave```` modes only)
* An adapter to process information read from pre-manifest

## Dependencies
Run ```pip3 install -r requirements.txt``` to install dependencies. Or run ```pip install -r requirements.txt``` if you are using virtualenv. The dependencies included in ````requirements.txt```` are listed below:

*   pyyaml
*   neo4j - version 1.7.6
*   boto3
*   requests

## Inputs
*   The location of the files to be copied
*   The name of the destination S3 bucket
*   A File Copier config file
*   The module name and class name of the adapter for the data being transferred
*   A pre-manifest file (in TSV format)
*   The names of the job and result SQS FIFO queues (````master```` and ````slave```` modes only)

## Outputs
The File Copier module will produce following outputs

*    Copies files into the specified S3 bucket
*    Generates two manifest files in the same place as pre-manifest file, one for DCF/IndexD, the other for Neo4j database.
*    Log messages to console as well as a log file inside ````tmp/```` folder.

## Configuration file
All the inputs of File Copier can be set in a YAML format configuration file by using the fields defined below. 

An example configuration file can be found in ````config/file-copier-config.example.yml````

*  ````domain````: The domain name of the project.
*  ````adapter_module````: The module name of the adapter that will be used by the File Copier during operation.
*  ````adapter_class````: The class name of the adapter that will be used by the File Copier during operation.
*  ````adapter_params````: An object which contains parameters for the adapter's constructor. Only available in configuration file, not as CLI arguments.
*  ````bucket````: The files in the source S3 Bucket will be copied into this destination S3 Bucket.
*  ````prefix````: Prefix for files being copied into the destination bucket.
*  ````first````: The first line to load. Lines are indexed starting with 1 and header lines are not counted.
*  ````count````: The number of files to be copy, a value of ````-1```` will copy all files.
*  ````retry````: The number of times that the File Copier will retry the copy operation.
*  ````mode````: The mode that the File Copier will run, the only valid inputs are ````master````, ````slave````, and ````solo````.
*  ````job_queue````: The File Copier will send jobs to the job SQS queue with the name specified by this input.
*  ````result_queue````: The results of the File Copier jobs will be sent to the result SQS queue with the name specified by this input.
*  ````pre_manifest````: The TSV file containing the details of the files to be copied.
*  ````overwrite````: Overwrites files even if they already exist in the destination and are the same size.
*  ````dryrun````: Runs checks on original files but does not perform the copy operation.
*  ````verify_md5````: Verify that the size and MD5 hash of the original file and the generated copy are the same.

## Command Line Arguments
* **Configuration File**
    * The YAML file containing the configuration details for the File Copier execution
    * Command : ````<configuration file>````
    * Required
    * Default Value: ````N/A````
* **Destination S3 Bucket Name**
    * The files in the source S3 Bucket will be copied into this destination S3 Bucket.
    * Command: ````-b/--bucket <S3 bucket name>````
    * Required
    * Default Value: ````N/A````
* **Project Domain Name**
    * The domain name of the project.
    * Command: ````--domain <domain name>````
    * Required when not in ````slave```` mode
    * Default Value: ````N/A````
* **File Prefix**
    * Prefix for files being copied into the destination bucket.
    * Command: ````-p/--prefix <prefix>````
    * Required when not in ````slave```` mode
    * Default Value: ````N/A````
* **First Line**
    * The first line to load. Lines are indexed starting with 1 and header lines are not counted.
    * Command: ````-f/--first <index of first line>````
    * Not Required
    * Default Value: ````1````
* **Number of Files to Copy**
    * The number of files to be copy, a value of ````-1```` will copy all files.
    * Command: ````-c/--count <number of files to copy>````
    * Not Required
    * Default Value: ````-1````
* **Enable Overwrite**
    * Overwrites files even if they already exist in the destination and are the same size.
    * Command: ````--overwrite````
    * Not Required
    * Default Value: ````false````
* **Enable Dry Run**
    * Runs checks on original files but does not perform the copy operation.
    * Command: ````-d/--dryrun````
    * Not Required
    * Default Value: ````false````
* **Verify Original MD5**
    * Verify that the size and MD5 hash of the original file and the generated copy are the same.
    * Command: ````-v/--verify-md5````
    * Not Required
    * Default Value: ````false````
* **Number of Times to Retry**
    * The number of times that the File Copier will retry the copy operation.
    * Command: ````-r/--retry````
    * Not Required
    * Default Value: ````3````
* **Running Mode**
    * The mode that the File Copier will run, the only valid inputs are ````master````, ````slave````, and ````solo````.
    * Command: ````-m/--mode````
    * Required
    * Default Value: ````N/A````
* **Job SQS Queue Name**
    * The File Copier will send jobs to the job SQS queue with the name specified by this input.
    * Command: ````--job-queue````
    * Required when not in ````solo```` mode
    * Default Value: ````N/A````
* **Result SQS Queue Name**
    * The results of the File Copier jobs will be sent to the result SQS queue with the name specified by this input.
    * Command: ````--result-queue````
    * Required when not in ````solo```` mode
    * Default Value: ````N/A````
* **Pre-manifest File**
    * The TSV file containing the details of the files to be copied.
    * Command: ````--pre-manifest````
    * Required when not in ````slave```` mode
    * Default Value: ````N/A````
* **Adapter Module Name**
    * The module name of the adapter that will be used by the File Copier during operation.
    * Command: ````--adapter-module````
    * Required when not in ````slave```` mode
    * Default Value: ````N/A````
* **Adapter Class Name**
    * The class name of the adapter that will be used by the File Copier during operation.
    * Command: ````--adapter-class````
    * Required when not in ````slave```` mode
    * Default Value: ````N/A````


## Usage Examples
Below are example commands to run the File Copier.

### Solo Mode
````
file_copier.py -b example_bucket --domain example_domain -p example_prefix -m solo --pre-manifest example_file.tsv --adapter-module example_module --adapter-class example_class example_config.yml 
````

### Master Mode
````
file_copier.py -b example_bucket --domain example_domain -p example_prefix -m master --job-queue example_job_queue --result-queue example_result_queue --pre-manifest example_file.tsv --adapter-module example_module --adapter-class example_class example_config.yml 
````

### Solo Mode
````
file_copier.py -b example_bucket -m slave --job-queue example_job_queue --result-queue example_result_queue example_config.yml 
````
### Example Inputs
* **Destination S3 Bucket Name**
    * ````example_bucket````
* **Project Domain Name**
    * ````example_domain````
* **File Prefix**
    * ````example_prefix````
* **Running Mode**
    * ````solo````
    * ````master````
    * ````slave````
* **Job SQS Queue Name**
    * ````example_job_queue````
* **Result SQS Queue Name**
    * ````example_result_queue````
* **Pre-manifest File**
    * ````example_file.tsv````
* **Adapter Module Name**
    * ````example_module````
* **Adapter Class Name**
    * ````example_class````
* **Configuration File**
    * ````example_config.yml````
