---
layout: default
nav_order: 1
title: Stream File Validator
---
# Stream File Validator
This is the user documentation for the Stream File Validator module contained in the ICDC-Dataloader utility.

## Introduction
The Stream File Validator module validates the uploaded file in the S3 bucket based on the manifest file from either the S3 bucket or local and then calls the Data Loader module to upload the file validation result to the designated S3 bucket.

The Stream File Validator can be found in this Github Repository: [ICDC-Dataloader](https://github.com/CBIIT/icdc-dataloader)

## Pre-requisites
* Python 3.9 or newer
* An AWS S3 bucket containing the data to be validated
* AWS Command Line Interface (CLI)

## Dependencies
Run ```pip3 install -r requirements.txt``` to install dependencies. Or run ```pip install -r requirements.txt``` if you are using virtualenv. The dependencies included in ````requirements.txt```` are listed below:

* pyyaml
* neo4j - version 4.4.*
* boto3
* requests
* elasticsearch - version 7.13.*
* tqdm
* requests_aws4auth
* pandas
* xlsxwriter
* prefect

## Inputs
* A YAML formatted Stream File Validator configuration file
* The column name for the file name column in the manifest file
* The column name for the file URL column in the manifest file
* The column name for the file size column in the manifest file
* The column name for the file MD5 column in the manifest file
* The validation S3 bucket, if the file url column is not available
* The validation S3 subfolder prefix, if the file url column is not available
* The designated S3 file upload bucket and subfolder prefix in S3 URL format if the user decides to upload the file validation result to a specific S3 bucket

## Outputs
The Stream File Validator generates a result TSV file that contains the file validation result.

## Command Line Arguments
* **Manifest File Location**
    * The manifest file's location
    * Command : ````--manifest-file````
    * Not required if specified in the configuration file
    * Default Value : ````N/A````
* **File Name Column**
    * The file name column in the input manifest file
    * Command : ````--file-name-column````
    * Not required if specified in the configuration file
    * Default Value : ````N/A````
* **File URL Column**
    * The file S3 URL column in the input manifest file
    * Command : ````--file-url-column````
    * Not required if the validation S3 bucket and the validation S3 subfolder prefix are provided or not required if specified in the configuration file
    * Default Value : ````N/A````
* **File Size Column**
    * The file size column in the input manifest file
    * Command : ````--file-size-column````
    * Not required if specified in the configuration file
    * Default Value : ````N/A````
* **File MD5 Column**
    * The file MD5 column in the input manifest file
    * Command : ````--file-md5-column````
    * Not required if specified in the configuration file
    * Default Value : ````N/A````
* **Validation S3 Bucket**
    * The S3 bucket of the uploaded files for the file validation
    * Command : ````--validation-s3-bucket````
    * Not required if specified in the configuration file or not required if the file URL column is provided.
    * Default Value : ````N/A````
* **Validation Prefix**
    * The S3 subfolder prefix of the uploaded files for the file validation
    * Command : ````--validation-prefix````
    * Not required if specified in the configuration file or not required if the file URL column is provided.
    * Default Value : ````N/A````
* **Upload S3 URL**
    * The designated S3 bucket URL for uploading the file validation result
    * Command : ````upload-s3-url````
    * Not Required
    * Default Value : ````N/A````
* **Configuration File**
    * The YAML file containing the configuration details for the Stream File Validator execution
    * Command : ````<configuration file>````
    * Required
    * Default Value : ````N/A````

## Usage Example
There are two ways to configure and run the Stream File Validator: using a configuration file or using command line arguments. Below are the example commands to run Stream File Validator by using a configuration file or by using command line arguments:
````
python3 stream_file_validator.py config/config.yml
````
or
````
python3 stream_file_validator.py --manifest-file tests/temp/NCATS-COP01_path_report_file_neo4j_neo4j_error_test.txt --file-name-column file_name --file-url-column file_location --file-size-column file_size --file-md5-column md5sum --validation-s3-bucket bruce-file-copier --validation-prefix test --upload-s3-url s3://bruce-file-copier/test2
````
### Example Inputs
* **Manifest File Location**
    * ````s3://bucket/test/manifest.tsv```` or ````temp/manifest.tsv````
* **File Name Column**
    * ````file_name````
* **File URL Column**
    * ````file_url````
* **File Size Column**
    * ````file_size````
* **File MD5 Column**
    * ````file_md5````
* **Validation S3 Bucket**
    * ````s3_bucket````
* **Validation Prefix**
    * ````s3_folder````
* **Upload S3 URL**
    * ````s3://bucket/file_validation_result````
