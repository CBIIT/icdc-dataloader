---
layout: default
nav_order: 1
title: Model Converter
---
# Model Converter
This is the user documentation for the Model Converter module contained in the ICDC-Dataloader utility.

[![Codacy Badge](https://app.codacy.com/project/badge/Grade/f4d5afb8403642dbab917cb4aa4ef47d)](https://www.codacy.com/gh/CBIIT/icdc-dataloader?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=CBIIT/icdc-dataloader&amp;utm_campaign=Badge_Grade)

## Introduction
The Model Converter uses a combination of YAML format schema files, a YAML formatted properties files, and a GraphQL formatted queries file to generate a GraphQL formatted schema. After the GraphQL schema is generated, the Model Converter also generates a simplified, more easily readable version of the GraphQL schema for documentation purposes.

The Model Converter can be found in this Github Repository: [ICDC-Dataloader](https://github.com/CBIIT/icdc-dataloader)

## Pre-requisites
* Python 3.6 or newer

## Dependencies
Run ```pip3 install -r requirements.txt``` to install dependencies. Or run ```pip install -r requirements.txt``` if you are using virtualenv. The dependencies included in ````requirements.txt```` are listed below:

* pyyaml
* neo4j - version 1.7.6
* boto3
* requests

## Inputs

* YAML formatted schema file(s)
* A YAML formatted properties file
* A GraphQL formatted queries file
* The filepath to be used for the generated output files

## Outputs

* A GraphQL formatted schema
* A simplified, more easily readable version of the GraphQL schema for documentation purposes

## Command Line Arguments

* Schema File(s)
    * The YAML formatted schema file(s) that contain the model specifications used to generate the GraphQL schema.
    * Command: ````-s/--schema````
    * At least one schema file is required. Use multiple –schema arguments (one for each schema file), if more than one schema files are needed
    * Default Value: ````N/A````
* Properties File
    * The YAML formatted schema properties file for the data model.
    * Command: ````-p/--prop-file````
    * Required
    * Example files can be found under ````config/```` folder, such as  ````config/props-bento-ext.yml```` for Bento reference implementation, and ````config/props-ctdc.yml```` for CTDC etc.
    * Default Value: ````N/A````
* Query File
    * A GraphQL formatted file containing the schema definition block and query type block. This file will be appended to the end of the generated GraphQL schema.
    * Command: ````-q/--query-file````
    * Required
    * Default Value: ````N/A````
* Output Filepath
    * The desired filepath of the output GraphQL schema, the simplified schema will have the same filepath as the GraphQL schema with ````-doc```` appended to the file name.
    * Command: ````-o/--output````
    * Required
    * Default Value: ````N/A````

## Usage Example
Below is an example command to run the model converter:
````
model-converter.py --schema schema-file-1.yml --schema schema-file-2.yml --prop-file schema-properties.yml --query-file queries.graphql --output output_folder\data-model-schema.graphql
````

### Example Inputs

* **Schema Files**
    * ````schema-file-1.yml````
    * ````schema-file-2.yml````
* **Properties File**
    * ````schema-properties.yml````
* **Query File**
    * ````queries.graphql````
* **Output Filepath**
    * ````output_folder\data-model-schema.graphql````

### Example Outputs

* **GraphQL Schema**
    * ````output_folder\data-model-schema.graphql````
* **Simplified Documentation Schema**
    * ````output_folder\data-model-schema-doc.graphql````
